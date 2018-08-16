#pragma once

#include <array>
#include <functional>
#include <memory>

#include "storage/column_iterables.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;

/**
 * @brief the base class of all table scan impls
 */
class BaseTableScanImpl {
 public:
  BaseTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                    const PredicateCondition predicate_condition)
      : _in_table{in_table}, _left_column_id{left_column_id}, _predicate_condition{predicate_condition} {}

  virtual ~BaseTableScanImpl() = default;

  virtual std::shared_ptr<PosList> scan_chunk(ChunkID chunk_id) = 0;

 protected:
  /**
   * @defgroup The hot loops of the table scan
   * @{
   */

  // Version with a constant value on the right side. Sometimes we prefer this over _unary_scan because we can use
  // with_comparator.
  template <bool LeftIsNullable, typename BinaryFunctor, typename LeftIterator, typename RightValue>
  void __attribute__((noinline))
  _unary_scan_with_value(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end, RightValue right_value,
                         const ChunkID chunk_id, PosList& matches_out) {
    // branchless scan, based on https://stackoverflow.com/a/38799396/2204581
    constexpr auto BUFFER_SIZE = 512l;
    std::array<ChunkOffset, BUFFER_SIZE> buffer;

    // In the beginning, we do blocks of BUFFER_SIZE values - this should allow the compiler to unroll the loop
    while (left_end - left_it > BUFFER_SIZE) {
      auto insert_index = 0;

      for (auto i = size_t{0}; i < BUFFER_SIZE; ++i, ++left_it) {
        const auto left = *left_it;

        buffer[insert_index] = left.chunk_offset();

        // This is where the "check" happens. If the value is not null and the condition is met, the insert_index is
        // moved forward. Otherwise, it remains where it is and the value that was written before will be overwritten.
        // Since this is not a condition, we don't introduce a branch.
        if constexpr (LeftIsNullable) {
          insert_index += (!left.is_null() & func(left.value(), right_value));
        } else {
          insert_index += func(left.value(), right_value);
        }
      }

      for (auto i = 0; i < insert_index; ++i) {
        matches_out.emplace_back(RowID{chunk_id, buffer[i]});
      }

      insert_index = 0;
    }

    // Do the remainder
    for (; left_it != left_end; ++left_it) {
      const auto left = *left_it;
      if ((!LeftIsNullable || !left.is_null()) & func(left.value(), right_value)) {
        matches_out.emplace_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  }

  template <typename UnaryFunctor, typename LeftIterator>
  // noinline reduces compile time drastically
  void __attribute__((noinline)) _unary_scan(const UnaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                                             const ChunkID chunk_id, PosList& matches_out) {
    for (; left_it != left_end; ++left_it) {
      const auto left = *left_it;

      if (left.is_null()) continue;

      if (func(left.value())) {
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  }

  template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  void __attribute__((noinline)) _binary_scan(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                                              RightIterator right_it, const ChunkID chunk_id, PosList& matches_out) {
    for (; left_it != left_end; ++left_it, ++right_it) {
      const auto left = *left_it;
      const auto right = *right_it;

      if (left.is_null() || right.is_null()) continue;

      if (func(left.value(), right.value())) {
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    }
  }

  /**@}*/

 protected:
  const std::shared_ptr<const Table> _in_table;
  const ColumnID _left_column_id;
  const PredicateCondition _predicate_condition;
};

}  // namespace opossum
