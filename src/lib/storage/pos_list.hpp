#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

/**
 * A PosList is a list of RowIDs. It is used to reference specific rows of a table.
 * The PosList by itself has no notion of a table, but only has semantic meaning together with it.
 */
class PosList : private pmr_vector<RowID> {
 private:
  using underlying = pmr_vector<RowID>;

 public:
  // Forward type declarations
  using underlying::allocator_type;
  using underlying::const_iterator;
  using underlying::const_pointer;
  using underlying::const_reference;
  using underlying::const_reverse_iterator;
  using underlying::difference_type;
  using underlying::iterator;
  using underlying::pointer;
  using underlying::reference;
  using underlying::reverse_iterator;
  using underlying::size_type;
  using underlying::value_type;

  // Meta information types
  enum class ChunkCardinality { SingleChunk, MultipleChunks };

  // Constructor
  PosList() noexcept(noexcept(underlying::allocator_type()));
  explicit PosList(const underlying::allocator_type& alloc) noexcept;
  PosList(size_type count, const RowID value, const underlying::allocator_type& alloc = underlying::allocator_type());
  explicit PosList(size_type count, const underlying::allocator_type& alloc = underlying::allocator_type());

  // Iterator range constructor
  template <class InputIt>
  PosList(InputIt first, InputIt last, const underlying::allocator_type& alloc = underlying::allocator_type())
      : underlying(first, last, alloc), _chunk_cardinality(ChunkCardinality::MultipleChunks) {}

  // No copy constructor / assignment
  PosList(const PosList& other) = delete;
  PosList& operator=(const PosList& other) = delete;

  // Move constructor / assignment
  PosList(PosList&& other) noexcept;
  PosList(PosList&& other, const underlying::allocator_type& alloc);
  PosList& operator=(PosList&& other);

  // Initializer list constructor / assignment
  PosList(std::initializer_list<RowID> init, const underlying::allocator_type& alloc = underlying::allocator_type());
  PosList& operator=(std::initializer_list<RowID> ilist);

  // Equality comparison helper
  // TODO: Do we want to compare meta information like ChunkCardinality if the vector contents are the same?
  bool is_equal_helper(const PosList& rhs) const;

  // Meta information
  ChunkCardinality chunk_cardinality() const;
  void set_chunk_cardinality(ChunkCardinality cardinality);

  // Forward vector interface
  using underlying::operator=;
  using underlying::assign;
  using underlying::at;
  using underlying::get_allocator;
  using underlying::operator[];
  using underlying::back;
  using underlying::begin;
  using underlying::capacity;
  using underlying::cbegin;
  using underlying::cend;
  using underlying::clear;
  using underlying::crbegin;
  using underlying::crend;
  using underlying::data;
  using underlying::emplace;
  using underlying::emplace_back;
  using underlying::empty;
  using underlying::end;
  using underlying::erase;
  using underlying::front;
  using underlying::insert;
  using underlying::max_size;
  using underlying::pop_back;
  using underlying::push_back;
  using underlying::rbegin;
  using underlying::rend;
  using underlying::reserve;
  using underlying::resize;
  using underlying::shrink_to_fit;
  using underlying::size;
  using underlying::swap;

  //  enum class TypeUniformity {
  //    AllSameType,
  //    HeterogenousTypes
  //  }

 private:
  // meta information
  ChunkCardinality _chunk_cardinality;
  //  TypeUniformity _type_uniformity;

 protected:
  // Helper method
  const underlying& as_vector() const;
};

// Equality comparison operators defined outside of the PosList scope
bool operator==(const opossum::PosList& lhs, const opossum::PosList& rhs);
bool operator!=(const PosList& lhs, const PosList& rhs);

}  // namespace opossum
