#pragma once

#include <memory>
#include <vector>

#include "statistics/chunk_statistics/abstract_filter.hpp"
#include "types.hpp"

namespace opossum {

class Table;

template <typename T>
class AbstractHistogram : public AbstractFilter {
  friend class HistogramPrivateTest;

 public:
  explicit AbstractHistogram(const std::shared_ptr<const Table>& table);
  AbstractHistogram(const std::shared_ptr<const Table>& table, const std::string& supported_characters);
  AbstractHistogram(const std::shared_ptr<const Table>& table, const std::string& supported_characters,
                    const uint64_t string_prefix_length);
  virtual ~AbstractHistogram() = default;

  virtual HistogramType histogram_type() const = 0;
  std::string description() const;
  const std::string& supported_characters() const;

  void generate(const ColumnID column_id, const size_t max_num_buckets);
  float estimate_selectivity(const PredicateCondition predicate_type, const T value,
                             const std::optional<T>& value2 = std::nullopt) const;
  float estimate_cardinality(const PredicateCondition predicate_type, const T value,
                             const std::optional<T>& value2 = std::nullopt) const;
  float estimate_distinct_count(const PredicateCondition predicate_type, const T value,
                                const std::optional<T>& value2 = std::nullopt) const;
  bool can_prune(const PredicateCondition predicate_type, const AllTypeVariant& variant_value,
                 const std::optional<AllTypeVariant>& variant_value2 = std::nullopt) const override;

  T min() const;
  T max() const;
  T previous_value(const T value, const bool pad_and_trim = true) const;
  T next_value(const T value, const bool pad_and_trim = true) const;

  virtual size_t num_buckets() const = 0;
  virtual uint64_t total_count() const = 0;
  virtual uint64_t total_count_distinct() const = 0;

 protected:
  const std::shared_ptr<const Table> _get_value_counts(const ColumnID column_id) const;
  virtual void _generate(const ColumnID column_id, const size_t max_num_buckets) = 0;

  uint64_t _ipow(uint64_t base, uint64_t exp) const;
  int64_t _convert_string_to_number_representation(const std::string& value) const;
  std::string _convert_number_representation_to_string(const int64_t) const;
  float _bucket_share(const BucketID bucket_id, const T value) const;

  virtual T _bucket_width(const BucketID index) const;
  // TODO(tim): ask experts how this works
  // virtual std::enable_if_t<std::is_integral_v<T>, T> _bucket_width(const BucketID index) const;

  virtual BucketID _bucket_for_value(const T value) const = 0;
  virtual BucketID _lower_bound_for_value(const T value) const = 0;
  virtual BucketID _upper_bound_for_value(const T value) const = 0;

  virtual T _bucket_min(const BucketID index) const = 0;
  virtual T _bucket_max(const BucketID index) const = 0;
  virtual uint64_t _bucket_count(const BucketID index) const = 0;
  virtual uint64_t _bucket_count_distinct(const BucketID index) const = 0;

  const std::weak_ptr<const Table> _table;
  std::string _supported_characters;
  uint64_t _string_prefix_length;
};

}  // namespace opossum
