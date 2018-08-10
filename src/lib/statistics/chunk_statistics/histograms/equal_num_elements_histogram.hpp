#pragma once

#include <memory>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
struct EqualNumElementsBucketStats {
  std::vector<T> mins;
  std::vector<T> maxs;
  std::vector<uint64_t> counts;
  uint64_t distinct_count_per_bucket;
  uint64_t num_buckets_with_extra_value;
};

class Table;

template <typename T>
class EqualNumElementsHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;
  EqualNumElementsHistogram(const std::vector<T>& mins, const std::vector<T>& maxs, const std::vector<uint64_t>& counts,
                            const uint64_t distinct_count_per_bucket, const uint64_t num_buckets_with_extra_value);
  EqualNumElementsHistogram(const std::vector<std::string>& mins, const std::vector<std::string>& maxs,
                            const std::vector<uint64_t>& counts, const uint64_t distinct_count_per_bucket,
                            const uint64_t num_buckets_with_extra_value, const std::string& supported_characters,
                            const uint64_t string_prefix_length);

  static std::shared_ptr<EqualNumElementsHistogram<T>> from_column(const std::shared_ptr<const BaseColumn>& column,
                                                                   const size_t max_num_buckets);

  static std::shared_ptr<EqualNumElementsHistogram<std::string>> from_column(
      const std::shared_ptr<const BaseColumn>& column, const size_t max_num_buckets,
      const std::string& supported_characters, const uint64_t string_prefix_length);

  std::shared_ptr<AbstractHistogram<T>> clone() const override;

  HistogramType histogram_type() const override;
  uint64_t total_count_distinct() const override;
  uint64_t total_count() const override;
  size_t num_buckets() const override;

 protected:
  void _generate(const std::shared_ptr<const ValueColumn<T>> distinct_column,
                 const std::shared_ptr<const ValueColumn<int64_t>> count_column, const size_t max_num_buckets) override;

  static EqualNumElementsBucketStats<T> _get_bucket_stats(const std::vector<std::pair<T, uint64_t>>& value_counts,
                                                          const uint64_t max_num_buckets);

  BucketID _bucket_for_value(const T value) const override;
  BucketID _lower_bound_for_value(const T value) const override;
  BucketID _upper_bound_for_value(const T value) const override;

  T _bucket_min(const BucketID index) const override;
  T _bucket_max(const BucketID index) const override;
  uint64_t _bucket_count(const BucketID index) const override;

  /**
   * Returns the number of distinct values that are part of this bucket.
   * This number is precise for the state of the table at time of generation.
   */
  uint64_t _bucket_count_distinct(const BucketID index) const override;

 private:
  std::vector<T> _mins;
  std::vector<T> _maxs;
  std::vector<uint64_t> _counts;
  uint64_t _distinct_count_per_bucket;
  uint64_t _num_buckets_with_extra_value;
};

}  // namespace opossum
