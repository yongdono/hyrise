#pragma once

#include <memory>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
struct EqualHeightBucketStats {
  std::vector<T> maxs;
  std::vector<uint64_t> distinct_counts;
  T min;
  uint64_t count_per_bucket;
  uint64_t total_count;
};

class Table;

template <typename T>
class EqualHeightHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;
  EqualHeightHistogram(const std::vector<T>& maxs, const std::vector<uint64_t>& distinct_counts, const T min,
                       const uint64_t _count_per_bucket, const uint64_t total_count);
  EqualHeightHistogram(const std::vector<std::string>& maxs, const std::vector<uint64_t>& distinct_counts,
                       const std::string& min, const uint64_t _count_per_bucket, const uint64_t total_count,
                       const std::string& supported_characters, const uint64_t string_prefix_length);

  static std::shared_ptr<EqualHeightHistogram<T>> from_column(const std::shared_ptr<const BaseColumn>& column,
                                                              const size_t max_num_buckets);

  static std::shared_ptr<EqualHeightHistogram<std::string>> from_column(const std::shared_ptr<const BaseColumn>& column,
                                                                        const size_t max_num_buckets,
                                                                        const std::string& supported_characters,
                                                                        const uint64_t string_prefix_length);

  std::shared_ptr<AbstractHistogram<T>> clone() const override;

  HistogramType histogram_type() const override;
  uint64_t total_count_distinct() const override;
  uint64_t total_count() const override;
  size_t num_buckets() const override;

 protected:
  void _generate(const std::shared_ptr<const ValueColumn<T>> distinct_column,
                 const std::shared_ptr<const ValueColumn<int64_t>> count_column, const size_t max_num_buckets) override;
  static EqualHeightBucketStats<T> _get_bucket_stats(const std::vector<std::pair<T, uint64_t>>& value_counts,
                                                     const uint64_t max_num_buckets);

  BucketID _bucket_for_value(const T value) const override;
  BucketID _lower_bound_for_value(const T value) const override;
  BucketID _upper_bound_for_value(const T value) const override;

  T _bucket_min(const BucketID index) const override;
  T _bucket_max(const BucketID index) const override;
  uint64_t _bucket_count(const BucketID index) const override;
  uint64_t _bucket_count_distinct(const BucketID index) const override;

 private:
  std::vector<T> _maxs;
  std::vector<uint64_t> _distinct_counts;
  T _min;
  uint64_t _count_per_bucket;
  // Exact total count can otherwise not necessarily be reconstructed.
  uint64_t _total_count;
};

}  // namespace opossum
