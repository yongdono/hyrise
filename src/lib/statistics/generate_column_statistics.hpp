#pragma once

#include <unordered_set>

#include "base_column_statistics.hpp"
#include "histogram_column_statistics.hpp"
#include "resolve_type.hpp"
#include "chunk_statistics/histograms/equal_num_elements_histogram.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"

namespace opossum {

/**
 * Generate the statistics of a single column. Used by generate_table_statistics()
 */
template <typename ColumnDataType>
std::shared_ptr<BaseColumnStatistics> generate_column_statistics(const std::shared_ptr<const Table>& table,
                                                                 const ColumnID column_id) {
  /**
   * The number of buckets is currently arbitrarily chosen.
   * TODO(tim): benchmark
   */
  auto histogram = std::make_shared<EqualNumElementsHistogram<ColumnDataType>>(table);
  histogram->generate(column_id, 100u);

  // TODO(tim): incorporate into Histograms
  const auto null_value_ratio = 0.0f;
  return std::make_shared<HistogramColumnStatistics<ColumnDataType>>(histogram, null_value_ratio);
}

// template <>
// std::shared_ptr<BaseColumnStatistics> generate_column_statistics<std::string>(const std::shared_ptr<const Table>& table,
//                                                                               const ColumnID column_id);

}  // namespace opossum
