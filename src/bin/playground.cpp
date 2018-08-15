#include <ctime>

#include <chrono>
#include <fstream>
#include <iostream>
#include <locale>
#include <tuple>

#include "constant_mappings.hpp"
#include "types.hpp"

#include "operators/aggregate.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "statistics/chunk_statistics/histograms/equal_height_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_num_elements_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "storage/table.hpp"
#include "utils/filesystem.hpp"
#include "utils/load_table.hpp"

using namespace opossum;  // NOLINT

template <typename Clock, typename Duration>
std::ostream& operator<<(std::ostream& stream, const std::chrono::time_point<Clock, Duration>& time_point) {
  const time_t time = Clock::to_time_t(time_point);
#if __GNUC__ > 4 || ((__GNUC__ == 4) && __GNUC_MINOR__ > 8 && __GNUC_REVISION__ > 1)
  // Maybe the put_time will be implemented later?
  struct tm tm;
  localtime_r(&time, &tm);
  return stream << std::put_time(&tm, "%c");  // Print standard date&time
#else
  char buffer[26];
  ctime_r(&time, buffer);
  buffer[24] = '\0';  // Removes the newline that is added
  return stream << buffer;
#endif
}

// lineitem
const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters = {
    /**
     * TPC-H 1
     */
    // L_SHIPDATE <= '1998-12-01' - interval '[DELTA]' day (3)
    {ColumnID{10}, PredicateCondition::LessThanEquals, "1998-11-28"},
    /**
     * TPC-H 4
     */
    // L_COMMITDATE < L_RECEIPTDATE
    {ColumnID{11}, PredicateCondition::LessThan, ColumnID{12}},
    /**
     * TPC-H 6
     */
    // L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < dateadd(yy, 1, cast('1994-01-01' as datetime))
    {ColumnID{10}, PredicateCondition::GreaterThanEquals, "1994-01-01"},
    {ColumnID{10}, PredicateCondition::LessThan, "1995-01-01"},
    // L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
    {ColumnID{6}, PredicateCondition::GreaterThanEquals, .05f},
    {ColumnID{6}, PredicateCondition::LessThanEquals, .07f},
    // L_QUANTITY < 24
    {ColumnID{4}, PredicateCondition::LessThan, 24},
    /**
     * TPC-H 7
     */
    // L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
    {ColumnID{10}, PredicateCondition::GreaterThanEquals, "1995-01-01"},
    {ColumnID{10}, PredicateCondition::LessThanEquals, "1996-12-31"},
    /**
     * TPC-H 10
     */
    // L_RETURNFLAG = 'R'
    {ColumnID{8}, PredicateCondition::Equals, "R"},
    /**
     * TPC-H 12
     */
    // TODO(tim): enable once TableScan supports IN predicate
    // L_SHIPMODE in ('[SHIPMODE1]', '[SHIPMODE2]')
    // {ColumnID{14}, PredicateCondition::In, {"MAIL", "SHIP"}},
    // L_SHIPDATE < L_COMMITTDATE
    {ColumnID{10}, PredicateCondition::LessThan, ColumnID{11}},
    // L_RECEIPTDATE >= date '[DATE]' AND L_RECEIPTDATE < date '[DATE]' + interval '1' year
    {ColumnID{12}, PredicateCondition::GreaterThanEquals, "1995-06-01"},
    {ColumnID{12}, PredicateCondition::LessThan, "1996-06-01"},
    /**
     * TPC-H 14
     */
    // L_SHIPDATE >= date '[DATE]' AND L_SHIPDATE < date '[DATE]' + interval '1' month
    {ColumnID{10}, PredicateCondition::GreaterThanEquals, "1995-06-01"},
    {ColumnID{10}, PredicateCondition::LessThan, "1995-07-01"},
    /**
     * TPC-H 15
     */
    // L_SHIPDATE >= date '[DATE]' AND L_SHIPDATE < date '[DATE]' + interval '3' month
    {ColumnID{10}, PredicateCondition::GreaterThanEquals, "1995-06-01"},
    {ColumnID{10}, PredicateCondition::LessThan, "1995-09-01"},
    /**
     * TPC-H 19
     */
    // TODO(tim): enable once TableScan supports IN predicate
    // L_SHIPMODE in ('AIR', 'AIR REG')
    // {ColumnID{14}, PredicateCondition::In, {"AIR", "AIR REG"}},
    // L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    {ColumnID{13}, PredicateCondition::Equals, "DELIVER IN PERSON"},
    // L_QUANTITY >= [QUANTITY1] AND L_QUANTITY <= [QUANTITY1] + 10
    {ColumnID{4}, PredicateCondition::GreaterThanEquals, 1},
    {ColumnID{4}, PredicateCondition::LessThanEquals, 11},
    // L_QUANTITY >= [QUANTITY1] AND L_QUANTITY <= [QUANTITY1] + 10
    {ColumnID{4}, PredicateCondition::GreaterThanEquals, 15},
    {ColumnID{4}, PredicateCondition::LessThanEquals, 25},
    // L_QUANTITY >= [QUANTITY1] AND L_QUANTITY <= [QUANTITY1] + 10
    {ColumnID{4}, PredicateCondition::GreaterThanEquals, 30},
    {ColumnID{4}, PredicateCondition::LessThanEquals, 40},
};

// // orders
// const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>> filters = {
//         /**
//          * TPC-H 3
//          */
//         // O_ORDERDATE < date '[DATE]'
//         {ColumnID{4}, PredicateCondition::LessThan, "1995-03-15"},
//         /**
//          * TPC-H 4
//          */
//         // O_ORDERDATE >= date '[DATE]' AND O_ORDERDATE < date '[DATE]' + interval '3' month
//         {ColumnID{4}, PredicateCondition::GreaterThanEquals, "1996-07-01"},
//         {ColumnID{4}, PredicateCondition::LessThan, "1996-10-01"},
//         /**
//          * TPC-H 5
//          */
//         // O_ORDERDATE >= date '[DATE]' AND O_ORDERDATE < date '[DATE]' + interval '1' year
//         {ColumnID{4}, PredicateCondition::GreaterThanEquals, "1994-01-01"},
//         {ColumnID{4}, PredicateCondition::LessThan, "1995-01-01"},
//         /**
//          * TPC-H 8
//          */
//         // O_ORDERDATE between date '1995-01-01' AND date '1996-12-31'
//         {ColumnID{4}, PredicateCondition::GreaterThanEquals, "1995-01-01"},
//         {ColumnID{4}, PredicateCondition::LessThanEquals, "1996-12-31"},
//         /**
//          * TPC-H 10
//          */
//         // O_ORDERDATE >= date '[DATE]' AND O_ORDERDATE < date '[DATE]' + interval '3' month
//         {ColumnID{4}, PredicateCondition::GreaterThanEquals, "1993-10-01"},
//         {ColumnID{4}, PredicateCondition::LessThan, "1994-01-01"},
//         /**
//          * TPC-H 21
//          */
//         // o_orderstatus = 'F'
//         {ColumnID{2}, PredicateCondition::Equals, "F"},
//         /**
//          * custom predicates
//          * totalprice column chosen because of its skewed distribution
//          */
//         {ColumnID{3}, PredicateCondition::LessThan, 10.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 100.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 1'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 1'453.8},
//         {ColumnID{3}, PredicateCondition::LessThan, 2'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 2'912.1},
//         {ColumnID{3}, PredicateCondition::LessThan, 3'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 3'239.9},
//         {ColumnID{3}, PredicateCondition::LessThan, 4'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 4'245.3},
//         {ColumnID{3}, PredicateCondition::LessThan, 5'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 5'456.4},
//         {ColumnID{3}, PredicateCondition::LessThan, 6'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 6'897.8},
//         {ColumnID{3}, PredicateCondition::LessThan, 7'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 7'211.2},
//         {ColumnID{3}, PredicateCondition::LessThan, 8'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 8'019.1},
//         {ColumnID{3}, PredicateCondition::LessThan, 9'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 9'349.9},
//         {ColumnID{3}, PredicateCondition::LessThan, 10'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 15'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 20'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 25'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 30'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 35'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 40'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 45'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 50'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 55'000.0},
//         {ColumnID{3}, PredicateCondition::LessThan, 60'000.0},
// };

int64_t get_predicate_count(const std::shared_ptr<Table>& table, const ColumnID column_id,
                            const PredicateCondition predicate_condition, const AllTypeVariant& value) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  auto table_scan = std::make_shared<TableScan>(table_wrapper, column_id, predicate_condition, value);
  table_scan->execute();

  const auto aggregate_args = std::vector<AggregateColumnDefinition>{{std::nullopt, AggregateFunction::Count}};
  auto aggregate = std::make_shared<Aggregate>(table_scan, aggregate_args, std::vector<ColumnID>{});
  aggregate->execute();

  // const auto print = std::make_shared<Print>(aggregate);
  // print->execute();

  return aggregate->get_output()->get_value<int64_t>(ColumnID{0u}, 0u);
}

uint64_t get_distinct_count(const std::shared_ptr<Table>& table, const ColumnID column_id) {
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  auto aggregate = std::make_shared<Aggregate>(table_wrapper, std::vector<AggregateColumnDefinition>{},
                                               std::vector<ColumnID>{column_id});
  aggregate->execute();

  // const auto print = std::make_shared<Print>(aggregate);
  // print->execute();

  return aggregate->get_output()->row_count();
}

std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>> get_filters_by_column(
    const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters) {
  std::unordered_map<ColumnID, std::vector<std::pair<PredicateCondition, AllTypeVariant>>> filters_by_column;

  for (const auto& filter : filters) {
    const auto column_id = std::get<0>(filter);
    const auto predicate_condition = std::get<1>(filter);
    const auto value = std::get<2>(filter);
    filters_by_column[column_id].emplace_back(
        std::pair<PredicateCondition, AllTypeVariant>{predicate_condition, value});
  }

  return filters_by_column;
};

std::unordered_map<ColumnID, std::unordered_map<PredicateCondition, std::unordered_map<AllTypeVariant, int64_t>>>
get_row_count_by_filter(const std::shared_ptr<Table>& table,
                        const std::vector<std::tuple<ColumnID, PredicateCondition, AllTypeVariant>>& filters) {
  std::unordered_map<ColumnID, std::unordered_map<PredicateCondition, std::unordered_map<AllTypeVariant, int64_t>>>
      row_count_by_filter;

  for (const auto& filter : filters) {
    const auto column_id = std::get<0>(filter);
    const auto predicate_condition = std::get<1>(filter);
    const auto value = std::get<2>(filter);
    row_count_by_filter[column_id][predicate_condition][value] =
        get_predicate_count(table, column_id, predicate_condition, value);
  }

  return row_count_by_filter;
};

template <typename T>
float estimate_cardinality(const std::shared_ptr<AbstractHistogram<T>>& hist,
                           const PredicateCondition predicate_condition, const AllTypeVariant value) {
  const auto t_value = type_cast<T>(value);
  return hist->estimate_cardinality(predicate_condition, t_value);
}

int main() {
  std::cout.imbue(std::locale("en_US.UTF-8"));
  std::cout << std::chrono::system_clock::now() << " Loading table...";
  auto start = std::chrono::high_resolution_clock::now();

  // const auto table = load_table("../src/test/tables/random_string_data.tbl");
  // const auto table = load_table("../src/test/tables/tpch-jan/orders.tbl");
  const auto table = load_table("../src/test/tables/jcch/sf-0.01/lineitem_header.tbl");
  // const auto table = load_table("../src/test/tables/jcch/sf-0.1/lineitem_header.tbl");
  // const auto table = load_table("../src/test/tables/jcch/sf-1/lineitem_header.tbl");

  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
            << std::endl;

  const auto num_bucket_combinations = {
      1u, 2u, 5u, 10u, 25u, 50u, 100u, 150u, 200u, 250u, 300u, 400u, 500u, 750u, 1000u,
  };

  auto results_log = std::ofstream("../estimation_results.log", std::ios_base::out | std::ios_base::trunc);
  results_log << "total_count,distinct_count,num_buckets,column_name,predicate_condition,value,actual_count,equal_"
                 "height_hist_count,equal_num_elements_hist_count,equal_width_hist_count\n";

  auto histogram_log = std::ofstream("../histogram_buckets.log", std::ios_base::out | std::ios_base::trunc);
  histogram_log << "histogram_type,column_id,actual_num_buckets,requested_num_buckets,bucket_id,bucket_min,bucket_max,"
                   "bucket_min_repr,bucket_max_repr,bucket_count,bucket_count_distinct\n";

  const auto filters_by_column = get_filters_by_column(filters);
  const auto row_count_by_filter = get_row_count_by_filter(table, filters);

  const auto total_count = table->row_count();

  for (auto num_buckets : num_bucket_combinations) {
    for (auto it : filters_by_column) {
      const auto column_id = it.first;
      const auto column_name = table->column_name(column_id);
      std::cout << column_name << std::endl;

      const auto column_data_type = table->column_data_types()[column_id];
      const auto distinct_count = get_distinct_count(table, column_id);

      resolve_data_type(column_data_type, [&](auto type) {
        using T = typename decltype(type)::type;

        std::cout << std::chrono::system_clock::now() << " Generating EqualHeightHistogram with " << num_buckets
                  << " buckets...";
        start = std::chrono::high_resolution_clock::now();

        const auto equal_height_hist =
            EqualHeightHistogram<T>::from_column(table->get_chunk(ChunkID{0})->get_column(column_id), num_buckets);

        end = std::chrono::high_resolution_clock::now();
        std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
                  << std::endl;

        std::cout << std::chrono::system_clock::now() << " Generating EqualNumElementsHistogram with " << num_buckets
                  << " buckets...";
        start = std::chrono::high_resolution_clock::now();

        const auto equal_num_elements_hist =
            EqualNumElementsHistogram<T>::from_column(table->get_chunk(ChunkID{0})->get_column(column_id), num_buckets);

        end = std::chrono::high_resolution_clock::now();
        std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
                  << std::endl;

        std::cout << std::chrono::system_clock::now() << " Generating EqualWidthHistogram with " << num_buckets
                  << " buckets...";
        start = std::chrono::high_resolution_clock::now();

        const auto equal_width_hist =
            EqualWidthHistogram<T>::from_column(table->get_chunk(ChunkID{0})->get_column(column_id), num_buckets);

        end = std::chrono::high_resolution_clock::now();
        std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
                  << std::endl;

        histogram_log << equal_height_hist->buckets_to_csv(false, column_id, num_buckets);
        histogram_log << equal_num_elements_hist->buckets_to_csv(false, column_id, num_buckets);
        histogram_log << equal_width_hist->buckets_to_csv(false, column_id, num_buckets);
        histogram_log.flush();

        for (const auto& pair : it.second) {
          const auto predicate_condition = pair.first;
          const auto value = pair.second;

          std::cout << " " << predicate_condition_to_string.left.at(predicate_condition) << " " << value << std::endl;

          const auto actual_count = row_count_by_filter.at(column_id).at(predicate_condition).at(value);

          std::cout << std::chrono::system_clock::now() << " Estimating with EqualHeightHistogram...";
          start = std::chrono::high_resolution_clock::now();

          const auto equal_height_hist_count = estimate_cardinality<T>(equal_height_hist, predicate_condition, value);

          end = std::chrono::high_resolution_clock::now();
          std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
                    << std::endl;

          std::cout << std::chrono::system_clock::now() << " Estimating with EqualNumElementsHistogram...";
          start = std::chrono::high_resolution_clock::now();

          const auto equal_num_elements_hist_count =
              estimate_cardinality<T>(equal_num_elements_hist, predicate_condition, value);

          end = std::chrono::high_resolution_clock::now();
          std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
                    << std::endl;

          std::cout << std::chrono::system_clock::now() << " Estimating with EqualWidthHistogram...";
          start = std::chrono::high_resolution_clock::now();

          const auto equal_width_hist_count = estimate_cardinality<T>(equal_width_hist, predicate_condition, value);

          end = std::chrono::high_resolution_clock::now();
          std::cout << "done (" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "µs)."
                    << std::endl;

          results_log << std::to_string(total_count) << "," << std::to_string(distinct_count) << ","
                      << std::to_string(num_buckets) << "," << column_name << ","
                      << predicate_condition_to_string.left.at(predicate_condition) << "," << value << ","
                      << std::to_string(actual_count) << "," << std::to_string(equal_height_hist_count) << ","
                      << std::to_string(equal_num_elements_hist_count) << "," << std::to_string(equal_width_hist_count)
                      << "\n";
          results_log.flush();
        }
      });
    }
  }

  return 0;
}
