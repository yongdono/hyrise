#include <fstream>
#include <iostream>

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

int64_t table_scan(
        const std::shared_ptr<Table>& table,
        const ColumnID column_id,
        const PredicateCondition predicate_condition,
        const AllTypeVariant value) {
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

template <typename T>
float estimate_cardinality(
        const std::shared_ptr<AbstractHistogram<T>>& hist,
        const PredicateCondition predicate_condition,
        const AllTypeVariant value) {
  const auto t_value = type_cast<T>(value);
  return hist->estimate_cardinality(t_value, predicate_condition);
}

int main() {
  // std::cout << filesystem::current_path().string() << std::endl;
  const auto table = load_table("../src/test/tables/random_string_data.tbl");
  const auto column_id = ColumnID{0};
  const auto predicate_conditions = {
          PredicateCondition::Equals,
          PredicateCondition::LessThan,
  };
  const auto values = {
          AllTypeVariant{"h9*XsTp8P7"},
          AllTypeVariant{"IcUT>XcIKr"},
          AllTypeVariant{"0000000000"},
  };
  const auto num_bucket_combinations = {
          10,
          25,
          50,
          100,
          150,
          200,
          250,
          300,
          400,
          500,
          750,
          1000
  };
  typedef typename std::string T;

  auto log = std::ofstream("../results.log", std::ios_base::out | std::ios_base::trunc);
  log << "num_buckets,predicate_condition,value,actual_count,equal_height_hist_count,equal_num_elements_hist_count,equal_width_hist_count\n";

  for (auto num_buckets : num_bucket_combinations) {
    // std::cout << "Buckets: " << num_buckets << std::endl;

    auto equal_height_hist = std::make_shared<EqualHeightHistogram<T>>(table);
    equal_height_hist->generate(column_id, num_buckets);

    auto equal_num_elements_hist = std::make_shared<EqualNumElementsHistogram<T>>(table);
    equal_num_elements_hist->generate(column_id, num_buckets);

    auto equal_width_hist = std::make_shared<EqualWidthHistogram<T>>(table);
    equal_width_hist->generate(column_id, num_buckets);

    for (auto predicate_condition : predicate_conditions) {
      for (auto value : values) {
        // std::cout << predicate_condition_to_string.left.at(predicate_condition) << " " << value << std::endl;

        const auto actual_count = table_scan(table, column_id, predicate_condition, value);
        const auto equal_height_hist_count = estimate_cardinality<T>(equal_height_hist, predicate_condition, value);
        const auto equal_num_elements_hist_count = estimate_cardinality<T>(equal_num_elements_hist, predicate_condition,
                                                                           value);
        const auto equal_width_hist_count = estimate_cardinality<T>(equal_width_hist, predicate_condition, value);

        log << std::to_string(num_buckets) << "," << predicate_condition_to_string.left.at(predicate_condition) << "," << value << "," << std::to_string(actual_count) << "," << std::to_string(equal_height_hist_count) << "," << std::to_string(equal_num_elements_hist_count) << "," << std::to_string(equal_width_hist_count) << "\n";
        // log << "Test\n";
        // std::cout << "Actual: " << actual_count << ", ";
        // std::cout << "Height: " << equal_height_hist_count << ", ";
        // std::cout << "Elements: " << equal_num_elements_hist_count << ", ";
        // std::cout << "Width: " << equal_width_hist_count << std::endl;
      }

      log.flush();

      // std::cout << std::endl;
    }

    // std::cout << std::endl;
  }


  return 0;
}
