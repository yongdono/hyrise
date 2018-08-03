#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "base_test.hpp"
#include "gtest/gtest.h"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "statistics/generate_table_statistics.hpp"
#include "statistics/histogram_column_statistics.hpp"

namespace opossum {

class HistogramColumnStatisticsTest : public BaseTest {
 protected:
  void SetUp() override {
    _int_float_double_string_gaps = load_table("src/test/tables/int_float_double_string_gaps.tbl", Chunk::MAX_SIZE);
    auto table_statistics1 = generate_table_statistics(_int_float_double_string_gaps, 3);
    _column_statistics_int = std::dynamic_pointer_cast<HistogramColumnStatistics<int32_t>>(
        std::const_pointer_cast<BaseColumnStatistics>(table_statistics1.column_statistics()[0]));
    _column_statistics_float = std::dynamic_pointer_cast<HistogramColumnStatistics<float>>(
        std::const_pointer_cast<BaseColumnStatistics>(table_statistics1.column_statistics()[1]));
    _column_statistics_double = std::dynamic_pointer_cast<HistogramColumnStatistics<double>>(
        std::const_pointer_cast<BaseColumnStatistics>(table_statistics1.column_statistics()[2]));
    _column_statistics_string = std::dynamic_pointer_cast<HistogramColumnStatistics<std::string>>(
        std::const_pointer_cast<BaseColumnStatistics>(table_statistics1.column_statistics()[3]));

    _int_equal_distribution = load_table("src/test/tables/int_equal_distribution.tbl", Chunk::MAX_SIZE);
    auto table_statistics2 = generate_table_statistics(_int_equal_distribution, 18);
    _column_statistics_int_equal_distribution = table_statistics2.column_statistics();
  }

  // // For two column scans (type of value1 is ColumnID)
  // void predict_selectivities_and_compare(
  //     const std::shared_ptr<Table>& table,
  //     const std::vector<std::shared_ptr<const BaseColumnStatistics>>& column_statistics,
  //     const PredicateCondition predicate_condition) {
  //   auto table_wrapper = std::make_shared<TableWrapper>(table);
  //   table_wrapper->execute();
  //   auto row_count = table->row_count();
  //   for (ColumnID::base_type column_1 = 0; column_1 < column_statistics.size(); ++column_1) {
  //     for (ColumnID::base_type column_2 = 0; column_2 < column_statistics.size() && column_1 != column_2; ++column_2) {
  //       auto result_container = column_statistics[column_1]->estimate_predicate_with_column(
  //           predicate_condition, *column_statistics[column_2]);
  //       auto table_scan =
  //           std::make_shared<TableScan>(table_wrapper, ColumnID{column_1}, predicate_condition, ColumnID{column_2});
  //       table_scan->execute();
  //       auto result_row_count = table_scan->get_output()->row_count();
  //       EXPECT_FLOAT_EQ(result_container.selectivity,
  //                       static_cast<float>(result_row_count) / static_cast<float>(row_count));
  //     }
  //   }
  // }

  // // For BETWEEN
  // template <typename T>
  // void predict_selectivities_and_compare(const std::shared_ptr<HistogramColumnStatistics<T>>& column_statistic,
  //                                        const PredicateCondition predicate_condition,
  //                                        const std::vector<std::pair<T, T>>& values,
  //                                        const std::vector<float>& expected_selectivities) {
  //   auto expected_selectivities_itr = expected_selectivities.begin();
  //   for (const auto& value_pair : values) {
  //     auto result_container = column_statistic->estimate_predicate_with_value(
  //         predicate_condition, AllTypeVariant(value_pair.first), AllTypeVariant(value_pair.second));
  //     EXPECT_FLOAT_EQ(result_container.selectivity, *expected_selectivities_itr++);
  //   }
  // }

  std::shared_ptr<Table> _int_equal_distribution;
  std::shared_ptr<Table> _int_float_double_string_gaps;
  std::shared_ptr<HistogramColumnStatistics<int32_t>> _column_statistics_int;
  std::shared_ptr<HistogramColumnStatistics<float>> _column_statistics_float;
  std::shared_ptr<HistogramColumnStatistics<double>> _column_statistics_double;
  std::shared_ptr<HistogramColumnStatistics<std::string>> _column_statistics_string;
  std::vector<std::shared_ptr<const BaseColumnStatistics>> _column_statistics_int_equal_distribution;

  //  {below min, min, between buckets, middle, max, above max}
  std::vector<int32_t> _int_values{0, 1, 3, 4, 7, 8};
  std::vector<float> _float_values{0.f, 1.f, 3.f, 4.f, 7.f, 8.f};
  std::vector<double> _double_values{0., 1., 3., 4., 7., 8.};
  std::vector<std::string> _string_values{"a", "b", "d", "e", "h", "i"};
};

TEST_F(HistogramColumnStatisticsTest, EqualsTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{0}).selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{1}).selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{3}).selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{4}).selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{7}).selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{8}).selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{0.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{1.f})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{3.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{4.f})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{7.f})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{8.f})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{0.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{1.})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{3.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{4.})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{7.})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{8.})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{"a"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{"b"})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{"d"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{"e"})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{"h"})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::Equals, AllTypeVariant{"i"})
          .selectivity,
      0.f);
}

TEST_F(HistogramColumnStatisticsTest, NotEqualsTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{0})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{1})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{3})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{4})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{7})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{8})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{0.f})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{1.f})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{3.f})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{4.f})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{7.f})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{8.f})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{0.})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{1.})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{3.})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{4.})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{7.})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{8.})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{"a"})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{"b"})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{"d"})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{"e"})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{"h"})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::NotEquals, AllTypeVariant{"i"})
          .selectivity,
      1.f);
}

TEST_F(HistogramColumnStatisticsTest, LessThanTest) {
  EXPECT_FLOAT_EQ(_column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{0})
                      .selectivity,
                  0.f);
  EXPECT_FLOAT_EQ(_column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{1})
                      .selectivity,
                  0.f);
  EXPECT_FLOAT_EQ(_column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{3})
                      .selectivity,
                  1.f / 3.f);
  EXPECT_FLOAT_EQ(_column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{4})
                      .selectivity,
                  1.f / 3.f);
  EXPECT_FLOAT_EQ(_column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{7})
                      .selectivity,
                  5.f / 6.f);
  EXPECT_FLOAT_EQ(_column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{8})
                      .selectivity,
                  1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{0.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{1.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{3.f})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{4.f})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{7.f})
          .selectivity,
      2.f / 3.f + (1.f / 3.f) * (7.f - 6.f) / std::nextafter(7.f - 6.f, 7.f - 6.f + 1));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{8.f})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{0.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{1.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{3.})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{4.})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{7.})
          .selectivity,
      static_cast<float>(2. / 3. + (1. / 3.) * (7. - 6.) / std::nextafter(7. - 6., 7. - 6. + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{8.})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{"a"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{"b"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{"d"})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{"e"})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{"h"})
          .selectivity,
      static_cast<float>(2. / 3. + (1. / 3.) * (7. - 6.) / std::nextafter(7. - 6., 7. - 6. + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThan, AllTypeVariant{"i"})
          .selectivity,
      1.f);
}

TEST_F(HistogramColumnStatisticsTest, LessThanEqualsTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{0})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{1})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{3})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{4})
          .selectivity,
      1.f / 2.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{7})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{8})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{0.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{1.f})
          .selectivity,
      1.f / 3.f * (std::nextafter(1.f, 1.f + 1) - 1.f) / std::nextafter(2.f - 1.f, 2.f - 1.f + 1));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{3.f})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{4.f})
          .selectivity,
      1.f / 3.f + 1.f / 3.f * (std::nextafter(4.f, 4.f + 1) - 4.f) / std::nextafter(5.f - 4.f, 5.f - 4.f + 1));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{7.f})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{8.f})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{0.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{1.})
          .selectivity,
      static_cast<float>(1. / 3. * (std::nextafter(1., 1. + 1) - 1.) / std::nextafter(2. - 1., 2. - 1. + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{3.})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{4.})
          .selectivity,
      static_cast<float>(1. / 3. + 1. / 3. * (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{7.})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{8.})
          .selectivity,
      1.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{"a"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{"b"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{"d"})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{"e"})
          .selectivity,
      static_cast<float>(1. / 3. + 1. / 3. * (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{"h"})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::LessThanEquals, AllTypeVariant{"i"})
          .selectivity,
      1.f);
}

TEST_F(HistogramColumnStatisticsTest, GreaterThanTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{0})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{1})
          .selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{3})
          .selectivity,
      4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{4})
          .selectivity,
      3.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{7})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{8})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{0.f})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{1.f})
          .selectivity,
      2.f / 3.f + 1.f / 3.f * (1 - (std::nextafter(1.f, 1.f + 1) - 1.f) / std::nextafter(2.f - 1.f, 2.f - 1.f + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{3.f})
          .selectivity,
      4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{4.f})
          .selectivity,
      1.f / 3.f + 1.f / 3.f * (1 - (std::nextafter(4.f, 4.f + 1) - 4.f) / std::nextafter(5.f - 4.f, 5.f - 4.f + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{7.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{8.f})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{0.})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{1.})
          .selectivity,
      static_cast<float>(2. / 3. +
                         1. / 3. * (1 - (std::nextafter(1., 1. + 1) - 1.) / std::nextafter(2. - 1., 2. - 1. + 1))));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{3.})
          .selectivity,
      4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{4.})
          .selectivity,
      static_cast<float>(1. / 3. +
                         1. / 3. * (1 - (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1))));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{7.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{8.})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{"a"})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{"b"})
          .selectivity,
      static_cast<float>(2. / 3. +
                         1. / 3. * (1 - (std::nextafter(1., 1. + 1) - 1.) / std::nextafter(2. - 1., 2. - 1. + 1))));
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{"d"})
          .selectivity,
      4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{"e"})
          .selectivity,
      static_cast<float>(1. / 3. +
                         1. / 3. * (1 - (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1))));
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{"h"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value(PredicateCondition::GreaterThan, AllTypeVariant{"i"})
          .selectivity,
      0.f);
}

TEST_F(HistogramColumnStatisticsTest, GreaterThanEqualsTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{0})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{1})
          .selectivity,
      1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{3})
          .selectivity,
      4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{4})
          .selectivity,
      4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{7})
          .selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{8})
          .selectivity,
      0.f);

  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{0.f})
                      .selectivity,
                  1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float
          ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{1.f})
          .selectivity,
      2.f / 3.f + 1.f / 3.f * (1 - (std::nextafter(1.f, 1.f + 1) - 1.f) / std::nextafter(2.f - 1.f, 2.f - 1.f + 1)));
  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{3.f})
                      .selectivity,
                  4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float
          ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{4.f})
          .selectivity,
      1.f / 3.f + 1.f / 3.f * (1 - (std::nextafter(4.f, 4.f + 1) - 4.f) / std::nextafter(5.f - 4.f, 5.f - 4.f + 1)));
  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{7.f})
                      .selectivity,
                  0.f);
  EXPECT_FLOAT_EQ(_column_statistics_float
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{8.f})
                      .selectivity,
                  0.f);

  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{0.})
                      .selectivity,
                  1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double
          ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{1.})
          .selectivity,
      static_cast<float>(2. / 3. +
                         1. / 3. * (1 - (std::nextafter(1., 1. + 1) - 1.) / std::nextafter(2. - 1., 2. - 1. + 1))));
  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{3.})
                      .selectivity,
                  4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double
          ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{4.})
          .selectivity,
      static_cast<float>(1. / 3. +
                         1. / 3. * (1 - (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1))));
  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{7.})
                      .selectivity,
                  0.f);
  EXPECT_FLOAT_EQ(_column_statistics_double
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{8.})
                      .selectivity,
                  0.f);

  EXPECT_FLOAT_EQ(_column_statistics_string
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{"a"})
                      .selectivity,
                  1.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string
          ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{"b"})
          .selectivity,
      static_cast<float>(2. / 3. +
                         1. / 3. * (1 - (std::nextafter(1., 1. + 1) - 1.) / std::nextafter(2. - 1., 2. - 1. + 1))));
  EXPECT_FLOAT_EQ(_column_statistics_string
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{"d"})
                      .selectivity,
                  4.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string
          ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{"e"})
          .selectivity,
      static_cast<float>(1. / 3. +
                         1. / 3. * (1 - (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1))));
  EXPECT_FLOAT_EQ(_column_statistics_string
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{"h"})
                      .selectivity,
                  0.f);
  EXPECT_FLOAT_EQ(_column_statistics_string
                      ->estimate_predicate_with_value(PredicateCondition::GreaterThanEquals, AllTypeVariant{"i"})
                      .selectivity,
                  0.f);
}

// TEST_F(HistogramColumnStatisticsTest, BetweenTest) {
//   PredicateCondition predicate_condition = PredicateCondition::Between;
//
//   std::vector<std::pair<int32_t, int32_t>> int_values{{-1, 0}, {-1, 2}, {1, 2}, {0, 7}, {5, 6}, {5, 8}, {7, 8}};
//   std::vector<float> selectivities_int{0.f, 1.f / 3.f, 1.f / 3.f, 1.f, 1.f / 3.f, 1.f / 3.f, 0.f};
//   predict_selectivities_and_compare(_column_statistics_int, predicate_condition, int_values, selectivities_int);
//
//   std::vector<std::pair<float, float>> float_values{{-1.f, 0.f}, {-1.f, 2.f}, {1.f, 2.f}, {0.f, 7.f},
//                                                     {5.f, 6.f},  {5.f, 8.f},  {7.f, 8.f}};
//   std::vector<float> selectivities_float{0.f, 1.f / 5.f, 1.f / 5.f, 1.f, 1.f / 5.f, 1.f / 5.f, 0.f};
//   predict_selectivities_and_compare(_column_statistics_float, predicate_condition, float_values, selectivities_float);
//
//   std::vector<std::pair<double, double>> double_values{{-1., 0.}, {-1., 2.}, {1., 2.}, {0., 7.},
//                                                        {5., 6.},  {5., 8.},  {7., 8.}};
//   predict_selectivities_and_compare(_column_statistics_double, predicate_condition, double_values, selectivities_float);
// }

TEST_F(HistogramColumnStatisticsTest, StoredProcedureEqualsTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::Equals).selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(PredicateCondition::Equals).selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(PredicateCondition::Equals).selectivity,
      1.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(PredicateCondition::Equals).selectivity,
      1.f / 6.f);
}

TEST_F(HistogramColumnStatisticsTest, StoredProcedureNotEqualsTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::NotEquals).selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(PredicateCondition::NotEquals).selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(PredicateCondition::NotEquals).selectivity,
      5.f / 6.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(PredicateCondition::NotEquals).selectivity,
      5.f / 6.f);
}

TEST_F(HistogramColumnStatisticsTest, StoredProcedureOpenEndedTest) {
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::LessThan).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(PredicateCondition::LessThan).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(PredicateCondition::LessThan).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(PredicateCondition::LessThan).selectivity,
      1.f / 3.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::LessThanEquals).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(PredicateCondition::LessThanEquals)
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(PredicateCondition::LessThanEquals)
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(PredicateCondition::LessThanEquals)
          .selectivity,
      1.f / 3.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThanEquals)
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThanEquals)
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThanEquals)
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThanEquals)
          .selectivity,
      1.f / 3.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThan).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThan).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThan).selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(PredicateCondition::GreaterThan).selectivity,
      1.f / 3.f);
}

TEST_F(HistogramColumnStatisticsTest, StoredProcedureBetweenTest) {
  PredicateCondition predicate_condition = PredicateCondition::Between;

  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{0})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{1})
          .selectivity,
      (1.f / 3.f) * (1.f / 6.f));
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{3})
          .selectivity,
      (1.f / 3.f) * (1.f / 3.f));
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{4})
          .selectivity,
      (1.f / 3.f) * (1.f / 2.f));
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{7})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_int->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{8})
          .selectivity,
      1.f / 3.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{0.f})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{1.f})
          .selectivity,
      (1.f / 3.f) * (1.f / 3.f * (std::nextafter(1.f, 1.f + 1) - 1.f) / std::nextafter(2.f - 1.f, 2.f - 1.f + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{3.f})
          .selectivity,
      (1.f / 3.f) * (1.f / 3.f));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{4.f})
          .selectivity,
      (1.f / 3.f) *
          (1.f / 3.f + 1.f / 3.f * (std::nextafter(4.f, 4.f + 1) - 4.f) / std::nextafter(5.f - 4.f, 5.f - 4.f + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{7.f})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_float->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{8.f})
          .selectivity,
      1.f / 3.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{0.})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{1.})
          .selectivity,
      static_cast<float>((1. / 3.) *
                         (1. / 3. * (std::nextafter(1., 1. + 1) - 1.) / std::nextafter(2. - 1., 2. - 1. + 1))));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{3.})
          .selectivity,
      (1.f / 3.f) * (1.f / 3.f));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{4.})
          .selectivity,
      static_cast<float>(
          (1. / 3.) * (1. / 3. + 1. / 3. * (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1))));
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{7.})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_double->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{8.})
          .selectivity,
      1.f / 3.f);

  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{"a"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{"b"})
          .selectivity,
      0.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{"d"})
          .selectivity,
      (1.f / 3.f) * (1.f / 3.f));
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{"e"})
          .selectivity,
      static_cast<float>((1. / 3.) * 1. / 3. +
                         1. / 3. * (std::nextafter(4., 4. + 1) - 4.) / std::nextafter(5. - 4., 5. - 4. + 1)));
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{"h"})
          .selectivity,
      1.f / 3.f);
  EXPECT_FLOAT_EQ(
      _column_statistics_string->estimate_predicate_with_value_placeholder(predicate_condition, AllTypeVariant{"i"})
          .selectivity,
      1.f / 3.f);
}

// TEST_F(HistogramColumnStatisticsTest, TwoColumnsEqualsTest) {
//   PredicateCondition predicate_condition = PredicateCondition::Equals;
//
//   auto col_stat1 = std::make_shared<HistogramColumnStatistics<int>>(0.0f, 10.f, 0, 10);
//   auto col_stat2 = std::make_shared<HistogramColumnStatistics<int>>(0.0f, 10.f, -10, 20);
//
//   auto result1 = col_stat1->estimate_predicate_with_column(predicate_condition, col_stat2);
//   auto result2 = col_stat2->estimate_predicate_with_column(predicate_condition, col_stat1);
//   float expected_selectivity = (11.f / 31.f) / 10.f;
//
//   EXPECT_FLOAT_EQ(result1.selectivity, expected_selectivity);
//   EXPECT_FLOAT_EQ(result2.selectivity, expected_selectivity);
//
//   auto col_stat3 = std::make_shared<HistogramColumnStatistics<float>>(0.0f, 10.f, 0.f, 10.f);
//   auto col_stat4 = std::make_shared<HistogramColumnStatistics<float>>(0.0f, 3.f, -10.f, 20.f);
//
//   auto result3 = col_stat3->estimate_predicate_with_column(predicate_condition, col_stat4);
//   auto result4 = col_stat4->estimate_predicate_with_column(predicate_condition, col_stat3);
//   expected_selectivity = (10.f / 30.f) / 10.f;
//
//   EXPECT_FLOAT_EQ(result3.selectivity, expected_selectivity);
//   EXPECT_FLOAT_EQ(result4.selectivity, expected_selectivity);
//
//   auto col_stat5 = std::make_shared<HistogramColumnStatistics<float>>(0.0f, 10.f, 20.f, 30.f);
//
//   auto result5 = col_stat3->estimate_predicate_with_column(predicate_condition, col_stat5);
//   auto result6 = col_stat5->estimate_predicate_with_column(predicate_condition, col_stat3);
//   expected_selectivity = 0.f;
//
//   EXPECT_FLOAT_EQ(result5.selectivity, expected_selectivity);
//   EXPECT_FLOAT_EQ(result6.selectivity, expected_selectivity);
// }
//
// TEST_F(HistogramColumnStatisticsTest, TwoColumnsLessThanTest) {
//   PredicateCondition predicate_condition = PredicateCondition::LessThan;
//
//   auto col_stat1 = std::make_shared<HistogramColumnStatistics<int>>(0.0f, 10.f, 1, 20);
//   auto col_stat2 = std::make_shared<HistogramColumnStatistics<int>>(0.0f, 30.f, 11, 40);
//
//   auto result1 = col_stat1->estimate_predicate_with_column(predicate_condition, col_stat2);
//   auto expected_selectivity = ((10.f / 20.f) * (10.f / 30.f) - 0.5f * 1.f / 30.f) * 0.5f + (10.f / 20.f) +
//                               (20.f / 30.f) - (10.f / 20.f) * (20.f / 30.f);
//   EXPECT_FLOAT_EQ(result1.selectivity, expected_selectivity);
//
//   auto result2 = col_stat2->estimate_predicate_with_column(predicate_condition, col_stat1);
//   expected_selectivity = ((10.f / 20.f) * (10.f / 30.f) - 0.5f * 1.f / 30.f) * 0.5f;
//   EXPECT_FLOAT_EQ(result2.selectivity, expected_selectivity);
//
//   auto col_stat3 = std::make_shared<HistogramColumnStatistics<float>>(0.0f, 6.f, 0, 10);
//   auto col_stat4 = std::make_shared<HistogramColumnStatistics<float>>(0.0f, 12.f, -10, 30);
//
//   auto result3 = col_stat3->estimate_predicate_with_column(predicate_condition, col_stat4);
//   expected_selectivity = ((10.f / 10.f) * (10.f / 40.f) - 1.f / (4 * 6)) * 0.5f + (20.f / 40.f);
//   EXPECT_FLOAT_EQ(result3.selectivity, expected_selectivity);
//
//   auto result4 = col_stat4->estimate_predicate_with_column(predicate_condition, col_stat3);
//   expected_selectivity = ((10.f / 10.f) * (10.f / 40.f) - 1.f / (4 * 6)) * 0.5f + (10.f / 40.f);
//   EXPECT_FLOAT_EQ(result4.selectivity, expected_selectivity);
// }
//
// TEST_F(HistogramColumnStatisticsTest, TwoColumnsRealDataTest) {
//   // test selectivity calculations for all predicate conditions and all column combinations of
//   // int_equal_distribution.tbl
//   std::vector<PredicateCondition> predicate_conditions{PredicateCondition::Equals, PredicateCondition::NotEquals,
//                                                        PredicateCondition::LessThan, PredicateCondition::LessThanEquals,
//                                                        PredicateCondition::GreaterThan};
//   for (auto predicate_condition : predicate_conditions) {
//     predict_selectivities_and_compare(_int_equal_distribution, _column_statistics_int_equal_distribution,
//                                       predicate_condition);
//   }
// }
//
// TEST_F(HistogramColumnStatisticsTest, NonNullRatioOneColumnTest) {
//   // null value ratio of 0 not tested here, since this is done in all other tests
//   _column_statistics_int->set_null_value_ratio(0.25f);   // non-null value ratio: 0.75
//   _column_statistics_float->set_null_value_ratio(0.5f);  // non-null value ratio: 0.5
//   _column_statistics_string->set_null_value_ratio(1.f);  // non-null value ratio: 0
//
//   auto predicate_condition = PredicateCondition::Equals;
//   auto result = _column_statistics_int->estimate_predicate_with_value(predicate_condition, AllTypeVariant(1));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.75f / 6.f);
//   result = _column_statistics_float->estimate_predicate_with_value(predicate_condition, AllTypeVariant(2.f));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.5f / 6.f);
//   result = _column_statistics_string->estimate_predicate_with_value(predicate_condition, AllTypeVariant("a"));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.f);
//
//   predicate_condition = PredicateCondition::NotEquals;
//   result = _column_statistics_int->estimate_predicate_with_value(predicate_condition, AllTypeVariant(1));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.75f * 5.f / 6.f);
//   result = _column_statistics_float->estimate_predicate_with_value(predicate_condition, AllTypeVariant(2.f));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.5f * 5.f / 6.f);
//   result = _column_statistics_string->estimate_predicate_with_value(predicate_condition, AllTypeVariant("a"));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.f);
//
//   predicate_condition = PredicateCondition::LessThan;
//   result = _column_statistics_int->estimate_predicate_with_value(predicate_condition, AllTypeVariant(3));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.75f * 2.f / 6.f);
//   result = _column_statistics_float->estimate_predicate_with_value(predicate_condition, AllTypeVariant(3.f));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.5f * 2.f / 5.f);
//   result = _column_statistics_string->estimate_predicate_with_value(predicate_condition, AllTypeVariant("c"));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.f);
//
//   predicate_condition = PredicateCondition::GreaterThanEquals;
//   result = _column_statistics_int->estimate_predicate_with_value(predicate_condition, AllTypeVariant(3));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.75f * 4.f / 6.f);
//   result = _column_statistics_float->estimate_predicate_with_value(predicate_condition, AllTypeVariant(3.f));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.5f * 3.f / 5.f);
//   result = _column_statistics_string->estimate_predicate_with_value(predicate_condition, AllTypeVariant("c"));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.f);
//
//   predicate_condition = PredicateCondition::Between;
//   result =
//       _column_statistics_int->estimate_predicate_with_value(predicate_condition, AllTypeVariant(2), AllTypeVariant(4));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.75f * 3.f / 6.f);
//   result = _column_statistics_float->estimate_predicate_with_value(predicate_condition, AllTypeVariant(4.f),
//                                                                    AllTypeVariant(6.f));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.5f * 2.f / 5.f);
//   result = _column_statistics_string->estimate_predicate_with_value(predicate_condition, AllTypeVariant("c"),
//                                                                     AllTypeVariant("d"));
//   EXPECT_FLOAT_EQ(result.selectivity, 0.f);
// }
//
// TEST_F(HistogramColumnStatisticsTest, NonNullRatioTwoColumnTest) {
//   auto stats_0 =
//       std::const_pointer_cast<BaseColumnStatistics>(_column_statistics_int_equal_distribution[0]);  // values from 0 to 5
//   auto stats_1 =
//       std::const_pointer_cast<BaseColumnStatistics>(_column_statistics_int_equal_distribution[1]);  // values from 0 to 2
//   auto stats_2 =
//       std::const_pointer_cast<BaseColumnStatistics>(_column_statistics_int_equal_distribution[2]);  // values from 1 to 2
//
//   stats_0->set_null_value_ratio(0.1);   // non-null value ratio: 0.9
//   stats_1->set_null_value_ratio(0.2);   // non-null value ratio: 0.8
//   stats_2->set_null_value_ratio(0.15);  // non-null value ratio: 0.85
//
//   auto predicate_condition = PredicateCondition::Equals;
//   auto result = stats_0->estimate_predicate_with_column(predicate_condition, stats_1);
//   EXPECT_FLOAT_EQ(result.selectivity, 0.9f * 0.8f * 0.5f / 3.f);
//
//   predicate_condition = PredicateCondition::LessThan;
//   result = stats_1->estimate_predicate_with_column(predicate_condition, stats_2);
//   EXPECT_FLOAT_EQ(result.selectivity, 0.8f * 0.85f * (1.f / 3.f + 1.f / 3.f * 1.f / 2.f));
// }

}  // namespace opossum
