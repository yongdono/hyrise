#include "gtest/gtest.h"

#include "storage/storage_manager.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "statistics/cardinality_cache.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/load_table.hpp"
#include "statistics/cardinality_cache_uncapped.hpp"
#include "statistics/cardinality_cache_lru.hpp"
#include "statistics/cardinality_cache_lag.hpp"

using namespace std::string_literals;
using namespace std::literals::chrono_literals;

namespace opossum {

class CardinalityEstimationCacheTest : public ::testing::Test {
 public:
  void SetUp() override {
    StorageManager::get().add_table("int_float", load_table("src/test/tables/int_float.tbl"));
    StorageManager::get().add_table("int_float2", load_table("src/test/tables/int_float2.tbl"));

    int_float = StoredTableNode::make("int_float");
    int_float->set_alias("some_alias_for_int_float");
    int_float2 = StoredTableNode::make("int_float2");

    int_float_a = int_float->get_column("a"s);
    int_float_b = int_float->get_column("b"s);
    int_float2_a = int_float2->get_column("a"s);
    int_float2_b = int_float2->get_column("b"s);

    int_float_a_eq_int_float_b = std::make_shared<const JoinPlanAtomicPredicate>(
      int_float_a, PredicateCondition::Equals, int_float_b
    );
    int_float_a_gt_int_float2_b = std::make_shared<const JoinPlanAtomicPredicate>(
      int_float_a, PredicateCondition::GreaterThan, int_float2_b
    );
    int_float_a_eq_five = std::make_shared<const JoinPlanAtomicPredicate>(
      int_float_a, PredicateCondition::Equals, 5
    );
    int_float_b_eq_hello = std::make_shared<const JoinPlanAtomicPredicate>(
      int_float_b, PredicateCondition::Equals, "Hello"
    );

    p1_and_p2_or_p3 = std::make_shared<const JoinPlanLogicalPredicate>(
      int_float_a_eq_int_float_b, JoinPlanPredicateLogicalOperator::And,
      std::make_shared<const JoinPlanLogicalPredicate>(
        int_float_a_eq_five, JoinPlanPredicateLogicalOperator::Or, int_float_a_gt_int_float2_b
      )
    );

    cache = std::make_shared<CardinalityCache>(std::make_shared<CardinalityCacheUncapped>());
  }

  void TearDown() override {
    StorageManager::reset();
  }

  std::shared_ptr<StoredTableNode> int_float, int_float2;
  LQPColumnReference int_float_a, int_float_b, int_float2_a, int_float2_b;
  std::shared_ptr<const AbstractJoinPlanPredicate> int_float_a_eq_int_float_b;
  std::shared_ptr<const AbstractJoinPlanPredicate> int_float_a_gt_int_float2_b;
  std::shared_ptr<const AbstractJoinPlanPredicate> int_float_a_eq_five;
  std::shared_ptr<const AbstractJoinPlanPredicate> int_float_b_eq_hello;
  std::shared_ptr<const AbstractJoinPlanPredicate> p1_and_p2_or_p3;
  std::shared_ptr<CardinalityCache> cache;
};

TEST_F(CardinalityEstimationCacheTest, Json) {
  cache->set_cardinality(BaseJoinGraph{{int_float}, {int_float_a_eq_int_float_b}}, 13);
  cache->set_cardinality(BaseJoinGraph{{int_float2, int_float}, {int_float_a_eq_int_float_b, int_float_a_gt_int_float2_b}}, 12);
  cache->set_cardinality(BaseJoinGraph{{int_float2, int_float}, {int_float_a_eq_five, int_float_a_eq_int_float_b, int_float_a_gt_int_float2_b}}, 11);
  cache->set_cardinality(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3}}, 19);
  cache->set_cardinality(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_b_eq_hello}}, 15);
  cache->set_timeout(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_a_eq_int_float_b}}, std::chrono::seconds{25});

  const auto json = cache->to_json();

  CardinalityCache cache_b(std::make_shared<CardinalityCacheUncapped>());
  cache_b.from_json(json);

  EXPECT_EQ(cache_b.get_cardinality(BaseJoinGraph{{int_float}, {int_float_a_eq_int_float_b}}), 13);
  EXPECT_EQ(cache_b.get_cardinality(BaseJoinGraph{{int_float, int_float2}, {int_float_a_gt_int_float2_b, int_float_a_eq_int_float_b}}), 12);
  EXPECT_EQ(cache_b.get_cardinality(BaseJoinGraph{{int_float2, int_float}, {int_float_a_eq_int_float_b, int_float_a_eq_five, int_float_a_gt_int_float2_b}}), 11);
  EXPECT_EQ(cache_b.get_cardinality(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3}}), 19);
  EXPECT_EQ(cache_b.get_cardinality(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_b_eq_hello}}), 15);
  EXPECT_EQ(cache_b.get_timeout(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_b_eq_hello}}), std::nullopt);
  EXPECT_EQ(cache_b.get_cardinality(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_a_eq_int_float_b}}), std::nullopt);
  EXPECT_EQ(cache_b.get_timeout(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_a_eq_int_float_b}}), std::chrono::seconds{25});
}

TEST_F(CardinalityEstimationCacheTest, LRU) {
  CardinalityCache cache{std::make_shared<CardinalityCacheLRU>(3)};

  const auto join_graph_a = BaseJoinGraph{{int_float}, {int_float_a_eq_int_float_b}};
  const auto join_graph_b = BaseJoinGraph{{int_float2, int_float}, {int_float_a_eq_int_float_b, int_float_a_gt_int_float2_b}};
  const auto join_graph_c = BaseJoinGraph{{int_float2, int_float}, {int_float_a_eq_five, int_float_a_eq_int_float_b, int_float_a_gt_int_float2_b}};
  const auto join_graph_d = BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3}};

  EXPECT_EQ(cache.get_cardinality(join_graph_a), std::nullopt);
  EXPECT_EQ(cache.get_cardinality(join_graph_b), std::nullopt);
  EXPECT_EQ(cache.size(), 2u);

  cache.set_cardinality(join_graph_a, 13);
  cache.set_timeout(join_graph_a, 100s);
  cache.set_cardinality(join_graph_b, 14);
  cache.set_timeout(join_graph_b, 101s);
  cache.set_cardinality(join_graph_c, 15);
  cache.set_timeout(join_graph_c, 102s);
  cache.set_cardinality(join_graph_d, 16);
  cache.set_timeout(join_graph_d, 103s);

  EXPECT_EQ(cache.get_cardinality(join_graph_a), std::nullopt);
  EXPECT_EQ(cache.get_timeout(join_graph_a), 100s);
  EXPECT_EQ(cache.get_cardinality(join_graph_b), 14);
  EXPECT_EQ(cache.get_timeout(join_graph_b), 101s);
  EXPECT_EQ(cache.get_cardinality(join_graph_c), 15);
  EXPECT_EQ(cache.get_timeout(join_graph_c), 102s);
  EXPECT_EQ(cache.get_cardinality(join_graph_d), 16);
  EXPECT_EQ(cache.get_timeout(join_graph_d), 103s);

  cache.set_cardinality(join_graph_a, 20);
  EXPECT_EQ(cache.get_cardinality(join_graph_a), 20);
  EXPECT_EQ(cache.get_timeout(join_graph_a), 100s);
  EXPECT_EQ(cache.get_cardinality(join_graph_b), std::nullopt);
  EXPECT_EQ(cache.get_timeout(join_graph_b), 101s);
  EXPECT_EQ(cache.size(), 4u);
}

TEST_F(CardinalityEstimationCacheTest, LAG) {
  CardinalityCache cache{std::make_shared<CardinalityCacheLAG>(3)};

  const auto join_graph_a = BaseJoinGraph{{int_float}, {int_float_a_eq_int_float_b}};
  const auto join_graph_b = BaseJoinGraph{{int_float2, int_float}, {int_float_a_eq_int_float_b, int_float_a_gt_int_float2_b}};
  const auto join_graph_c = BaseJoinGraph{{int_float2, int_float}, {int_float_a_eq_five, int_float_a_eq_int_float_b, int_float_a_gt_int_float2_b}};
  const auto join_graph_d = BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3}};
  const auto join_graph_e = BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_a_eq_five}};

  cache.set_cardinality(join_graph_a, 13, 24);
  cache.set_timeout(join_graph_a, 100s);
  cache.set_cardinality(join_graph_b, 14, 21);
  cache.set_timeout(join_graph_b, 101s);
  cache.set_cardinality(join_graph_c, 15, 25);
  cache.set_timeout(join_graph_c, 102s);
  cache.set_cardinality(join_graph_d, 16, 30);
  cache.set_timeout(join_graph_d, 103s);

  EXPECT_EQ(cache.get_cardinality(join_graph_a), 13);
  EXPECT_EQ(cache.get_cardinality(join_graph_b), std::nullopt);
  EXPECT_EQ(cache.get_cardinality(join_graph_c), 15);
  EXPECT_EQ(cache.get_cardinality(join_graph_d), 16);

  cache.set_cardinality(join_graph_e, 16, 16);
  cache.set_timeout(join_graph_e, 104s);

  EXPECT_EQ(cache.get_cardinality(join_graph_e), std::nullopt);
  EXPECT_EQ(cache.get_timeout(join_graph_e), 104s);
}

}  // namespace opossum
