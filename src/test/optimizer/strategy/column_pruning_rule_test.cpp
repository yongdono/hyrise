#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "optimizer/strategy/column_pruning_rule.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ColumnPruningRuleTest: public ::testing::Test {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}});
    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "u"}, {DataType::Int, "v"}, {DataType::Int, "w"}});
    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}});

    a = node_a->get_column("a");
    b = node_a->get_column("b");
    c = node_a->get_column("c");
    u = node_b->get_column("u");
    v = node_b->get_column("v");
    w = node_b->get_column("w");
    x = node_c->get_column("x");
    y = node_c->get_column("y");
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c;
  LQPColumnReference a, b, c, u, v, w, x, y;
};

TEST_F(ColumnPruningRuleTest, ) {
  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(add_(mul_(a, u), 5)),
    PredicateNode::make(greater_than_(5, c)),
      JoinNode::make(
        node_a,
        SortNode::make(expression_vector(u, w), std::vector<OrderByMode>{OrderByMode::Ascending, OrderByMode::Descending},  // NOLINT
          node_b)));
  // clang-format on

  
  
}

}  // namespace opossum
