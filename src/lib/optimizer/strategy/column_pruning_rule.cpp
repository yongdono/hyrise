#include "column_pruning_rule.hpp"

#include <unordered_map>

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::string ColumnPruningRule::name() const {
  return "Column Pruning Rule";
}

bool ColumnPruningRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  // Stores expressions required in a node an all nodes above it (aka, in the "super plan")
  auto expressions_consumed_in_super_plan_by_node = ExpressionsByNode{};

  // The output columns of the plan are never pruned and form the initial set of do-not-prune expressions
  const auto plan_output_expressions = root->column_expressions();
  const auto plan_output_expressions_set = ExpressionUnorderedSet{plan_output_expressions.begin(), plan_output_expressions.end()};

  _collect_expressions_consumed_in_super_plans(plan_output_expressions_set, expressions_consumed_in_super_plan_by_node, root);

  for (const auto& pair: expressions_consumed_in_super_plan_by_node) {
    std::cout << pair.first->description() << ": ";
    for (const auto& expression : pair.second) {
      std::cout << expression->as_column_name() << ", ";
    }
    std::cout << std::endl;
  }

  auto plan_changed = false;

  plan_changed |= _prune(expressions_consumed_in_super_plan_by_node, root, LQPInputSide::Left);
  plan_changed |= _prune(expressions_consumed_in_super_plan_by_node, root, LQPInputSide::Right);

  return plan_changed;
}

void ColumnPruningRule::_collect_expressions_consumed_in_super_plans(const ExpressionUnorderedSet& expressions_consumed_in_output_super_plan,
                                                         ExpressionsByNode& expressions_consumed_in_super_plan_by_node,
                                                         const std::shared_ptr<AbstractLQPNode>& node) {
  const auto expressions_consumed_by_node = _get_expressions_consumed_by_node(*node);
  auto& expression_consumed_by_super_plan = expressions_consumed_in_super_plan_by_node[node];

  expression_consumed_by_super_plan.insert(expressions_consumed_by_node.begin(), expressions_consumed_by_node.end());
  expression_consumed_by_super_plan.insert(expressions_consumed_in_output_super_plan.begin(), expressions_consumed_in_output_super_plan.end());

  if (node->left_input()) {
    _collect_expressions_consumed_in_super_plans(expression_consumed_by_super_plan,
                                                expressions_consumed_in_super_plan_by_node,
                                                node->left_input());
  }

  if (node->right_input()) {
    _collect_expressions_consumed_in_super_plans(expression_consumed_by_super_plan,
                                                expressions_consumed_in_super_plan_by_node,
                                                node->right_input());
  }
}

bool ColumnPruningRule::_prune(ExpressionsByNode& expressions_consumed_in_super_plan_by_node,
                               const std::shared_ptr<AbstractLQPNode>& node, const LQPInputSide input_side) {
  const auto input = node->input(input_side);
  if (!input) return false;

  const auto& expressions_consumed_in_super_plan = expressions_consumed_in_super_plan_by_node[node];

  std::vector<std::shared_ptr<AbstractExpression>> consumed_input_expressions;
  for (const auto& input_expression : input->column_expressions()) {
    const auto expression_iter = expressions_consumed_in_super_plan.find(input_expression);
    if (expression_iter != expressions_consumed_in_super_plan.end()) {
      consumed_input_expressions.emplace_back(input_expression);
    }
  }

  auto plan_changed = false;

  if (consumed_input_expressions.size() != input->column_expressions().size()) {
    if (consumed_input_expressions.empty()) {
      node->set_input(input_side, DummyTableNode::make());
      return true;
    }
    lqp_insert_node(node, input_side, ProjectionNode::make(consumed_input_expressions));
  }

  plan_changed |= _prune(expressions_consumed_in_super_plan_by_node, input, LQPInputSide::Left);
  plan_changed |= _prune(expressions_consumed_in_super_plan_by_node, input, LQPInputSide::Right);

  return plan_changed;
}

std::vector<std::shared_ptr<AbstractExpression>> ColumnPruningRule::_get_expressions_consumed_by_node(const AbstractLQPNode& node) {
  std::vector<std::shared_ptr<AbstractExpression>> consumed_expressions;

  switch (node.type) {
    case LQPNodeType::Projection:
      for (const auto& expression : node.column_expressions()) {
        visit_expression(expression, [&](const auto& sub_expression) {
          const auto column_id = node.left_input()->find_column_id(*sub_expression);
          if (column_id) {
            consumed_expressions.emplace_back(sub_expression);
            return ExpressionVisitation::DoNotVisitArguments;
          } else {
            return ExpressionVisitation::VisitArguments;
          }
        });
      }
      break;

    case LQPNodeType::Aggregate: {
      const auto& aggregate_node = dynamic_cast<const AggregateNode&>(node);
      for (const auto &expression : aggregate_node.aggregate_expressions) {
        for (const auto &argument : expression->arguments) {
          consumed_expressions.emplace_back(argument);
        }
      }
      consumed_expressions.insert(consumed_expressions.end(), aggregate_node.group_by_expressions.begin(), aggregate_node.group_by_expressions.end());
    } break;

    case LQPNodeType::Predicate: {
      const auto &predicate_node = dynamic_cast<const PredicateNode &>(node);
      for (const auto &argument : predicate_node.predicate->arguments) {
        consumed_expressions.emplace_back(argument);
      }
    } break;

    case LQPNodeType::Join: {
      const auto &join_node = dynamic_cast<const JoinNode &>(node);
      if (join_node.join_predicate) {
        for (const auto &argument : join_node.join_predicate->arguments) {
          consumed_expressions.emplace_back(argument);
        }
      }
    } break;

    case LQPNodeType::Sort: {
      const auto &sort_node = dynamic_cast<const SortNode &>(node);
      for (const auto &expression : sort_node.expressions) {
        consumed_expressions.emplace_back(expression);
      }
    } break;

    default: {}

  }

  return consumed_expressions;
}

}  // namespace opossum
