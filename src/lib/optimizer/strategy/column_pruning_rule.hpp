#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * Removes never-referenced columns from the LQP by inserting ProjectionNodes that select only those expressions needed
 * "further up" in the plan.
 */
class ColumnPruningRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  using ExpressionsByNode = std::unordered_map<std::shared_ptr<AbstractLQPNode>, ExpressionUnorderedSet>;

  static void _collect_expressions_consumed_in_super_plans(const ExpressionUnorderedSet& expressions_consumed_in_super_plan,
                                                          ExpressionsByNode& expressions_consumed_in_super_plan_by_node,
                                                          const std::shared_ptr<AbstractLQPNode>& node);

  static bool _prune(ExpressionsByNode& expressions_consumed_in_super_plan_by_node,
                                                          const std::shared_ptr<AbstractLQPNode>& node,
                     const LQPInputSide input_side);

  static std::vector<std::shared_ptr<AbstractExpression>> _get_expressions_consumed_by_node(const AbstractLQPNode& node);
};

}  // namespace opossum
