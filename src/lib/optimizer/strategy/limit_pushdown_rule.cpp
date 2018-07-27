#include "limit_pushdown_rule.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

std::string LimitPushdownRule::name() const { return "Limit Pushdown Rule"; }

bool LimitPushdownRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type() != LQPNodeType::Limit) {
    return _apply_to_inputs(node);
  }

  auto input = node->left_input();
  // limit can only be pushed down to nodes with one output
  if (input->output_count() != 1) {
    return _apply_to_inputs(node);
  }

  if (input->type() == LQPNodeType::Limit) {
    // merge two consecutive limits
    auto current_limit = std::dynamic_pointer_cast<LimitNode>(node);
    auto input_limit = std::dynamic_pointer_cast<LimitNode>(input);
    if (current_limit->num_rows() < input_limit->num_rows()) {
      input_limit->set_num_rows(current_limit->num_rows());
    }
    node->remove_from_tree();
    return apply_to(node);
  }

  if (input->type() != LQPNodeType::Projection) {
    return _apply_to_inputs(node);
  }

  while ((input->left_input()->type() == LQPNodeType::Projection) && (input->left_input()->output_count() == 1)) {
    input = input->left_input();
  }

  node->remove_from_tree();
  const auto previous_left_input = input->left_input();

  input->set_left_input(node);
  node->set_left_input(previous_left_input);

  _apply_to_inputs(input);
  return true;
}

}  // namespace opossum
