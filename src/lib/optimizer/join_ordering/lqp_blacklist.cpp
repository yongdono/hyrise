#include "lqp_blacklist.hpp"

#include <iostream>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class AbstractLQPNode;

bool NodeCmp::operator()(const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs) const {
  return !lhs->find_first_subplan_mismatch(rhs);
}

size_t NodeHash::operator()(const std::shared_ptr<AbstractLQPNode>& lhs) const {
  return lhs->hash();
}

void LQPBlacklist::put(const std::shared_ptr<AbstractLQPNode>& lqp) {
  const auto emplaced = _blacklist.emplace(lqp).second;

  if (emplaced) {
    std::cout << "New LQPBlacklist entry: " << lqp->hash() << ", " << _blacklist.size() << " total" << std::endl;
  } else {
    std::cout << "Recurring LQPBlacklist entry: " << lqp->hash() << ", " << _blacklist.size() << " total" << std::endl;
  }
}

bool LQPBlacklist::consider(const std::shared_ptr<AbstractOperator>& op) {
  if (op->get_output() || !op->lqp_node()) return false;

  const auto plan_runtime = get_plan_runtime(op);

  if (plan_runtime > threshold) {
    put(op->lqp_node());
    return true;
  } else {
    std::cout << "Subplan runtime " << plan_runtime.count() << "µs less than threshold of " << threshold.count() << "µs" << std::endl;
    return false;
  }
}

bool LQPBlacklist::test(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  if (_blacklist.find(lqp) != _blacklist.end()) return true;

  if (lqp->left_input() && test(lqp->left_input())) return true;
  if (lqp->right_input() && test(lqp->right_input())) return true;

  return false;
}

void LQPBlacklist::print() const {
  std::cout << "------------- LQPBlacklist ------------------" << std::endl;
  for (const auto& node : _blacklist) {
    std::cout << "LQP: " << node->hash() << std::endl;
    node->print();
    std::cout << std::endl;
  }
  std::cout << "------------- /LQPBlacklist ------------------" << std::endl;
}

std::chrono::microseconds LQPBlacklist::get_plan_runtime(const std::shared_ptr<AbstractOperator>& op) {
  return op->base_performance_data().total +
  (op->input_left() ? get_plan_runtime(op->mutable_input_left()) : std::chrono::microseconds{0}) +
  (op->input_right() ? get_plan_runtime(op->mutable_input_right()) : std::chrono::microseconds{0});
}

}  // namespace opossum
