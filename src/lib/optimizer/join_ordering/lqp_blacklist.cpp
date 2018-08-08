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
  }

  return false;     
}

bool LQPBlacklist::test(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  const auto hash_blacklisted = std::any_of(_blacklist.begin(), _blacklist.end(), [&](const auto& entry) {
    return entry->hash() == lqp->hash();
  });

  auto lqp_blacklisted = _blacklist.find(lqp) != _blacklist.end();

  if (hash_blacklisted != lqp_blacklisted) {
    std::cout << "BLACKLIST hash/entry " << lqp->hash() << " mismatch: " << hash_blacklisted << " " << lqp_blacklisted << std::endl;
  } else {
    std::cout << "BLACKLIST " << lqp->hash() << " blacklisted: " << lqp_blacklisted << std::endl;
  }

  if (lqp_blacklisted) return true;

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

}  // namespace opossum
