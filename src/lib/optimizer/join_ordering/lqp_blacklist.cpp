#include "lqp_blacklist.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

class AbstractLQPNode;

bool NodeCmp::operator()(const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs) const {
  return !lhs->find_first_subplan_mismatch(rhs);
}

size_t NodeHash::operator()(const std::shared_ptr<AbstractLQPNode>& lhs) const {
  return lhs->hash();
}

void LQPBlacklist::put(const std::shared_ptr<AbstractLQPNode>& lqp) {
  _blacklist.emplace(lqp);
}

bool LQPBlacklist::test(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  return _blacklist.find(lqp) != _blacklist.end();
}

}  // namespace opossum
