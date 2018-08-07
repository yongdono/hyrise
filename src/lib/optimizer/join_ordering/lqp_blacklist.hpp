#pragma once

#include <memory>
#include <unordered_set>

namespace opossum {

class AbstractLQPNode;

struct NodeCmp {
  bool operator()(const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs) const;
};

struct NodeHash {
  size_t operator()(const std::shared_ptr<AbstractLQPNode>& lhs) const;
};

class LQPBlacklist {
 public:
  void put(const std::shared_ptr<AbstractLQPNode>& lqp);
  bool test(const std::shared_ptr<AbstractLQPNode>& lqp) const;
  
 private:
  std::unordered_set<std::shared_ptr<AbstractLQPNode>, NodeHash, NodeCmp> _blacklist;
};

}  // namespace opossum
