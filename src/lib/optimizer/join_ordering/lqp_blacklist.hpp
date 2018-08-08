#pragma once

#include <chrono>
#include <memory>
#include <unordered_set>

namespace opossum {

class AbstractLQPNode;
class AbstractOperator;

struct NodeCmp {
  bool operator()(const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs) const;
};

struct NodeHash {
  size_t operator()(const std::shared_ptr<AbstractLQPNode>& lhs) const;
};

class LQPBlacklist {
 public:
  void put(const std::shared_ptr<AbstractLQPNode>& lqp);
  bool consider(const std::shared_ptr<AbstractOperator>& op);
  bool test(const std::shared_ptr<AbstractLQPNode>& lqp) const;

  void print() const;

  static std::chrono::microseconds get_plan_runtime(const std::shared_ptr<AbstractOperator>& op);

  std::chrono::microseconds threshold{1'800'000};

 private:
  std::unordered_set<std::shared_ptr<AbstractLQPNode>, NodeHash, NodeCmp> _blacklist;
};

}  // namespace opossum
