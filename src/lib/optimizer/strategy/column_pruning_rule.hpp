#pragma once

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * Removes never-referenced columns from the LQP by inserting ProjectionNodes that select only those expressions needed
 * "further up" in the plan.
 */
class ChunkPruningRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
