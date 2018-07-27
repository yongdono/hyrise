#pragma once

#include <memory>
#include <string>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;

// This optimizer rule is responsible for pushing down limits in the lqp as much as possible
// to reduce the result set early on when Jitting is used.
class LimitPushdownRule : public AbstractRule {
public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) override;
};

}  // namespace opossum
