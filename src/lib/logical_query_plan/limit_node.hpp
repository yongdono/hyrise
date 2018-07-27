#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents limiting a result to a certain number of rows (LIMIT operator).
 */
class LimitNode : public EnableMakeForLQPNode<LimitNode>, public AbstractLQPNode {
 public:
  explicit LimitNode(const size_t num_rows);

  std::string description() const override;

  size_t num_rows() const;

  void set_num_rows(const size_t num_rows);

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;

 private:
  size_t _num_rows;
};

}  // namespace opossum
