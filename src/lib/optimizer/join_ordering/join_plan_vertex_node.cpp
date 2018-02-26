#include "join_plan_vertex_node.hpp"

namespace opossum {

JoinPlanVertexNode::JoinPlanVertexNode(const std::shared_ptr<AbstractLQPNode>& vertex_node,
                                       const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates)
    : AbstractJoinPlanNode(JoinPlanNodeType::Vertex), _lqp_node(vertex_node), _predicates(predicates) {
  _statistics = _lqp_node->get_statistics();

  // TODO(moritz) Order predicates among themselves for minimal cost

  _order_predicates(_predicates, _statistics);

  for (const auto& predicate : _predicates) {
    const auto predicate_estimate = _estimate_predicate(predicate, _statistics);

    _node_cost += predicate_estimate.cost;
    _statistics = predicate_estimate.statistics;
  }

  _plan_cost = _node_cost;
}

std::shared_ptr<AbstractLQPNode> JoinPlanVertexNode::lqp_node() const {
  return _lqp_node;
}

const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& JoinPlanVertexNode::predicates() const {
  return _predicates;
}

bool JoinPlanVertexNode::contains_vertex(const std::shared_ptr<AbstractLQPNode>& node) const {
  return node == _lqp_node;
}

std::optional<ColumnID> JoinPlanVertexNode::find_column_id(const LQPColumnReference& column_reference) const {
  return _lqp_node->find_output_column_id(column_reference);
}

std::shared_ptr<AbstractLQPNode> JoinPlanVertexNode::to_lqp() const {
  auto lqp = _lqp_node;
  for (const auto& predicate : _predicates) {
    lqp = _insert_predicate(lqp, predicate);
  }

  return lqp;
}

size_t JoinPlanVertexNode::output_column_count() const { return _lqp_node->output_column_count(); }

std::string JoinPlanVertexNode::description() const {
  std::stringstream stream;
  stream << _lqp_node->description();
  stream << " Predicates: {";
  for (size_t predicate_idx = 0; predicate_idx < _predicates.size(); ++predicate_idx) {
    _predicates[predicate_idx]->print(stream);
    if (predicate_idx + 1 < _predicates.size()) {
      stream << ", ";
    }
  }
  stream << "}";
  return stream.str();
}

}  // namespace opossum
