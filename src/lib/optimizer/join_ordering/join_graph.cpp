#include "join_graph.hpp"

#include <memory>
#include <optional>
#include <utility>
#include <queue>
#include <vector>

#include "constant_mappings.hpp"
#include "join_edge.hpp"
#include "join_graph_builder.hpp"
#include "join_plan_predicate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<JoinGraph> JoinGraph::from_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) {
  return JoinGraphBuilder{}(lqp);  // NOLINT - doesn't like {} followed by ()
}

JoinGraph::JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>> vertices,
                     std::vector<LQPOutputRelation> output_relations, std::vector<std::shared_ptr<JoinEdge>> edges)
    : vertices(std::move(vertices)), output_relations(std::move(output_relations)), edges(std::move(edges)) {


}

std::shared_ptr<JoinGraph> JoinGraph::bfs_order() const {
  JoinVertexSet visited{vertices.size()};
  std::vector<size_t> new_to_old_order;
  std::queue<size_t> vertex_queue;
  vertex_queue.emplace(0);

  while(!vertex_queue.empty()) {
    const auto current_vertex = vertex_queue.front();
    vertex_queue.pop();

    if (visited.test(current_vertex)) continue;
    visited.set(current_vertex);

    new_to_old_order.emplace_back(current_vertex);

    for (const auto& edge : edges) {
      auto vertex_set = edge->vertex_set;
      if (!vertex_set.test(current_vertex)) continue;

      auto adjacent = vertex_set.find_first();
      do {
        if (adjacent != current_vertex) vertex_queue.push(adjacent);
      } while ((adjacent = vertex_set.find_next(adjacent)) != JoinVertexSet::npos);
    }
  }

  Assert(new_to_old_order.size() == vertices.size(), "");

  std::vector<size_t> old_to_new(vertices.size());

  for (auto new_idx = size_t{0}; new_idx < vertices.size(); ++new_idx) {
    old_to_new[new_to_old_order[new_idx]] = new_idx;
  }

  std::vector<std::shared_ptr<AbstractLQPNode>> bfs_vertices;
  for (const auto& old_idx : new_to_old_order) {
    bfs_vertices.emplace_back(vertices[old_idx]);
  }

  std::vector<std::shared_ptr<JoinEdge>> bfs_edges;
  for (const auto& old_edge : edges) {
    auto new_vertex_set = JoinVertexSet{vertices.size()};

    auto adjacent = old_edge->vertex_set.find_first();
    do {
      new_vertex_set.set(old_to_new[adjacent]);
    } while ((adjacent = old_edge->vertex_set.find_next(adjacent)) != JoinVertexSet::npos);

    const auto edge = std::make_shared<JoinEdge>(new_vertex_set, old_edge->predicates);
    bfs_edges.emplace_back(edge);
  }

  return std::make_shared<JoinGraph>(bfs_vertices, output_relations, bfs_edges);
}

std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> JoinGraph::find_predicates(
    const JoinVertexSet& vertex_set) const {
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates;

  for (const auto& edge : edges) {
    if (edge->vertex_set != vertex_set) continue;

    for (const auto& predicate : edge->predicates) {
      predicates.emplace_back(predicate);
    }
  }

  return predicates;
}

std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> JoinGraph::find_predicates(
    const JoinVertexSet& vertex_set_a, const JoinVertexSet& vertex_set_b) const {
  DebugAssert((vertex_set_a & vertex_set_b).none(), "Vertex sets are not distinct");

  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates;

  for (const auto& edge : edges) {
    if ((edge->vertex_set & vertex_set_a).none() || (edge->vertex_set & vertex_set_b).none()) continue;
    if (!edge->vertex_set.is_subset_of(vertex_set_a | vertex_set_b)) continue;

    for (const auto& predicate : edge->predicates) {
      predicates.emplace_back(predicate);
    }
  }

  return predicates;
}

std::shared_ptr<JoinEdge> JoinGraph::find_edge(const JoinVertexSet& vertex_set) const {
  const auto iter =
      std::find_if(edges.begin(), edges.end(), [&](const auto& edge) { return edge->vertex_set == vertex_set; });
  return iter == edges.end() ? nullptr : *iter;
}

void JoinGraph::print(std::ostream& stream) const {
  stream << "==== Vertices ====" << std::endl;
  if (vertices.empty()) {
    stream << "<none>" << std::endl;
  } else {
    for (const auto& vertex : vertices) {
      stream << vertex->description() << std::endl;
    }
  }
  stream << "===== Edges ======" << std::endl;
  if (edges.empty()) {
    stream << "<none>" << std::endl;
  } else {
    for (const auto& edge : edges) {
      edge->print(stream);
    }
  }
  std::cout << std::endl;
}

}  // namespace opossum
