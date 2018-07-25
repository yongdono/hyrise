
#include <chrono>
#include <iostream>
#include <fstream>
#include <experimental/filesystem>

#include <json.hpp>

#include "types.hpp"

#include "logical_query_plan/stored_table_node.hpp"
#include "import_export/csv_parser.hpp"
#include "cost_model/cost_model_naive.hpp"
#include "cost_model/cost_model_linear.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/print.hpp"
#include "operators/export_binary.hpp"
#include "operators/import_binary.hpp"
#include "operators/import_csv.hpp"
#include "operators/limit.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/chunk.hpp"
#include "storage/base_column.hpp"
#include "storage/table.hpp"
#include "storage/storage_manager.hpp"
#include "sql/sql.hpp"
#include "utils/timer.hpp"
#include "utils/format_duration.hpp"
#include "statistics/statistics_import_export.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "resolve_type.hpp"
#include "utils/format_duration.hpp"
#include "utils/load_table.hpp"
#include "utils/timer.hpp"
#include "statistics/cardinality_estimator_column_statistics.hpp"
#include "optimizer/join_ordering/enumerate_ccp.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/join_edge.hpp"

using namespace opossum;  // NOLINT
using namespace std::string_literals;  // NOLINT

class CardinalityEstimatorDummy : public AbstractCardinalityEstimator {
 public:
  std::optional<Cardinality> estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& relations,
                                      const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) const override {return 0.0f;};
};

struct Sample {
  size_t relation_count;
  std::chrono::microseconds enumeration_time;
  std::chrono::microseconds runtime;
};

std::shared_ptr<JoinGraph> make_chain(const size_t relation_count) {
  std::vector<std::shared_ptr<AbstractLQPNode>> vertices;
  std::vector<LQPOutputRelation> output_relations;
  std::vector<std::shared_ptr<JoinEdge>> edges;

  for (auto relation_idx = size_t{0}; relation_idx < relation_count; ++relation_idx) {
    vertices.emplace_back(std::make_shared<StoredTableNode>("t"));
  }

  for (auto relation_idx = size_t{0}; relation_idx + 1 < relation_count; ++relation_idx) {
    JoinVertexSet vertex_set{relation_count};
    vertex_set.set(relation_idx);
    vertex_set.set(relation_idx + 1);

    const auto left_operand = LQPColumnReference{vertices[relation_idx], ColumnID{0}};
    const auto right_operand = LQPColumnReference{vertices[relation_idx + 1], ColumnID{0}};

    const auto predicate = std::make_shared<const JoinPlanAtomicPredicate>(left_operand, PredicateCondition::Equals, right_operand);

    std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates{predicate};

    const auto edge = std::make_shared<JoinEdge>(vertex_set, predicates);

    edges.emplace_back(edge);
  }

  return std::make_shared<JoinGraph>(vertices, output_relations, edges);
}

std::shared_ptr<JoinGraph> make_cycle(const size_t relation_count) {
  std::vector<std::shared_ptr<AbstractLQPNode>> vertices;
  std::vector<LQPOutputRelation> output_relations;
  std::vector<std::shared_ptr<JoinEdge>> edges;

  for (auto relation_idx = size_t{0}; relation_idx < relation_count; ++relation_idx) {
    vertices.emplace_back(std::make_shared<StoredTableNode>("t"));
  }

  for (auto relation_idx = size_t{0}; relation_idx < relation_count; ++relation_idx) {
    JoinVertexSet vertex_set{relation_count};
    vertex_set.set(relation_idx);
    vertex_set.set((relation_idx + 1) % relation_count);

    const auto left_operand = LQPColumnReference{vertices[relation_idx], ColumnID{0}};
    const auto right_operand = LQPColumnReference{vertices[(relation_idx + 1) % relation_count], ColumnID{0}};

    const auto predicate = std::make_shared<const JoinPlanAtomicPredicate>(left_operand, PredicateCondition::Equals, right_operand);

    std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates{predicate};

    const auto edge = std::make_shared<JoinEdge>(vertex_set, predicates);

    edges.emplace_back(edge);
  }

  return std::make_shared<JoinGraph>(vertices, output_relations, edges);
}

std::shared_ptr<JoinGraph> make_star(const size_t relation_count) {
  std::vector<std::shared_ptr<AbstractLQPNode>> vertices;
  std::vector<LQPOutputRelation> output_relations;
  std::vector<std::shared_ptr<JoinEdge>> edges;

  for (auto relation_idx = size_t{0}; relation_idx < relation_count; ++relation_idx) {
    vertices.emplace_back(std::make_shared<StoredTableNode>("t"));
  }

  for (auto relation_idx_a = size_t{1}; relation_idx_a < relation_count; ++relation_idx_a) {
    JoinVertexSet vertex_set{relation_count};
    vertex_set.set(0);
    vertex_set.set(relation_idx_a);

    const auto left_operand = LQPColumnReference{vertices[0], ColumnID{0}};
    const auto right_operand = LQPColumnReference{vertices[relation_idx_a], ColumnID{0}};

    const auto predicate = std::make_shared<const JoinPlanAtomicPredicate>(left_operand, PredicateCondition::Equals,
                                                                           right_operand);

    std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates{predicate};

    const auto edge = std::make_shared<JoinEdge>(vertex_set, predicates);

    edges.emplace_back(edge);
  }

  return std::make_shared<JoinGraph>(vertices, output_relations, edges);
}

std::shared_ptr<JoinGraph> make_clique(const size_t relation_count) {
  std::vector<std::shared_ptr<AbstractLQPNode>> vertices;
  std::vector<LQPOutputRelation> output_relations;
  std::vector<std::shared_ptr<JoinEdge>> edges;

  for (auto relation_idx = size_t{0}; relation_idx < relation_count; ++relation_idx) {
    vertices.emplace_back(std::make_shared<StoredTableNode>("t"));
  }

  for (auto relation_idx_a = size_t{0}; relation_idx_a < relation_count; ++relation_idx_a) {
    for (auto relation_idx_b = size_t{relation_idx_a + 1}; relation_idx_b < relation_count; ++relation_idx_b) {
      JoinVertexSet vertex_set{relation_count};
      vertex_set.set(relation_idx_a);
      vertex_set.set(relation_idx_b);

      const auto left_operand = LQPColumnReference{vertices[relation_idx_a], ColumnID{0}};
      const auto right_operand = LQPColumnReference{vertices[relation_idx_b], ColumnID{0}};

      const auto predicate = std::make_shared<const JoinPlanAtomicPredicate>(left_operand, PredicateCondition::Equals,
                                                                             right_operand);

      std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates{predicate};

      const auto edge = std::make_shared<JoinEdge>(vertex_set, predicates);

      edges.emplace_back(edge);
    }
  }

  return std::make_shared<JoinGraph>(vertices, output_relations, edges);
}

template<typename Generator>
void run(const std::string& name, Generator generator, const size_t max_relation_count) {
  const auto cost_model = std::make_shared<CostModelNaive>();
  const auto cardinality_estimator = std::make_shared<CardinalityEstimatorDummy>();

  std::vector<Sample> samples;

  for (auto relation_count = size_t{2}; relation_count <= max_relation_count; ++relation_count) {
    const auto join_graph = generator(relation_count);

    Sample sample;
    sample.relation_count = relation_count;

    /**
     * Measure enumeration
     */
    {
      const auto edges = enumerate_ccp_edges_from_join_graph(*join_graph);

      Timer timer;

      auto iteration_count = size_t{0};

      while (timer.elapsed() < std::chrono::microseconds{1000000}) {
        EnumerateCcp{relation_count, edges}();
        iteration_count++;
      }

      sample.enumeration_time = timer.lap() / iteration_count;

    }

    /**
     * Measure runtime
     */
//    {
//      Timer timer;
//      DpCcp{cost_model, cardinality_estimator}(join_graph);
//
//      sample.runtime = timer.lap();
//    }
    sample.runtime = std::chrono::microseconds{0};

    samples.emplace_back(sample);

    std::cout << name << ": " << relation_count << std::endl;
  }

  std::ofstream file{name + ".csv"};
  file << "RelationCount,EnumerationTime,Runtime" << std::endl;
  for (const auto& sample : samples) {
    file << sample.relation_count << "," << sample.enumeration_time.count() << "," << sample.runtime.count() << std::endl;
  }
}

int main(int argc, char ** argv) {
  StorageManager::get().add_table("t", load_table("src/test/tables/int_float.tbl"));

  run("chain", make_chain, 20);
  run("clique", make_clique, 16);
  run("star", make_star, 20);
  run("cycle", make_cycle, 20);

  return 0;
}
