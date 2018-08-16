#include "base_cardinality_cache.hpp"

#include <algorithm>
#include <chrono>
#include <experimental/filesystem>
#include <iostream>
#include <fstream>
#include <thread>

#include "boost/interprocess/sync/file_lock.hpp"
#include "boost/interprocess/sync/sharable_lock.hpp"
#include "boost/interprocess/sync/scoped_lock.hpp"

#include "json.hpp"

#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "cardinality_cache_uncapped.hpp"

namespace opossum {

std::optional<Cardinality> BaseCardinalityCache::get(const BaseJoinGraph& join_graph) {
  const auto entry = get_entry(join_graph);

  if (_log) (*_log) << "BaseCardinalityCache [" << (entry->request_count == 0 ? "I" : "S") << "]";

  ++entry->request_count;

  std::optional<Cardinality> result;

  if (entry->cardinality) {
    if (_log) (*_log) << "[HIT ]: ";
    ++_hit_count;
    result = *entry->cardinality;
  } else {
    if (_log) (*_log) << "[MISS]: ";
    ++_miss_count;
  }

  if (_log) (*_log) << join_graph.description();

  if (entry->cardinality) {
    if (_log) (*_log) << ": " << *entry->cardinality;
  }

  if (_log) (*_log) << std::endl;

  return result;
}

void BaseCardinalityCache::put(const BaseJoinGraph& join_graph, const Cardinality cardinality) {
  const auto entry = get_entry(join_graph);

  if (_log && !entry->cardinality) {
    (*_log) << "BaseCardinalityCache [" << (entry->request_count == 0 ? "I" : "S") << "][PUT ]: " << join_graph.description() << ": " << cardinality << std::endl;
  }

  entry->cardinality = cardinality;
}

std::optional<std::chrono::seconds> BaseCardinalityCache::get_timeout(const BaseJoinGraph& join_graph) {
  return get_entry(join_graph)->timeout;
}

void BaseCardinalityCache::set_timeout(const BaseJoinGraph& join_graph, const std::optional<std::chrono::seconds>& timeout) {
  get_entry(join_graph)->timeout = timeout;
}

size_t BaseCardinalityCache::cache_hit_count() const {
  return _hit_count;
}

size_t BaseCardinalityCache::cache_miss_count() const {
  return _miss_count;
}

size_t BaseCardinalityCache::distinct_hit_count() const {
  auto count = size_t{0};

  visit_entries([&](const auto& key, const auto& value) {
    if (value->second.cardinality && value->second.request_count > 0) ++count;
  });

  return count;
}

size_t BaseCardinalityCache::distinct_miss_count() const {
  auto count = size_t{0};

  visit_entries([&](const auto& key, const auto& value) {
    if (!value->second.cardinality && value->second.request_count > 0) ++count;
  });

  return count;
}

void BaseCardinalityCache::reset_distinct_hit_miss_counts() {
  visit_entries([&](const auto& key, const auto& value) {
    value->request_count = 0;
  });
}

void BaseCardinalityCache::clear() {
  on_clear();
  _hit_count = 0;
  _miss_count = 0;
  _log.reset();
}

void BaseCardinalityCache::set_log(const std::shared_ptr<std::ostream>& log) {
  _log = log;
}

void BaseCardinalityCache::print(std::ostream& stream) const {
  stream << "-------------------- ENGAGED ENTRIES ------------------------" << std::endl;
  visit_entries([&](const auto& key, const auto& value) {
    if (value->cardinality) {
      stream << key->description() << ": " << *value->cardinality << std::endl;
    }
  });
  stream << std::endl;
  stream << "------------------- DISENGAGED ENTRIES ------------------------" << std::endl;
  visit_entries([&](const auto& key, const auto& value) {
    if (!value->cardinality) {
      stream << key->description() << ": " << "-" << std::endl;
    }
  });
}

BaseJoinGraph BaseCardinalityCache::_normalize(const BaseJoinGraph& join_graph) {
  auto normalized_join_graph = join_graph;

  for (auto& predicate : normalized_join_graph.predicates) {
    predicate = _normalize(predicate);
  }

  return normalized_join_graph;
}

void BaseCardinalityCache::load(const std::string& path) {
  std::ifstream istream{path};
  Assert(istream.is_open(), "Couldn't open persistent BaseCardinalityCache");

  boost::interprocess::file_lock file_lock(path.c_str());
  boost::interprocess::scoped_lock<boost::interprocess::file_lock> lock(file_lock);

  load(istream);
}

void BaseCardinalityCache::load(std::istream& stream) {
  stream.seekg(0, std::ios_base::end);
  if (stream.tellg() == 0) return;
  stream.seekg(0, std::ios_base::beg);

  nlohmann::json json;
  stream >> json;

  from_json(json);
}

void BaseCardinalityCache::from_json(const nlohmann::json& json) {
  for (const auto& pair : json) {
    const auto key = BaseJoinGraph::from_json(pair["key"]);
    auto entry = get_entry(key);

    if (pair.count("timeout")) entry->timeout = std::chrono::seconds{pair["timeout"].get<int>()};
    if (pair.count("value")) entry->cardinality = pair["value"].get<Cardinality>();
  }
}

void BaseCardinalityCache::store(const std::string& path) const {
  std::ofstream stream{path};
  stream << to_json();
}

void BaseCardinalityCache::update(const std::string& path) const {
  std::ifstream istream;
  std::ofstream ostream;

  // "Touch" the file
  if (!std::experimental::filesystem::exists(path)) {
    std::cout << "Touch cardinality estimation cache '" << path << "'" << std::endl;
    std::ofstream{path, std::ios_base::app};
  }

  {
    boost::interprocess::file_lock file_lock(path.c_str());
    boost::interprocess::scoped_lock<boost::interprocess::file_lock> lock(file_lock);

    istream.open(path);

    CardinalityCacheUncapped persistent_cache;
    persistent_cache.load(istream);
    visit_entries([&](const auto& key, const auto& value) {
      persistent_cache->get_entry(key) = value;
    });

    ostream.open(path);
    ostream << persistent_cache->to_json();

    ostream.flush(); // Make sure write-to-disk happened before unlocking
  }
}

nlohmann::json BaseCardinalityCache::to_json() const {
  nlohmann::json json;

  json = nlohmann::json::array();

  visit_entries([&](const auto& key, const auto& value) {
    auto entry_json = nlohmann::json();

    entry_json["key"] = key.to_json();
    if (value->cardinality) entry_json["value"] = *value->cardinality;
    if (value->timeout) entry_json["timeout"] = value->timeout->count();

    json.push_back(entry_json);
  });

  return json;
}

std::shared_ptr<const AbstractJoinPlanPredicate> BaseCardinalityCache::_normalize(const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate) {
  if (predicate->type() == JoinPlanPredicateType::Atomic) {
    const auto atomic_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(predicate);
    if (is_lqp_column_reference(atomic_predicate->right_operand)) {
      const auto right_operand_column_reference = boost::get<LQPColumnReference>(atomic_predicate->right_operand);

      if (std::hash<LQPColumnReference>{}(right_operand_column_reference) < std::hash<LQPColumnReference>{}(atomic_predicate->left_operand)) {
        if (atomic_predicate->predicate_condition != PredicateCondition::Like) {
          const auto flipped_predicate_condition = flip_predicate_condition(atomic_predicate->predicate_condition);
          return std::make_shared<JoinPlanAtomicPredicate>(right_operand_column_reference, flipped_predicate_condition, atomic_predicate->left_operand);
        }
      }
    }
  } else {
    const auto logical_predicate = std::static_pointer_cast<const JoinPlanLogicalPredicate>(predicate);

    auto normalized_left = _normalize(logical_predicate->left_operand);
    auto normalized_right = _normalize(logical_predicate->right_operand);

    if (normalized_right->hash() < normalized_left->hash()) {
      std::swap(normalized_left, normalized_right);
    }

    return std::make_shared<JoinPlanLogicalPredicate>(normalized_left,
                                                      logical_predicate->logical_operator,
                                                      normalized_right);

  }

  return predicate;
}

size_t BaseCardinalityCache::memory_consumption() const {
  auto memory = size() * 4;  // Entries = float

  visit_entries([&](const auto& key, const auto& value) {
    const auto& join_graph = key;

    memory += join_graph.vertices.size() * 8; // Pointers to the vertices
    memory += join_graph.predicates.size() * 8; // Pointers to the predicates

    for (const auto& vertex : join_graph.vertices) {
      const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(vertex);
      Assert(stored_table_node, "Expected StoredTableNode");
      memory += stored_table_node->table_name().size();
    }

    for (const auto& predicate : join_graph.predicates) {
      memory += predicate->memory_consumption();
    }
  });

  return memory;
}

size_t BaseCardinalityCache::memory_consumption_alt() const {
  auto memory = size() * 4;  // Entries = float

  visit_entries([&](const auto& key, const auto& value) {
    const auto& join_graph = key;

    memory += 8;

    for (const auto& predicate : join_graph.predicates) {
      std::stringstream stream;
      predicate->print(stream);
      memory += stream.str().size();
    }
  });

  return memory;
}

}  // namespace opossum