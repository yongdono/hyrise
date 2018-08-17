#pragma once

#include <chrono>
#include <list>
#include <optional>
#include <unordered_map>
#include <random>

#include "abstract_cardinality_estimator.hpp"
#include "cardinality_cache_entry.hpp"
#include "optimizer/join_ordering/base_join_graph.hpp"
#include "abstract_engaged_cardinality_cache.hpp"

namespace opossum {

class CardinalityCache {
 public:
  explicit CardinalityCache(const std::shared_ptr<AbstractEngagedCardinalityCache>& engaged_cache);
  virtual ~CardinalityCache() = default;

  std::optional<Cardinality> get_cardinality(const BaseJoinGraph& join_graph) ;
  void set_cardinality(const BaseJoinGraph& join_graph, const Cardinality cardinality, const Cardinality cardinality_estimation = 0.0f);

  std::optional<std::chrono::seconds> get_timeout(const BaseJoinGraph& join_graph);
  void set_timeout(const BaseJoinGraph& join_graph, const std::optional<std::chrono::seconds>& timeout);

  size_t cache_hit_count() const;
  size_t cache_miss_count() const;

  size_t distinct_hit_count() const;
  size_t distinct_miss_count() const;
  void reset_distinct_hit_miss_counts();

  void clear();

  void set_log(const std::shared_ptr<std::ostream>& log);

  void print(std::ostream& stream) const;

  void load(const std::string& path);
  void load(std::istream& stream);

  void from_json(const nlohmann::json& json);

  void store(const std::string& path) const;
  void update(const std::string& path) const;
  nlohmann::json to_json() const;

  size_t memory_consumption() const;
  size_t memory_consumption_alt() const;

  template<typename Functor>
  void visit_entries(Functor functor) const {
    struct Visitor : CardinalityCacheVisitor {
      Functor functor;

      Visitor(const Functor functor): functor(functor) {}

      void visit(const BaseJoinGraph& key, const std::shared_ptr<CardinalityCacheEntry>& value) const override {
        functor(key, value);
      }
    };
    _engaged_cache->visit(Visitor{functor});

    for (const auto& pair : _disengaged_cache) {
      functor(pair.first, pair.second);
    }
  }

  size_t size() const;

 protected:
  std::shared_ptr<CardinalityCacheEntry> _get_entry(const BaseJoinGraph &join_graph);

  std::shared_ptr<AbstractEngagedCardinalityCache> _engaged_cache;
  std::unordered_map<BaseJoinGraph, std::shared_ptr<CardinalityCacheEntry>> _disengaged_cache;

  std::shared_ptr<std::ostream> _log;
  size_t _hit_count{0};
  size_t _miss_count{0};
};

}  // namespace opossum
