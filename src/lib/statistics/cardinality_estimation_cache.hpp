#pragma once

#include <chrono>
#include <list>
#include <optional>
#include <unordered_map>
#include <random>

#include "abstract_cardinality_estimator.hpp"
#include "optimizer/join_ordering/base_join_graph.hpp"

namespace opossum {

enum class CacheEvictionStrategy {
  None, Random, LRU, LAG /* Least accuracy gain */
};

class CardinalityEstimationCache final {
 public:
  // Cardinality that is assumed if a join graph timed out
  static constexpr auto TIMEOUT_CARDINALITY = 1e+12;

  struct Entry {
    std::optional<std::chrono::seconds> timeout;
    std::optional<Cardinality> cardinality;
    size_t request_count{0};
  };

  CardinalityEstimationCache(const CacheEvictionStrategy cache_eviction_strategy, const size_t eviction_threshold);

  std::optional<Cardinality> get(const BaseJoinGraph& join_graph) ;
  void put(const BaseJoinGraph& join_graph, const Cardinality cardinality);

  std::optional<std::chrono::seconds> get_timeout(const BaseJoinGraph& join_graph);
  void set_timeout(const BaseJoinGraph& join_graph, const std::optional<std::chrono::seconds>& timeout);

  std::shared_ptr<Entry> get_entry(const BaseJoinGraph& join_graph);

  size_t cache_hit_count() const;
  size_t cache_miss_count() const;

  size_t size() const;

  size_t distinct_request_count() const;
  size_t distinct_hit_count() const;
  size_t distinct_miss_count() const;
  void reset_distinct_hit_miss_counts();

  void clear();

  void set_log(const std::shared_ptr<std::ostream>& log);

  void print(std::ostream& stream) const;

  static std::shared_ptr<CardinalityEstimationCache> load(const std::string& path);
  static std::shared_ptr<CardinalityEstimationCache> load(std::istream& stream);
  static std::shared_ptr<CardinalityEstimationCache> from_json(const nlohmann::json& json);

  void store(const std::string& path) const;
  void update(const std::string& path) const;
  nlohmann::json to_json() const;

  size_t memory_consumption() const;
  size_t memory_consumption_alt() const;

  const CacheEvictionStrategy cache_eviction_strategy;
  const size_t eviction_threshold{0};

 private:
  static BaseJoinGraph _normalize(const BaseJoinGraph& join_graph);
  static std::shared_ptr<const AbstractJoinPlanPredicate> _normalize(const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate);

  void _add_eviction_entry(const std::shared_ptr<Entry>& entry);
  void _evict_one();
  void _evict_random();
  void _evict_lru();
  void _evict_lag();

  std::unordered_map<BaseJoinGraph, std::shared_ptr<Entry>> _cache;

  std::shared_ptr<std::ostream> _log;
  size_t _hit_count{0};
  size_t _miss_count{0};

  /**
   * Random eviction
   */
  std::mt19937 _random_generator;
  /**
   * LRU eviction
   */
  std::list<BaseJoinGraph> _lru_queue;

  /**
   * LAG eviction
   */
  struct LAGEntry {
    BaseJoinGraph join_graph;
    Cardinality gain;
  };

  std::list<LAGEntry> _lag_queue;
};

}  // namespace opossum
