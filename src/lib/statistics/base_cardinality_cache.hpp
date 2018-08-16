#pragma once

#include <chrono>
#include <list>
#include <optional>
#include <unordered_map>
#include <random>

#include "abstract_cardinality_estimator.hpp"
#include "optimizer/join_ordering/base_join_graph.hpp"

namespace opossum {

class BaseCardinalityCache {
 public:
  // Cardinality that is assumed if a join graph timed out
  static constexpr auto TIMEOUT_CARDINALITY = 1e+12;

  struct Entry {
    std::optional<std::chrono::seconds> timeout;
    std::optional<Cardinality> cardinality;
    size_t request_count{0};
  };

  struct CardinalityCacheVisitor {
    virtual void visit(const BaseJoinGraph& key, const std::shared_ptr<Entry>& value) const = 0;
  };

  virtual ~BaseCardinalityCache() = default;

  std::optional<Cardinality> get(const BaseJoinGraph& join_graph) ;
  void put(const BaseJoinGraph& join_graph, const Cardinality cardinality);

  std::optional<std::chrono::seconds> get_timeout(const BaseJoinGraph& join_graph);
  void set_timeout(const BaseJoinGraph& join_graph, const std::optional<std::chrono::seconds>& timeout);

  std::shared_ptr<Entry> get_entry(const BaseJoinGraph &join_graph);
  std::shared_ptr<Entry> get_or_create_disengaged_entry(const BaseJoinGraph &join_graph);
  std::shared_ptr<Entry> get_or_create_engaged_entry(const BaseJoinGraph &join_graph);

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

      void visit(const BaseJoinGraph& key, const std::shared_ptr<Entry>& value) const override {
        functor(key, value);
      }
    };
    visit_engaged_entries_impl(Visitor{functor});

    for (const auto& pair : _disengaged_cache) {
      functor(pair.first, pair.second);
    }
  }

  virtual std::shared_ptr<Entry> get_engaged_entry(const BaseJoinGraph &join_graph) = 0;
  virtual void set_engaged_entry(const BaseJoinGraph &join_graph, const std::shared_ptr<Entry>& entry) = 0;
  virtual void visit_engaged_entries_impl(const CardinalityCacheVisitor &visitor) const = 0;
  virtual void clear_engaged_entries() = 0;
  virtual size_t engaged_size() const = 0;

  size_t size() const;

 protected:
  static BaseJoinGraph _normalize(const BaseJoinGraph& join_graph);
  static std::shared_ptr<const AbstractJoinPlanPredicate> _normalize(const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate);

  void _disengage_entry(const BaseJoinGraph &join_graph, const std::shared_ptr<Entry>& entry);

  std::unordered_map<BaseJoinGraph, std::shared_ptr<BaseCardinalityCache::Entry>> _disengaged_cache;

  std::shared_ptr<std::ostream> _log;
  size_t _hit_count{0};
  size_t _miss_count{0};
};

}  // namespace opossum
