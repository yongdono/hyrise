#pragma once

#include <set>
#include <map>

#include "abstract_engaged_cardinality_cache.hpp"

namespace opossum {

class CardinalityCacheLAG : public AbstractEngagedCardinalityCache {
 public:
  explicit CardinalityCacheLAG(const size_t capacity);

  std::shared_ptr<CardinalityCacheEntry> get(const BaseJoinGraph &join_graph) override;
  std::optional<KeyValuePair> set(const BaseJoinGraph &join_graph, const std::shared_ptr<CardinalityCacheEntry>& entry) override;
  void visit(const CardinalityCacheVisitor &visitor) const override;
  void clear() override;
  size_t size() const override;

 private:
  struct LAGCmp {
    bool operator()(const KeyValuePair& lhs, const KeyValuePair& rhs) const;
  };

  const size_t _capacity;
  std::set<KeyValuePair, LAGCmp> _lag;
  std::unordered_map<BaseJoinGraph, std::shared_ptr<CardinalityCacheEntry>> _map;
};


}  // namespace opossum
