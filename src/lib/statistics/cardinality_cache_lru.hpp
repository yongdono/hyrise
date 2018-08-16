#pragma once

#include "base_cardinality_cache.hpp"
#include "sql/lru_cache.hpp"

namespace opossum {

class CardinalityCacheLRU : public BaseCardinalityCache {
 public:
  using BaseCardinalityCache::Entry;

  explicit CardinalityCacheLRU(const size_t capacity);

  std::shared_ptr<Entry> get_engaged_entry(const BaseJoinGraph &join_graph) override;
  void set_engaged_entry(const BaseJoinGraph &join_graph, const std::shared_ptr<Entry>& entry) override;
  void visit_engaged_entries_impl(const CardinalityCacheVisitor &visitor) const override;
  void clear_engaged_entries() override;
  size_t engaged_size() const override;

 private:
  LRUCache<BaseJoinGraph, std::shared_ptr<Entry>> _cache;
};


}  // namespace opossum
