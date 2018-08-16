#pragma once

#include "base_cardinality_cache.hpp"
#include "sql/lru_cache.hpp"

namespace opossum {

class CardinalityCacheLRU : public BaseCardinalityCache {
 public:
  using BaseCardinalityCache::Entry;

  std::shared_ptr<Entry> get_entry(const BaseJoinGraph& join_graph) override;

 private:
  LRUCache<BaseJoinGraph, Cardinality> _cache;
};


}  // namespace opossum
