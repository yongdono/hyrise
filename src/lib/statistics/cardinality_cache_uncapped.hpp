#pragma once

#include "base_cardinality_cache.hpp"

namespace opossum {

class CardinalityCacheUncapped : public BaseCardinalityCache {
 public:
  using BaseCardinalityCache::Entry;

  std::shared_ptr<Entry> get_entry(const BaseJoinGraph& join_graph) override;
  void visit_entries_impl(const CardinalityCacheVisitor& visitor) override;
  void on_clear() override;
  size_t size() const override;


 private:
  std::unordered_map<BaseJoinGraph, std::shared_ptr<BaseCardinalityCache::Entry>> _cache;
};


}  // namespace opossum
