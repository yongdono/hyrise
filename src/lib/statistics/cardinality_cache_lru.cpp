#include "cardinality_cache_lru.hpp"

namespace opossum {

CardinalityCacheLRU::CardinalityCacheLRU(const size_t capacity):
  _cache(capacity) {}

std::shared_ptr<CardinalityCacheEntry> CardinalityCacheLRU::get(const BaseJoinGraph &join_graph) {
  const auto normalized_join_graph = join_graph.normalized();
  if (!_cache.has(normalized_join_graph)) return nullptr;
  return _cache.get(normalized_join_graph);
}

std::optional<CardinalityCacheLRU::KeyValuePair> CardinalityCacheLRU::set(const BaseJoinGraph &join_graph, const std::shared_ptr<CardinalityCacheEntry>& entry) {
  return _cache.set(join_graph.normalized(), entry);
}

void CardinalityCacheLRU::visit(const CardinalityCacheVisitor &visitor) const {
  for (const auto& pair : _cache._list) {
    visitor.visit(pair.first, pair.second);
  }
}

void CardinalityCacheLRU::clear() {
  _cache.clear();
}

size_t CardinalityCacheLRU::size() const {
  return _cache.size();
}

}  // namespace opossum