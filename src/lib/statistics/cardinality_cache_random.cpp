#include "cardinality_cache_random.hpp"

namespace opossum {

CardinalityCacheRandom::CardinalityCacheRandom(const size_t capacity):
  _cache(capacity) {}

std::shared_ptr<CardinalityCacheEntry> CardinalityCacheRandom::get(const BaseJoinGraph &join_graph) {
  const auto normalized_join_graph = join_graph.normalized();
  if (!_cache.has(normalized_join_graph)) return nullptr;
  return _cache.get(normalized_join_graph);
}

std::optional<CardinalityCacheRandom::KeyValuePair> CardinalityCacheRandom::set(const BaseJoinGraph &join_graph, const std::shared_ptr<CardinalityCacheEntry>& entry) {
  return _cache.set(join_graph.normalized(), entry);
}

void CardinalityCacheRandom::visit(const CardinalityCacheVisitor &visitor) const {
  for (const auto& pair : _cache._list) {
    visitor.visit(pair.first, pair.second);
  }
}

void CardinalityCacheRandom::clear() {
  _cache.clear();
}

size_t CardinalityCacheRandom::size() const {
  return _cache.size();
}

}  // namespace opossum
