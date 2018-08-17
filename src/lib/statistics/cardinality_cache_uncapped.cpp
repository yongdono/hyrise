#include "cardinality_cache_uncapped.hpp"

namespace opossum {

std::shared_ptr<CardinalityCacheEntry> CardinalityCacheUncapped::get(
const BaseJoinGraph &join_graph) {
  auto iter = _cache.find(join_graph.normalized());
  if (iter != _cache.end()) return iter->second;

  return nullptr;
}

std::optional<CardinalityCacheUncapped::KeyValuePair> CardinalityCacheUncapped::set(const BaseJoinGraph &join_graph, const std::shared_ptr<CardinalityCacheEntry>& entry) {
  _cache[join_graph.normalized()] = entry;
  return std::nullopt;
}

void CardinalityCacheUncapped::visit(const CardinalityCacheVisitor &visitor) const {
  for (const auto& pair : _cache) {
    visitor.visit(pair.first, pair.second);
  }
}

void CardinalityCacheUncapped::clear() {
  _cache.clear();
}

size_t CardinalityCacheUncapped::size() const {
  return _cache.size();   
}

}  // namespace opossum
