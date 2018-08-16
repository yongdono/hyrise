#include "cardinality_cache_random.hpp"

namespace opossum {

CardinalityCacheRandom::CardinalityCacheRandom(const size_t capacity):
  _cache(capacity) {}

std::shared_ptr<CardinalityCacheRandom::Entry> CardinalityCacheRandom::get_engaged_entry(const BaseJoinGraph &join_graph) {
  const auto normalized_join_graph = _normalize(join_graph);
  if (!_cache.has(normalized_join_graph)) return nullptr;
  return _cache.get(normalized_join_graph);
}

void CardinalityCacheRandom::set_engaged_entry(const BaseJoinGraph &join_graph, const std::shared_ptr<Entry>& entry) {
  const auto disengaged = _cache.set(_normalize(join_graph), entry);
  if (disengaged) {
    _disengage_entry(disengaged->first, disengaged->second);
  }
}

void CardinalityCacheRandom::visit_engaged_entries_impl(const CardinalityCacheVisitor &visitor) const {
  for (const auto& pair : _cache._list) {
    visitor.visit(pair.first, pair.second);
  }
}

void CardinalityCacheRandom::clear_engaged_entries() {
  _cache.clear();
}

size_t CardinalityCacheRandom::engaged_size() const {
  return _cache.size();
}

}  // namespace opossum
