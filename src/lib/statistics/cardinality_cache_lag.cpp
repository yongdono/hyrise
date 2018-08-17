#include "cardinality_cache_lag.hpp"

namespace opossum {

CardinalityCacheLAG::CardinalityCacheLAG(const size_t capacity):
_cache(capacity) {}

std::shared_ptr<CardinalityCacheLAG::Entry> CardinalityCacheLAG::get_engaged_entry(const BaseJoinGraph &join_graph) {

}

void CardinalityCacheLAG::set_engaged_entry(const BaseJoinGraph &join_graph, const std::shared_ptr<Entry>& entry) {
  const auto disengaged = _cache.set(_normalize(join_graph), entry);
  if (disengaged) {
    _disengage_entry(disengaged->first, disengaged->second);
  }
}

void CardinalityCacheLAG::visit_engaged_entries_impl(const CardinalityCacheVisitor &visitor) const {
  for (const auto& pair : _cache._list) {
    visitor.visit(pair.first, pair.second);
  }
}

void CardinalityCacheLAG::clear_engaged_entries() {
  _cache.clear();
}

size_t CardinalityCacheLAG::engaged_size() const {
  return _cache.size();
}

}  // namespace opossum