#include "cardinality_cache_uncapped.hpp"

namespace opossum {

std::shared_ptr<CardinalityCacheUncapped::Entry> CardinalityCacheUncapped::get_engaged_entry(
const BaseJoinGraph &join_graph) {
  auto normalized_join_graph = _normalize(join_graph);

  std::shared_ptr<Entry> entry;

  auto iter = _cache.find(normalized_join_graph);
  if (iter == _cache.end()) {
    entry = std::make_shared<Entry>();
    _cache.emplace(normalized_join_graph, entry);
    return entry;
  } else {
    return iter->second;
  }
}

void CardinalityCacheUncapped::set_engaged_entry(const BaseJoinGraph &join_graph, const std::shared_ptr<Entry>& entry) {
  _cache[_normalize(join_graph)] = entry;
}

void CardinalityCacheUncapped::visit_engaged_entries_impl(const CardinalityCacheVisitor &visitor) const {
  for (const auto& pair : _cache) {
    visitor.visit(pair.first, pair.second);
  }
}

void CardinalityCacheUncapped::clear_engaged_entries() {
  _cache.clear();
}

size_t CardinalityCacheUncapped::engaged_size() const {
  return _cache.size();   
}

}  // namespace opossum
