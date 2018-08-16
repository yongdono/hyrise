#include "cardinality_cache_uncapped.hpp"

namespace opossum {

std::shared_ptr<CardinalityCacheUncapped::Entry> CardinalityCacheUncapped::get_entry(const BaseJoinGraph& join_graph) {
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

void CardinalityCacheUncapped::visit_entries_impl(const CardinalityCacheVisitor& visitor) {
  for (const auto& pair : _cache) {
    visitor.visit(pair.first, pair.second);
  }
}

void CardinalityCacheUncapped::on_clear() {
  _cache.clear();
}

size_t CardinalityCacheUncapped::size() const {
  return _cache.size();   
}

}  // namespace opossum
