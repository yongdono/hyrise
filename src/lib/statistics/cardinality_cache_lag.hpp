//#pragma once
//
//#include <set>
//#include <map>
//
//#include "cardinality_cache.hpp"
//
//namespace opossum {
//
//class CardinalityCacheLAG : public CardinalityCache {
// public:
//  explicit CardinalityCacheLAG(const size_t capacity);
//
//  std::shared_ptr<CardinalityCacheEntry> get_engaged_entry(const BaseJoinGraph &join_graph) override;
//  void set_engaged_entry(const BaseJoinGraph &join_graph, const std::shared_ptr<CardinalityCacheEntry>& entry) override;
//  void visit_engaged_entries_impl(const CardinalityCacheVisitor &visitor) const override;
//  void clear_engaged_entries() override;
//  size_t engaged_size() const override;
//
// private:
//  std::set<std::pair<BaseJoinGraph, std::shared_ptr<CardinalityCacheEntry>>> _lag;
//  std::unordered_map<BaseJoinGraph, std::shared_ptr<CardinalityCacheEntry>> _map;
//};
//
//
//}  // namespace opossum
