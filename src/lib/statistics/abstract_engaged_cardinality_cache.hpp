#pragma once

#include <optional>

#include "cardinality_cache.hpp"
#include "sql/lru_cache.hpp"

namespace opossum {

struct CardinalityCacheVisitor {
  virtual void visit(const BaseJoinGraph& key, const std::shared_ptr<CardinalityCacheEntry>& value) const = 0;
};

class AbstractEngagedCardinalityCache {
 public:
  using KeyValuePair = std::pair<BaseJoinGraph, std::shared_ptr<CardinalityCacheEntry>>;

  virtual ~AbstractEngagedCardinalityCache() = default;

  virtual std::shared_ptr<CardinalityCacheEntry> get(const BaseJoinGraph &join_graph) = 0;
  virtual std::optional<KeyValuePair> set(const BaseJoinGraph &join_graph, const std::shared_ptr<CardinalityCacheEntry>& entry) = 0;
  virtual void visit(const CardinalityCacheVisitor &visitor) const = 0;
  virtual void clear() = 0;
  virtual size_t size() const = 0;
};


}  // namespace opossum
