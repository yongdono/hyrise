#pragma once

#include <map>
#include <unordered_map>
#include <memory>
#include <set>

#include "abstract_dp_subplan_cache.hpp"
#include "boost/dynamic_bitset.hpp"
#include "boost/functional/hash.hpp"

namespace std {
template<typename B, typename A>
struct hash<boost::dynamic_bitset<B, A>> {
  std::size_t operator()(const boost::dynamic_bitset<B, A> &v) const {
    return boost::hash_value(v.to_ulong());
  }
};
}

namespace opossum {

class AbstractJoinPlanNode;


class DpSubplanCacheBest : public AbstractDpSubplanCache {
 public:
  void clear() override;

  std::optional<JoinPlanNode> get_best_plan(const boost::dynamic_bitset<>& vertex_set) const override;
  void cache_plan(const boost::dynamic_bitset<>& vertex_set,
                  const JoinPlanNode& plan) override;

 private:
  mutable std::unordered_map<boost::dynamic_bitset<>, JoinPlanNode> _plan_by_vertex_set;
};

}  // namespace opossum
