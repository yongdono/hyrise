#pragma once

#include <map>
#include <memory>
#include <set>

#include "boost/dynamic_bitset.hpp"
#include "abstract_dp_subplan_cache.hpp"

namespace opossum {

class AbstractJoinPlanNode;

class DpSubplanCacheBest : public AbstractDpSubplanCache {
 public:
  std::shared_ptr<const AbstractJoinPlanNode> get_best_plan(const boost::dynamic_bitset<> &vertex_set) const override;
  void cache_plan(const boost::dynamic_bitset<> &vertex_set, const std::shared_ptr<const AbstractJoinPlanNode> &plan) override;

 private:
  mutable std::map<boost::dynamic_bitset<>, std::shared_ptr<const AbstractJoinPlanNode>> _plan_by_vertex_set;
};

}  // namespace opossum