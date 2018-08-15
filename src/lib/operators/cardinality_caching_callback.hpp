#pragma once

#include "abstract_operator_callback.hpp"

namespace opossum {

class BaseCardinalityCache;

class CardinalityCachingCallback : public AbstractOperatorCallback {
 public:
  CardinalityCachingCallback(const std::shared_ptr<BaseCardinalityCache>& cardinality_estimation_cache);

  void operator()(const std::shared_ptr<AbstractOperator>& op) override;

 private:
  std::shared_ptr<BaseCardinalityCache> _cardinality_estimation_cache;
};

}