#pragma once

#include "abstract_operator_callback.hpp"

namespace opossum {

class CardinalityCache;

class CardinalityCachingCallback : public AbstractOperatorCallback {
 public:
  CardinalityCachingCallback(const std::shared_ptr<CardinalityCache>& cardinality_estimation_cache);

  void operator()(const std::shared_ptr<AbstractOperator>& op) override;

 private:
  std::shared_ptr<CardinalityCache> _cardinality_estimation_cache;
};

}