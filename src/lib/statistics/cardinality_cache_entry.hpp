#pragma once

#include <chrono>
#include <optional>

#include "statistics/abstract_cardinality_estimator.hpp"

namespace opossum {

struct CardinalityCacheEntry {
  std::optional<Cardinality> cardinality;
  std::optional<Cardinality> estimated_cardinality;
  std::optional<std::chrono::seconds> timeout;
  size_t request_count{0};
};

}  // namespace opossum