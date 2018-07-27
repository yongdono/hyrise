#include "types.hpp"

namespace opossum {

bool operator==(const PosList& lhs, const PosList& rhs) { return lhs.operator==(rhs); }
bool operator!=(const PosList& lhs, const PosList& rhs) { return !(lhs == rhs); }

}  // namespace opossum
