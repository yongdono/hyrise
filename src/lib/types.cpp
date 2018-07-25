#include "types.hpp"

namespace opossum {

bool operator==(const PosList& lhs, const PosList& rhs) { return lhs.as_vector() == rhs.as_vector(); }
bool operator!=(const PosList& lhs, const PosList& rhs) { return lhs.as_vector() != rhs.as_vector(); }

} // namespace opossum
