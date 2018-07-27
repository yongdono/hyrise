#pragma once

#include "abstract_jittable.hpp"
#include "concurrency/transaction_context.hpp"
#include "types.hpp"

namespace opossum {

/* The JitValidate operator validates visibility of tuples
 * within the context of a given transaction
 */
template <bool use_ref_pos_list = false>
class JitValidate : public AbstractJittable {
 public:
  JitValidate();

  std::string description() const final;

 protected:
  void _consume(JitRuntimeContext& context) const final;
};

}  // namespace opossum
