#include "jit_utils.hpp"

#include <iostream>

#include "operators/jit_expression.hpp"
#include "operators/jit_aggregate.hpp"

namespace opossum {

namespace no_inline {

Mvcc unpack_mvcc(const MvccColumns& columns, ChunkOffset chunk_offset) {
  return {columns.tids[chunk_offset].load(), columns.begin_cids[chunk_offset], columns.end_cids[chunk_offset]};
}

void compute_binary(const JitTupleValue& lhs, const ExpressionType expression_type, const JitTupleValue& rhs,
                    const JitTupleValue& result, JitRuntimeContext& context) {
  opossum::compute_binary(lhs, expression_type, rhs, result, context);
}

void jit_aggregate_consume(const JitAggregate& jit_aggregate, JitRuntimeContext& context) {
  jit_aggregate.consume(context);
}

}  // namespace no_inline

}  // namespace opossum
