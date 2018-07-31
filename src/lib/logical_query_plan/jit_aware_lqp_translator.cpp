#include "jit_aware_lqp_translator.hpp"

#if HYRISE_JIT_SUPPORT

#include <boost/range/adaptors.hpp>
#include <boost/range/combine.hpp>

#include <queue>
#include <unordered_set>

#include "constant_mappings.hpp"
#include "global.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/jit_operator/operators/jit_aggregate.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_filter.hpp"
#include "operators/jit_operator/operators/jit_limit.hpp"
#include "operators/jit_operator/operators/jit_read_tuples.hpp"
#include "operators/jit_operator/operators/jit_validate.hpp"
#include "operators/jit_operator/operators/jit_write_tuples.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

namespace {

void column_name_for_aggregate_rec(const std::shared_ptr<opossum::LQPExpression>& expression, std::stringstream& stream,
                                   const bool brackets = true) {
  if (expression->type() == ExpressionType::Column) {
    const auto& column_reference = expression->column_reference();
    stream << column_reference.original_node()->output_column_names()[column_reference.original_column_id()];
  } else if (expression->type() == ExpressionType::Literal) {
    stream << expression->value();
  } else {
    if (brackets) stream << "(";
    DebugAssert(expression->is_arithmetic_operator(),
                "Unsupported expression type: " + expression_type_to_string.at(expression->type()));
    column_name_for_aggregate_rec(expression->left_child(), stream);
    stream << " " << expression_type_to_operator_string.at(expression->type()) << " ";
    column_name_for_aggregate_rec(expression->right_child(), stream);
    if (brackets) stream << ")";
  }
}

std::string column_name_for_aggregate(const std::shared_ptr<opossum::LQPExpression>& expression) {
  std::stringstream stream;
  column_name_for_aggregate_rec(expression, stream, false);
  return stream.str();
}

TableType input_table_type(const std::shared_ptr<AbstractLQPNode>& node) {
  switch (node->type()) {
    case LQPNodeType::Validate:
    case LQPNodeType::Predicate:
    case LQPNodeType::Aggregate:
    case LQPNodeType::Join:
    case LQPNodeType::Limit:
    case LQPNodeType::Sort:
      return TableType::References;
    default:
      return TableType::Data;
  }
}

}  // namespace

JitAwareLQPTranslator::JitAwareLQPTranslator() {
#if !HYRISE_JIT_SUPPORT
  Fail("Query translation with JIT operators requested, but jitting is not available");
#endif
  {}  // Stop clang-tidy from complaining about an empty constructor
}

std::shared_ptr<AbstractOperator> JitAwareLQPTranslator::translate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  auto jit_operator = _try_translate_node_to_jit_operators(node);
  return jit_operator ? jit_operator : LQPTranslator::translate_node(node);
}

std::shared_ptr<JitOperatorWrapper> JitAwareLQPTranslator::_try_translate_node_to_jit_operators(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  uint32_t num_jittable_nodes{0};
  std::unordered_set<std::shared_ptr<AbstractLQPNode>> input_nodes;

  bool use_validate = false;
  bool allow_aggregate = true;

  // Traverse query tree until a non-jittable nodes is found in each branch
  _visit(node, [&](auto& current_node) {
    const auto is_root_node = current_node == node;
    if (_node_is_jittable(current_node, allow_aggregate, is_root_node)) {
      use_validate |= current_node->type() == LQPNodeType::Validate;
      ++num_jittable_nodes;
      allow_aggregate &= current_node->type() == LQPNodeType::Limit;
      return true;
    } else {
      input_nodes.insert(current_node);
      return false;
    }
  });

  // We use a really simple heuristic to decide when to introduce jittable operators:
  // For aggregate operations replacing a single AggregateNode with a JitAggregate operator already yields significant
  // runtime benefits. Otherwise, it does not make sense to create a JitOperatorWrapper for fewer than 2 LQP nodes.
  if (num_jittable_nodes < 1 || (num_jittable_nodes < 2 && node->type() != LQPNodeType::Aggregate) ||
      input_nodes.size() != 1) {
    return nullptr;
  }

  // limit can only be the root node
  const bool use_limit = node->type() == LQPNodeType::Limit;
  size_t limit_rows;
  if (use_limit) {
    auto limit_node = std::dynamic_pointer_cast<LimitNode>(node);
    limit_rows = limit_node->num_rows();
  }
  const auto& last_node = use_limit ? node->left_input() : node;

  // The input_node is not being integrated into the operator chain, but instead serves as the input to the JitOperators
  const auto input_node = *input_nodes.begin();

  auto jit_operator = std::make_shared<JitOperatorWrapper>(translate_node(input_node));
  auto read_tuple = std::make_shared<JitReadTuples>(use_validate, limit_rows);
  jit_operator->add_jit_operator(read_tuple);

  auto filter_node = node;
  while (filter_node != input_node && filter_node->type() != LQPNodeType::Predicate &&
         filter_node->type() != LQPNodeType::Union) {
    filter_node = filter_node->left_input();
  }

  // If we can reach the input node without encountering a UnionNode or PredicateNode,
  // there is no need to filter any tuples
  if (filter_node != input_node) {
    // However, if we do need to filter, we first convert the filter node to a JitExpression ...
    const auto expression = _try_translate_node_to_jit_expression(filter_node, *read_tuple, input_node);
    if (!expression) return nullptr;
    // make sure that the expression gets computed ...
    jit_operator->add_jit_operator(std::make_shared<JitCompute>(expression));
    // and then filter on the resulting boolean.
    jit_operator->add_jit_operator(std::make_shared<JitFilter>(expression->result()));
  }

  if (use_validate) {
    if (input_table_type(input_node) == TableType::Data) {
      jit_operator->add_jit_operator(std::make_shared<JitValidate<TableType::Data>>());
    } else {
      jit_operator->add_jit_operator(std::make_shared<JitValidate<TableType::References>>());
    }
  }

  if (last_node->type() == LQPNodeType::Aggregate) {
    // Since aggregate nodes cause materialization, there is at most one JitAggregate operator in each operator chain
    // and it must be the last operator of the chain. The _node_is_jittable function takes care of this by rejecting
    // aggregate nodes that would be placed in the middle of an operator chain.
    const auto aggregate_node = std::static_pointer_cast<AggregateNode>(last_node);

    auto aggregate = !use_limit ? std::make_shared<JitAggregate>() : std::make_shared<JitLimitAggregate>();
    const auto column_names = aggregate_node->output_column_names();
    const auto groupby_columns = aggregate_node->groupby_column_references();
    const auto aggregate_columns = aggregate_node->aggregate_expressions();

    // Split the output column names between groupby columns and aggregate expressions.
    // The AggregateNode::_update_output() function node will always output groupby columns first.
    const auto groupby_column_names = boost::adaptors::slice(column_names, 0, groupby_columns.size());
    const auto aggregate_column_names =
        boost::adaptors::slice(column_names, groupby_columns.size(), column_names.size());

    bool has_string_columns = false;

    for (const auto& groupby_column : boost::combine(groupby_column_names, groupby_columns)) {
      const auto expression = _try_translate_column_to_jit_expression(groupby_column.get<1>(), *read_tuple, input_node);
      if (!expression) return nullptr;
      has_string_columns |= expression->result().data_type() == DataType::String;
      // Create a JitCompute operator for each computed groupby column ...
      if (expression->expression_type() != ExpressionType::Column) {
        jit_operator->add_jit_operator(std::make_shared<JitCompute>(expression));
      }
      // ... and add the column to the JitAggregate operator.
      aggregate->add_groupby_column(groupby_column.get<0>(), expression->result());
    }

    for (const auto& aggregate_column : boost::combine(aggregate_column_names, aggregate_columns)) {
      const auto aggregate_expression = aggregate_column.get<1>();
      DebugAssert(aggregate_expression->type() == ExpressionType::Function, "Expression is not a function.");
      const auto function_arg = aggregate_expression->aggregate_function_arguments()[0];
      const auto expression = _try_translate_expression_to_jit_expression(*function_arg, *read_tuple, input_node);
      if (!expression) return nullptr;
      has_string_columns |= expression->result().data_type() == DataType::String;
      // Create a JitCompute operator for each aggregate expression on a computed value ...
      if (expression->expression_type() != ExpressionType::Column) {
        jit_operator->add_jit_operator(std::make_shared<JitCompute>(expression));
      }
      // ... resolve the aggregate column name ...
      const std::string& aggregate_column_name = aggregate_expression->alias().value_or(
          aggregate_function_to_string.left.at(aggregate_expression->aggregate_function()) + "(" +
          column_name_for_aggregate(function_arg) + ")");
      // ... and add the aggregate expression to the JitAggregate operator.
      aggregate->add_aggregate_column(aggregate_column_name, expression->result(),
                                      aggregate_expression->aggregate_function());
    }

    aggregate->set_has_string_columns(has_string_columns);

    jit_operator->add_jit_operator(aggregate);
  } else {
    if (use_limit) jit_operator->add_jit_operator(std::make_shared<JitLimit>());

    // Add a compute operator for each computed output column (i.e., a column that is not from a stored table).
    auto write_table = std::make_shared<JitWriteTuples>();
    for (const auto& output_column :
         boost::combine(last_node->output_column_names(), last_node->output_column_references())) {
      const auto expression = _try_translate_column_to_jit_expression(output_column.get<1>(), *read_tuple, input_node);
      if (!expression) return nullptr;
      // If the JitExpression is of type ExpressionType::Column, there is no need to add a compute node, since it
      // would not compute anything anyway
      if (expression->expression_type() != ExpressionType::Column) {
        jit_operator->add_jit_operator(std::make_shared<JitCompute>(expression));
      }
      write_table->add_output_column(output_column.get<0>(), expression->result());
    }

    jit_operator->add_jit_operator(write_table);
  }

  return jit_operator;
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_try_translate_node_to_jit_expression(
    const std::shared_ptr<AbstractLQPNode>& node, JitReadTuples& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  std::shared_ptr<const JitExpression> left, right;
  switch (node->type()) {
    case LQPNodeType::Predicate:
      // If the PredicateNode has further conditions (either PredicateNodes or UnionNodes) as children, we need a
      // logical AND here. In this case, the condition represented by the PredicateNode itself becomes the left side
      // and the input to the predicate node becomes the right side of the conjunction.
      // If the PredicateNode has no further conditions (i.e., all tuples pass into the PredicateNode unconditionally),
      // there is no need to create a dedicated AND-expression.
      if (_input_is_filtered(node->left_input())) {
        left = _try_translate_node_to_jit_expression(node->left_input(), jit_source, input_node);
        right = _try_translate_predicate_to_jit_expression(std::dynamic_pointer_cast<PredicateNode>(node), jit_source,
                                                           input_node);
        return left && right
                   ? std::make_shared<JitExpression>(left, ExpressionType::And, right, jit_source.add_temporary_value())
                   : nullptr;
      } else {
        return _try_translate_predicate_to_jit_expression(std::dynamic_pointer_cast<PredicateNode>(node), jit_source,
                                                          input_node);
      }

    case LQPNodeType::Union:
      left = _try_translate_node_to_jit_expression(node->left_input(), jit_source, input_node);
      right = _try_translate_node_to_jit_expression(node->right_input(), jit_source, input_node);
      return left && right
                 ? std::make_shared<JitExpression>(left, ExpressionType::Or, right, jit_source.add_temporary_value())
                 : nullptr;

    case LQPNodeType::Limit:
      // Limit is handled separately in _try_translate_node_to_jit_operators(...)
      return nullptr;
    case LQPNodeType::Projection:
      // We don't care about projection nodes here, since they do not perform any tuple filtering
      return _try_translate_node_to_jit_expression(node->left_input(), jit_source, input_node);

    case LQPNodeType::Validate:
      // The validate filters independently to the filter node
      if (Global::get().jit_validate) {
        return _try_translate_node_to_jit_expression(node->left_input(), jit_source, input_node);
      } else {
        return nullptr;
      }

    default:
      return nullptr;
  }
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_try_translate_predicate_to_jit_expression(
    const std::shared_ptr<PredicateNode>& node, JitReadTuples& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  auto condition = predicate_condition_to_expression_type.at(node->predicate_condition());
  auto left = _try_translate_column_to_jit_expression(node->column_reference(), jit_source, input_node);
  if (!left) return nullptr;

  switch (condition) {
    /* Binary predicates */
    case ExpressionType::Equals:
    case ExpressionType::NotEquals:
    case ExpressionType::LessThan:
    case ExpressionType::LessThanEquals:
    case ExpressionType::GreaterThan:
    case ExpressionType::GreaterThanEquals:
    case ExpressionType::Like:
    case ExpressionType::NotLike: {
      auto right = _try_translate_variant_to_jit_expression(node->value(), jit_source, input_node);
      return right ? std::make_shared<JitExpression>(left, condition, right, jit_source.add_temporary_value())
                   : nullptr;
    }
    /* Unary predicates */
    case ExpressionType::IsNull:
    case ExpressionType::IsNotNull:
      return std::make_shared<JitExpression>(left, condition, jit_source.add_temporary_value());
    case ExpressionType::Between: {
      DebugAssert(static_cast<bool>(node->value2()), "Predicate condition BETWEEN requires a second value");
      PerformanceWarning("Jit Expression executes BETWEEN as two separate scans");
      auto lower_value = _try_translate_variant_to_jit_expression(node->value(), jit_source, input_node);
      auto upper_value = _try_translate_variant_to_jit_expression(*node->value2(), jit_source, input_node);
      if (!lower_value || !upper_value) return nullptr;
      auto lower_bound = std::make_shared<JitExpression>(left, ExpressionType::GreaterThanEquals, lower_value,
                                                         jit_source.add_temporary_value());
      auto upper_bound = std::make_shared<JitExpression>(left, ExpressionType::LessThanEquals, upper_value,
                                                         jit_source.add_temporary_value());
      return std::make_shared<JitExpression>(lower_bound, ExpressionType::And, upper_bound,
                                             jit_source.add_temporary_value());
    }
    default:
      return nullptr;
  }
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_try_translate_expression_to_jit_expression(
    const LQPExpression& lqp_expression, JitReadTuples& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  std::shared_ptr<const JitExpression> left, right;
  switch (lqp_expression.type()) {
    case ExpressionType::Literal:
      return _try_translate_variant_to_jit_expression(lqp_expression.value(), jit_source, input_node);

    case ExpressionType::Column:
      return _try_translate_column_to_jit_expression(lqp_expression.column_reference(), jit_source, input_node);

    /* Binary operators */
    case ExpressionType::Addition:
    case ExpressionType::Subtraction:
    case ExpressionType::Multiplication:
    case ExpressionType::Division:
    case ExpressionType::Modulo:
    case ExpressionType::Power:
    case ExpressionType::Equals:
    case ExpressionType::NotEquals:
    case ExpressionType::LessThan:
    case ExpressionType::LessThanEquals:
    case ExpressionType::GreaterThan:
    case ExpressionType::GreaterThanEquals:
    case ExpressionType::Like:
    case ExpressionType::NotLike:
    case ExpressionType::And:
    case ExpressionType::Or:
      left = _try_translate_expression_to_jit_expression(*lqp_expression.left_child(), jit_source, input_node);
      right = _try_translate_expression_to_jit_expression(*lqp_expression.right_child(), jit_source, input_node);
      return std::make_shared<JitExpression>(left, lqp_expression.type(), right, jit_source.add_temporary_value());

    /* Unary operators */
    case ExpressionType::Not:
    case ExpressionType::IsNull:
    case ExpressionType::IsNotNull:
      left = _try_translate_expression_to_jit_expression(*lqp_expression.left_child(), jit_source, input_node);
      return std::make_shared<JitExpression>(left, lqp_expression.type(), jit_source.add_temporary_value());

    default:
      return nullptr;
  }
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_try_translate_column_to_jit_expression(
    const LQPColumnReference& lqp_column_reference, JitReadTuples& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  const auto column_id = input_node->find_output_column_id(lqp_column_reference);
  const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(lqp_column_reference.original_node());
  const auto projection_node = std::dynamic_pointer_cast<const ProjectionNode>(lqp_column_reference.original_node());

  if (column_id && stored_table_node) {
    // we reached a "raw" data column and register it as an input column
    const auto original_column_id = lqp_column_reference.original_column_id();
    const auto table_name = stored_table_node->table_name();
    const auto table = StorageManager::get().get_table(table_name);
    const auto data_type = table->column_data_type(original_column_id);
    const auto is_nullable = table->column_is_nullable(original_column_id);
    const auto tuple_value = jit_source.add_input_column(data_type, is_nullable, column_id.value());
    return std::make_shared<JitExpression>(tuple_value);
  } else if (projection_node) {
    // if the LQPColumnReference references a computed column, we need to compute that expression as well
    const auto lqp_expression = projection_node->column_expressions()[lqp_column_reference.original_column_id()];
    return _try_translate_expression_to_jit_expression(*lqp_expression, jit_source, input_node);
  } else {
    return nullptr;
  }
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_try_translate_variant_to_jit_expression(
    const AllParameterVariant& value, JitReadTuples& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  if (is_lqp_column_reference(value)) {
    return _try_translate_column_to_jit_expression(boost::get<LQPColumnReference>(value), jit_source, input_node);
  } else if (is_variant(value)) {
    const auto variant = boost::get<AllTypeVariant>(value);
    const auto tuple_value = jit_source.add_literal_value(variant);
    return std::make_shared<JitExpression>(tuple_value);
  } else {
    return nullptr;
  }
}

bool JitAwareLQPTranslator::_input_is_filtered(const std::shared_ptr<AbstractLQPNode>& node) const {
  auto current_node = node;
  while (current_node->type() == LQPNodeType::Projection) {
    current_node = current_node->left_input();
  }
  if (auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(current_node)) {
    return predicate_node->scan_type() == ScanType::TableScan;
  }
  return current_node->type() == LQPNodeType::Union;
}

bool JitAwareLQPTranslator::_node_is_jittable(const std::shared_ptr<AbstractLQPNode>& node,
                                              const bool allow_aggregate_node, const bool allow_limit_node) const {
  if (node->type() == LQPNodeType::Aggregate) {
    // We do not support the count distinct function yet and thus need to check all aggregate expressions.
    auto aggregate_node = std::static_pointer_cast<AggregateNode>(node);
    auto aggregate_expressions = aggregate_node->aggregate_expressions();
    auto has_count_distict =
        std::count_if(aggregate_expressions.begin(), aggregate_expressions.end(), [](auto& expression) {
          return expression->aggregate_function() == AggregateFunction::CountDistinct;
        }) > 0;
    return allow_aggregate_node && !has_count_distict;
  }

  if (auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node)) {
    return predicate_node->scan_type() == ScanType::TableScan;
  }

  if (Global::get().jit_validate && node->type() == LQPNodeType::Validate) {
    return true;
  }

  if (allow_limit_node && node->type() == LQPNodeType::Limit) {
    return true;
  }

  return (node->type() == LQPNodeType::Projection || node->type() == LQPNodeType::Union);
}

void JitAwareLQPTranslator::_visit(const std::shared_ptr<AbstractLQPNode>& node,
                                   const std::function<bool(const std::shared_ptr<AbstractLQPNode>&)>& func) const {
  std::unordered_set<std::shared_ptr<const AbstractLQPNode>> visited;
  std::queue<std::shared_ptr<AbstractLQPNode>> queue({node});

  while (!queue.empty()) {
    auto current_node = queue.front();
    queue.pop();

    if (!current_node || visited.count(current_node)) {
      continue;
    }
    visited.insert(current_node);

    if (func(current_node)) {
      queue.push(current_node->left_input());
      queue.push(current_node->right_input());
    }
  }
}

}  // namespace opossum

#endif
