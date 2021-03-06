#include <memory>
#include <string>
#include <utility>
#include "../base_test.hpp"

#include "SQLParser.h"
#include "SQLParserResult.h"
#include "gtest/gtest.h"
#include "logical_query_plan/join_node.hpp"

#include "operators/abstract_join_operator.hpp"
#include "operators/print.hpp"
#include "operators/validate.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/storage_manager.hpp"

namespace {
// This function is a slightly hacky way to check whether an LQP was optimized. This relies on JoinDetectionRule and
// could break if something is changed within the optimizer.
// It assumes that for the query: SELECT * from a, b WHERE a.a = b.a will be translated to a Cross Join with a filter
// predicate and then optimized to a Join.
std::function<bool(const std::shared_ptr<opossum::AbstractLQPNode>&)> contains_cross =
    [](const std::shared_ptr<opossum::AbstractLQPNode>& node) {
      if (node->type != opossum::LQPNodeType::Join) return false;
      if (auto join_node = std::dynamic_pointer_cast<opossum::JoinNode>(node)) {
        return join_node->join_mode == opossum::JoinMode::Cross;
      }
      return false;
    };
}  // namespace

namespace opossum {

class SQLPipelineTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", _table_a);

    _table_a_multi = load_table("src/test/tables/int_float.tbl", 2);
    _table_a_multi->append({11, 11.11});
    StorageManager::get().add_table("table_a_multi", _table_a_multi);

    _table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", _table_b);

    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int);
    column_definitions.emplace_back("b", DataType::Float);
    column_definitions.emplace_back("bb", DataType::Float);
    _join_result = std::make_shared<Table>(column_definitions, TableType::Data);
    _join_result->append({12345, 458.7f, 456.7f});
    _join_result->append({12345, 458.7f, 457.7f});

    SQLQueryCache<SQLQueryPlan>::get().clear();
  }

  std::shared_ptr<Table> _table_a;
  std::shared_ptr<Table> _table_a_multi;
  std::shared_ptr<Table> _table_b;
  std::shared_ptr<Table> _join_result;

  const std::string _select_query_a = "SELECT * FROM table_a";
  const std::string _invalid_sql = "SELECT FROM table_a";
  const std::string _fail_query = "SELECT * FROM table_does_not_exist";
  const std::string _join_query =
      "SELECT table_a.a, table_a.b, table_b.b AS bb FROM table_a, table_b WHERE table_a.a = table_b.a AND table_a.a "
      "> 1000";
  const std::string _multi_statement_query = "INSERT INTO table_a VALUES (11, 11.11); SELECT * FROM table_a";
  const std::string _multi_statement_dependent = "CREATE VIEW foo AS SELECT * FROM table_a; SELECT * FROM foo;";
  // VIEW --> VIE
  const std::string _multi_statement_invalid = "CREATE VIE foo AS SELECT * FROM table_a; SELECT * FROM foo;";
};

TEST_F(SQLPipelineTest, SimpleCreation) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.statement_count(), 1u);
}

TEST_F(SQLPipelineTest, SimpleCreationWithoutMVCC) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.statement_count(), 1u);
}

TEST_F(SQLPipelineTest, SimpleCreationWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.with_transaction_context(context).create_pipeline();

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.statement_count(), 1u);
}

TEST_F(SQLPipelineTest, SimpleCreationMulti) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.statement_count(), 2u);
}

TEST_F(SQLPipelineTest, SimpleCreationWithoutMVCCMulti) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.disable_mvcc().create_pipeline();

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.statement_count(), 2u);
}

TEST_F(SQLPipelineTest, SimpleCreationWithCustomTransactionContextMulti) {
  auto context = TransactionManager::get().new_transaction_context();
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.with_transaction_context(context).create_pipeline();

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.statement_count(), 2u);
}

TEST_F(SQLPipelineTest, SimpleCreationInvalid) {
  EXPECT_THROW(auto sql_pipeline = SQLPipelineBuilder{_multi_statement_invalid}.create_pipeline(), std::exception);
}

TEST_F(SQLPipelineTest, ConstructorCombinations) {
  // Simple sanity test for all other constructor options
  const auto optimizer = Optimizer::create_default_optimizer();
  auto prepared_cache = std::make_shared<PreparedStatementCache>(5);
  auto transaction_context = TransactionManager::get().new_transaction_context();

  // No transaction context
  EXPECT_NO_THROW(
      SQLPipelineBuilder(_select_query_a).with_optimizer(optimizer).with_mvcc(UseMvcc::Yes).create_pipeline());
  EXPECT_NO_THROW(SQLPipelineBuilder(_select_query_a)
                      .with_prepared_statement_cache(prepared_cache)
                      .with_mvcc(UseMvcc::No)
                      .create_pipeline());
  EXPECT_NO_THROW(SQLPipelineBuilder(_select_query_a)
                      .with_optimizer(optimizer)
                      .with_prepared_statement_cache(prepared_cache)
                      .with_mvcc(UseMvcc::Yes)
                      .create_pipeline());

  // With transaction context
  EXPECT_NO_THROW(SQLPipelineBuilder(_select_query_a)
                      .with_transaction_context(transaction_context)
                      .with_optimizer(optimizer)
                      .with_mvcc(UseMvcc::Yes)
                      .create_pipeline());
  EXPECT_NO_THROW(SQLPipelineBuilder(_select_query_a)
                      .with_transaction_context(transaction_context)
                      .with_optimizer(optimizer)
                      .with_prepared_statement_cache(prepared_cache)
                      .with_mvcc(UseMvcc::Yes)
                      .create_pipeline());
}

TEST_F(SQLPipelineTest, GetParsedSQLStatements) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  const auto& parsed_sql_statements = sql_pipeline.get_parsed_sql_statements();

  EXPECT_EQ(parsed_sql_statements.size(), 1u);
  EXPECT_TRUE(parsed_sql_statements.front()->isValid());
}

TEST_F(SQLPipelineTest, GetParsedSQLStatementsExecutionRequired) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline();
  EXPECT_NO_THROW(sql_pipeline.get_parsed_sql_statements());
}

TEST_F(SQLPipelineTest, GetParsedSQLStatementsMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& parsed_sql_statements = sql_pipeline.get_parsed_sql_statements();

  EXPECT_EQ(parsed_sql_statements.size(), 2u);
  EXPECT_TRUE(parsed_sql_statements.front()->isValid());
  EXPECT_TRUE(parsed_sql_statements.back()->isValid());
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPs) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  const auto& lqps = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqps.size(), 1u);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPsMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& lqps = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqps.size(), 2u);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  sql_pipeline.get_unoptimized_logical_plans();
  const auto& lqps = sql_pipeline.get_unoptimized_logical_plans();

  EXPECT_EQ(lqps.size(), 1u);
}

TEST_F(SQLPipelineTest, GetUnoptimizedLQPExecutionRequired) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline();

  try {
    sql_pipeline.get_unoptimized_logical_plans();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception& e) {
    const auto error_msg = std::string(e.what());
    // Check that the correct error was thrown
    EXPECT_TRUE(error_msg.find("Cannot translate") != std::string::npos);
  }
}

TEST_F(SQLPipelineTest, GetOptimizedLQP) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  const auto& lqps = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqps.size(), 1u);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPsMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& lqps = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqps.size(), 2u);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  sql_pipeline.get_unoptimized_logical_plans();
  const auto& lqps = sql_pipeline.get_optimized_logical_plans();

  EXPECT_EQ(lqps.size(), 1u);
}

TEST_F(SQLPipelineTest, GetOptimizedLQPExecutionRequired) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline();

  try {
    sql_pipeline.get_optimized_logical_plans();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception& e) {
    const auto error_msg = std::string(e.what());
    // Check that the correct error was thrown
    EXPECT_TRUE(error_msg.find("Cannot translate") != std::string::npos);
  }
}

TEST_F(SQLPipelineTest, GetQueryPlans) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  const auto& plans = sql_pipeline.get_query_plans();

  EXPECT_EQ(plans.size(), 1u);
}

TEST_F(SQLPipelineTest, GetQueryPlansMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& plans = sql_pipeline.get_query_plans();

  EXPECT_EQ(plans.size(), 2u);
}

TEST_F(SQLPipelineTest, GetQueryPlanTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  const auto& metrics = sql_pipeline.metrics();

  sql_pipeline.get_query_plans();
  ASSERT_EQ(metrics.statement_metrics.size(), 1u);
  auto duration = metrics.statement_metrics[0]->compile_time_micros;

  const auto& plans = sql_pipeline.get_query_plans();
  auto duration2 = metrics.statement_metrics[0]->compile_time_micros;

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);
  EXPECT_EQ(plans.size(), 1u);
}

TEST_F(SQLPipelineTest, GetQueryPlansExecutionRequired) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline();
  try {
    sql_pipeline.get_query_plans();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception& e) {
    const auto error_msg = std::string(e.what());
    // Check that the correct error was thrown
    EXPECT_TRUE(error_msg.find("Cannot compile") != std::string::npos);
  }
}

TEST_F(SQLPipelineTest, GetTasks) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 1u);
}

TEST_F(SQLPipelineTest, GetTasksMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 2u);
}

TEST_F(SQLPipelineTest, GetTasksTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  sql_pipeline.get_tasks();
  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 1u);
}

TEST_F(SQLPipelineTest, GetTasksExecutionRequired) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline();

  try {
    sql_pipeline.get_tasks();
    // Fail if this did not throw an exception
    FAIL();
  } catch (const std::exception& e) {
    const auto error_msg = std::string(e.what());
    // Check that the correct error was thrown
    EXPECT_TRUE(error_msg.find("Cannot generate tasks") != std::string::npos);
  }
}

TEST_F(SQLPipelineTest, GetResultTable) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineTest, GetResultTableMultiple) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a_multi)
}

TEST_F(SQLPipelineTest, GetResultTableTwice) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  const auto& metrics = sql_pipeline.metrics();

  sql_pipeline.get_result_table();
  ASSERT_EQ(metrics.statement_metrics.size(), 1u);
  auto duration = metrics.statement_metrics[0]->execution_time_micros;

  const auto& table = sql_pipeline.get_result_table();
  ASSERT_EQ(metrics.statement_metrics.size(), 1u);
  auto duration2 = metrics.statement_metrics[0]->execution_time_micros;

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);
  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineTest, GetResultTableExecutionRequired) {
  auto sql_pipeline = SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline();
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineTest, GetResultTableWithScheduler) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline();

  Topology::use_fake_numa_topology(8, 4);
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>());
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineTest, CleanupWithScheduler) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.create_pipeline();

  Topology::use_fake_numa_topology(8, 4);
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>());
  sql_pipeline.get_result_table();

  for (auto task_it = sql_pipeline.get_tasks()[0].cbegin(); task_it != sql_pipeline.get_tasks()[0].cend() - 1;
       ++task_it) {
    EXPECT_EQ(std::dynamic_pointer_cast<OperatorTask>(*task_it)->get_operator()->get_output(), nullptr);
  }
}

TEST_F(SQLPipelineTest, DisabledCleanupWithScheduler) {
  auto sql_pipeline = SQLPipelineBuilder{_join_query}.dont_cleanup_temporaries().create_pipeline();

  Topology::use_fake_numa_topology(8, 4);
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>());
  sql_pipeline.get_result_table();

  for (auto task_it = sql_pipeline.get_tasks()[0].cbegin(); task_it != sql_pipeline.get_tasks()[0].cend() - 1;
       ++task_it) {
    EXPECT_NE(std::dynamic_pointer_cast<OperatorTask>(*task_it)->get_operator()->get_output(), nullptr);
  }
}

TEST_F(SQLPipelineTest, GetResultTableBadQuery) {
  auto sql = "SELECT a + not_a_column FROM table_a";
  auto sql_pipeline = SQLPipelineBuilder{sql}.create_pipeline();

  EXPECT_THROW(sql_pipeline.get_result_table(), std::exception);
}

TEST_F(SQLPipelineTest, GetResultTableNoOutput) {
  const auto sql = "UPDATE table_a SET a = 1 WHERE a < 150";
  auto sql_pipeline = SQLPipelineBuilder{sql}.create_pipeline();

  const auto& table = sql_pipeline.get_result_table();
  EXPECT_EQ(table, nullptr);

  // Check that this doesn't crash. This should return the previous table.
  const auto& table2 = sql_pipeline.get_result_table();
  EXPECT_EQ(table2, nullptr);
}

TEST_F(SQLPipelineTest, GetTimes) {
  auto sql_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();

  const auto& metrics = sql_pipeline.metrics();
  ASSERT_EQ(metrics.statement_metrics.size(), 1u);
  const auto& statement_metrics = metrics.statement_metrics[0];

  const auto zero_duration = std::chrono::microseconds::zero();

  EXPECT_EQ(statement_metrics->translate_time_micros, zero_duration);
  EXPECT_EQ(statement_metrics->optimize_time_micros, zero_duration);
  EXPECT_EQ(statement_metrics->compile_time_micros, zero_duration);
  EXPECT_EQ(statement_metrics->execution_time_micros, zero_duration);

  // Run to get times
  sql_pipeline.get_result_table();

  EXPECT_GT(metrics.parse_time_micros, zero_duration);
  EXPECT_GT(statement_metrics->translate_time_micros, zero_duration);
  EXPECT_GT(statement_metrics->optimize_time_micros, zero_duration);
  EXPECT_GT(statement_metrics->compile_time_micros, zero_duration);
  EXPECT_GT(statement_metrics->execution_time_micros, zero_duration);
}

TEST_F(SQLPipelineTest, RequiresExecutionVariations) {
  EXPECT_FALSE(SQLPipelineBuilder{_select_query_a}.create_pipeline().requires_execution());
  EXPECT_FALSE(SQLPipelineBuilder{_join_query}.create_pipeline().requires_execution());
  EXPECT_FALSE(SQLPipelineBuilder{_multi_statement_query}.create_pipeline().requires_execution());
  EXPECT_TRUE(SQLPipelineBuilder{_multi_statement_dependent}.create_pipeline().requires_execution());

  const std::string create_view_single = "CREATE VIEW blub AS SELECT * FROM foo;";
  EXPECT_FALSE(SQLPipelineBuilder{create_view_single}.create_pipeline().requires_execution());

  const std::string create_view_multi_reverse = "SELECT * FROM blub; " + create_view_single;
  EXPECT_TRUE(SQLPipelineBuilder{create_view_multi_reverse}.create_pipeline().requires_execution());

  const std::string create_view_multi_middle = create_view_multi_reverse + " SELECT * FROM foo;";
  EXPECT_TRUE(SQLPipelineBuilder{create_view_multi_reverse}.create_pipeline().requires_execution());

  const std::string create_table_single = "CREATE TABLE foo2 (c int);";
  EXPECT_FALSE(SQLPipelineBuilder{create_table_single}.create_pipeline().requires_execution());

  const std::string create_table_multi = create_table_single + "SELECT * FROM foo2;";
  EXPECT_TRUE(SQLPipelineBuilder{create_table_multi}.create_pipeline().requires_execution());

  const std::string drop_table_single = "DROP TABLE foo;";
  EXPECT_FALSE(SQLPipelineBuilder{drop_table_single}.create_pipeline().requires_execution());

  const std::string drop_table_multi = "SELECT * FROM foo; " + drop_table_single;
  EXPECT_TRUE(SQLPipelineBuilder{drop_table_multi}.create_pipeline().requires_execution());

  const std::string multi_no_exec =
      "SELECT * FROM foo; INSERT INTO foo VALUES (2); SELECT * FROM blub; DELETE FROM foo WHERE a = 2;";
  EXPECT_FALSE(SQLPipelineBuilder{multi_no_exec}.create_pipeline().requires_execution());
}

TEST_F(SQLPipelineTest, CorrectStatementStringSplitting) {
  // Tests that the string passed into the pipeline is correctly split into the statement substrings
  auto select_pipeline = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  const auto& select_strings = select_pipeline.get_sql_strings();
  EXPECT_EQ(select_strings.size(), 1u);
  EXPECT_EQ(select_strings.at(0), _select_query_a);

  auto dependent_pipeline = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  const auto& dependent_strings = dependent_pipeline.get_sql_strings();
  EXPECT_EQ(dependent_strings.size(), 2u);
  // "INSERT INTO table_a VALUES (11, 11.11); SELECT * FROM table_a";
  EXPECT_EQ(dependent_strings.at(0), "INSERT INTO table_a VALUES (11, 11.11);");
  EXPECT_EQ(dependent_strings.at(1), "SELECT * FROM table_a");  // leading whitespace should be removed

  // Add newlines, tabd and weird spacing
  auto spacing_sql = "\n\t\n SELECT\na, b, c,d,e FROM\t(SELECT * FROM foo);    \t  ";
  auto spacing_pipeline = SQLPipelineBuilder{spacing_sql}.create_pipeline();
  const auto& spacing_strings = spacing_pipeline.get_sql_strings();
  EXPECT_EQ(spacing_strings.size(), 1u);
  EXPECT_EQ(spacing_strings.at(0),
            "SELECT\na, b, c,d,e FROM\t(SELECT * FROM foo);");  // internal formatting is not done

  auto multi_line_sql = R"(
  SELECT *
  FROM foo, bar
  WHERE foo.x = 17
    AND bar.y = 25
  ORDER BY foo.x ASC
  )";
  auto multi_line_pipeline = SQLPipelineBuilder{multi_line_sql}.create_pipeline();
  const auto& multi_line_strings = multi_line_pipeline.get_sql_strings();
  EXPECT_EQ(multi_line_strings.size(), 1u);
  EXPECT_EQ(multi_line_strings.at(0),
            "SELECT *\n  FROM foo, bar\n  WHERE foo.x = 17\n    AND bar.y = 25\n  ORDER BY foo.x ASC");
}

TEST_F(SQLPipelineTest, CacheQueryPlanTwice) {
  auto sql_pipeline1 = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  sql_pipeline1.get_result_table();

  // INSERT INTO table_a VALUES (11, 11.11); SELECT * FROM table_a
  auto sql_pipeline2 = SQLPipelineBuilder{_multi_statement_query}.create_pipeline();
  sql_pipeline2.get_result_table();

  // The second part of _multi_statement_query is _select_query_a, which is already cached
  const auto& cache = SQLQueryCache<SQLQueryPlan>::get();
  EXPECT_EQ(cache.size(), 2u);
  EXPECT_TRUE(cache.has(_select_query_a));
  EXPECT_TRUE(cache.has("INSERT INTO table_a VALUES (11, 11.11);"));

  auto sql_pipeline3 = SQLPipelineBuilder{_select_query_a}.create_pipeline();
  sql_pipeline3.get_result_table();

  // Make sure the cache hasn't changed
  EXPECT_EQ(cache.size(), 2u);
  EXPECT_TRUE(cache.has(_select_query_a));
  EXPECT_TRUE(cache.has("INSERT INTO table_a VALUES (11, 11.11);"));
}

}  // namespace opossum
