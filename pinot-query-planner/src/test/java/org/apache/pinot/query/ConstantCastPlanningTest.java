/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/// Verifies that SQL-to-rel conversion reduces constant string casts without changing the resulting plan,
/// and restores the planner's executor afterward. The expression-reduction hook is thread-local and
/// closed after each compilation.
public class ConstantCastPlanningTest extends QueryEnvironmentTestBase {
  @DataProvider
  public Object[][] timestampQueries() {
    return new Object[][]{
        {false, "account_1", false},
        {false, "account_2", false},
        {true, "account_1", false},
        {true, "account_2", false},
        {false, "account_1", true},
        {false, "account_2", true},
        {true, "account_1", true},
        {true, "account_2", true}
    };
  }

  @Test(dataProvider = "timestampQueries")
  // The hook resource installs and removes the callback; it is not otherwise referenced.
  @SuppressWarnings("try")
  public void testConstantTimestampCastPlanning(boolean window, String accountId, boolean epochString) {
    String lowerBound = epochString ? "1704067200000" : "2024-01-01 00:00:00";
    String upperBound = epochString ? "1706745600000" : "2024-02-01 00:00:00";
    String castQuery = query(window, accountId, "CAST('" + lowerBound + "' AS TIMESTAMP)",
        "CAST('" + upperBound + "' AS TIMESTAMP)");
    String literalQuery = query(window, accountId, "TIMESTAMP '2024-01-01 00:00:00'",
        "TIMESTAMP '2024-02-01 00:00:00'");
    SqlNodeAndOptions parsedQuery = CalciteSqlParser.compileToSqlNodeAndOptions(castQuery);
    AtomicInteger parsedCasts = new AtomicInteger();
    parsedQuery.getSqlNode().accept(new SqlBasicVisitor<Void>() {
      @Override
      public Void visit(SqlCall call) {
        if (call.getKind() == SqlKind.CAST) {
          parsedCasts.incrementAndGet();
        }
        return super.visit(call);
      }
    });
    assertEquals(parsedCasts.get(), 2, "The query must reach compilation with both string-to-timestamp casts");

    AtomicInteger reductions = new AtomicInteger();
    try (QueryEnvironment.CompiledQuery literalPlan = _queryEnvironment.compile(literalQuery);
        Hook.Closeable ignored = Hook.EXPRESSION_REDUCER.addThread(value -> {
          reductions.incrementAndGet();
        });
        QueryEnvironment.CompiledQuery castPlan = _queryEnvironment.compile(castQuery, parsedQuery)) {
      assertEquals(RelOptUtil.toString(castPlan.getRelNode()), RelOptUtil.toString(literalPlan.getRelNode()));
      assertEquals(castPlan.getRelRoot().validatedRowType, literalPlan.getRelRoot().validatedRowType);
      assertEquals(castPlan.getTableNames(), literalPlan.getTableNames());
      assertNull(castPlan.getPlannerContext().getRelOptPlanner().getExecutor(),
          "SQL-to-rel conversion must restore the planner's executor before optimization");
      assertEquals(reductions.get(), 0, "Timestamp casts must use cached templates instead of the fallback reducer");
    }
  }

  @DataProvider
  public Object[][] bigintQueries() {
    List<Object[]> cases = new ArrayList<>();
    for (String value : List.of("0", "123", "9007199254740993", "9223372036854775807", "-9223372036854775808")) {
      for (boolean explicitCast : List.of(false, true)) {
        for (boolean window : List.of(false, true)) {
          cases.add(new Object[]{value, explicitCast, window});
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "bigintQueries")
  @SuppressWarnings("try") // The resource scopes the thread-local fallback-reducer hook.
  public void testConstantBigintCastPlanning(String value, boolean explicitCast, boolean window) {
    String quoted = explicitCast ? "CAST('" + value + "' AS BIGINT)" : "'" + value + "'";
    String numeric = "CAST(" + value + " AS BIGINT)";
    AtomicInteger reductions = new AtomicInteger();
    try (QueryEnvironment.CompiledQuery literalPlan = _queryEnvironment.compile(bigintQuery(numeric, window));
        Hook.Closeable ignored = Hook.EXPRESSION_REDUCER.addThread(v -> {
          reductions.incrementAndGet();
        });
        QueryEnvironment.CompiledQuery castPlan = _queryEnvironment.compile(bigintQuery(quoted, window))) {
      assertEquals(RelOptUtil.toString(castPlan.getRelNode()), RelOptUtil.toString(literalPlan.getRelNode()));
      assertEquals(castPlan.getRelRoot().validatedRowType, literalPlan.getRelRoot().validatedRowType);
      assertEquals(castPlan.getTableNames(), literalPlan.getTableNames());
      assertNull(castPlan.getPlannerContext().getRelOptPlanner().getExecutor());
      assertEquals(reductions.get(), 0, "BIGINT casts must use cached templates instead of the fallback reducer");
    }
  }

  @DataProvider
  public Object[][] scalarCastQueries() {
    return new Object[][]{
        {"INTEGER", "2147483647", "CAST(2147483647 AS INTEGER)", SqlTypeName.INTEGER},
        {"SMALLINT", "32767", "CAST(32767 AS SMALLINT)", SqlTypeName.SMALLINT},
        {"TINYINT", "127", "CAST(127 AS TINYINT)", SqlTypeName.TINYINT},
        {"REAL", "1.25", "CAST(1.25 AS REAL)", SqlTypeName.REAL},
        {"FLOAT", "1.25", "CAST(1.25 AS FLOAT)", SqlTypeName.FLOAT},
        {"DOUBLE", "1.25", "CAST(1.25 AS DOUBLE)", SqlTypeName.DOUBLE},
        {"DECIMAL(6, 2)", "1234.50", "CAST(1234.50 AS DECIMAL(6, 2))", SqlTypeName.DECIMAL},
        {"DECIMAL(20, 4)", "9007199254740993.1250", "CAST(9007199254740993.1250 AS DECIMAL(20, 4))",
            SqlTypeName.DECIMAL},
        {"BOOLEAN", "true", "TRUE", SqlTypeName.BOOLEAN},
        {"BOOLEAN", "false", "FALSE", SqlTypeName.BOOLEAN},
        {"DATE", "2024-02-29", "DATE '2024-02-29'", SqlTypeName.DATE},
        {"TIME", "12:34:56", "TIME '12:34:56'", SqlTypeName.TIME},
        {"CHAR(3)", "abcdef", "'abc'", SqlTypeName.CHAR}
    };
  }

  @Test(dataProvider = "scalarCastQueries")
  public void testConstantScalarCastPlanning(String targetType, String value, String literal,
      SqlTypeName expectedType) {
    String castQuery = "SELECT CAST('" + value + "' AS " + targetType + ") AS cast_value, col3 FROM a WHERE col3 > 0";
    String literalQuery = "SELECT " + literal + " AS cast_value, col3 FROM a WHERE col3 > 0";
    try (QueryEnvironment.CompiledQuery literalPlan = _queryEnvironment.compile(literalQuery);
        QueryEnvironment.CompiledQuery castPlan = _queryEnvironment.compile(castQuery)) {
      assertEquals(RelOptUtil.toString(castPlan.getRelNode()), RelOptUtil.toString(literalPlan.getRelNode()));
      assertEquals(castPlan.getRelRoot().validatedRowType, literalPlan.getRelRoot().validatedRowType);
      assertEquals(castPlan.getRelRoot().validatedRowType.getFieldList().get(0).getType().getSqlTypeName(),
          expectedType);
      assertEquals(castPlan.getTableNames(), literalPlan.getTableNames());
      assertNull(castPlan.getPlannerContext().getRelOptPlanner().getExecutor());
      // Exercise conversion to dispatchable stages as well as SQL-to-rel conversion and optimization.
      assertNotNull(literalPlan.planQuery(0).getQueryPlan());
      assertNotNull(castPlan.planQuery(0).getQueryPlan());
    }
  }

  private static String bigintQuery(String value, boolean window) {
    String filter = " FROM a WHERE col7 = " + value;
    if (window) {
      return "SELECT col7, col3 FROM (SELECT col7, col3, ROW_NUMBER() OVER "
          + "(PARTITION BY col7 ORDER BY col3 DESC) AS row_num" + filter + ") WHERE row_num = 1";
    }
    return "SELECT col7, col3" + filter;
  }

  private static String query(boolean window, String accountId, String lowerBound, String upperBound) {
    String filter = " FROM a WHERE col1 = '" + accountId + "' AND ts_timestamp >= " + lowerBound
        + " AND ts_timestamp < " + upperBound;
    if (window) {
      return "SELECT ts_timestamp, col3 FROM (SELECT ts_timestamp, col3, ROW_NUMBER() OVER "
          + "(PARTITION BY col1, ts_timestamp ORDER BY col3 DESC, col7 DESC) AS row_num" + filter
          + ") WHERE row_num = 1 ORDER BY ts_timestamp LIMIT 1000";
    }
    return "SELECT ts_timestamp, col3" + filter + " ORDER BY ts_timestamp LIMIT 1000";
  }
}
