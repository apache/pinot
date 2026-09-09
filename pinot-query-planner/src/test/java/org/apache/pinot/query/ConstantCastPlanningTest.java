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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Verifies that SQL-to-rel conversion reduces constant timestamp casts with its scoped executor without changing the
/// resulting plan, and restores the planner's executor afterward. The expression-reduction hook is thread-local and
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
      assertEquals(reductions.get(), 0, "Constant timestamp casts must not invoke Calcite's generated-code reducer");
    }
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
