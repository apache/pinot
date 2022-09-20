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
package org.apache.pinot.query.runtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.service.QueryDispatcher;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryRunnerTest extends QueryRunnerTestBase {

  @Test(dataProvider = "testDataWithSqlToFinalRowCount")
  public void testSqlWithFinalRowCountChecker(String sql, int expectedRows)
      throws Exception {
    List<Object[]> resultRows = queryRunner(sql);
    Assert.assertEquals(resultRows.size(), expectedRows);
  }

  @Test(dataProvider = "testDataWithSql")
  public void testSqlWithH2Checker(String sql)
      throws Exception {
    List<Object[]> resultRows = queryRunner(sql);
    // query H2 for data
    List<Object[]> expectedRows = queryH2(sql);
    compareRowEquals(resultRows, expectedRows);
  }

  private List<Object[]> queryRunner(String sql) {
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    Map<String, String> requestMetadataMap =
        ImmutableMap.of("REQUEST_ID", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));
    MailboxReceiveOperator mailboxReceiveOperator = null;
    for (int stageId : queryPlan.getStageMetadataMap().keySet()) {
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(stageId);
        mailboxReceiveOperator = QueryDispatcher.createReduceStageOperator(_mailboxService,
            queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(),
            Long.parseLong(requestMetadataMap.get("REQUEST_ID")), reduceNode.getSenderStageId(),
            reduceNode.getDataSchema(), "localhost", _reducerGrpcPort);
      } else {
        for (ServerInstance serverInstance : queryPlan.getStageMetadataMap().get(stageId).getServerInstances()) {
          DistributedStagePlan distributedStagePlan =
              QueryDispatcher.constructDistributedStagePlan(queryPlan, stageId, serverInstance);
          _servers.get(serverInstance).processQuery(distributedStagePlan, requestMetadataMap);
        }
      }
    }
    Preconditions.checkNotNull(mailboxReceiveOperator);
    return QueryDispatcher.toResultTable(QueryDispatcher.reduceMailboxReceive(mailboxReceiveOperator),
        queryPlan.getQueryResultFields()).getRows();
  }

  private List<Object[]> queryH2(String sql)
      throws Exception {
    Statement h2statement = _h2Connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    h2statement.execute(sql);
    ResultSet h2ResultSet = h2statement.getResultSet();
    int columnCount = h2ResultSet.getMetaData().getColumnCount();
    List<Object[]> result = new ArrayList<>();
    while (h2ResultSet.next()) {
      Object[] row = new Object[columnCount];
      for (int i = 0; i < columnCount; i++) {
        row[i] = h2ResultSet.getObject(i + 1);
      }
      result.add(row);
    }
    return result;
  }

  private void compareRowEquals(List<Object[]> resultRows, List<Object[]> expectedRows) {
    Assert.assertEquals(resultRows.size(), expectedRows.size());

    Comparator<Object> valueComp = (l, r) -> {
      if (l instanceof Integer) {
        return Integer.compare((Integer) l, ((Number) r).intValue());
      } else if (l instanceof Long) {
        return Long.compare((Long) l, ((Number) r).longValue());
      } else if (l instanceof Float) {
        return Float.compare((Float) l, ((Number) r).floatValue());
      } else if (l instanceof Double) {
        return Double.compare((Double) l, ((Number) r).doubleValue());
      } else if (l instanceof String) {
        return ((String) l).compareTo((String) r);
      } else {
        throw new RuntimeException("non supported type");
      }
    };
    Comparator<Object[]> rowComp = (l, r) -> {
      int cmp = 0;
      for (int i = 0; i < l.length; i++) {
        cmp = valueComp.compare(l[i], r[i]);
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    };
    resultRows.sort(rowComp);
    expectedRows.sort(rowComp);
    for (int i = 0; i < resultRows.size(); i++) {
      Object[] resultRow = resultRows.get(i);
      Object[] expectedRow = expectedRows.get(i);
      for (int j = 0; j < resultRow.length; j++) {
        Assert.assertEquals(valueComp.compare(resultRow[j], expectedRow[j]), 0,
            "Not match at (" + i + "," + j + ")! Expected: " + expectedRow[j] + " Actual: " + resultRow[j]);
      }
    }
  }

  @DataProvider(name = "testDataWithSql")
  private Object[][] provideTestSql() {
    return new Object[][]{
        // Order BY LIMIT
        new Object[]{"SELECT * FROM b ORDER BY col1, col2 DESC LIMIT 3"},
        new Object[]{"SELECT * FROM a ORDER BY col1, ts LIMIT 10"},
        new Object[]{"SELECT * FROM a ORDER BY col1 LIMIT 20"},

        // No match filter
        new Object[]{"SELECT * FROM b WHERE col3 < 0.5"},

        // Hybrid table
        new Object[]{"SELECT * FROM d"},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // thus the final JOIN result will be 15 x 1 = 15.
        // Next join with table C which has (5 on server1 and 10 on server2), since data is identical. each of the row
        // of the A JOIN B will have identical value of col3 as table C.col3 has. Since the values are cycling between
        // (1, 42, 1, 42, 1). we will have 9 1s, and 6 42s, total result count will be 9 * 9 + 6 * 6 = 117
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col1 JOIN c ON a.col3 = c.col3"},
        // Reverse the order of join condition and join table order.
        new Object[]{"SELECT * FROM a JOIN b ON b.col1 = a.col1 JOIN c ON c.col3 = a.col3"},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // thus the final JOIN result will be 15 x 1 = 15.
        new Object[]{"SELECT * FROM a JOIN b on a.col1 = b.col1"},

        // Query with function in JOIN keys, table A and B are both (1, 42, 1, 42, 1), with table A cycling 3 times.
        // Because:
        //   - MOD(a.col3, 2) will have 6 (42)s equal to 0 and 9 (1)s equals to 1
        //   - MOD(b.col3, 3) will have 2 (42)s equal to 0 and 3 (1)s equals to 1;
        // final results are 6 * 2 + 9 * 3 = 39 rows
        new Object[]{"SELECT a.col1, a.col3, b.col3 FROM a JOIN b ON MOD(a.col3, 2) = MOD(b.col3, 3)"},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // thus the final JOIN result will be 15 x 1 = 15.
        new Object[]{"SELECT * FROM a JOIN b on a.col1 = b.col1 AND a.col2 = b.col2"},
        // Reverse the order of join condition and join table order.
        new Object[]{"SELECT * FROM a JOIN b on b.col1 = a.col1 AND b.col2 = a.col2"},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // but only 1 out of 5 rows from table A will be selected out; and all in table B will be selected.
        // thus the final JOIN result will be 1 x 3 x 1 = 3.
        new Object[]{"SELECT a.col1, a.ts, b.col2, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND a.col2 = 'alice' AND b.col3 >= 0"},

        // Join query with IN and Not-IN clause. Table A's side of join will return 9 rows and Table B's side will
        // return 2 rows. Join will be only on col1=bar and since A will return 3 rows with that value and B will return
        // 1 row, the final output will have 3 rows.
        new Object[]{"SELECT a.col1, b.col2 FROM a JOIN b ON a.col1 = b.col1 "
            + " WHERE a.col1 IN ('foo', 'bar', 'alice') AND b.col2 NOT IN ('foo', 'alice')"},

        // Same query as above but written using OR/AND instead of IN.
        new Object[]{"SELECT a.col1, b.col2 FROM a JOIN b ON a.col1 = b.col1 "
            + " WHERE (a.col1 = 'foo' OR a.col1 = 'bar' OR a.col1 = 'alice') AND b.col2 != 'foo'"
            + " AND b.col2 != 'alice'"},

        // Same as above but with single argument IN clauses. Left side of the join returns 3 rows, and the right side
        // returns 5 rows. Only key where join succeeds is col1=foo, and since table B has only 1 row with that value,
        // the number of rows should be 3.
        new Object[]{"SELECT a.col1, b.col2 FROM a JOIN b ON a.col1 = b.col1 "
            + " WHERE a.col1 IN ('foo') AND b.col2 NOT IN ('')"},

        // Projection pushdown
        new Object[]{"SELECT a.col1, a.col3 + a.col3 FROM a WHERE a.col3 >= 0 AND a.col2 = 'alice'"},

        // Partial filter pushdown
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0 AND a.col3 > b.col3"},

        // Aggregation with group by
        new Object[]{"SELECT a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 GROUP BY a.col1"},

        // Aggregation with multiple group key
        new Object[]{"SELECT a.col2, a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 GROUP BY a.col1, a.col2"},

        // Aggregation without GROUP BY
        new Object[]{"SELECT SUM(col3) FROM a WHERE a.col3 >= 0 AND a.col2 = 'alice'"},

        // Aggregation with GROUP BY on a count star reference
        new Object[]{"SELECT a.col1, COUNT(*) FROM a WHERE a.col3 >= 0 GROUP BY a.col1"},

        // project in intermediate stage
        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // col1 on both are "foo", "bar", "alice", "bob", "charlie"
        // col2 on both are "foo", "bar", "alice", "foo", "bar",
        //   filtered at :    ^                      ^
        // thus the final JOIN result will have 6 rows: 3 "foo" <-> "foo"; and 3 "bob" <-> "bob"
        new Object[]{"SELECT a.col1, a.col2, a.ts, b.col1, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND a.col2 = 'foo' AND b.col3 >= 0"},

        // Making transform after JOIN, number of rows should be the same as JOIN result.
        new Object[]{"SELECT a.col1, a.ts, a.col3 - b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND b.col3 >= 0"},

        // Making transform after GROUP-BY, number of rows should be the same as GROUP-BY result.
        new Object[]{"SELECT a.col1, a.col2, SUM(a.col3) - MIN(a.col3) FROM a"
            + " WHERE a.col3 >= 0 GROUP BY a.col1, a.col2"},

        // GROUP BY after JOIN
        //   - optimizable transport for GROUP BY key after JOIN, using SINGLETON exchange
        //     only 3 GROUP BY key exist because b.col2 cycles between "foo", "bar", "alice".
        new Object[]{"SELECT a.col1, SUM(b.col3), COUNT(*), SUM(2) FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 GROUP BY a.col1"},
        //   - non-optimizable transport for GROUP BY key after JOIN, using HASH exchange
        //     only 2 GROUP BY key exist for b.col3.
        new Object[]{"SELECT b.col3, SUM(a.col3) FROM a JOIN b"
            + " on a.col1 = b.col1 AND a.col2 = b.col2 GROUP BY b.col3"},

        // Sub-query
        new Object[]{"SELECT b.col1, b.col3, i.maxVal FROM b JOIN "
            + "  (SELECT a.col2 AS joinKey, MAX(a.col3) AS maxVal FROM a GROUP BY a.col2) AS i "
            + "  ON b.col1 = i.joinKey"},

        // Sub-query with IN clause to SEMI JOIN.
        new Object[]{"SELECT b.col1, b.col2, SUM(b.col3) * 100 / COUNT(b.col3) FROM b WHERE b.col1 IN "
            + "(SELECT a.col2 FROM a WHERE a.col2 != 'foo') GROUP BY b.col1, b.col2"},

        // Aggregate query with HAVING clause, "foo" and "bar" occurred 6/2 times each and "alice" occurred 3/1 times
        // numbers are cycle in (1, 42, 1, 42, 1), and (foo, bar, alice, foo, bar)
        // - COUNT(*) < 5 matches "alice" (3 times)
        // - COUNT(*) > 5 matches "foo" and "bar" (6 times); so both will be selected out SUM(a.col3) = (1 + 42) * 3
        // - last condition doesn't match anything.
        // total to 3 rows.
        new Object[]{"SELECT a.col2, COUNT(*), MAX(a.col3), MIN(a.col3), SUM(a.col3) FROM a GROUP BY a.col2 "
            + "HAVING COUNT(*) < 5 OR (COUNT(*) > 5 AND SUM(a.col3) >= 10)"
            + "OR (MIN(a.col3) != 20 AND SUM(a.col3) = 100)"},
        new Object[]{"SELECT COUNT(*) AS Count, MAX(a.col3) AS \"max\" FROM a GROUP BY a.col2 "
            + "HAVING Count > 1 AND \"max\" < 50"},

        // Order-by
        new Object[]{"SELECT a.col1, a.col3, b.col3 FROM a JOIN b ON a.col1 = b.col1 ORDER BY a.col3, b.col3 DESC"},
        new Object[]{"SELECT MAX(a.col3) FROM a GROUP BY a.col2 ORDER BY MAX(a.col3) - MIN(a.col3)"},

        // Test CAST
        //   - implicit CAST
        new Object[]{"SELECT a.col1, a.col2, AVG(a.col3) FROM a GROUP BY a.col1, a.col2"},
        //   - explicit CAST
        new Object[]{"SELECT a.col1, CAST(SUM(a.col3) AS BIGINT) FROM a GROUP BY a.col1"},

        // Test DISTINCT
        //   - distinct value done via GROUP BY with empty expr aggregation list.
        new Object[]{"SELECT a.col2, a.col3 FROM a JOIN b ON a.col1 = b.col1 "
            + " WHERE b.col3 > 0 GROUP BY a.col2, a.col3"},
    };
  }

  @DataProvider(name = "testDataWithSqlToFinalRowCount")
  private Object[][] provideTestSqlAndRowCount() {
    return new Object[][] {
        // using join clause
        new Object[]{"SELECT * FROM a JOIN b USING (col1)", 15},

        // test dateTrunc
        //   - on leaf stage
        new Object[]{"SELECT dateTrunc('DAY', ts) FROM a LIMIT 10", 15},
        //   - on intermediate stage
        new Object[]{"SELECT dateTrunc('DAY', round(a.ts, b.ts)) FROM a JOIN b "
            + "ON a.col1 = b.col1 AND a.col2 = b.col2", 15},
    };
  }
}
