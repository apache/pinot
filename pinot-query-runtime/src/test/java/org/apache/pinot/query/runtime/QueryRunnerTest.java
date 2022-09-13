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
  public void testSqlWithFinalRowCountChecker(String sql, int expectedRowCount) {
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

    List<Object[]> resultRows = toRows(QueryDispatcher.reduceMailboxReceive(mailboxReceiveOperator));
    Assert.assertEquals(resultRows.size(), expectedRowCount);
  }

  @DataProvider(name = "testDataWithSqlToFinalRowCount")
  private Object[][] provideTestSqlAndRowCount() {
    return new Object[][] {
        new Object[]{"SELECT * FROM b ORDER BY col1, col2 DESC LIMIT 3", 3},
        new Object[]{"SELECT * FROM a ORDER BY col1 LIMIT 20", 15},

        // No match filter
        new Object[]{"SELECT * FROM b WHERE col3 < 0", 0},

        // Hybrid table
        new Object[]{"SELECT * FROM d", 15},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // thus the final JOIN result will be 15 x 1 = 15.
        // Next join with table C which has (5 on server1 and 10 on server2), since data is identical. each of the row
        // of the A JOIN B will have identical value of col3 as table C.col3 has. Since the values are cycling between
        // (1, 42, 1, 42, 1). we will have 9 1s, and 6 42s, total result count will be 9 * 9 + 6 * 6 = 117
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col1 JOIN c ON a.col3 = c.col3", 117},
        // Reverse the order of join condition and join table order.
        new Object[]{"SELECT * FROM a JOIN b ON b.col1 = a.col1 JOIN c ON c.col3 = a.col3", 117},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // thus the final JOIN result will be 15 x 1 = 15.
        new Object[]{"SELECT * FROM a JOIN b on a.col1 = b.col1", 15},
        new Object[]{"SELECT * FROM a JOIN b USING (col1)", 15},


        // Query with function in JOIN keys, table A and B are both (1, 42, 1, 42, 1), with table A cycling 3 times.
        // Because:
        //   - MOD(a.col3, 2) will have 6 (42)s equal to 0 and 9 (1)s equals to 1
        //   - MOD(b.col3, 3) will have 2 (42)s equal to 0 and 3 (1)s equals to 1;
        // final results are 6 * 2 + 9 * 3 = 39 rows
        new Object[]{"SELECT a.col1, a.col3, b.col3 FROM a JOIN b ON MOD(a.col3, 2) = MOD(b.col3, 3)", 39},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // thus the final JOIN result will be 15 x 1 = 15.
        new Object[]{"SELECT * FROM a JOIN b on a.col1 = b.col1 AND a.col2 = b.col2", 15},
        // Reverse the order of join condition and join table order.
        new Object[]{"SELECT * FROM a JOIN b on b.col1 = a.col1 AND b.col2 = a.col2", 15},

        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // but only 1 out of 5 rows from table A will be selected out; and all in table B will be selected.
        // thus the final JOIN result will be 1 x 3 x 1 = 3.
        new Object[]{"SELECT a.col1, a.ts, b.col2, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND a.col2 = 'alice' AND b.col3 >= 0", 3},

        // Projection pushdown
        new Object[]{"SELECT a.col1, a.col3 + a.col3 FROM a WHERE a.col3 >= 0 AND a.col2 = 'alice'", 3},

        // Partial filter pushdown
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0 AND a.col3 > b.col3", 3},

        // Aggregation with group by
        new Object[]{"SELECT a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 GROUP BY a.col1", 5},

        // Aggregation with multiple group key
        new Object[]{"SELECT a.col2, a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 GROUP BY a.col1, a.col2", 5},

        // Aggregation without GROUP BY
        new Object[]{"SELECT COUNT(*) FROM a WHERE a.col3 >= 0 AND a.col2 = 'alice'", 1},

        // Aggregation with GROUP BY on a count star reference
        new Object[]{"SELECT a.col1, COUNT(*) FROM a WHERE a.col3 >= 0 GROUP BY a.col1", 5},

        // project in intermediate stage
        // Specifically table A has 15 rows (10 on server1 and 5 on server2) and table B has 5 rows (all on server1),
        // col1 on both are "foo", "bar", "alice", "bob", "charlie"
        // col2 on both are "foo", "bar", "alice", "foo", "bar",
        //   filtered at :    ^                      ^
        // thus the final JOIN result will have 6 rows: 3 "foo" <-> "foo"; and 3 "bob" <-> "bob"
        new Object[]{"SELECT a.col1, a.col2, a.ts, b.col1, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND a.col2 = 'foo' AND b.col3 >= 0", 6},

        // Making transform after JOIN, number of rows should be the same as JOIN result.
        new Object[]{"SELECT a.col1, a.ts, a.col3 - b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND b.col3 >= 0", 15},

        // Making transform after GROUP-BY, number of rows should be the same as GROUP-BY result.
        new Object[]{"SELECT a.col1, a.col2, SUM(a.col3) - MIN(a.col3) FROM a"
            + " WHERE a.col3 >= 0 GROUP BY a.col1, a.col2", 5},

        // GROUP BY after JOIN
        //   - optimizable transport for GROUP BY key after JOIN, using SINGLETON exchange
        //     only 3 GROUP BY key exist because b.col2 cycles between "foo", "bar", "alice".
        new Object[]{"SELECT a.col1, SUM(b.col3), COUNT(*), SUM(2) FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 GROUP BY a.col1", 3},
        //   - non-optimizable transport for GROUP BY key after JOIN, using HASH exchange
        //     only 2 GROUP BY key exist for b.col3.
        new Object[]{"SELECT b.col3, SUM(a.col3) FROM a JOIN b"
            + " on a.col1 = b.col1 AND a.col2 = b.col2 GROUP BY b.col3", 2},

        // Sub-query
        new Object[]{"SELECT b.col1, b.col3, i.maxVal FROM b JOIN "
            + "  (SELECT a.col2 AS joinKey, MAX(a.col3) AS maxVal FROM a GROUP BY a.col2) AS i "
            + "  ON b.col1 = i.joinKey", 3},

        // Sub-query with IN clause to SEMI JOIN.
        new Object[]{"SELECT b.col1, b.col3 FROM b WHERE b.col1 IN "
            + "(SELECT DISTINCT a.col2 FROM a WHERE a.col2 != 'foo')", 9},

        // Aggregate query with HAVING clause, "foo" and "bar" occurred 6/2 times each and "alice" occurred 3/1 times
        // numbers are cycle in (1, 42, 1, 42, 1), and (foo, bar, alice, foo, bar)
        // - COUNT(*) < 5 matches "alice" (3 times)
        // - COUNT(*) > 5 matches "foo" and "bar" (6 times); so both will be selected out SUM(a.col3) = (1 + 42) * 3
        // - last condition doesn't match anything.
        // total to 3 rows.
        new Object[]{"SELECT a.col2, COUNT(*), MAX(a.col3), MIN(a.col3), SUM(a.col3) FROM a GROUP BY a.col2 "
            + "HAVING COUNT(*) < 5 OR (COUNT(*) > 5 AND SUM(a.col3) >= 10)"
            + "OR (MIN(a.col3) != 20 AND SUM(a.col3) = 100)", 3},

        // Order-by
        new Object[]{"SELECT a.col1, a.col3, b.col3 FROM a JOIN b ON a.col1 = b.col1 ORDER BY a.col3, b.col3 DESC", 15},

        // test customized function
        //   - on leaf stage
        new Object[]{"SELECT dateTrunc('DAY', ts) FROM a LIMIT 10", 15},
        //   - on intermediate stage
        new Object[]{"SELECT dateTrunc('DAY', round(a.ts, b.ts)) FROM a JOIN b "
            + "ON a.col1 = b.col1 AND a.col2 = b.col2", 15},

        // Test CAST
        //   - implicit CAST
        new Object[]{"SELECT a.col1, a.col2, AVG(a.col3) FROM a GROUP BY a.col1, a.col2", 5},
        //   - explicit CAST
        new Object[]{"SELECT a.col1, dateTrunc('DAY', CAST(SUM(a.col3) AS BIGINT)) FROM a GROUP BY a.col1", 5},

        // Test DISTINCT
        //   - distinct value done via GROUP BY with empty expr aggregation list.
        new Object[]{"SELECT a.col2, a.col3 FROM a JOIN b ON a.col1 = b.col1 "
            + " WHERE b.col3 > 0 GROUP BY a.col2, a.col3", 5},
    };
  }
}
