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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.BasePlanNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class QueryCompilationTest extends QueryEnvironmentTestBase {

  @Test(dataProvider = "testQueryLogicalPlanDataProvider")
  public void testQueryPlanExplainLogical(String query, String digest) {
    testQueryPlanExplain(query, digest);
  }

  private void testQueryPlanExplain(String query, String digest) {
    long requestId = RANDOM_REQUEST_ID_GEN.nextLong();
    String explainedPlan = _queryEnvironment.explainQuery(query, requestId);
    assertEquals(explainedPlan, digest);
  }

  @Test(dataProvider = "testQueryDataProvider")
  public void testQueryPlanWithoutException(String query) {
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
    assertNotNull(dispatchableSubPlan);
  }

  @Test(dataProvider = "testQueryExceptionDataProvider")
  public void testQueryWithException(String query, String exceptionSnippet) {
    try {
      _queryEnvironment.planQuery(query);
      fail("query plan should throw exception");
    } catch (RuntimeException e) {
      assertTrue(e.getCause().getMessage().contains(exceptionSnippet));
    }
  }

  @Test
  public void testAggregateCaseToFilter() {
    // Tests that queries like "SELECT SUM(CASE WHEN col1 = 'a' THEN 1 ELSE 0 END) FROM a" are rewritten to
    // "SELECT COUNT(a) FROM a WHERE col1 = 'a'"
    String query = "EXPLAIN PLAN FOR SELECT SUM(CASE WHEN col1 = 'a' THEN 1 ELSE 0 END) FROM a";

    String explain = _queryEnvironment.explainQuery(query, RANDOM_REQUEST_ID_GEN.nextLong());
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
        + "LogicalProject(EXPR$0=[CASE(=($1, 0), null:BIGINT, $0)])\n"
        + "  PinotLogicalAggregate(group=[{}], agg#0=[COUNT($0)], agg#1=[COUNT($1)], aggType=[FINAL])\n"
        + "    PinotLogicalExchange(distribution=[hash])\n"
        + "      PinotLogicalAggregate(group=[{}], agg#0=[COUNT() FILTER $0], agg#1=[COUNT()], aggType=[LEAF])\n"
        + "        LogicalProject($f1=[=($0, _UTF-8'a')])\n"
        + "          PinotLogicalTableScan(table=[[default, a]])\n");
    //@formatter:on
  }

  private static void assertGroupBySingletonAfterJoin(DispatchableSubPlan dispatchableSubPlan, boolean shouldRewrite) {
    for (int stageId = 0; stageId < dispatchableSubPlan.getQueryStageMap().size(); stageId++) {
      if (dispatchableSubPlan.getTableNames().size() == 0 && !PlannerUtils.isRootPlanFragment(stageId)) {
        PlanNode node = dispatchableSubPlan.getQueryStageMap().get(stageId).getPlanFragment().getFragmentRoot();
        while (node != null) {
          if (node instanceof JoinNode) {
            // JOIN is exchanged with hash distribution (data shuffle)
            MailboxReceiveNode left = (MailboxReceiveNode) node.getInputs().get(0);
            MailboxReceiveNode right = (MailboxReceiveNode) node.getInputs().get(1);
            assertEquals(left.getDistributionType(), RelDistribution.Type.HASH_DISTRIBUTED);
            assertEquals(right.getDistributionType(), RelDistribution.Type.HASH_DISTRIBUTED);
            break;
          }
          if (node instanceof AggregateNode && node.getInputs().get(0) instanceof MailboxReceiveNode) {
            // AGG is exchanged with singleton since it has already been distributed by JOIN.
            MailboxReceiveNode input = (MailboxReceiveNode) node.getInputs().get(0);
            if (shouldRewrite) {
              assertEquals(input.getDistributionType(), RelDistribution.Type.SINGLETON);
            } else {
              assertNotEquals(input.getDistributionType(), RelDistribution.Type.SINGLETON);
            }
            break;
          }
          node = node.getInputs().get(0);
        }
      }
    }
  }

  @Test
  public void testQueryAndAssertStageContentForJoin() {
    String query = "SELECT * FROM a JOIN b ON a.col1 = b.col2";
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
    Set<DispatchablePlanFragment> stagePlans = dispatchableSubPlan.getQueryStages();
    int numStages = stagePlans.size();
    assertEquals(numStages, 4);
    for (DispatchablePlanFragment stagePlan : stagePlans) {
      int stageId = stagePlan.getPlanFragment().getFragmentId();
      Map<QueryServerInstance, List<Integer>> serverToWorkerIdsMap = stagePlan.getServerInstanceToWorkerIdMap();
      int numServers = serverToWorkerIdsMap.size();
      String tableName = stagePlan.getTableName();
      if (tableName != null) {
        // table scan stages; for tableA it should have 2 hosts, for tableB it should have only 1
        if (tableName.equals("a")) {
          assertEquals(numServers, 2);
          for (QueryServerInstance server : serverToWorkerIdsMap.keySet()) {
            int port = server.getQueryMailboxPort();
            assertTrue(port == 1 || port == 2);
          }
        } else {
          assertEquals(numServers, 1);
          assertEquals(serverToWorkerIdsMap.keySet().iterator().next().getQueryMailboxPort(), 1);
        }
      } else if (!PlannerUtils.isRootPlanFragment(stageId)) {
        // join stage should have both servers used.
        assertEquals(numServers, 2);
        for (QueryServerInstance server : serverToWorkerIdsMap.keySet()) {
          int port = server.getQueryMailboxPort();
          assertTrue(port == 1 || port == 2);
        }
      } else {
        // reduce stage should have the reducer instance.
        assertEquals(numServers, 1);
        assertEquals(serverToWorkerIdsMap.keySet().iterator().next().getQueryMailboxPort(), 3);
      }
    }
  }

  @Test
  public void testQueryProjectFilterPushDownForJoin() {
    String query = "SELECT a.col1, a.ts, b.col2, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
        + "WHERE a.col3 >= 0 AND a.col2 IN ('b') AND b.col3 < 0";
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
    List<DispatchablePlanFragment> intermediateStages = dispatchableSubPlan.getQueryStageMap().values().stream()
        .filter(q -> q.getTableName() == null)
        .collect(Collectors.toList());
    // Assert that no project of filter node for any intermediate stage because all should've been pushed down.
    for (DispatchablePlanFragment dispatchablePlanFragment : intermediateStages) {
      PlanNode roots = dispatchablePlanFragment.getPlanFragment().getFragmentRoot();
      assertNodeTypeNotIn(roots, ImmutableList.of(ProjectNode.class, FilterNode.class));
    }
  }

  @Test
  public void testQueryRoutingManagerCompilation() {
    String query = "SELECT * FROM d_OFFLINE";
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
    List<DispatchablePlanFragment> tableScanMetadataList = dispatchableSubPlan.getQueryStageMap().values().stream()
        .filter(stageMetadata -> stageMetadata.getTableName() != null)
        .collect(Collectors.toList());
    assertEquals(tableScanMetadataList.size(), 1);
    assertEquals(tableScanMetadataList.get(0).getServerInstanceToWorkerIdMap().size(), 2);

    query = "SELECT * FROM d_REALTIME";
    dispatchableSubPlan = _queryEnvironment.planQuery(query);
    tableScanMetadataList = dispatchableSubPlan.getQueryStageMap().values().stream()
        .filter(stageMetadata -> stageMetadata.getTableName() != null)
        .collect(Collectors.toList());
    assertEquals(tableScanMetadataList.size(), 1);
    assertEquals(tableScanMetadataList.get(0).getServerInstanceToWorkerIdMap().size(), 1);

    query = "SELECT * FROM d";
    dispatchableSubPlan = _queryEnvironment.planQuery(query);
    tableScanMetadataList = dispatchableSubPlan.getQueryStageMap().values().stream()
        .filter(stageMetadata -> stageMetadata.getTableName() != null)
        .collect(Collectors.toList());
    assertEquals(tableScanMetadataList.size(), 1);
    assertEquals(tableScanMetadataList.get(0).getServerInstanceToWorkerIdMap().size(), 2);
  }

  // Test that plan query can be run as multi-thread.
  @Test
  public void testPlanQueryMultiThread()
      throws Exception {
    Map<String, ArrayList<DispatchableSubPlan>> queryPlans = new HashMap<>();
    Lock lock = new ReentrantLock();
    Runnable joinQuery = () -> {
      String query = "SELECT a.col1, a.ts, b.col2, b.col3 FROM a JOIN b ON a.col1 = b.col2";
      DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
      lock.lock();
      if (!queryPlans.containsKey(dispatchableSubPlan)) {
        queryPlans.put(query, new ArrayList<>());
      }
      queryPlans.get(query).add(dispatchableSubPlan);
      lock.unlock();
    };
    Runnable selectQuery = () -> {
      String query = "SELECT * FROM a";
      DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
      lock.lock();
      if (!queryPlans.containsKey(dispatchableSubPlan)) {
        queryPlans.put(query, new ArrayList<>());
      }
      queryPlans.get(query).add(dispatchableSubPlan);
      lock.unlock();
    };
    ArrayList<Thread> threads = new ArrayList<>();
    final int numThreads = 10;
    for (int i = 0; i < numThreads; i++) {
      Thread thread = null;
      if (i % 2 == 0) {
        thread = new Thread(joinQuery);
      } else {
        thread = new Thread(selectQuery);
      }
      threads.add(thread);
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    for (ArrayList<DispatchableSubPlan> plans : queryPlans.values()) {
      for (DispatchableSubPlan plan : plans) {
        assertTrue(plan.equals(plans.get(0)));
      }
    }
  }

  @Test
  public void testQueryWithHint() {
    // Hinting the query to use final stage aggregation makes server directly return final result
    // This is useful when data is already partitioned by col1
    String query =
        "SELECT /*+ aggOptions(is_partitioned_by_group_by_keys='true') */ col1, COUNT(*) FROM b GROUP BY col1";
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
    Set<DispatchablePlanFragment> stagePlans = dispatchableSubPlan.getQueryStages();
    int numStages = stagePlans.size();
    assertEquals(numStages, 2);
    for (DispatchablePlanFragment stagePlan : stagePlans) {
      int stageId = stagePlan.getPlanFragment().getFragmentId();
      Map<QueryServerInstance, List<Integer>> serverToWorkerIdsMap = stagePlan.getServerInstanceToWorkerIdMap();
      int numServers = serverToWorkerIdsMap.size();
      String tableName = stagePlan.getTableName();
      if (tableName != null) {
        // table scan stages; for tableB it should have only 1
        assertEquals(numServers, 1);
        assertEquals(stagePlan.getServerInstanceToWorkerIdMap().keySet().iterator().next().getQueryMailboxPort(), 1);
      } else if (!PlannerUtils.isRootPlanFragment(stageId)) {
        // join stage should have both servers used.
        assertEquals(numServers, 2);
        for (QueryServerInstance server : serverToWorkerIdsMap.keySet()) {
          int port = server.getQueryMailboxPort();
          assertTrue(port == 1 || port == 2);
        }
      }
    }
  }

  @Test
  public void testGetTableNamesForQuery() {
    // A simple filter query with one table
    String query = "Select * from a where col1 = 'a'";
    List<String> tableNames = _queryEnvironment.getTableNamesForQuery(query);
    assertEquals(tableNames.size(), 1);
    assertEquals(tableNames.get(0), "a");

    // query with IN / NOT IN clause
    query = "SELECT COUNT(*) FROM a WHERE col1 IN (SELECT col1 FROM b) " + "and col1 NOT IN (SELECT col1 from c)";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    assertEquals(tableNames.size(), 3);
    Collections.sort(tableNames);
    assertEquals(tableNames.get(0), "a");
    assertEquals(tableNames.get(1), "b");
    assertEquals(tableNames.get(2), "c");

    // query with JOIN clause
    query = "SELECT a.col1, b.col2 FROM a JOIN b ON a.col3 = b.col3 WHERE a.col1 = 'a'";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    assertEquals(tableNames.size(), 2);
    Collections.sort(tableNames);
    assertEquals(tableNames.get(0), "a");
    assertEquals(tableNames.get(1), "b");

    // query with WHERE clause JOIN
    query = "SELECT a.col1, b.col2 FROM a, b WHERE a.col3 = b.col3 AND a.col1 = 'a'";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    assertEquals(tableNames.size(), 2);
    Collections.sort(tableNames);
    assertEquals(tableNames.get(0), "a");
    assertEquals(tableNames.get(1), "b");

    // query with JOIN clause and table alias
    query = "SELECT A.col1, B.col2 FROM a AS A JOIN b AS B ON A.col3 = B.col3 WHERE A.col1 = 'a'";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    assertEquals(tableNames.size(), 2);
    Collections.sort(tableNames);
    assertEquals(tableNames.get(0), "a");
    assertEquals(tableNames.get(1), "b");

    // query with UNION clause
    query = "SELECT * FROM a UNION ALL SELECT * FROM b UNION ALL SELECT * FROM c";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    assertEquals(tableNames.size(), 3);
    Collections.sort(tableNames);
    assertEquals(tableNames.get(0), "a");
    assertEquals(tableNames.get(1), "b");
    assertEquals(tableNames.get(2), "c");

    // query with UNION clause and table alias
    query = "SELECT * FROM (SELECT * FROM a) AS t1 UNION SELECT * FROM ( SELECT * FROM b) AS t2";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    assertEquals(tableNames.size(), 2);
    Collections.sort(tableNames);
    assertEquals(tableNames.get(0), "a");
    assertEquals(tableNames.get(1), "b");

    // query with UNION clause and table alias using WITH clause
    query = "WITH tmp1 AS (SELECT * FROM a), \n" + "tmp2 AS (SELECT * FROM b) \n"
        + "SELECT * FROM tmp1 UNION ALL SELECT * FROM tmp2";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    assertEquals(tableNames.size(), 2);
    Collections.sort(tableNames);
    assertEquals(tableNames.get(0), "a");
    assertEquals(tableNames.get(1), "b");

    // query with aliases, JOIN, IN/NOT-IN, group-by
    query = "with tmp as (select col1, sum(col3) as col3, count(*) from a where col1 = 'a' group by col1), "
        + "tmp2 as (select A.col1, B.col3 from b as A JOIN c AS B on A.col1 = B.col1) "
        + "select sum(col3) from tmp where col1 in (select col1 from tmp2) and col1 not in (select col1 from d)";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    assertEquals(tableNames.size(), 4);
    assertEquals(tableNames.get(0), "a");
    assertEquals(tableNames.get(1), "b");
    assertEquals(tableNames.get(2), "c");
    assertEquals(tableNames.get(3), "d");

    // query with aliases, JOIN, IN/NOT-IN, group-by and explain
    query = "explain plan for with tmp as (select col1, sum(col3) as col3, count(*) from a where col1 = 'a' "
        + "group by col1), tmp2 as (select A.col1, B.col3 from b as A JOIN c AS B on A.col1 = B.col1) "
        + "select sum(col3) from tmp where col1 in (select col1 from tmp2) and col1 not in (select col1 from d)";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    assertEquals(tableNames.size(), 4);
    assertEquals(tableNames.get(0), "a");
    assertEquals(tableNames.get(1), "b");
    assertEquals(tableNames.get(2), "c");
    assertEquals(tableNames.get(3), "d");

    // lateral join query
    query = "EXPLAIN PLAN FOR SELECT a.col1, newb.sum_col3 FROM a JOIN LATERAL "
        + "(SELECT SUM(col3) as sum_col3 FROM b WHERE col2 = a.col2) AS newb ON TRUE";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    assertEquals(tableNames.size(), 2);
    assertEquals(tableNames.get(0), "a");
    assertEquals(tableNames.get(1), "b");

    // test for self join queries
    query = "SELECT a.col1 FROM a JOIN(SELECT col2 FROM a) as self ON a.col1=self.col2 ";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    assertEquals(tableNames.size(), 1);
    assertEquals(tableNames.get(0), "a");
  }

  @Test
  public void testDuplicateWithAlias() {
    String query = "WITH tmp AS (SELECT * FROM a LIMIT 1), tmp AS (SELECT * FROM a LIMIT 2) SELECT * FROM tmp";
    RuntimeException e = expectThrows(RuntimeException.class, () -> _queryEnvironment.getTableNamesForQuery(query));
    assertTrue(e.getCause().getMessage().contains("Duplicate alias in WITH: 'tmp'"));
  }

  @Test
  public void testWindowFunctions() {
    String queryWithDefaultWindow = "SELECT col1, col2, RANK() OVER (PARTITION BY col1 ORDER BY col2) FROM a";
    _queryEnvironment.planQuery(queryWithDefaultWindow);

    String sumQueryWithCustomRowsWindow =
        "SELECT col1, col2, SUM(col3) OVER (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
            + " FROM a";
    _queryEnvironment.planQuery(sumQueryWithCustomRowsWindow);

    String queryWithUnboundedFollowingAsLowerBound =
        "SELECT col1, col2, SUM(col3) OVER (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN UNBOUNDED FOLLOWING AND "
            + "UNBOUNDED FOLLOWING) FROM a";
    RuntimeException e = expectThrows(RuntimeException.class,
        () -> _queryEnvironment.planQuery(queryWithUnboundedFollowingAsLowerBound));
    assertTrue(
        e.getCause().getMessage().contains("UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary"));

    String queryWithUnboundedPrecedingAsUpperBound =
        "SELECT col1, col2, SUM(col3) OVER (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN UNBOUNDED PRECEDING AND "
            + "UNBOUNDED PRECEDING) FROM a";
    e = expectThrows(RuntimeException.class,
        () -> _queryEnvironment.planQuery(queryWithUnboundedPrecedingAsUpperBound));
    assertTrue(
        e.getCause().getMessage().contains("UNBOUNDED PRECEDING cannot be specified for the upper frame boundary"));

    String queryWithOffsetFollowingAsLowerBoundAndOffsetPrecedingAsUpperBound =
        "SELECT col1, col2, SUM(col3) OVER (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING) "
            + "FROM a";
    e = expectThrows(RuntimeException.class,
        () -> _queryEnvironment.planQuery(queryWithOffsetFollowingAsLowerBoundAndOffsetPrecedingAsUpperBound));
    assertTrue(e.getCause().getMessage()
        .contains("Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING"));

    String queryWithValidBounds =
        "SELECT col1, col2, SUM(col3) OVER (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) "
            + "FROM a";
    _queryEnvironment.planQuery(queryWithValidBounds);

    // Custom RANGE window frame is not currently supported by Pinot
    String sumQueryWithCustomRangeWindow =
        "SELECT col1, col2, SUM(col3) OVER (PARTITION BY col1 ORDER BY col3 RANGE BETWEEN UNBOUNDED PRECEDING AND 1 "
            + "FOLLOWING) FROM a";
    e = expectThrows(RuntimeException.class, () -> _queryEnvironment.planQuery(sumQueryWithCustomRangeWindow));
    assertTrue(e.getCause().getMessage()
        .contains("RANGE window frame with offset PRECEDING / FOLLOWING is not supported"));

    // RANK, DENSE_RANK, ROW_NUMBER, NTILE, LAG, LEAD with custom window frame are invalid
    String rankQuery =
        "SELECT col1, col2, RANK() OVER (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) "
            + "FROM a";
    e = expectThrows(RuntimeException.class, () -> _queryEnvironment.planQuery(rankQuery));
    assertTrue(e.getMessage().contains("ROW/RANGE not allowed"));

    String denseRankQuery =
        "SELECT col1, col2, DENSE_RANK() OVER (PARTITION BY col1 ORDER BY col2 RANGE BETWEEN UNBOUNDED PRECEDING AND "
            + "1 FOLLOWING) FROM a";
    e = expectThrows(RuntimeException.class, () -> _queryEnvironment.planQuery(denseRankQuery));
    assertTrue(e.getMessage().contains("ROW/RANGE not allowed"));

    String rowNumberQuery =
        "SELECT col1, col2, ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2 RANGE BETWEEN UNBOUNDED PRECEDING AND "
            + "CURRENT ROW) FROM a";
    e = expectThrows(RuntimeException.class, () -> _queryEnvironment.planQuery(rowNumberQuery));
    assertTrue(e.getMessage().contains("ROW/RANGE not allowed"));

    String ntileQuery =
        "SELECT col1, col2, NTILE(10) OVER (PARTITION BY col1 ORDER BY col2 RANGE BETWEEN UNBOUNDED PRECEDING AND "
            + "CURRENT ROW) FROM a";
    e = expectThrows(RuntimeException.class, () -> _queryEnvironment.planQuery(ntileQuery));
    assertTrue(e.getMessage().contains("ROW/RANGE not allowed"));

    String lagQuery =
        "SELECT col1, col2, LAG(col2, 1) OVER (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN UNBOUNDED PRECEDING AND "
            + "UNBOUNDED FOLLOWING) FROM a";
    e = expectThrows(RuntimeException.class, () -> _queryEnvironment.planQuery(lagQuery));
    assertTrue(e.getMessage().contains("ROW/RANGE not allowed"));

    String leadQuery =
        "SELECT col1, col2, LEAD(col2, 1) OVER (PARTITION BY col1 ORDER BY col2 RANGE BETWEEN CURRENT ROW AND "
            + "UNBOUNDED FOLLOWING) FROM a";
    e = expectThrows(RuntimeException.class, () -> _queryEnvironment.planQuery(leadQuery));
    assertTrue(e.getMessage().contains("ROW/RANGE not allowed"));

    String ntileQueryWithNoArg =
        "SELECT col1, col2, NTILE() OVER (PARTITION BY col1 ORDER BY col2 RANGE BETWEEN UNBOUNDED PRECEDING AND "
            + "CURRENT ROW) FROM a";
    e = expectThrows(RuntimeException.class, () -> _queryEnvironment.planQuery(ntileQueryWithNoArg));
    assertTrue(e.getMessage().contains("expecting 1 argument"));

    String excludeCurrentRowQuery =
        "SELECT col1, col2, SUM(col3) OVER (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN UNBOUNDED PRECEDING AND "
            + "CURRENT ROW EXCLUDE CURRENT ROW) FROM a";
    e = expectThrows(RuntimeException.class, () -> _queryEnvironment.planQuery(excludeCurrentRowQuery));
    assertTrue(e.getMessage().contains("EXCLUDE clauses for window functions are not currently supported"));
  }

  @Test
  public void testLargeIn() {
    String query = "SELECT col1\n"
        + "FROM (\n"
        + "         SELECT col1\n"
        + "         FROM a\n"
        + "         WHERE col1 IN (\n"
        + "             'a0', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9',\n"
        + "             'a10', 'a11', 'a12', 'a13', 'a14', 'a15', 'a16', 'a17', 'a18', 'a19',\n"
        + "             'a20', 'a21', 'a22', 'a23', 'a24', 'a25', 'a26', 'a27', 'a28', 'a29',\n"
        + "             'a30', 'a31', 'a32', 'a33', 'a34', 'a35', 'a36', 'a37', 'a38', 'a39',\n"
        + "             'a40', 'a41', 'a42', 'a43', 'a44', 'a45', 'a46', 'a47', 'a48', 'a49',\n"
        + "             'a50', 'a51', 'a52', 'a53', 'a54', 'a55', 'a56', 'a57', 'a58', 'a59',\n"
        + "             'a60', 'a61', 'a62', 'a63', 'a64', 'a65', 'a66', 'a67', 'a68', 'a69',\n"
        + "             'a70', 'a71', 'a72', 'a73', 'a74', 'a75', 'a76', 'a77', 'a78', 'a79',\n"
        + "             'a80', 'a81', 'a82', 'a83', 'a84', 'a85', 'a86', 'a87', 'a88', 'a89',\n"
        + "             'a90', 'a91', 'a92', 'a93', 'a94', 'a95', 'a96', 'a97', 'a98', 'a99',\n"
        + "             'a100', 'a101', 'a102', 'a103', 'a104', 'a105', 'a106', 'a107', 'a108', 'a109',\n"
        + "             'a110', 'a111', 'a112', 'a113', 'a114', 'a115', 'a116', 'a117', 'a118', 'a119',\n"
        + "             'a120', 'a121', 'a122', 'a123', 'a124', 'a125', 'a126', 'a127', 'a128', 'a129',\n"
        + "             'a130', 'a131', 'a132', 'a133', 'a134', 'a135', 'a136', 'a137', 'a138', 'a139',\n"
        + "             'a140', 'a141', 'a142', 'a143', 'a144', 'a145', 'a146', 'a147', 'a148', 'a149',\n"
        + "             'a150', 'a151', 'a152', 'a153', 'a154', 'a155', 'a156', 'a157', 'a158', 'a159',\n"
        + "             'a160', 'a161', 'a162', 'a163', 'a164', 'a165', 'a166', 'a167', 'a168', 'a169',\n"
        + "             'a170', 'a171', 'a172', 'a173', 'a174', 'a175', 'a176', 'a177', 'a178', 'a179',\n"
        + "             'a180', 'a181', 'a182', 'a183', 'a184', 'a185', 'a186', 'a187', 'a188', 'a189',\n"
        + "             'a190', 'a191', 'a192', 'a193', 'a194', 'a195', 'a196', 'a197', 'a198', 'a199',\n"
        + "             'a200', 'a201', 'a202', 'a203', 'a204', 'a205', 'a206', 'a207', 'a208', 'a209',\n"
        + "             'a210', 'a211', 'a212', 'a213', 'a214', 'a215', 'a216', 'a217', 'a218', 'a219',\n"
        + "             'a220', 'a221', 'a222', 'a223', 'a224', 'a225', 'a226', 'a227', 'a228', 'a229',\n"
        + "             'a230', 'a231', 'a232', 'a233', 'a234', 'a235', 'a236', 'a237', 'a238', 'a239',\n"
        + "             'a240', 'a241', 'a242', 'a243', 'a244', 'a245', 'a246', 'a247', 'a248', 'a249',\n"
        + "             'a250', 'a251', 'a252', 'a253', 'a254', 'a255', 'a256', 'a257', 'a258', 'a259',\n"
        + "             'a260', 'a261', 'a262', 'a263', 'a264', 'a265', 'a266', 'a267', 'a268', 'a269',\n"
        + "             'a270', 'a271', 'a272', 'a273', 'a274', 'a275', 'a276', 'a277', 'a278', 'a279',\n"
        + "             'a280', 'a281', 'a282', 'a283', 'a284', 'a285', 'a286', 'a287', 'a288', 'a289',\n"
        + "             'a290', 'a291', 'a292', 'a293', 'a294', 'a295', 'a296', 'a297', 'a298', 'a299',\n"
        + "             'a300', 'a301', 'a302', 'a303', 'a304', 'a305', 'a306', 'a307', 'a308', 'a309',\n"
        + "             'a310', 'a311', 'a312', 'a313', 'a314', 'a315', 'a316', 'a317', 'a318', 'a319',\n"
        + "             'a320', 'a321', 'a322', 'a323', 'a324', 'a325', 'a326', 'a327', 'a328', 'a329',\n"
        + "             'a330', 'a331', 'a332', 'a333', 'a334', 'a335', 'a336', 'a337', 'a338', 'a339',\n"
        + "             'a340', 'a341', 'a342', 'a343', 'a344', 'a345', 'a346', 'a347', 'a348', 'a349',\n"
        + "             'a350', 'a351', 'a352', 'a353', 'a354', 'a355', 'a356', 'a357', 'a358', 'a359',\n"
        + "             'a360', 'a361', 'a362', 'a363', 'a364', 'a365', 'a366', 'a367', 'a368', 'a369',\n"
        + "             'a370', 'a371', 'a372', 'a373', 'a374', 'a375', 'a376', 'a377', 'a378', 'a379',\n"
        + "             'a380', 'a381', 'a382', 'a383', 'a384', 'a385', 'a386', 'a387', 'a388', 'a389',\n"
        + "             'a390', 'a391', 'a392', 'a393', 'a394', 'a395', 'a396', 'a397', 'a398', 'a399',\n"
        + "             'a400', 'a401', 'a402', 'a403', 'a404', 'a405', 'a406', 'a407', 'a408', 'a409',\n"
        + "             'a410', 'a411', 'a412', 'a413', 'a414', 'a415', 'a416', 'a417', 'a418', 'a419',\n"
        + "             'a420', 'a421', 'a422', 'a423', 'a424', 'a425', 'a426', 'a427', 'a428', 'a429',\n"
        + "             'a430', 'a431', 'a432', 'a433', 'a434', 'a435', 'a436', 'a437', 'a438', 'a439',\n"
        + "             'a440', 'a441', 'a442', 'a443', 'a444', 'a445', 'a446', 'a447', 'a448', 'a449',\n"
        + "             'a450', 'a451', 'a452', 'a453', 'a454', 'a455', 'a456', 'a457', 'a458', 'a459',\n"
        + "             'a460', 'a461', 'a462', 'a463', 'a464', 'a465', 'a466', 'a467', 'a468', 'a469',\n"
        + "             'a470', 'a471', 'a472', 'a473', 'a474', 'a475', 'a476', 'a477', 'a478', 'a479',\n"
        + "             'a480', 'a481', 'a482', 'a483', 'a484', 'a485', 'a486', 'a487', 'a488', 'a489',\n"
        + "             'a490', 'a491', 'a492', 'a493', 'a494', 'a495', 'a496', 'a497', 'a498', 'a499'\n"
        + "         )\n"
        + "     )\n"
        + "GROUP BY col1;";
    _queryEnvironment.planQuery(query);
  }

  // --------------------------------------------------------------------------
  // Test Utils.
  // --------------------------------------------------------------------------

  private static void assertNodeTypeNotIn(PlanNode node, List<Class<? extends BasePlanNode>> bannedNodeType) {
    assertFalse(isOneOf(bannedNodeType, node));
    for (PlanNode child : node.getInputs()) {
      assertNodeTypeNotIn(child, bannedNodeType);
    }
  }

  private static boolean isOneOf(List<Class<? extends BasePlanNode>> allowedNodeTypes, PlanNode node) {
    for (Class<? extends BasePlanNode> allowedNodeType : allowedNodeTypes) {
      if (node.getClass() == allowedNodeType) {
        return true;
      }
    }
    return false;
  }

  @DataProvider(name = "testQueryExceptionDataProvider")
  private Object[][] provideQueriesWithException() {
    return new Object[][]{
        // wrong table is being used after JOIN
        new Object[]{"SELECT b.col1 - a.col3 FROM a JOIN c ON a.col1 = c.col3", "Table 'b' not found"},
        // non-agg column not being grouped
        new Object[]{"SELECT a.col1, SUM(a.col3) FROM a", "'a.col1' is not being grouped"},
        // empty IN clause fails compilation
        new Object[]{"SELECT a.col1 FROM a WHERE a.col1 IN ()", "Encountered \"\" at line"},
        // AT TIME ZONE should fail
        new Object[]{"SELECT a.col1 AT TIME ZONE 'PST' FROM a", "No match found for function signature AT_TIME_ZONE"},
    };
  }

  @DataProvider(name = "testQueryLogicalPlanDataProvider")
  private Object[][] provideQueriesWithExplainedLogicalPlan() {
    //@formatter:off
    return new Object[][] {
        new Object[]{"EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR SELECT col1, col3 FROM a",
              "{\n"
            + "  \"rels\": [\n"
            + "    {\n"
            + "      \"id\": \"0\",\n"
            + "      \"relOp\": \"org.apache.pinot.calcite.rel.logical.PinotLogicalTableScan\",\n"
            + "      \"table\": [\n"
            + "        \"default\",\n"
            + "        \"a\"\n"
            + "      ],\n"
            + "      \"inputs\": [],\n"
            + "      \"type\": \"PinotLogicalTableScan\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": \"1\",\n"
            + "      \"relOp\": \"LogicalProject\",\n"
            + "      \"fields\": [\n"
            + "        \"col1\",\n"
            + "        \"col3\"\n"
            + "      ],\n"
            + "      \"exprs\": [\n"
            + "        {\n"
            + "          \"input\": 0,\n"
            + "          \"name\": \"$0\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"input\": 2,\n"
            + "          \"name\": \"$2\"\n"
            + "        }\n"
            + "      ],\n"
            + "      \"type\": \"LogicalProject\"\n"
            + "    }\n"
            + "  ]\n"
            + "}"},
        new Object[]{"EXPLAIN PLAN EXCLUDING ATTRIBUTES AS DOT FOR SELECT col1, COUNT(*) FROM a GROUP BY col1",
              "Execution Plan\n"
            + "digraph {\n"
            + "\"PinotLogicalExchange\\n\" -> \"PinotLogicalAggregat\\ne\\n\" [label=\"0\"]\n"
            + "\"PinotLogicalAggregat\\ne\\n\" -> \"PinotLogicalExchange\\n\" [label=\"0\"]\n"
            + "\"PinotLogicalTableSca\\nn\\n\" -> \"PinotLogicalAggregat\\ne\\n\" [label=\"0\"]\n"
            + "}\n"
        },
        new Object[]{"EXPLAIN PLAN FOR SELECT a.col1, b.col3 FROM a JOIN b ON a.col1 = b.col1",
              "Execution Plan\n"
            + "LogicalProject(col1=[$0], col3=[$2])\n"
            + "  LogicalJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "    PinotLogicalExchange(distribution=[hash[0]])\n"
            + "      LogicalProject(col1=[$0])\n"
            + "        PinotLogicalTableScan(table=[[default, a]])\n"
            + "    PinotLogicalExchange(distribution=[hash[0]])\n"
            + "      LogicalProject(col1=[$0], col3=[$2])\n"
            + "        PinotLogicalTableScan(table=[[default, b]])\n"
        },
    };
    //@formatter:on
  }
}
