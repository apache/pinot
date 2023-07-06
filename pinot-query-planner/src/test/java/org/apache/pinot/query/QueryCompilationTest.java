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
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.DispatchablePlanFragment;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.apache.pinot.query.planner.ExplainPlanPlanVisitor;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryCompilationTest extends QueryEnvironmentTestBase {

  @Test(dataProvider = "testQueryPlanDataProvider")
  public void testQueryPlanExplain(String query, String digest)
      throws Exception {
    try {
      long requestId = RANDOM_REQUEST_ID_GEN.nextLong();
      String explainedPlan = _queryEnvironment.explainQuery(query, requestId);
      Assert.assertEquals(stripPhysicalPlan(explainedPlan), digest);
    } catch (RuntimeException e) {
      Assert.fail("failed to explain query: " + query, e);
    }
  }

  @Test(dataProvider = "testQueryDataProvider")
  public void testQueryPlanWithoutException(String query)
      throws Exception {
    try {
      DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
      Assert.assertNotNull(dispatchableSubPlan);
    } catch (RuntimeException e) {
      Assert.fail("failed to plan query: " + query, e);
    }
  }

  @Test(dataProvider = "testQueryExceptionDataProvider")
  public void testQueryWithException(String query, String exceptionSnippet) {
    try {
      _queryEnvironment.planQuery(query);
      Assert.fail("query plan should throw exception");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause().getMessage().contains(exceptionSnippet));
    }
  }

  private static void assertGroupBySingletonAfterJoin(DispatchableSubPlan dispatchableSubPlan, boolean shouldRewrite)
      throws Exception {

    for (int stageId = 0; stageId < dispatchableSubPlan.getQueryStageList().size(); stageId++) {
      if (dispatchableSubPlan.getTableNames().size() == 0 && !PlannerUtils.isRootPlanFragment(stageId)) {
        PlanNode node = dispatchableSubPlan.getQueryStageList().get(stageId).getPlanFragment().getFragmentRoot();
        while (node != null) {
          if (node instanceof JoinNode) {
            // JOIN is exchanged with hash distribution (data shuffle)
            MailboxReceiveNode left = (MailboxReceiveNode) node.getInputs().get(0);
            MailboxReceiveNode right = (MailboxReceiveNode) node.getInputs().get(1);
            Assert.assertEquals(left.getDistributionType(), RelDistribution.Type.HASH_DISTRIBUTED);
            Assert.assertEquals(right.getDistributionType(), RelDistribution.Type.HASH_DISTRIBUTED);
            break;
          }
          if (node instanceof AggregateNode && node.getInputs().get(0) instanceof MailboxReceiveNode) {
            // AGG is exchanged with singleton since it has already been distributed by JOIN.
            MailboxReceiveNode input = (MailboxReceiveNode) node.getInputs().get(0);
            if (shouldRewrite) {
              Assert.assertEquals(input.getDistributionType(), RelDistribution.Type.SINGLETON);
            } else {
              Assert.assertNotEquals(input.getDistributionType(), RelDistribution.Type.SINGLETON);
            }
            break;
          }
          node = node.getInputs().get(0);
        }
      }
    }
  }

  @Test
  public void testQueryAndAssertStageContentForJoin()
      throws Exception {
    String query = "SELECT * FROM a JOIN b ON a.col1 = b.col2";
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
    Assert.assertEquals(dispatchableSubPlan.getQueryStageList().size(), 4);

    for (int stageId = 0; stageId < dispatchableSubPlan.getQueryStageList().size(); stageId++) {
      DispatchablePlanFragment dispatchablePlanFragment = dispatchableSubPlan.getQueryStageList().get(stageId);
      String tableName = dispatchablePlanFragment.getTableName();
      if (tableName != null) {
        // table scan stages; for tableA it should have 2 hosts, for tableB it should have only 1
        Assert.assertEquals(dispatchablePlanFragment.getServerInstanceToWorkerIdMap().entrySet().stream()
                .map(ExplainPlanPlanVisitor::stringifyQueryServerInstanceToWorkerIdsEntry).collect(Collectors.toSet()),
            tableName.equals("a") ? ImmutableList.of("localhost@{1,1}|[1]", "localhost@{2,2}|[0]")
                : ImmutableList.of("localhost@{1,1}|[0]"));
      } else if (!PlannerUtils.isRootPlanFragment(stageId)) {
        // join stage should have both servers used.
        Assert.assertEquals(dispatchablePlanFragment.getServerInstanceToWorkerIdMap().entrySet().stream()
                .map(ExplainPlanPlanVisitor::stringifyQueryServerInstanceToWorkerIdsEntry).collect(Collectors.toSet()),
            ImmutableSet.of("localhost@{1,1}|[1]", "localhost@{2,2}|[0]"));
      } else {
        // reduce stage should have the reducer instance.
        Assert.assertEquals(dispatchablePlanFragment.getServerInstanceToWorkerIdMap().entrySet().stream()
                .map(ExplainPlanPlanVisitor::stringifyQueryServerInstanceToWorkerIdsEntry).collect(Collectors.toSet()),
            ImmutableSet.of("localhost@{3,3}|[0]"));
      }
    }
  }

  @Test
  public void testQueryProjectFilterPushDownForJoin() {
    String query = "SELECT a.col1, a.ts, b.col2, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
        + "WHERE a.col3 >= 0 AND a.col2 IN ('b') AND b.col3 < 0";
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
    List<DispatchablePlanFragment> intermediateStages =
        dispatchableSubPlan.getQueryStageList().stream().filter(q -> q.getTableName() == null)
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
    List<DispatchablePlanFragment> tableScanMetadataList = dispatchableSubPlan.getQueryStageList().stream()
        .filter(stageMetadata -> stageMetadata.getTableName() != null).collect(Collectors.toList());
    Assert.assertEquals(tableScanMetadataList.size(), 1);
    Assert.assertEquals(tableScanMetadataList.get(0).getServerInstanceToWorkerIdMap().size(), 2);

    query = "SELECT * FROM d_REALTIME";
    dispatchableSubPlan = _queryEnvironment.planQuery(query);
    tableScanMetadataList = dispatchableSubPlan.getQueryStageList().stream()
        .filter(stageMetadata -> stageMetadata.getTableName() != null).collect(Collectors.toList());
    Assert.assertEquals(tableScanMetadataList.size(), 1);
    Assert.assertEquals(tableScanMetadataList.get(0).getServerInstanceToWorkerIdMap().size(), 1);

    query = "SELECT * FROM d";
    dispatchableSubPlan = _queryEnvironment.planQuery(query);
    tableScanMetadataList = dispatchableSubPlan.getQueryStageList().stream()
        .filter(stageMetadata -> stageMetadata.getTableName() != null).collect(Collectors.toList());
    Assert.assertEquals(tableScanMetadataList.size(), 1);
    Assert.assertEquals(tableScanMetadataList.get(0).getServerInstanceToWorkerIdMap().size(), 2);
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
        Assert.assertTrue(plan.equals(plans.get(0)));
      }
    }
  }

  @Test
  public void testQueryWithHint()
      throws Exception {
    // Hinting the query to use final stage aggregation makes server directly return final result
    // This is useful when data is already partitioned by col1
    String query = "SELECT /*+ aggOptionsInternal(agg_type='DIRECT') */ col1, COUNT(*) FROM b GROUP BY col1";
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
    Assert.assertEquals(dispatchableSubPlan.getQueryStageList().size(), 2);
    for (int stageId = 0; stageId < dispatchableSubPlan.getQueryStageList().size(); stageId++) {
      DispatchablePlanFragment dispatchablePlanFragment = dispatchableSubPlan.getQueryStageList().get(stageId);
      String tableName = dispatchablePlanFragment.getTableName();
      if (tableName != null) {
        // table scan stages; for tableB it should have only 1
        Assert.assertEquals(dispatchablePlanFragment.getServerInstanceToWorkerIdMap().entrySet().stream()
                .map(ExplainPlanPlanVisitor::stringifyQueryServerInstanceToWorkerIdsEntry).collect(Collectors.toSet()),
            ImmutableList.of("localhost@{1,1}|[0]"));
      } else if (!PlannerUtils.isRootPlanFragment(stageId)) {
        // join stage should have both servers used.
        Assert.assertEquals(dispatchablePlanFragment.getServerInstanceToWorkerIdMap().entrySet().stream()
                .map(ExplainPlanPlanVisitor::stringifyQueryServerInstanceToWorkerIdsEntry).collect(Collectors.toSet()),
            ImmutableList.of("localhost@{1,1}|[1]", "localhost@{2,2}|[0]"));
      }
    }
  }

  @Test
  public void testGetTableNamesForQuery() {
    // A simple filter query with one table
    String query = "Select * from a where col1 = 'a'";
    List<String> tableNames = _queryEnvironment.getTableNamesForQuery(query);
    Assert.assertEquals(tableNames.size(), 1);
    Assert.assertEquals(tableNames.get(0), "a");

    // query with IN / NOT IN clause
    query = "SELECT COUNT(*) FROM a WHERE col1 IN (SELECT col1 FROM b) "
        + "and col1 NOT IN (SELECT col1 from c)";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    Assert.assertEquals(tableNames.size(), 3);
    Collections.sort(tableNames);
    Assert.assertEquals(tableNames.get(0), "a");
    Assert.assertEquals(tableNames.get(1), "b");
    Assert.assertEquals(tableNames.get(2), "c");

    // query with JOIN clause
    query = "SELECT a.col1, b.col2 FROM a JOIN b ON a.col3 = b.col3 WHERE a.col1 = 'a'";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    Assert.assertEquals(tableNames.size(), 2);
    Collections.sort(tableNames);
    Assert.assertEquals(tableNames.get(0), "a");
    Assert.assertEquals(tableNames.get(1), "b");

    // query with WHERE clause JOIN
    query = "SELECT a.col1, b.col2 FROM a, b WHERE a.col3 = b.col3 AND a.col1 = 'a'";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    Assert.assertEquals(tableNames.size(), 2);
    Collections.sort(tableNames);
    Assert.assertEquals(tableNames.get(0), "a");
    Assert.assertEquals(tableNames.get(1), "b");

    // query with JOIN clause and table alias
    query = "SELECT A.col1, B.col2 FROM a AS A JOIN b AS B ON A.col3 = B.col3 WHERE A.col1 = 'a'";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    Assert.assertEquals(tableNames.size(), 2);
    Collections.sort(tableNames);
    Assert.assertEquals(tableNames.get(0), "a");
    Assert.assertEquals(tableNames.get(1), "b");

    // query with UNION clause
    query = "SELECT * FROM a UNION ALL SELECT * FROM b UNION ALL SELECT * FROM c";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    Assert.assertEquals(tableNames.size(), 3);
    Collections.sort(tableNames);
    Assert.assertEquals(tableNames.get(0), "a");
    Assert.assertEquals(tableNames.get(1), "b");
    Assert.assertEquals(tableNames.get(2), "c");

    // query with UNION clause and table alias
    query = "SELECT * FROM (SELECT * FROM a) AS t1 UNION SELECT * FROM ( SELECT * FROM b) AS t2";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    Assert.assertEquals(tableNames.size(), 2);
    Collections.sort(tableNames);
    Assert.assertEquals(tableNames.get(0), "a");
    Assert.assertEquals(tableNames.get(1), "b");

    // query with UNION clause and table alias using WITH clause
    query = "WITH tmp1 AS (SELECT * FROM a), \n"
        + "tmp2 AS (SELECT * FROM b) \n"
        + "SELECT * FROM tmp1 UNION ALL SELECT * FROM tmp2";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    Assert.assertEquals(tableNames.size(), 2);
    Collections.sort(tableNames);
    Assert.assertEquals(tableNames.get(0), "a");
    Assert.assertEquals(tableNames.get(1), "b");

    // query with aliases, JOIN, IN/NOT-IN, group-by
    query = "with tmp as (select col1, sum(col3) as col3, count(*) from a where col1 = 'a' group by col1), "
        + "tmp2 as (select A.col1, B.col3 from b as A JOIN c AS B on A.col1 = B.col1) "
        + "select sum(col3) from tmp where col1 in (select col1 from tmp2) and col1 not in (select col1 from d)";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    Assert.assertEquals(tableNames.size(), 4);
    Assert.assertEquals(tableNames.get(0), "a");
    Assert.assertEquals(tableNames.get(1), "b");
    Assert.assertEquals(tableNames.get(2), "c");
    Assert.assertEquals(tableNames.get(3), "d");

    // query with aliases, JOIN, IN/NOT-IN, group-by and explain
    query = "explain plan for with tmp as (select col1, sum(col3) as col3, count(*) from a where col1 = 'a' "
        + "group by col1), tmp2 as (select A.col1, B.col3 from b as A JOIN c AS B on A.col1 = B.col1) "
        + "select sum(col3) from tmp where col1 in (select col1 from tmp2) and col1 not in (select col1 from d)";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    Assert.assertEquals(tableNames.size(), 4);
    Assert.assertEquals(tableNames.get(0), "a");
    Assert.assertEquals(tableNames.get(1), "b");
    Assert.assertEquals(tableNames.get(2), "c");
    Assert.assertEquals(tableNames.get(3), "d");

    // lateral join query
    query = "EXPLAIN PLAN FOR SELECT a.col1, newb.sum_col3 FROM a JOIN LATERAL "
        + "(SELECT SUM(col3) as sum_col3 FROM b WHERE col2 = a.col2) AS newb ON TRUE";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    Assert.assertEquals(tableNames.size(), 2);
    Assert.assertEquals(tableNames.get(0), "a");
    Assert.assertEquals(tableNames.get(1), "b");

    // test for self join queries
    query = "SELECT a.col1 FROM a JOIN(SELECT col2 FROM a) as self ON a.col1=self.col2 ";
    tableNames = _queryEnvironment.getTableNamesForQuery(query);
    Assert.assertEquals(tableNames.size(), 1);
    Assert.assertEquals(tableNames.get(0), "a");
  }

  // --------------------------------------------------------------------------
  // Test Utils.
  // --------------------------------------------------------------------------

  private static void assertNodeTypeNotIn(PlanNode node, List<Class<? extends AbstractPlanNode>> bannedNodeType) {
    Assert.assertFalse(isOneOf(bannedNodeType, node));
    for (PlanNode child : node.getInputs()) {
      assertNodeTypeNotIn(child, bannedNodeType);
    }
  }

  private static boolean isOneOf(List<Class<? extends AbstractPlanNode>> allowedNodeTypes, PlanNode node) {
    for (Class<? extends AbstractPlanNode> allowedNodeType : allowedNodeTypes) {
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
        // CASE WHEN with non-consolidated result type at compile time.
        new Object[]{
            "SELECT SUM(CASE WHEN col3 > 10 THEN 1 WHEN col3 > 20 THEN 2 WHEN col3 > 30 THEN 3 "
                + "WHEN col3 > 40 THEN 4 WHEN col3 > 50 THEN '5' ELSE 0 END) FROM a", "while converting CASE WHEN"
        },
    };
  }

  @DataProvider(name = "testQueryPlanDataProvider")
  private Object[][] provideQueriesWithExplainedPlan() {
    //@formatter:off
    return new Object[][] {
        new Object[]{"EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR SELECT col1, col3 FROM a",
              "{\n"
            + "  \"rels\": [\n"
            + "    {\n"
            + "      \"id\": \"0\",\n"
            + "      \"relOp\": \"LogicalTableScan\",\n"
            + "      \"table\": [\n"
            + "        \"a\"\n"
            + "      ],\n"
            + "      \"inputs\": []\n"
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
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
            + "}"},
        new Object[]{"EXPLAIN PLAN EXCLUDING ATTRIBUTES AS DOT FOR SELECT col1, COUNT(*) FROM a GROUP BY col1",
              "Execution Plan\n"
            + "digraph {\n"
            + "\"PinotLogicalExchange\\n\" -> \"LogicalAggregate\\n\" [label=\"0\"]\n"
            + "\"LogicalAggregate\\n\" -> \"PinotLogicalExchange\\n\" [label=\"0\"]\n"
            + "\"LogicalTableScan\\n\" -> \"LogicalAggregate\\n\" [label=\"0\"]\n"
            + "}\n"},
        new Object[]{"EXPLAIN PLAN FOR SELECT a.col1, b.col3 FROM a JOIN b ON a.col1 = b.col1",
              "Execution Plan\n"
            + "LogicalProject(col1=[$0], col3=[$2])\n"
            + "  LogicalJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "    PinotLogicalExchange(distribution=[hash[0]])\n"
            + "      LogicalProject(col1=[$0])\n"
            + "        LogicalTableScan(table=[[a]])\n"
            + "    PinotLogicalExchange(distribution=[hash[0]])\n"
            + "      LogicalProject(col1=[$0], col3=[$2])\n"
            + "        LogicalTableScan(table=[[b]])\n"
        },
    };
    //@formatter:on
  }
}
