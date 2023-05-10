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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.ExplainPlanPlanVisitor;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.physical.DispatchablePlanMetadata;
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
      String explainedPlan = _queryEnvironment.explainQuery(query);
      Assert.assertEquals(explainedPlan, digest);
    } catch (RuntimeException e) {
      Assert.fail("failed to explain query: " + query, e);
    }
  }

  @Test(dataProvider = "testQueryDataProvider")
  public void testQueryPlanWithoutException(String query)
      throws Exception {
    try {
      QueryPlan queryPlan = _queryEnvironment.planQuery(query);
      Assert.assertNotNull(queryPlan);
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

  private static void assertGroupBySingletonAfterJoin(QueryPlan queryPlan, boolean shouldRewrite)
      throws Exception {
    for (Map.Entry<Integer, DispatchablePlanMetadata> e : queryPlan.getDispatchablePlanMetadataMap().entrySet()) {
      if (e.getValue().getScannedTables().size() == 0 && !PlannerUtils.isRootPlanFragment(e.getKey())) {
        PlanNode node = queryPlan.getQueryStageMap().get(e.getKey());
        while (node != null) {
          if (node instanceof JoinNode) {
            // JOIN is exchanged with hash distribution (data shuffle)
            MailboxReceiveNode left = (MailboxReceiveNode) node.getInputs().get(0);
            MailboxReceiveNode right = (MailboxReceiveNode) node.getInputs().get(1);
            Assert.assertEquals(left.getExchangeType(), RelDistribution.Type.HASH_DISTRIBUTED);
            Assert.assertEquals(right.getExchangeType(), RelDistribution.Type.HASH_DISTRIBUTED);
            break;
          }
          if (node instanceof AggregateNode && node.getInputs().get(0) instanceof MailboxReceiveNode) {
            // AGG is exchanged with singleton since it has already been distributed by JOIN.
            MailboxReceiveNode input = (MailboxReceiveNode) node.getInputs().get(0);
            if (shouldRewrite) {
              Assert.assertEquals(input.getExchangeType(), RelDistribution.Type.SINGLETON);
            } else {
              Assert.assertNotEquals(input.getExchangeType(), RelDistribution.Type.SINGLETON);
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
    QueryPlan queryPlan = _queryEnvironment.planQuery(query);
    Assert.assertEquals(queryPlan.getQueryStageMap().size(), 4);
    Assert.assertEquals(queryPlan.getDispatchablePlanMetadataMap().size(), 4);
    for (Map.Entry<Integer, DispatchablePlanMetadata> e : queryPlan.getDispatchablePlanMetadataMap().entrySet()) {
      List<String> tables = e.getValue().getScannedTables();
      if (tables.size() == 1) {
        // table scan stages; for tableA it should have 2 hosts, for tableB it should have only 1
        Assert.assertEquals(
            e.getValue().getServerInstanceToWorkerIdMap().entrySet().stream()
                .map(ExplainPlanPlanVisitor::stringifyQueryServerInstanceToWorkerIdsEntry)
                .collect(Collectors.toSet()),
            tables.get(0).equals("a") ? ImmutableList.of("localhost@{1,1}|[1]", "localhost@{2,2}|[0]")
                : ImmutableList.of("localhost@{1,1}|[0]"));
      } else if (!PlannerUtils.isRootPlanFragment(e.getKey())) {
        // join stage should have both servers used.
        Assert.assertEquals(
            e.getValue().getServerInstanceToWorkerIdMap().entrySet().stream()
                .map(ExplainPlanPlanVisitor::stringifyQueryServerInstanceToWorkerIdsEntry)
                .collect(Collectors.toSet()),
            ImmutableSet.of("localhost@{1,1}|[1]", "localhost@{2,2}|[0]"));
      } else {
        // reduce stage should have the reducer instance.
        Assert.assertEquals(
            e.getValue().getServerInstanceToWorkerIdMap().entrySet().stream()
                .map(ExplainPlanPlanVisitor::stringifyQueryServerInstanceToWorkerIdsEntry)
                .collect(Collectors.toSet()),
            ImmutableSet.of("localhost@{3,3}|[0]"));
      }
    }
  }

  @Test
  public void testQueryProjectFilterPushDownForJoin() {
    String query = "SELECT a.col1, a.ts, b.col2, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
        + "WHERE a.col3 >= 0 AND a.col2 IN ('b') AND b.col3 < 0";
    QueryPlan queryPlan = _queryEnvironment.planQuery(query);
    List<PlanNode> intermediateStageRoots =
        queryPlan.getDispatchablePlanMetadataMap().entrySet().stream()
            .filter(e -> e.getValue().getScannedTables().size() == 0)
            .map(e -> queryPlan.getQueryStageMap().get(e.getKey())).collect(Collectors.toList());
    // Assert that no project of filter node for any intermediate stage because all should've been pushed down.
    for (PlanNode roots : intermediateStageRoots) {
      assertNodeTypeNotIn(roots, ImmutableList.of(ProjectNode.class, FilterNode.class));
    }
  }

  @Test
  public void testQueryRoutingManagerCompilation() {
    String query = "SELECT * FROM d_OFFLINE";
    QueryPlan queryPlan = _queryEnvironment.planQuery(query);
    List<DispatchablePlanMetadata> tableScanMetadataList = queryPlan.getDispatchablePlanMetadataMap().values().stream()
        .filter(planFragmentMetadata -> planFragmentMetadata.getScannedTables().size() != 0)
        .collect(Collectors.toList());
    Assert.assertEquals(tableScanMetadataList.size(), 1);
    Assert.assertEquals(tableScanMetadataList.get(0).getServerInstanceToWorkerIdMap().size(), 2);

    query = "SELECT * FROM d_REALTIME";
    queryPlan = _queryEnvironment.planQuery(query);
    tableScanMetadataList = queryPlan.getDispatchablePlanMetadataMap().values().stream()
        .filter(planFragmentMetadata -> planFragmentMetadata.getScannedTables().size() != 0)
        .collect(Collectors.toList());
    Assert.assertEquals(tableScanMetadataList.size(), 1);
    Assert.assertEquals(tableScanMetadataList.get(0).getServerInstanceToWorkerIdMap().size(), 1);

    query = "SELECT * FROM d";
    queryPlan = _queryEnvironment.planQuery(query);
    tableScanMetadataList = queryPlan.getDispatchablePlanMetadataMap().values().stream()
        .filter(planFragmentMetadata -> planFragmentMetadata.getScannedTables().size() != 0)
        .collect(Collectors.toList());
    Assert.assertEquals(tableScanMetadataList.size(), 1);
    Assert.assertEquals(tableScanMetadataList.get(0).getServerInstanceToWorkerIdMap().size(), 2);
  }

  // Test that plan query can be run as multi-thread.
  @Test
  public void testPlanQueryMultiThread()
      throws Exception {
    Map<String, ArrayList<QueryPlan>> queryPlans = new HashMap<>();
    Lock lock = new ReentrantLock();
    Runnable joinQuery = () -> {
      String query = "SELECT a.col1, a.ts, b.col2, b.col3 FROM a JOIN b ON a.col1 = b.col2";
      QueryPlan queryPlan = _queryEnvironment.planQuery(query);
      lock.lock();
      if (!queryPlans.containsKey(queryPlan)) {
        queryPlans.put(query, new ArrayList<>());
      }
      queryPlans.get(query).add(queryPlan);
      lock.unlock();
    };
    Runnable selectQuery = () -> {
      String query = "SELECT * FROM a";
      QueryPlan queryPlan = _queryEnvironment.planQuery(query);
      lock.lock();
      if (!queryPlans.containsKey(queryPlan)) {
        queryPlans.put(query, new ArrayList<>());
      }
      queryPlans.get(query).add(queryPlan);
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
    for (ArrayList<QueryPlan> plans : queryPlans.values()) {
      for (QueryPlan plan : plans) {
        Assert.assertTrue(plan.equals(plans.get(0)));
      }
    }
  }

  @Test
  public void testQueryWithHint()
      throws Exception {
    // Hinting the query to use final stage aggregation makes server directly return final result
    // This is useful when data is already partitioned by col1
    String query = "SELECT /*+ aggFinalStage */ col1, COUNT(*) FROM b GROUP BY col1";
    QueryPlan queryPlan = _queryEnvironment.planQuery(query);
    Assert.assertEquals(queryPlan.getQueryStageMap().size(), 2);
    Assert.assertEquals(queryPlan.getDispatchablePlanMetadataMap().size(), 2);
    for (Map.Entry<Integer, DispatchablePlanMetadata> e : queryPlan.getDispatchablePlanMetadataMap().entrySet()) {
      List<String> tables = e.getValue().getScannedTables();
      if (tables.size() != 0) {
        // table scan stages; for tableB it should have only 1
        Assert.assertEquals(e.getValue().getServerInstanceToWorkerIdMap().entrySet().stream()
                .map(ExplainPlanPlanVisitor::stringifyQueryServerInstanceToWorkerIdsEntry)
                .collect(Collectors.toSet()),
            ImmutableList.of("localhost@{1,1}|[0]"));
      } else if (!PlannerUtils.isRootPlanFragment(e.getKey())) {
        // join stage should have both servers used.
        Assert.assertEquals(e.getValue().getServerInstanceToWorkerIdMap().entrySet().stream()
                .map(ExplainPlanPlanVisitor::stringifyQueryServerInstanceToWorkerIdsEntry)
                .collect(Collectors.toSet()),
            ImmutableList.of("localhost@{1,1}|[1]", "localhost@{2,2}|[0]"));
      }
    }
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

  private static boolean isOneOf(List<Class<? extends AbstractPlanNode>> allowedNodeTypes,
      PlanNode node) {
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
            + "\"LogicalExchange\\n\" -> \"LogicalAggregate\\n\" [label=\"0\"]\n"
            + "\"LogicalAggregate\\n\" -> \"LogicalExchange\\n\" [label=\"0\"]\n"
            + "\"LogicalTableScan\\n\" -> \"LogicalAggregate\\n\" [label=\"0\"]\n"
            + "}\n"},
        new Object[]{"EXPLAIN PLAN FOR SELECT a.col1, b.col3 FROM a JOIN b ON a.col1 = b.col1",
              "Execution Plan\n"
            + "LogicalProject(col1=[$0], col3=[$2])\n"
            + "  LogicalJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "    LogicalExchange(distribution=[hash[0]])\n"
            + "      LogicalProject(col1=[$0])\n"
            + "        LogicalTableScan(table=[[a]])\n"
            + "    LogicalExchange(distribution=[hash[0]])\n"
            + "      LogicalProject(col1=[$0], col3=[$2])\n"
            + "        LogicalTableScan(table=[[b]])\n"
        },
    };
    //@formatter:on
  }
}
