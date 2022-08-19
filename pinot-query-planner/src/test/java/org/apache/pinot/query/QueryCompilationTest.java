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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.AbstractStageNode;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryCompilationTest extends QueryEnvironmentTestBase {

  @Test(dataProvider = "testQueryParserDataProvider")
  public void testQueryParser(String query, String digest)
      throws Exception {
    PlannerContext plannerContext = new PlannerContext();
    SqlNode sqlNode = _queryEnvironment.parse(query, plannerContext);
    _queryEnvironment.validate(sqlNode);
    Assert.assertEquals(sqlNode.toString(), digest);
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

  @Test
  public void testQueryGroupByAfterJoinShouldNotDoDataShuffle()
      throws Exception {
    String query = "SELECT a.col1, a.col2, AVG(b.col3) FROM a JOIN b ON a.col1 = b.col2 "
        + " WHERE a.col3 >= 0 AND a.col2 = 'a' AND b.col3 < 0 GROUP BY a.col1, a.col2";
    QueryPlan queryPlan = _queryEnvironment.planQuery(query);
    Assert.assertEquals(queryPlan.getQueryStageMap().size(), 5);
    Assert.assertEquals(queryPlan.getStageMetadataMap().size(), 5);
    for (Map.Entry<Integer, StageMetadata> e : queryPlan.getStageMetadataMap().entrySet()) {
      if (e.getValue().getScannedTables().size() == 0 && !PlannerUtils.isRootStage(e.getKey())) {
        StageNode node = queryPlan.getQueryStageMap().get(e.getKey());
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
            Assert.assertEquals(input.getExchangeType(), RelDistribution.Type.SINGLETON);
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
    Assert.assertEquals(queryPlan.getStageMetadataMap().size(), 4);
    for (Map.Entry<Integer, StageMetadata> e : queryPlan.getStageMetadataMap().entrySet()) {
      List<String> tables = e.getValue().getScannedTables();
      if (tables.size() == 1) {
        // table scan stages; for tableA it should have 2 hosts, for tableB it should have only 1
        Assert.assertEquals(
            e.getValue().getServerInstances().stream().map(ServerInstance::toString).collect(Collectors.toList()),
            tables.get(0).equals("a") ? ImmutableList.of("Server_localhost_1", "Server_localhost_2")
                : ImmutableList.of("Server_localhost_1"));
      } else if (!PlannerUtils.isRootStage(e.getKey())) {
        // join stage should have both servers used.
        Assert.assertEquals(
            e.getValue().getServerInstances().stream().map(ServerInstance::toString).collect(Collectors.toList()),
            ImmutableList.of("Server_localhost_1", "Server_localhost_2"));
      } else {
        // reduce stage should have the reducer instance.
        Assert.assertEquals(
            e.getValue().getServerInstances().stream().map(ServerInstance::toString).collect(Collectors.toList()),
            ImmutableList.of("Server_localhost_3"));
      }
    }
  }

  @Test
  public void testQueryProjectFilterPushDownForJoin() {
    String query = "SELECT a.col1, a.ts, b.col2, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
        + "WHERE a.col3 >= 0 AND a.col2 IN  ('a', 'b') AND b.col3 < 0";
    QueryPlan queryPlan = _queryEnvironment.planQuery(query);
    List<StageNode> intermediateStageRoots =
        queryPlan.getStageMetadataMap().entrySet().stream().filter(e -> e.getValue().getScannedTables().size() == 0)
            .map(e -> queryPlan.getQueryStageMap().get(e.getKey())).collect(Collectors.toList());
    // Assert that no project of filter node for any intermediate stage because all should've been pushed down.
    for (StageNode roots : intermediateStageRoots) {
      assertNodeTypeNotIn(roots, ImmutableList.of(ProjectNode.class, FilterNode.class));
    }
  }

  // --------------------------------------------------------------------------
  // Test Utils.
  // --------------------------------------------------------------------------

  private static void assertNodeTypeNotIn(StageNode node, List<Class<? extends AbstractStageNode>> bannedNodeType) {
    Assert.assertFalse(isOneOf(bannedNodeType, node));
    for (StageNode child : node.getInputs()) {
      assertNodeTypeNotIn(child, bannedNodeType);
    }
  }

  private static boolean isOneOf(List<Class<? extends AbstractStageNode>> allowedNodeTypes,
      StageNode node) {
    for (Class<? extends AbstractStageNode> allowedNodeType : allowedNodeTypes) {
      if (node.getClass() == allowedNodeType) {
        return true;
      }
    }
    return false;
  }

  @DataProvider(name = "testQueryParserDataProvider")
  private Object[][] provideQueriesAndDigest() {
    return new Object[][] {
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0",
            "SELECT *\n" + "FROM `a`\n" + "INNER JOIN `b` ON `a`.`col1` = `b`.`col2`\n" + "WHERE `a`.`col3` >= 0"},
    };
  }

  @DataProvider(name = "testQueryExceptionDataProvider")
  private Object[][] provideQueriesWithException() {
    return new Object[][] {
        // wrong table is being used after JOIN
        new Object[]{"SELECT b.col1 - a.col3 FROM a JOIN c ON a.col1 = c.col3", "Table 'b' not found"},
        // non-agg column not being grouped
        new Object[]{"SELECT a.col1, SUM(a.col3) FROM a", "'a.col1' is not being grouped"},
    };
  }
}
