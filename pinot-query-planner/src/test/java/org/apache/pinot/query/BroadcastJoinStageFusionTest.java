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
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Verifies the worker-stage boundaries selected for adjacent broadcast joins.
public class BroadcastJoinStageFusionTest extends QueryEnvironmentTestBase {
  @DataProvider
  public Object[][] distributions() {
    return new Object[][]{
        {"random", "broadcast", "", "JOIN", false, List.of(1, 1), 2, 3},
        {"random", "broadcast", ", fuse_broadcast_joins='true'", "JOIN", false, List.of(2), 1, 3},
        {"random", "broadcast", ", fuse_broadcast_joins='true'", "LEFT JOIN", false, List.of(1, 1), 2, 3},
        {"random", "broadcast", "", "JOIN", true, List.of(2), 1, 3},
        {"random", "broadcast", ", fuse_broadcast_joins='false'", "JOIN", true, List.of(1, 1), 2, 3},
        {"hash", "hash", ", fuse_broadcast_joins='true'", "JOIN", false, List.of(1, 1), 0, 1}
    };
  }

  @Test(dataProvider = "distributions")
  public void testDispatchableJoinStages(String probe, String build, String fuseHint, String upperJoin, boolean option,
      List<Integer> expectedJoinsPerStage, int expectedRandom, int expectedBroadcast) {
    String query = "SELECT /*+ joinOptions(left_distribution_type='" + probe + "', right_distribution_type='" + build
        + "'" + fuseHint + ") */ b.col3, c.col3, COUNT(*), SUM(a.col7) FROM a JOIN b ON a.col1=b.col1 "
        + upperJoin + " c ON a.col2=c.col2 GROUP BY b.col3,c.col3";
    SqlNodeAndOptions parsed = CalciteSqlParser.compileToSqlNodeAndOptions(query);
    if (option) {
      parsed.setExtraOptions(Map.of("fuseBroadcastJoins", "true"));
    }
    DispatchableSubPlan plan;
    try (QueryEnvironment.CompiledQuery compiled = _queryEnvironment.compile(query, parsed)) {
      plan = compiled.planQuery(1).getQueryPlan();
    }
    List<Integer> joinsPerStage = new ArrayList<>();
    int random = 0;
    int broadcast = 0;
    for (var stage : plan.getQueryStages()) {
      PlanNode root = stage.getPlanFragment().getFragmentRoot();
      int joins = countJoins(root);
      if (joins > 0) {
        joinsPerStage.add(joins);
      }
      if (root instanceof MailboxSendNode send) {
        if (send.getDistributionType() == RelDistribution.Type.RANDOM_DISTRIBUTED) {
          random++;
        } else if (send.getDistributionType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
          broadcast++;
        }
      }
    }
    assertEquals(joinsPerStage, expectedJoinsPerStage);
    assertEquals(random, expectedRandom);
    assertEquals(broadcast, expectedBroadcast);
  }

  private static int countJoins(PlanNode node) {
    return (node instanceof JoinNode ? 1 : 0)
        + node.getInputs().stream().mapToInt(BroadcastJoinStageFusionTest::countJoins).sum();
  }
}
