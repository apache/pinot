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
package org.apache.pinot.query.planner.physical;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.runtime.PairList;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.plannode.BaseMailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxMergeReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class DispatchablePlanVisitorTest {
  private static final int SORT_STAGE = 1;
  private static final int SENDER_STAGE = 2;
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"col"}, new ColumnDataType[]{ColumnDataType.INT});
  private static final List<RelFieldCollation> COLLATIONS = List.of(new RelFieldCollation(0));

  /// Only a Sort over a hash-partitioned exchange that keeps every row orders each partition independently. Every
  /// other Sort over a mailbox establishes or trims a total order and needs a singleton worker.
  @Test(dataProvider = "sortsOverReceive")
  public void testSortOverReceiveRequiresSingleton(RelDistribution.Type distributionType, List<Integer> keys,
      int fetch, int offset, boolean expectedSingleton) {
    MailboxSendNode sender = new MailboxSendNode(SENDER_STAGE, DATA_SCHEMA,
        List.of(new ValueNode(SENDER_STAGE, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of())), SORT_STAGE,
        PinotRelExchangeType.STREAMING, distributionType, keys, false, COLLATIONS, false, "absHashCode");
    List<BaseMailboxReceiveNode> receives = List.of(
        new MailboxReceiveNode(SORT_STAGE, DATA_SCHEMA, SENDER_STAGE, PinotRelExchangeType.STREAMING,
            distributionType, keys, COLLATIONS, false, false, sender),
        new MailboxMergeReceiveNode(SORT_STAGE, DATA_SCHEMA, SENDER_STAGE, PinotRelExchangeType.STREAMING,
            distributionType, keys, COLLATIONS, sender));
    for (BaseMailboxReceiveNode receive : receives) {
      SortNode sort =
          new SortNode(SORT_STAGE, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(receive), COLLATIONS, fetch, offset);

      PlannerContext plannerContext = Mockito.mock(PlannerContext.class);
      Mockito.when(plannerContext.getOptions()).thenReturn(Map.of());
      Mockito.when(plannerContext.getEnvConfig()).thenReturn(Mockito.mock(QueryEnvironment.Config.class));
      DispatchablePlanContext context =
          new DispatchablePlanContext(null, 0, plannerContext, PairList.of(0, "col"), Set.of());
      sort.visit(new DispatchablePlanVisitor(null, null), context);

      assertEquals(context.getDispatchablePlanMetadataMap().get(SORT_STAGE).isRequiresSingletonInstance(),
          expectedSingleton, receive.getClass().getSimpleName());
      if (fetch < 0 && offset < 0) {
        // A compatibility-encoded complete-input sort must make the same worker decision as an unbounded sort.
        SortNode completeInputSort = new SortNode(SORT_STAGE, DATA_SCHEMA, PlanNode.NodeHint.EMPTY,
            List.of(receive), COLLATIONS, Integer.MAX_VALUE, -1).requiringSingleRun();
        completeInputSort.visit(new DispatchablePlanVisitor(null, null), context);
        assertEquals(context.getDispatchablePlanMetadataMap().get(SORT_STAGE).isRequiresSingletonInstance(),
            expectedSingleton, receive.getClass().getSimpleName());
      }
    }
  }

  @DataProvider(name = "sortsOverReceive")
  public Object[][] sortsOverReceive() {
    return new Object[][]{
        // Window / WITHIN GROUP input: partitioned, keeps every row
        {RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), -1, -1, false},
        // Trimming a partitioned exchange still needs every row in one place
        {RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), 10, -1, true},
        {RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), Integer.MAX_VALUE, -1, true},
        {RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), -1, 5, true},
        {RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), 10, 0, true},
        // Final ORDER BY: keyless exchange
        {RelDistribution.Type.HASH_DISTRIBUTED, List.of(), -1, -1, true},
        {RelDistribution.Type.HASH_DISTRIBUTED, List.of(), 10, 0, true},
        // Exchanges that do not partition by key
        {RelDistribution.Type.RANDOM_DISTRIBUTED, List.of(), -1, -1, true},
        {RelDistribution.Type.BROADCAST_DISTRIBUTED, List.of(), -1, -1, true},
        {RelDistribution.Type.SINGLETON, List.of(), -1, -1, true}
    };
  }
}
