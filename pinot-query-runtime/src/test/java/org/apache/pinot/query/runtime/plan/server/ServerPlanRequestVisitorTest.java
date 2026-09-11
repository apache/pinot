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
package org.apache.pinot.query.runtime.plan.server;

import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.routing.StagePlan;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Tests the boundary between the V1 leaf request and the MSE operator chain.
public class ServerPlanRequestVisitorTest {
  private static final int STAGE_ID = 1;
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"orderKey"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
  private static final List<RelFieldCollation> COLLATIONS = List.of(new RelFieldCollation(0));

  /// The visitor establishes this boundary before a logical or hybrid table is expanded into physical leaf requests.
  /// Pushing the sort into V1 here would therefore sort each request independently instead of the complete mailbox
  /// stream, so the explicit sender SortNode must remain in the MSE op-chain above the boundary.
  @Test
  public void shouldKeepExplicitSenderSortAboveLeafBoundary() {
    TableScanNode tableScan = new TableScanNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(),
        "testTable", List.of("orderKey"));
    SortNode sortNode = new SortNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(tableScan), COLLATIONS,
        -1, -1);
    MailboxSendNode sendNode = new MailboxSendNode(STAGE_ID, DATA_SCHEMA, List.of(sortNode), 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED, List.of(), false, COLLATIONS, true,
        null);
    assertTrue(sendNode.hasExplicitSortInput());
    ServerPlanRequestContext context = new ServerPlanRequestContext(new StagePlan(sendNode, null), null, null, null);

    ServerPlanRequestVisitor.walkPlanNode(sendNode, context);

    assertEquals(context.getPinotQuery().getDataSource().getTableName(), "testTable",
        "The subtree below the retained sort must still be compiled into the V1 request");
    assertSame(context.getLeafStageBoundaryNode(), tableScan);
    assertNull(context.getPinotQuery().getOrderByList(),
        "The explicit sender sort must not be pushed independently into each physical leaf request");
    assertSame(sortNode.getInputs().get(0), context.getLeafStageBoundaryNode());
  }

  /// An ordinary receiver-sorted exchange does not establish sender ordering. Its SortNode remains eligible for V1
  /// pushdown, preserving the existing leaf optimization when the explicit sender-sort invariant does not hold.
  @Test
  public void shouldPushOrdinarySortIntoLeafRequest() {
    TableScanNode tableScan = new TableScanNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(),
        "testTable", List.of("orderKey"));
    SortNode sortNode = new SortNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(tableScan), COLLATIONS,
        -1, -1);
    MailboxSendNode sendNode = new MailboxSendNode(STAGE_ID, DATA_SCHEMA, List.of(sortNode), 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED, List.of(), false, COLLATIONS, false,
        null);
    ServerPlanRequestContext context = new ServerPlanRequestContext(new StagePlan(sendNode, null), null, null, null);

    ServerPlanRequestVisitor.walkPlanNode(sendNode, context);

    assertSame(context.getLeafStageBoundaryNode(), sortNode);
    assertNotNull(context.getPinotQuery().getOrderByList(),
        "An ordinary SortNode should continue to be folded into the V1 leaf request");
  }
}
