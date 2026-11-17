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
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Tests the boundary between the V1 leaf request and the MSE operator chain.
public class ServerPlanRequestVisitorTest {
  private static final int STAGE_ID = 1;
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"orderKey"}, new ColumnDataType[]{ColumnDataType.INT});
  private static final List<RelFieldCollation> COLLATIONS = List.of(new RelFieldCollation(0));
  private static final DataSchema TWO_COLUMN_SCHEMA = new DataSchema(new String[]{"orderKey", "otherKey"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});

  /// The visitor establishes this boundary before a logical or hybrid table is expanded into physical leaf requests.
  /// In the default context (not known to be a single physical request, merge mode not requested), pushing the sort
  /// into V1 here would sort each request independently instead of the complete mailbox stream, so the explicit
  /// sender SortNode must remain in the MSE op-chain above the boundary.
  @Test
  public void shouldKeepExplicitSenderSortAboveLeafBoundary() {
    MailboxSendNode sendNode = unboundedSortSend(tableScan(), COLLATIONS);
    PlanNode sortNode = sendNode.getInputs().get(0);
    ServerPlanRequestContext context = new ServerPlanRequestContext(new StagePlan(sendNode, null), null, null, null);

    ServerPlanRequestVisitor.walkPlanNode(sendNode, context);

    assertEquals(context.getPinotQuery().getDataSource().getTableName(), "testTable",
        "The subtree below the retained sort must still be compiled into the V1 request");
    assertNull(context.getPinotQuery().getOrderByList(),
        "The explicit sender sort must not be pushed independently into each physical leaf request");
    assertSame(context.getLeafStageBoundaryNode(), sortNode.getInputs().get(0));
  }

  /// An ordinary receiver-sorted exchange does not establish sender ordering. Its SortNode remains eligible for V1
  /// pushdown, preserving the existing leaf optimization when the explicit sender-sort invariant does not hold.
  @Test
  public void shouldPushOrdinarySortIntoLeafRequest() {
    SortNode sortNode = new SortNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(tableScan()), COLLATIONS,
        Integer.MAX_VALUE, -1);
    MailboxSendNode sendNode = send(sortNode, COLLATIONS);
    ServerPlanRequestContext context = new ServerPlanRequestContext(new StagePlan(sendNode, null), null, null, null);

    ServerPlanRequestVisitor.walkPlanNode(sendNode, context);

    assertSame(context.getLeafStageBoundaryNode(), sortNode);
    assertNotNull(context.getPinotQuery().getOrderByList(),
        "An ordinary SortNode should continue to be folded into the V1 leaf request");
  }

  /// With a single physical request and the merge mode requested, the leaf produces one sorted run, so the unbounded
  /// sender sort is folded into the V1 request with exactly its collation (direction and null order) and no limit.
  @Test
  public void shouldPushUnboundedSortForSinglePhysicalRequestWhenMergeRequested() {
    List<RelFieldCollation> collations = List.of(
        new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST),
        new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));
    MailboxSendNode sendNode = unboundedSortSend(twoColumnScan(), collations);
    ServerPlanRequestContext context = context(sendNode, true, true);

    ServerPlanRequestVisitor.walkPlanNode(sendNode, context);

    assertEquals(context.getPinotQuery().getOrderByList(), List.of(
        RequestUtils.getFunctionExpression("nullsfirst",
            RequestUtils.getFunctionExpression("asc", RequestUtils.getIdentifierExpression("otherKey"))),
        RequestUtils.getFunctionExpression("nullslast",
            RequestUtils.getFunctionExpression("desc", RequestUtils.getIdentifierExpression("orderKey")))));
    assertEquals(context.getPinotQuery().getLimit(), Integer.MAX_VALUE);
    assertEquals(context.getPinotQuery().getOffset(), 0);
    assertSame(context.getLeafStageBoundaryNode(), sendNode.getInputs().get(0));
  }

  /// A filter only removes rows, so a sort above it is still pushed.
  @Test
  public void shouldPushUnboundedSortAboveFilter() {
    FilterNode filter = new FilterNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(tableScan()),
        new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.GREATER_THAN.name(),
            List.of(new RexExpression.InputRef(0), new RexExpression.Literal(ColumnDataType.INT, 5))));
    MailboxSendNode sendNode = unboundedSortSend(filter, COLLATIONS);
    ServerPlanRequestContext context = context(sendNode, true, true);

    ServerPlanRequestVisitor.walkPlanNode(sendNode, context);

    assertNotNull(context.getPinotQuery().getFilterExpression());
    assertEquals(context.getPinotQuery().getOrderByList(), List.of(
        RequestUtils.getFunctionExpression("asc", RequestUtils.getIdentifierExpression("orderKey"))));
    assertSame(context.getLeafStageBoundaryNode(), sendNode.getInputs().get(0));
  }

  /// Several physical requests, mode not requested, and a narrowed leaf limit keep the sort in the MSE op-chain.
  @Test
  public void shouldKeepUnboundedSortWhenNotEligible() {
    MailboxSendNode multi = unboundedSortSend(tableScan(), COLLATIONS);
    assertNotPushed(multi, context(multi, false, true));

    MailboxSendNode off = unboundedSortSend(tableScan(), COLLATIONS);
    assertNotPushed(off, context(off, true, false));

    MailboxSendNode limited = unboundedSortSend(tableScan(), COLLATIONS);
    ServerPlanRequestContext limitedContext = context(limited, true, true);
    limitedContext.getPinotQuery().setLimit(100);
    assertNotPushed(limited, limitedContext);
  }

  /// A sort with its own offset is not pushed: offset + Integer.MAX_VALUE would overflow V1's row budget.
  @Test
  public void shouldKeepUnboundedSortWithOffset() {
    SortNode sortNode = new SortNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(tableScan()), COLLATIONS,
        -1, 5).requiringSingleRun();
    MailboxSendNode sendNode = send(sortNode, List.of());
    assertNotPushed(sendNode, context(sendNode, true, true));
  }

  /// A bounded sort below already set ORDER BY and LIMIT in the V1 request; the outer sort must not overwrite them.
  @Test
  public void shouldKeepUnboundedSortAboveBoundedSort() {
    SortNode inner = new SortNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(tableScan()), COLLATIONS,
        10, -1);
    MailboxSendNode sendNode = unboundedSortSend(inner, COLLATIONS);
    ServerPlanRequestContext context = context(sendNode, true, true);

    ServerPlanRequestVisitor.walkPlanNode(sendNode, context);

    assertEquals(context.getPinotQuery().getLimit(), 10);
    assertSame(context.getLeafStageBoundaryNode(), inner);
  }

  /// A sort key computed by a projection is not pushed: the leaf converts V1 values to the MSE type, and a narrowing
  /// conversion could reorder them.
  @Test
  public void shouldKeepUnboundedSortOnComputedKey() {
    ProjectNode project = new ProjectNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(tableScan()),
        List.of(new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.PLUS.name(),
            List.of(new RexExpression.InputRef(0), new RexExpression.Literal(ColumnDataType.INT, 1)))));
    MailboxSendNode sendNode = unboundedSortSend(project, COLLATIONS);
    assertNotPushed(sendNode, context(sendNode, true, true));
  }

  /// An aggregating leaf sets its own boundary in visitAggregate, so the sort above it stays in MSE.
  @Test
  public void shouldKeepUnboundedSortAboveAggregate() {
    AggregateNode aggregate = new AggregateNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(tableScan()),
        List.of(), List.of(), List.of(0), AggregateNode.AggType.LEAF, false, null, 0);
    MailboxSendNode sendNode = unboundedSortSend(aggregate, COLLATIONS);
    ServerPlanRequestContext context = context(sendNode, true, true);

    ServerPlanRequestVisitor.walkPlanNode(sendNode, context);

    assertNull(context.getPinotQuery().getOrderByList());
    assertSame(context.getLeafStageBoundaryNode(), aggregate);
  }

  @Test
  public void shouldDetectSinglePhysicalRequest() {
    StageMetadata plainStage = stage(Map.of());
    assertTrue(ServerPlanRequestUtils.isSinglePhysicalRequest(plainStage, worker(Map.of("OFFLINE", List.of("s1")),
        null)));
    assertFalse(ServerPlanRequestUtils.isSinglePhysicalRequest(plainStage,
        worker(Map.of("OFFLINE", List.of("s1"), "REALTIME", List.of("s2")), null)));
    assertFalse(ServerPlanRequestUtils.isSinglePhysicalRequest(plainStage,
        worker(Map.of("OFFLINE", List.of("s1")), Map.of("t_OFFLINE", List.of("s1")))));
    // A hybrid table's worker that only holds OFFLINE segments must decide like the stage's other workers.
    StageMetadata hybridStage = stage(Map.of(DispatchablePlanFragment.TIME_BOUNDARY_COLUMN_KEY, "ts",
        DispatchablePlanFragment.TIME_BOUNDARY_VALUE_KEY, "1000"));
    assertFalse(ServerPlanRequestUtils.isSinglePhysicalRequest(hybridStage,
        worker(Map.of("OFFLINE", List.of("s1")), null)));
  }

  @Test
  public void shouldResolveSortedSelectionMergeRequested() {
    String key = CommonConstants.Broker.Request.QueryOptionKey.SORTED_SELECTION_MERGE_MODE;
    // Absent: the server default (OFF).
    assertFalse(ServerPlanRequestUtils.isSortedSelectionMergeRequested(Map.of()));
    assertFalse(ServerPlanRequestUtils.isSortedSelectionMergeRequested(Map.of(key, "OFF")));
    assertTrue(ServerPlanRequestUtils.isSortedSelectionMergeRequested(Map.of(key, "ON")));
    assertTrue(ServerPlanRequestUtils.isSortedSelectionMergeRequested(Map.of(key, "auto")));
    // Invalid: not requested here; V1 still rejects it for selection leaves.
    assertFalse(ServerPlanRequestUtils.isSortedSelectionMergeRequested(Map.of(key, "bogus")));
  }

  private static MailboxSendNode unboundedSortSend(PlanNode sortInput, List<RelFieldCollation> collations) {
    SortNode sortNode = new SortNode(STAGE_ID, sortInput.getDataSchema(), PlanNode.NodeHint.EMPTY, List.of(sortInput),
        collations, -1, -1).requiringSingleRun();
    return send(sortNode, List.of());
  }

  private static MailboxSendNode send(PlanNode input, List<RelFieldCollation> collations) {
    return new MailboxSendNode(STAGE_ID, input.getDataSchema(), List.of(input), 2, PinotRelExchangeType.STREAMING,
        RelDistribution.Type.HASH_DISTRIBUTED, List.of(), false, collations, false, null);
  }

  private static TableScanNode tableScan() {
    return new TableScanNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), "testTable",
        List.of("orderKey"));
  }

  private static TableScanNode twoColumnScan() {
    return new TableScanNode(STAGE_ID, TWO_COLUMN_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), "testTable",
        List.of("orderKey", "otherKey"));
  }

  private static ServerPlanRequestContext context(MailboxSendNode sendNode, boolean singlePhysicalRequest,
      boolean sortedSelectionMergeRequested) {
    ServerPlanRequestContext context = new ServerPlanRequestContext(new StagePlan(sendNode, null), null, null, null,
        singlePhysicalRequest, sortedSelectionMergeRequested);
    context.getPinotQuery().setLimit(Integer.MAX_VALUE);
    return context;
  }

  private static void assertNotPushed(MailboxSendNode sendNode, ServerPlanRequestContext context) {
    ServerPlanRequestVisitor.walkPlanNode(sendNode, context);
    assertNull(context.getPinotQuery().getOrderByList());
    assertSame(context.getLeafStageBoundaryNode(), sendNode.getInputs().get(0).getInputs().get(0));
  }

  private static StageMetadata stage(Map<String, String> customProperties) {
    return new StageMetadata(STAGE_ID, List.of(), customProperties);
  }

  private static WorkerMetadata worker(Map<String, List<String>> tableSegments,
      Map<String, List<String>> logicalSegments) {
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of());
    workerMetadata.setTableSegmentsMap(tableSegments);
    if (logicalSegments != null) {
      workerMetadata.setLogicalTableSegmentsMap(logicalSegments);
    }
    return workerMetadata;
  }
}
