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

import com.google.common.util.concurrent.MoreExecutors;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.LeafOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies runtime pruning of probe-side leaf execution after an empty dynamic-broadcast build.
///
/// The class has no shared mutable state and does not start executor threads.
public class ServerPlanRequestUtilsTest {
  private static final int STAGE_ID = 1;
  private static final int BUILD_STAGE_ID = 2;
  private static final int RECEIVER_STAGE_ID = 0;
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.INT});

  @DataProvider(name = "dynamicFilterCases")
  public Object[][] dynamicFilterCases() {
    return new Object[][]{
        {false, false, true, 0},
        {true, false, true, 0},
        {false, true, true, 0},
        {false, false, false, 1}
    };
  }

  @Test(dataProvider = "dynamicFilterCases")
  public void testEmptyDynamicBroadcastBuildSkipsLeafExecution(boolean enrichedJoin, boolean logicalTable,
      boolean hasSegments, int expectedNonActiveWorkers) {
    // Execute unexpected submissions inline so the no-work assertion fails without waiting for a query timeout.
    try (ExecutorService executorService = spy(MoreExecutors.newDirectExecutorService())) {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      SendingMailbox sendingMailbox = mock(SendingMailbox.class);
      MailboxService mailboxService = mock(MailboxService.class);
      when(mailboxService.getSendingMailbox(anyString(), anyInt(), anyString(), anyLong(), any()))
          .thenReturn(sendingMailbox);
      MultiStageQueryStats receivedStats = MultiStageQueryStats.emptyStats(RECEIVER_STAGE_ID);
      doAnswer(invocation -> {
        List<DataBuffer> serializedStats = invocation.getArgument(1);
        receivedStats.mergeUpstream(serializedStats);
        return true;
      }).when(sendingMailbox).send(any(MseBlock.Eos.class), anyList());

      PlanNode dynamicFilter = newDynamicFilterNode(enrichedJoin);
      MailboxSendNode root = new MailboxSendNode(STAGE_ID, DATA_SCHEMA, List.of(dynamicFilter), RECEIVER_STAGE_ID,
          PinotRelExchangeType.STREAMING, RelDistribution.Type.RANDOM_DISTRIBUTED, List.of(), false, List.of(), false,
          "MURMUR3");

      MailboxInfo receiver = new MailboxInfo("localhost", 9000, List.of(0));
      WorkerMetadata workerMetadata =
          new WorkerMetadata(0, Map.of(RECEIVER_STAGE_ID, new MailboxInfos(receiver)));
      if (logicalTable) {
        workerMetadata.setLogicalTableSegmentsMap(Map.of("probe", hasSegments ? List.of("segment") : List.of()));
      } else {
        workerMetadata.setTableSegmentsMap(
            Map.of(TableType.OFFLINE.name(), hasSegments ? List.of("segment") : List.of()));
      }
      OpChainExecutionContext context = executionContext(mailboxService, workerMetadata, dynamicFilter);

      try (OpChain opChain = ServerPlanRequestUtils.compileLeafStage(context,
          new StagePlan(root, context.getStageMetadata()), queryExecutor, executorService, Map.of())) {
        assertTrue(opChain.getRoot().nextBlock().isSuccess());
      }

      verifyNoInteractions(queryExecutor, executorService);
      verify(sendingMailbox).send(any(MseBlock.Eos.class), anyList());
      MultiStageQueryStats.StageStats.Closed stageStats = receivedStats.getUpstreamStageStats(STAGE_ID);
      assertNotNull(stageStats);
      assertEquals(stageStats.getOperatorType(0), MultiStageOperator.Type.LEAF);
      assertEquals(stageStats.getOperatorType(1), MultiStageOperator.Type.MAILBOX_SEND);
      @SuppressWarnings("unchecked")
      StatMap<LeafOperator.StatKey> leafStats = (StatMap<LeafOperator.StatKey>) stageStats.getOperatorStats(0);
      assertEquals(leafStats.getInt(LeafOperator.StatKey.NON_ACTIVE_WORKERS), expectedNonActiveWorkers);
      assertNotNull(receivedStats.getUpstreamStageStats(BUILD_STAGE_ID));
    }
  }

  @DataProvider(name = "emptyInputAggregates")
  public Object[][] emptyInputAggregates() {
    return new Object[][]{
        {List.of(), List.of()},
        {List.of(0), List.of(List.of(0), List.of())}
    };
  }

  @Test(dataProvider = "emptyInputAggregates")
  public void testEmptyDynamicBroadcastBuildDoesNotSkipEmptyInputAggregate(List<Integer> groupKeys,
      List<List<Integer>> groupingSets) {
    PlanNode dynamicFilter = newDynamicFilterNode(false);
    DataSchema aggregateSchema = new DataSchema(new String[]{"count"}, new ColumnDataType[]{ColumnDataType.LONG});
    AggregateNode aggregate = new AggregateNode(STAGE_ID, aggregateSchema, PlanNode.NodeHint.EMPTY,
        List.of(dynamicFilter),
        List.of(new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())), List.of(-1), groupKeys,
        AggType.LEAF, false, null, -1, groupingSets);
    ServerPlanRequestContext context =
        new ServerPlanRequestContext(new StagePlan(aggregate, mock(StageMetadata.class)), mock(QueryExecutor.class),
            mock(ExecutorService.class), emptyBuild(dynamicFilter));

    ServerPlanRequestVisitor.walkPlanNode(aggregate, context);

    assertFalse(context.shouldSkipLeafQueryExecution());
  }

  @Test
  public void testEmptyDynamicBroadcastBuildDoesNotSkipExplain() {
    QueryExecutor queryExecutor = mock(QueryExecutor.class);
    AssertionError requestConstruction = new AssertionError("EXPLAIN constructs the leaf request");
    when(queryExecutor.getInstanceDataManager()).thenThrow(requestConstruction);
    PlanNode dynamicFilter = newDynamicFilterNode(false);
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of());
    workerMetadata.setTableSegmentsMap(Map.of(TableType.OFFLINE.name(), List.of("segment")));
    OpChainExecutionContext context = executionContext(mock(MailboxService.class), workerMetadata, dynamicFilter);
    StagePlan stagePlan = new StagePlan(dynamicFilter, context.getStageMetadata());

    assertSame(expectThrows(AssertionError.class,
        () -> ServerPlanRequestUtils.compileLeafStage(context, stagePlan, queryExecutor, mock(ExecutorService.class),
            (planNode, operator) -> {
            }, true, Map.of())), requestConstruction);
  }

  private static OpChainExecutionContext executionContext(MailboxService mailboxService, WorkerMetadata worker,
      PlanNode dynamicFilter) {
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(8000);
    StageMetadata stageMetadata =
        new StageMetadata(STAGE_ID, List.of(worker), Map.of(DispatchablePlanFragment.TABLE_NAME_KEY, "probe"));
    long deadlineMs = System.currentTimeMillis() + 60_000;
    return new OpChainExecutionContext(mailboxService, 123L, "cid", deadlineMs, deadlineMs, "broker", Map.of(),
        stageMetadata, worker, emptyBuild(dynamicFilter), true, true);
  }

  private static PipelineBreakerResult emptyBuild(PlanNode dynamicFilter) {
    MultiStageQueryStats stats = MultiStageQueryStats.emptyStats(STAGE_ID);
    stats.mergeUpstream(MultiStageQueryStats.emptyStats(BUILD_STAGE_ID));
    return new PipelineBreakerResult(Map.of(dynamicFilter.getInputs().get(1), 0), Map.of(0, List.of()), null, stats);
  }

  @SuppressWarnings("removal")
  private static PlanNode newDynamicFilterNode(boolean enrichedJoin) {
    MailboxReceiveNode buildInput = new MailboxReceiveNode(STAGE_ID, DATA_SCHEMA, BUILD_STAGE_ID,
        PinotRelExchangeType.PIPELINE_BREAKER, RelDistribution.Type.SINGLETON, List.of(0), List.of(), false, false,
        null);
    TableScanNode probeInput =
        new TableScanNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), "probe", List.of("key"));
    if (enrichedJoin) {
      // Retain coverage for plans produced by older brokers during a rolling upgrade.
      return new EnrichedJoinNode(STAGE_ID, DATA_SCHEMA, DATA_SCHEMA, PlanNode.NodeHint.EMPTY,
          List.of(probeInput, buildInput), JoinRelType.SEMI, List.of(0), List.of(0), List.of(),
          JoinNode.JoinStrategy.HASH, null, List.of(), -1, 0);
    }
    return new JoinNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(probeInput, buildInput),
        JoinRelType.SEMI, List.of(0), List.of(0), List.of(), JoinNode.JoinStrategy.HASH);
  }
}
