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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.pinot.spi.executor.ExecutorServiceUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Verifies runtime pruning of probe-side leaf execution after an empty dynamic-broadcast build.
///
/// The class has no shared mutable state; each test owns and closes its executor.
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

  @SuppressWarnings("removal")
  @Test(dataProvider = "dynamicFilterCases")
  public void testEmptyDynamicBroadcastBuildSkipsLeafExecution(boolean enrichedJoin, boolean logicalTable,
      boolean hasSegments, int expectedNonActiveWorkers) {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.getInstanceDataManager())
          .thenThrow(new AssertionError("Empty dynamic-filter build must not construct a leaf request"));

      SendingMailbox sendingMailbox = mock(SendingMailbox.class);
      MailboxService mailboxService = mock(MailboxService.class);
      when(mailboxService.getHostname()).thenReturn("localhost");
      when(mailboxService.getPort()).thenReturn(8000);
      when(mailboxService.getSendingMailbox(anyString(), anyInt(), anyString(), anyLong(), any()))
          .thenReturn(sendingMailbox);
      MultiStageQueryStats receivedStats = MultiStageQueryStats.emptyStats(RECEIVER_STAGE_ID);
      doAnswer(invocation -> {
        List<DataBuffer> serializedStats = invocation.getArgument(1);
        receivedStats.mergeUpstream(serializedStats);
        return true;
      }).when(sendingMailbox).send(any(MseBlock.Eos.class), anyList());

      MailboxReceiveNode buildInput = new MailboxReceiveNode(STAGE_ID, DATA_SCHEMA, BUILD_STAGE_ID,
          PinotRelExchangeType.PIPELINE_BREAKER, RelDistribution.Type.SINGLETON, List.of(0), List.of(), false, false,
          null);
      TableScanNode probeInput =
          new TableScanNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), "probe", List.of("key"));
      PlanNode dynamicFilter =
          newDynamicFilterNode(enrichedJoin, probeInput, buildInput);
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
      StageMetadata stageMetadata = new StageMetadata(STAGE_ID, List.of(workerMetadata),
          Map.of(DispatchablePlanFragment.TABLE_NAME_KEY, "probe"));
      MultiStageQueryStats pipelineBreakerStats = MultiStageQueryStats.emptyStats(STAGE_ID);
      pipelineBreakerStats.mergeUpstream(MultiStageQueryStats.emptyStats(BUILD_STAGE_ID));
      PipelineBreakerResult emptyBuild =
          new PipelineBreakerResult(Map.of(buildInput, 0), Map.of(0, List.of()), null, pipelineBreakerStats);
      OpChainExecutionContext context = new OpChainExecutionContext(mailboxService, 123L, "cid",
          System.currentTimeMillis() + 60_000, System.currentTimeMillis() + 60_000, "broker", Map.of(), stageMetadata,
          workerMetadata, emptyBuild, true, true);

      OpChain opChain = ServerPlanRequestUtils.compileLeafStage(context, new StagePlan(root, stageMetadata),
          queryExecutor, executorService, Map.of());
      try {
        MseBlock block = opChain.getRoot().nextBlock();
        assertTrue(block.isSuccess());
      } finally {
        opChain.close();
      }

      verify(queryExecutor, never()).getInstanceDataManager();
      verify(queryExecutor, never()).execute(any(), any(), any());
      verify(sendingMailbox).send(any(MseBlock.Eos.class), anyList());
      MultiStageQueryStats.StageStats.Closed stageStats = receivedStats.getUpstreamStageStats(STAGE_ID);
      assertNotNull(stageStats);
      assertEquals(stageStats.getOperatorType(0), MultiStageOperator.Type.LEAF);
      assertEquals(stageStats.getOperatorType(1), MultiStageOperator.Type.MAILBOX_SEND);
      @SuppressWarnings("unchecked")
      StatMap<LeafOperator.StatKey> leafStats = (StatMap<LeafOperator.StatKey>) stageStats.getOperatorStats(0);
      assertEquals(leafStats.getInt(LeafOperator.StatKey.NON_ACTIVE_WORKERS), expectedNonActiveWorkers);
      assertNotNull(receivedStats.getUpstreamStageStats(BUILD_STAGE_ID));
    } finally {
      ExecutorServiceUtils.close(executorService);
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
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      MailboxReceiveNode buildInput = new MailboxReceiveNode(STAGE_ID, DATA_SCHEMA, BUILD_STAGE_ID,
          PinotRelExchangeType.PIPELINE_BREAKER, RelDistribution.Type.SINGLETON, List.of(0), List.of(), false, false,
          null);
      TableScanNode probeInput =
          new TableScanNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), "probe", List.of("key"));
      PlanNode dynamicFilter = newDynamicFilterNode(false, probeInput, buildInput);
      DataSchema aggregateSchema = new DataSchema(new String[]{"count"}, new ColumnDataType[]{ColumnDataType.LONG});
      AggregateNode aggregate = new AggregateNode(STAGE_ID, aggregateSchema, PlanNode.NodeHint.EMPTY,
          List.of(dynamicFilter),
          List.of(new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())), List.of(-1), groupKeys,
          AggType.LEAF, false, null, -1, groupingSets);
      PipelineBreakerResult emptyBuild =
          new PipelineBreakerResult(Map.of(buildInput, 0), Map.of(0, List.of()), null, null);
      ServerPlanRequestContext context =
          new ServerPlanRequestContext(new StagePlan(aggregate, mock(StageMetadata.class)), mock(QueryExecutor.class),
              executorService, emptyBuild);

      ServerPlanRequestVisitor.walkPlanNode(aggregate, context);

      assertFalse(context.shouldSkipLeafQueryExecution());
    } finally {
      ExecutorServiceUtils.close(executorService);
    }
  }

  @Test
  public void testEmptyDynamicBroadcastBuildDoesNotSkipExplain() {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.getInstanceDataManager())
          .thenThrow(new AssertionError("EXPLAIN must construct the leaf request"));
      MailboxReceiveNode buildInput = new MailboxReceiveNode(STAGE_ID, DATA_SCHEMA, BUILD_STAGE_ID,
          PinotRelExchangeType.PIPELINE_BREAKER, RelDistribution.Type.SINGLETON, List.of(0), List.of(), false, false,
          null);
      TableScanNode probeInput =
          new TableScanNode(STAGE_ID, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), "probe", List.of("key"));
      PlanNode dynamicFilter = newDynamicFilterNode(false, probeInput, buildInput);
      PipelineBreakerResult emptyBuild =
          new PipelineBreakerResult(Map.of(buildInput, 0), Map.of(0, List.of()), null, null);
      WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of());
      workerMetadata.setTableSegmentsMap(Map.of(TableType.OFFLINE.name(), List.of("segment")));
      StageMetadata stageMetadata = new StageMetadata(STAGE_ID, List.of(workerMetadata),
          Map.of(DispatchablePlanFragment.TABLE_NAME_KEY, "probe"));
      OpChainExecutionContext context =
          new OpChainExecutionContext(mock(MailboxService.class), 123L, "cid", System.currentTimeMillis() + 60_000,
              System.currentTimeMillis() + 60_000, "broker", Map.of(), stageMetadata, workerMetadata, emptyBuild,
              false, false);
      StagePlan stagePlan = new StagePlan(dynamicFilter, stageMetadata);

      assertThrows(AssertionError.class,
          () -> ServerPlanRequestUtils.compileLeafStage(context, stagePlan, queryExecutor, executorService,
              (planNode, operator) -> {
              }, true, Map.of()));
    } finally {
      ExecutorServiceUtils.close(executorService);
    }
  }

  @SuppressWarnings("removal")
  private static PlanNode newDynamicFilterNode(boolean enrichedJoin, PlanNode probeInput, PlanNode buildInput) {
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
