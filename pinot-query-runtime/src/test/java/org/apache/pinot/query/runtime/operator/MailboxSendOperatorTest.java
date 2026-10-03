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
package org.apache.pinot.query.runtime.operator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class MailboxSendOperatorTest {
  private static final int SENDER_STAGE_ID = 1;

  private AutoCloseable _mocks;
  @Mock
  private MailboxService _mailboxService;
  @Mock
  private MultiStageOperator _input;
  @Mock
  private BlockExchange _exchange;

  @BeforeMethod
  public void setUpMethod() {
    _mocks = openMocks(this);
    when(_mailboxService.getHostname()).thenReturn("localhost");
    when(_mailboxService.getPort()).thenReturn(1234);

    when(_input.calculateStats()).thenReturn(MultiStageQueryStats.emptyStats(1));
  }

  @AfterMethod
  public void tearDownMethod()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldSendErrorBlock()
      throws Exception {
    // Given:
    ErrorMseBlock errorBlock = ErrorMseBlock.fromException(new Exception("TEST ERROR"));
    when(_input.nextBlock()).thenReturn(errorBlock);

    // When:
    MseBlock block = getOperator().nextBlock();

    // Then:
    assertSame(block, errorBlock, "expected error block to propagate");
    verify(_exchange).send(eq(errorBlock), anyList());
  }

  @Test
  public void shouldSendErrorBlockWhenInputThrows()
      throws Exception {
    // Given:
    when(_input.nextBlock()).thenThrow(new RuntimeException("TEST ERROR"));

    // When:
    MseBlock block = getOperator().nextBlock();

    // Then:
    assertTrue(block.isError(), "expected error block to propagate");
    ArgumentCaptor<MseBlock.Eos> captor = ArgumentCaptor.forClass(MseBlock.Eos.class);
    verify(_exchange).send(captor.capture(), anyList());
    assertTrue(captor.getValue().isError(), "expected to send error block to exchange");
  }

  @Test
  public void shouldSendEosBlock()
      throws Exception {
    // Given:
    MseBlock eosBlock = SuccessMseBlock.INSTANCE;
    when(_input.nextBlock()).thenReturn(eosBlock);

    // When:
    MseBlock block = getOperator().nextBlock();

    // Then:
    assertSame(block, eosBlock, "expected EOS block to propagate");
    ArgumentCaptor<MseBlock.Eos> captor = ArgumentCaptor.forClass(MseBlock.Eos.class);
    verify(_exchange).send(captor.capture(), anyList());
    assertTrue(captor.getValue().isSuccess(), "expected to send EOS block to exchange");
  }

  @Test
  public void shouldSendDataBlock()
      throws Exception {
    // Given:
    MseBlock dataBlock1 = getDummyDataBlock();
    MseBlock dataBlock2 = getDummyDataBlock();
    MseBlock eosBlock = SuccessMseBlock.INSTANCE;
    when(_input.nextBlock()).thenReturn(dataBlock1, dataBlock2, eosBlock);

    // When:
    MailboxSendOperator mailboxSendOperator = getOperator();
    MseBlock block = mailboxSendOperator.nextBlock();
    // Then:
    assertSame(block, dataBlock1, "expected first data block to propagate");

    // When:
    block = mailboxSendOperator.nextBlock();
    // Then:
    assertSame(block, dataBlock2, "expected second data block to propagate");

    // When:
    block = mailboxSendOperator.nextBlock();
    // Then:
    assertSame(block, eosBlock, "expected EOS block to propagate");

    ArgumentCaptor<MseBlock.Data> dataCaptor = ArgumentCaptor.forClass(MseBlock.Data.class);
    verify(_exchange, times(2)).send(dataCaptor.capture());
    List<MseBlock.Data> blocks = dataCaptor.getAllValues();
    assertSame(blocks.get(0), dataBlock1, "expected to send first data block to exchange on first call");
    assertSame(blocks.get(1), dataBlock2, "expected to send second data block to exchange on second call");

    ArgumentCaptor<MseBlock.Eos> eosCaptor = ArgumentCaptor.forClass(MseBlock.Eos.class);
    verify(_exchange, times(1)).send(eosCaptor.capture(), anyList());
    assertTrue(eosCaptor.getValue().isSuccess(), "expected to send EOS block to exchange");
  }

  @Test
  public void shouldEarlyTerminateWhenUpstreamWhenIndicated()
      throws Exception {
    // Given:
    MseBlock dataBlock = getDummyDataBlock();
    when(_input.nextBlock()).thenReturn(dataBlock);
    doReturn(true).when(_exchange).send(any());

    // When:
    getOperator().nextBlock();

    // Then:
    verify(_input).earlyTerminate();
  }

  /// The stats a send operator reports travel inside the end-of-stream block it sends, so they are collected from
  /// inside its own getNextBlock() call. Everything that call has spent, including the input call it contains, must
  /// already be accounted by then: otherwise this operator reports less time than its own input, and the stats tree
  /// renders a negative self time for the stage.
  @Test
  public void shouldAccountCurrentBlockBeforeReportingStats()
      throws Exception {
    // Given: an input that takes a measurable time and then reports EOS without ever producing a data block, so the
    // end-of-stream block is the only block this operator ever handles.
    when(_input.nextBlock()).thenAnswer(invocation -> {
      Thread.sleep(50);
      return SuccessMseBlock.INSTANCE;
    });
    long[] reportedAtSendTime = {-1};
    MailboxSendOperator[] operatorRef = new MailboxSendOperator[1];
    doAnswer(invocation -> {
      reportedAtSendTime[0] =
          operatorRef[0].copyStatMaps().getLong(MailboxSendOperator.StatKey.EXECUTION_TIME_MS);
      return null;
    }).when(_exchange).send(any(MseBlock.Eos.class), anyList());

    // When:
    MailboxSendOperator operator = getOperator();
    operatorRef[0] = operator;
    long startNs = System.nanoTime();
    operator.nextBlock();
    long wallTimeMs = (System.nanoTime() - startNs) / 1_000_000;

    // Then: the time the input spent is already part of what this operator reports when it hands its stats over.
    assertTrue(reportedAtSendTime[0] > 0,
        "expected the current block to be accounted before the stats are collected, got " + reportedAtSendTime[0]);
    long total = operator.copyStatMaps().getLong(MailboxSendOperator.StatKey.EXECUTION_TIME_MS);
    assertTrue(total >= reportedAtSendTime[0],
        "total " + total + " must not be below what was already reported " + reportedAtSendTime[0]);
    // What was accounted early must not be counted a second time when the call returns. An operator can never have
    // spent more than the call it was made from took, so double counting shows up as exceeding the wall time.
    assertTrue(total <= wallTimeMs,
        "total " + total + " exceeds the " + wallTimeMs + "ms the call actually took, so it was counted twice");
  }

  @Test
  public void shouldReportPerWorkerStats()
      throws Exception {
    // Given: a worker that sends two single-row blocks
    when(_input.nextBlock()).thenReturn(getDummyDataBlock(), getDummyDataBlock(), SuccessMseBlock.INSTANCE);

    // When:
    MailboxSendOperator operator = getOperator();
    drain(operator);

    // Then: the per-worker view is this single worker, so the max is its own emitted rows and it is not idle
    StatMap<MailboxSendOperator.StatKey> statMap = operator.copyStatMaps();
    assertEquals(statMap.getLong(MailboxSendOperator.StatKey.EMITTED_ROWS), 2L, "expected 2 emitted rows");
    assertEquals(statMap.getLong(MailboxSendOperator.StatKey.MAX_EMITTED_ROWS), 2L, "expected max to be 2");
    assertEquals(statMap.getInt(MailboxSendOperator.StatKey.NON_ACTIVE_WORKERS), 0, "expected no idle worker");
  }

  /// A send operator is idle exactly when it sent no row, so a worker that sent nothing is counted however much
  /// work the stage was given.
  @Test
  public void shouldReportIdleWorkerWhenNoRowIsSent()
      throws Exception {
    // Given: a worker that sends no data block at all
    when(_input.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);

    // When:
    MailboxSendOperator operator = getOperator();
    drain(operator);

    // Then:
    StatMap<MailboxSendOperator.StatKey> statMap = operator.copyStatMaps();
    assertEquals(statMap.getInt(MailboxSendOperator.StatKey.NON_ACTIVE_WORKERS), 1, "expected one idle worker");
    assertEquals(statMap.getLong(MailboxSendOperator.StatKey.MAX_EMITTED_ROWS), 0L, "expected no max");
  }

  /// [MultiStageOperator#calculateStats()] runs more than once per opchain, so the derived stats must not
  /// accumulate on the operator's own stat map.
  @Test
  public void shouldNotDoubleCountIdleWorkersOnRepeatedStatCollection()
      throws Exception {
    // Given: a worker that sends nothing, so it is the one counted as idle
    when(_input.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);

    // When: stats are collected several times, as the runtime does
    MailboxSendOperator operator = getOperator();
    drain(operator);
    operator.copyStatMaps();
    operator.copyStatMaps();
    StatMap<MailboxSendOperator.StatKey> statMap = operator.copyStatMaps();

    // Then: the worker is still counted exactly once
    assertEquals(statMap.getInt(MailboxSendOperator.StatKey.NON_ACTIVE_WORKERS), 1, "expected counted once");
  }

  /// Every stat is merged across the workers of the stage before being reported, which is where these stats stop
  /// describing one worker and start describing the distribution.
  @Test
  public void shouldMergePerWorkerStatsAcrossWorkers() {
    // Given: three workers of the same stage, one of which sent nothing
    StatMap<MailboxSendOperator.StatKey> stage = workerStats(5);
    stage.merge(workerStats(100));
    stage.merge(workerStats(0));

    // Then:
    assertEquals(stage.getLong(MailboxSendOperator.StatKey.EMITTED_ROWS), 105L, "expected rows to be summed");
    assertEquals(stage.getInt(MailboxSendOperator.StatKey.NON_ACTIVE_WORKERS), 1, "expected 1 of 3 workers idle");
    assertEquals(stage.getLong(MailboxSendOperator.StatKey.MAX_EMITTED_ROWS), 100L, "expected max across workers");
  }

  /// The stage's clock time is derived by dividing the summed execution time by the parallelism, which assumes the
  /// work was spread evenly. This is the same measure on the worker that took longest, so it must survive the
  /// merge as a maximum rather than a sum.
  @Test
  public void shouldReportTheSlowestWorkersClockTime() {
    StatMap<MailboxSendOperator.StatKey> slow = new StatMap<>(MailboxSendOperator.StatKey.class);
    slow.merge(MailboxSendOperator.StatKey.MAX_CLOCK_TIME_MS, 500L);
    StatMap<MailboxSendOperator.StatKey> fast = new StatMap<>(MailboxSendOperator.StatKey.class);
    fast.merge(MailboxSendOperator.StatKey.MAX_CLOCK_TIME_MS, 10L);

    slow.merge(fast);

    assertEquals(slow.getLong(MailboxSendOperator.StatKey.MAX_CLOCK_TIME_MS), 500L,
        "expected the slowest worker to win the merge, not the sum of the two");
  }

  @Test
  public void shouldPreservePerWorkerStatsAcrossSerialization()
      throws Exception {
    // Given: two workers whose stats are merged through the serialized form, as they are across servers
    StatMap<MailboxSendOperator.StatKey> stage = workerStats(0);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    workerStats(100).serialize(new DataOutputStream(bytes));

    // When:
    stage.merge(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

    // Then:
    assertEquals(stage.getInt(MailboxSendOperator.StatKey.NON_ACTIVE_WORKERS), 1, "expected one idle worker");
    assertEquals(stage.getLong(MailboxSendOperator.StatKey.MAX_EMITTED_ROWS), 100L, "expected max across workers");
  }

  /// Builds the stats a single worker sending `emittedRows` rows would report.
  ///
  /// Each worker gets its own input mock so that draining one does not exhaust the stubbing of the next.
  private StatMap<MailboxSendOperator.StatKey> workerStats(int emittedRows) {
    MultiStageOperator input = mock(MultiStageOperator.class);
    when(input.calculateStats()).thenReturn(MultiStageQueryStats.emptyStats(SENDER_STAGE_ID));
    if (emittedRows == 0) {
      when(input.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    } else {
      when(input.nextBlock()).thenReturn(getDummyDataBlock(emittedRows), SuccessMseBlock.INSTANCE);
    }
    MailboxSendOperator operator = getOperator(input);
    drain(operator);
    return operator.copyStatMaps();
  }

  private static void drain(MailboxSendOperator operator) {
    MseBlock block = operator.nextBlock();
    while (block.isData()) {
      block = operator.nextBlock();
    }
  }

  private MailboxSendOperator getOperator() {
    return getOperator(_input);
  }

  private MailboxSendOperator getOperator(MultiStageOperator input) {
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of(), Map.of());
    StageMetadata stageMetadata = new StageMetadata(SENDER_STAGE_ID, List.of(workerMetadata), Map.of());
    OpChainExecutionContext context =
        OpChainExecutionContext.fromQueryContext(_mailboxService, Map.of(), stageMetadata, workerMetadata, null, true,
            true, QueryExecutionContext.forMseTest());
    return new MailboxSendOperator(context, input, statMap -> _exchange);
  }

  private static MseBlock.Data getDummyDataBlock() {
    return getDummyDataBlock(1);
  }

  /// Returns a single data block holding `numRows` rows, which must be at least one.
  private static MseBlock.Data getDummyDataBlock(int numRows) {
    Object[][] rows = new Object[numRows][];
    Arrays.setAll(rows, i -> new Object[]{i});
    return OperatorTestUtil.block(new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{ColumnDataType.INT}),
        rows);
  }
}
