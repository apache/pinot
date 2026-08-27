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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.SharedMailboxInfos;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.LONG;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class SortedMailboxReceiveOperatorTest {
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
  private static final List<RelFieldCollation> FIELD_COLLATIONS =
      List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
  private static final String MAILBOX_ID_1 = MailboxIdUtils.toMailboxId(0, 1, 0, 0, 0);
  private static final String MAILBOX_ID_2 = MailboxIdUtils.toMailboxId(0, 1, 1, 0, 0);

  private StageMetadata _stageMetadataBoth;
  private StageMetadata _stageMetadata1;

  private AutoCloseable _mocks;
  @Mock
  private MailboxService _mailboxService;
  @Mock
  private ReceivingMailbox _mailbox1;
  @Mock
  private ReceivingMailbox _mailbox2;

  @BeforeClass
  public void setUp() {
    MailboxInfos mailboxInfosBoth = new SharedMailboxInfos(new MailboxInfo("localhost", 1234, List.of(0, 1)));
    _stageMetadataBoth = new StageMetadata(0,
        Stream.of(0, 1).map(workerId -> new WorkerMetadata(workerId, Map.of(1, mailboxInfosBoth), Map.of()))
            .collect(Collectors.toList()), Map.of());
    MailboxInfos mailboxInfos1 = new SharedMailboxInfos(new MailboxInfo("localhost", 1234, List.of(0)));
    _stageMetadata1 =
        new StageMetadata(0, List.of(new WorkerMetadata(0, Map.of(1, mailboxInfos1), Map.of())), Map.of());
  }

  @BeforeMethod
  public void setUpMethod() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_mailboxService.getHostname()).thenReturn("localhost");
    when(_mailboxService.getPort()).thenReturn(1234);
    when(_mailbox1.getStatMap()).thenReturn(new StatMap<>(ReceivingMailbox.StatKey.class));
    when(_mailbox2.getStatMap()).thenReturn(new StatMap<>(ReceivingMailbox.StatKey.class));
  }

  @AfterMethod
  public void tearDownMethod()
      throws Exception {
    _mocks.close();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*RANGE_DISTRIBUTED.*")
  public void shouldThrowRangeDistributionNotSupported() {
    getOperator(_stageMetadata1, RelDistribution.Type.RANGE_DISTRIBUTED);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Field collations.*")
  public void shouldThrowOnEmptyCollationKey() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    getOperator(_stageMetadata1, RelDistribution.Type.SINGLETON, DATA_SCHEMA, List.of(), Long.MAX_VALUE);
  }

  @Test
  public void shouldTimeout() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    try (SortedMailboxReceiveOperator operator = getOperator(_stageMetadata1, RelDistribution.Type.SINGLETON,
        DATA_SCHEMA, FIELD_COLLATIONS, System.currentTimeMillis() + 100L)) {
      MseBlock block = operator.nextBlock();
      assertTrue(block.isError());
      assertTrue(((ErrorMseBlock) block).getErrorMessages().containsKey(QueryErrorCode.EXECUTION_TIMEOUT));
    }
  }

  @Test
  public void shouldReceiveEosDirectlyFromSender() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getOperator(_stageMetadata1, RelDistribution.Type.SINGLETON)) {
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldReceiveSingletonMailbox() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    Object[] row = new Object[]{1, 1};
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row),
        OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getOperator(_stageMetadata1, RelDistribution.Type.SINGLETON)) {
      List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
      assertEquals(resultRows.size(), 1);
      assertEquals(resultRows.get(0), row);
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldReceiveSingletonErrorMailbox() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    String errorMessage = "TEST ERROR";
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.errorWithEmptyStats(new RuntimeException(errorMessage)));
    try (SortedMailboxReceiveOperator operator = getOperator(_stageMetadata1, RelDistribution.Type.SINGLETON)) {
      MseBlock block = operator.nextBlock();
      assertTrue(block.isError());
      assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.UNKNOWN).contains(errorMessage));
    }
  }

  @Test
  public void shouldReceiveMailboxFromTwoServersOneNull() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(null, OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    Object[] row = new Object[]{1, 1};
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row),
        OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
      assertEquals(resultRows.size(), 1);
      assertEquals(resultRows.get(0), row);
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldGetReceptionReceiveErrorMailbox() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    String errorMessage = "TEST ERROR";
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.errorWithEmptyStats(new RuntimeException(errorMessage)));
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    Object[] row = new Object[]{3, 3};
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row),
        OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      MseBlock block = operator.nextBlock();
      assertTrue(block.isError());
      assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.UNKNOWN).contains(errorMessage));
    }
  }

  @Test
  public void shouldReceiveMailboxFromTwoServersWithCollationKey() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    Object[] row1 = new Object[]{3, 3};
    Object[] row2 = new Object[]{1, 1};
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row1),
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row2),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    Object[] row3 = new Object[]{4, 2};
    Object[] row4 = new Object[]{2, 4};
    Object[] row5 = new Object[]{-1, 95};
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row3),
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row4),
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row5),
        OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      assertEquals(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows(), List.of(row5, row2, row4, row1, row3));
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldReceiveMailboxFromTwoServersWithCollationKeyTwoColumns() {
    DataSchema dataSchema =
        new DataSchema(new String[]{"col1", "col2", "col3"}, new DataSchema.ColumnDataType[]{INT, INT, STRING});
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(2, Direction.DESCENDING, NullDirection.FIRST),
        new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    Object[] row1 = new Object[]{3, 3, "queen"};
    Object[] row2 = new Object[]{1, 1, "pink floyd"};
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(dataSchema, row1),
        OperatorTestUtil.blockWithStats(dataSchema, row2),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    Object[] row3 = new Object[]{4, 2, "pink floyd"};
    Object[] row4 = new Object[]{2, 4, "aerosmith"};
    Object[] row5 = new Object[]{-1, 95, "foo fighters"};
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(dataSchema, row3),
        OperatorTestUtil.blockWithStats(dataSchema, row4),
        OperatorTestUtil.blockWithStats(dataSchema, row5),
        OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getOperator(_stageMetadataBoth, RelDistribution.Type.HASH_DISTRIBUTED,
        dataSchema, collations, Long.MAX_VALUE)) {
      assertEquals(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows(), List.of(row1, row2, row3, row5, row4));
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldMergeFromTwoServersInOrder() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}, new Object[]{3, 3}, new Object[]{5, 5}),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}, new Object[]{4, 4}, new Object[]{6, 6}),
        OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
      assertEquals(rows.size(), 6);
      for (int i = 0; i < 6; i++) {
        assertEquals(rows.get(i)[0], i + 1);
      }
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldMergeWithTiedKeysPreservingMultiset() {
    // Rows with equal collation keys (col0) but distinct payloads (col1). The k-way merge (PriorityQueue) is not a
    // stable sort, so tie order may differ from the accumulate-then-sort path; assert what IS guaranteed: the output
    // is globally non-decreasing by the collation key and the row multiset is preserved.
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 10}, new Object[]{1, 11}, new Object[]{2, 12}),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 20}, new Object[]{2, 21}, new Object[]{2, 22}),
        OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
      assertEquals(rows.size(), 6);
      for (int i = 1; i < rows.size(); i++) {
        assertTrue((int) rows.get(i)[0] >= (int) rows.get(i - 1)[0], "merge output must be non-decreasing by key");
      }
      // Multiset equality, independent of tie order: sort both sides by (col0, col1) and compare element-wise.
      List<Object[]> actualSorted = new ArrayList<>(rows);
      actualSorted.sort((x, y) -> (int) x[0] != (int) y[0] ? (int) x[0] - (int) y[0] : (int) x[1] - (int) y[1]);
      List<Object[]> expected = List.of(new Object[]{1, 10}, new Object[]{1, 11}, new Object[]{1, 20},
          new Object[]{2, 12}, new Object[]{2, 21}, new Object[]{2, 22});
      assertRowsEqual(actualSorted, expected);
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldMergeWithStaggeredEos() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}, new Object[]{3, 3}, new Object[]{4, 4}),
        OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
      assertEquals(rows.size(), 4);
      for (int i = 0; i < 4; i++) {
        assertEquals(rows.get(i)[0], i + 1);
      }
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldMergeInterleavedNotReady() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    // Capture the reader BEFORE constructing the operator (the ctor registers it). Single-consumer-thread test, so
    // reading the captured reference from a later poll Answer needs no synchronization.
    ReceivingMailbox.Reader[] reader1 = new ReceivingMailbox.Reader[1];
    doAnswer(inv -> {
      reader1[0] = inv.getArgument(0);
      return null;
    }).when(_mailbox1).registeredReader(any());
    // data -> (self-wake + null) -> data -> EOS. The self-wake makes the blocking poll return immediately.
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}))
        .thenAnswer(inv -> {
          reader1[0].blockReadyToRead();
          return null;
        })
        .thenReturn(OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}))
        .thenReturn(OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadata1, RelDistribution.Type.SINGLETON,
        DATA_SCHEMA, FIELD_COLLATIONS, System.currentTimeMillis() + 30_000L,
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true"))) {
      List<Object[]> all = new ArrayList<>();
      MseBlock block = operator.nextBlock();
      while (block.isData()) {
        all.addAll(((MseBlock.Data) block).asRowHeap().getRows());
        block = operator.nextBlock();
      }
      assertTrue(block.isSuccess());
      assertEquals(all.size(), 2);
      assertEquals(all.get(0)[0], 1);
      assertEquals(all.get(1)[0], 2);
    }
  }

  /// Regression for the streaming k-way merge pipeline deadlock: when the merge needs the next row from one stream that
  /// is momentarily empty, it must drain the OTHER ready stream (relieving that sender's backpressure) instead of
  /// head-of-line blocking on the starved stream. Here mailbox1 is starved (returns null) and only becomes ready when
  /// mailbox2 is polled during draining; a merge that blocks on mailbox1 alone never polls mailbox2 and times out.
  @Test
  public void shouldDrainReadySiblingWhenOtherStreamStarved() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    // Capture mailbox1's reader (registered in the operator ctor) so mailbox2's poll can wake it.
    ReceivingMailbox.Reader[] reader1 = new ReceivingMailbox.Reader[1];
    doAnswer(inv -> {
      reader1[0] = inv.getArgument(0);
      return null;
    }).when(_mailbox1).registeredReader(any());
    // mailbox1: {1}, then starved (null), then {3}, then EOS. The null forces the merge to look elsewhere.
    when(_mailbox1.poll()).thenReturn(
            OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}))
        .thenReturn(null)
        .thenReturn(OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{3, 3}))
        .thenReturn(OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    // mailbox2: {2}; polling for the next block ({4}) wakes mailbox1 (models the sibling drain unblocking the starved
    // sender); then EOS.
    when(_mailbox2.poll()).thenReturn(
            OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}))
        .thenAnswer(inv -> {
          reader1[0].blockReadyToRead();
          return OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{4, 4});
        })
        .thenReturn(OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED, DATA_SCHEMA, FIELD_COLLATIONS,
        System.currentTimeMillis() + 30_000L,
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true"))) {
      List<Object[]> all = new ArrayList<>();
      MseBlock block = operator.nextBlock();
      while (block.isData()) {
        all.addAll(((MseBlock.Data) block).asRowHeap().getRows());
        block = operator.nextBlock();
      }
      assertTrue(block.isSuccess());
      assertEquals(all.size(), 4);
      for (int i = 0; i < 4; i++) {
        assertEquals(all.get(i)[0], i + 1);
      }
      // Prove the sibling drain actually ran (not just that the output happens to be correct): mailbox2 must have been
      // polled past its first block (drained for row {4,4} and its EOS) while mailbox1 was starved, and mailbox1 must
      // have been re-polled after the starved (null) response instead of parking on it forever.
      verify(_mailbox2, atLeast(3)).poll();
      verify(_mailbox1, atLeast(3)).poll();
    }
  }

  /// Regression for error propagation during the sibling drain: while refilling a starved stream, the merge polls
  /// sibling streams non-blocking; if a sibling yields an error mid-drain, that error must short-circuit the merge
  /// immediately rather than being swallowed or causing a hang while the starved stream is still awaited.
  @Test
  public void shouldPropagateErrorFromSiblingDuringDrain() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    // mailbox1: {1}, then starved forever (null) -- it never itself produces the error or EOS.
    when(_mailbox1.poll()).thenReturn(
            OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}))
        .thenReturn(null);
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    // mailbox2: {2}, then an error -- surfaced while mailbox2 is drained as a sibling of the starved mailbox1.
    String errorMessage = "SIBLING ERROR";
    when(_mailbox2.poll()).thenReturn(
            OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}))
        .thenReturn(OperatorTestUtil.errorWithEmptyStats(new RuntimeException(errorMessage)));
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED, DATA_SCHEMA, FIELD_COLLATIONS,
        System.currentTimeMillis() + 30_000L,
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true"))) {
      MseBlock block = operator.nextBlock();
      assertTrue(block.isError());
      assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.UNKNOWN).contains(errorMessage));
    }
  }

  /// Regression for the early-termination drain path (drainToEos), which was rewritten to be cooperative for the same
  /// reason as the merge itself: it must not head-of-line block on one handle while a sibling still has buffered data,
  /// or the sibling's sender backpressures and the pipeline deadlocks. mailbox1 is starved (null) until mailbox2 is
  /// polled during the round-robin drain; a drain that blocks on mailbox1 alone never polls mailbox2 and times out.
  @Test
  public void shouldDrainSiblingsCooperativelyOnEarlyTerminate() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    ReceivingMailbox.Reader[] reader1 = new ReceivingMailbox.Reader[1];
    doAnswer(inv -> {
      reader1[0] = inv.getArgument(0);
      return null;
    }).when(_mailbox1).registeredReader(any());
    // mailbox1: starved (null), then EOS. It only makes progress once woken by mailbox2's drain.
    when(_mailbox1.poll()).thenReturn(null).thenReturn(OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    // mailbox2: a data block (discarded on early-termination) whose poll wakes mailbox1, then EOS.
    when(_mailbox2.poll())
        .thenAnswer(inv -> {
          reader1[0].blockReadyToRead();
          return OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1});
        })
        .thenReturn(OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED, DATA_SCHEMA, FIELD_COLLATIONS,
        System.currentTimeMillis() + 10_000L,
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true"))) {
      operator.earlyTerminate();
      assertTrue(operator.nextBlock().isSuccess());
      verify(_mailbox1).earlyTerminate();
      verify(_mailbox2).earlyTerminate();
      // Both mailboxes were drained to EOS cooperatively (round-robin), not head-of-line blocked on the starved one.
      verify(_mailbox1, atLeast(2)).poll();
      verify(_mailbox2, atLeast(2)).poll();
    }
  }

  /// Regression proving the per-stream backlog is served in FIFO order. While mailbox1 (the current min source) is
  /// starved, the greedy sibling drain buffers ALL of mailbox2's ready blocks ({3},{5}) into mailbox2's backlog at
  /// once; mailbox2 also reaches EOS during that drain. The merge must then serve that multi-block backlog oldest-first
  /// so the global output stays sorted (1,2,3,4,5). A LIFO backlog would emit 5 before 3.
  @Test
  public void shouldServeMultiBlockBacklogInOrderWhenStreamStarved() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    // mailbox1 (min source): {1}, starved (null), {4}, EOS.
    when(_mailbox1.poll()).thenReturn(
            OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}))
        .thenReturn(null)
        .thenReturn(OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{4, 4}))
        .thenReturn(OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    // mailbox2 (fast sibling): {2}, then {3},{5} buffered together during the drain, then EOS (also during the drain).
    when(_mailbox2.poll()).thenReturn(
            OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}),
            OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{3, 3}),
            OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{5, 5}),
            OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED, DATA_SCHEMA, FIELD_COLLATIONS,
        System.currentTimeMillis() + 10_000L,
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true"))) {
      List<Object[]> all = new ArrayList<>();
      MseBlock block = operator.nextBlock();
      while (block.isData()) {
        all.addAll(((MseBlock.Data) block).asRowHeap().getRows());
        block = operator.nextBlock();
      }
      assertTrue(block.isSuccess());
      assertEquals(all.size(), 5);
      for (int i = 0; i < 5; i++) {
        assertEquals(all.get(i)[0], i + 1);
      }
    }
  }

  @Test
  public void shouldMergeBoundedMultiBlock() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}, new Object[]{3, 3}, new Object[]{5, 5}),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}, new Object[]{4, 4}, new Object[]{6, 6}),
        OperatorTestUtil.eosWithEmptyStats());
    SortUtils.SortComparator comparator = new SortUtils.SortComparator(FIELD_COLLATIONS, false);
    Map<String, String> hints = Map.of(
        CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true",
        CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE_BLOCK_SIZE, "2");
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED, DATA_SCHEMA, FIELD_COLLATIONS, Long.MAX_VALUE, hints)) {
      Object[] prevLast = null;
      int dataBlocks = 0;
      List<Object[]> all = new ArrayList<>();
      MseBlock block = operator.nextBlock();
      while (block.isData()) {
        List<Object[]> rows = ((MseBlock.Data) block).asRowHeap().getRows();
        assertEquals(rows.size(), 2);
        if (prevLast != null) {
          assertTrue(comparator.compare(prevLast, rows.get(0)) <= 0);
        }
        prevLast = rows.get(rows.size() - 1);
        all.addAll(rows);
        dataBlocks++;
        block = operator.nextBlock();
      }
      assertEquals(dataBlocks, 3);
      for (int i = 0; i < 6; i++) {
        assertEquals(all.get(i)[0], i + 1);
      }
      assertTrue(block.isSuccess());
    }
  }

  @Test
  public void shouldMergeWithDescAndNullDirection() {
    DataSchema dataSchema =
        new DataSchema(new String[]{"col1", "col2", "col3"}, new DataSchema.ColumnDataType[]{INT, INT, STRING});
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(2, Direction.DESCENDING, NullDirection.FIRST),
        new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortUtils.SortComparator comparator = new SortUtils.SortComparator(collations, false);
    Object[] row1 = new Object[]{3, 3, "queen"};
    Object[] row2 = new Object[]{1, 1, "pink floyd"};
    Object[] row3 = new Object[]{4, 2, "pink floyd"};
    Object[] row4 = new Object[]{2, 4, "aerosmith"};
    Object[] row5 = new Object[]{-1, 95, null};
    List<Object[]> mb1 = new ArrayList<>(List.of(row1, row2));
    mb1.sort(comparator);
    List<Object[]> mb2 = new ArrayList<>(List.of(row3, row4, row5));
    mb2.sort(comparator);
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(dataSchema, mb1.toArray(new Object[0][])),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(dataSchema, mb2.toArray(new Object[0][])),
        OperatorTestUtil.eosWithEmptyStats());
    List<Object[]> reference = new ArrayList<>(List.of(row1, row2, row3, row4, row5));
    reference.sort(comparator);
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED, dataSchema, collations, Long.MAX_VALUE, Map.of(
            CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true"))) {
      assertRowsEqual(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows(), reference);
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldReturnErrorWhenMergeMailboxErrors() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    String errorMessage = "TEST ERROR";
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.errorWithEmptyStats(new RuntimeException(errorMessage)));
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{3, 3}),
        OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      MseBlock block = operator.nextBlock();
      assertTrue(block.isError());
      assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.UNKNOWN).contains(errorMessage));
    }
  }

  @Test
  public void shouldTimeoutInMergeMode() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadata1, RelDistribution.Type.SINGLETON,
        DATA_SCHEMA, FIELD_COLLATIONS, System.currentTimeMillis() + 100L, Map.of(
            CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true"))) {
      MseBlock block = operator.nextBlock();
      assertTrue(block.isError());
      assertTrue(((ErrorMseBlock) block).getErrorMessages().containsKey(QueryErrorCode.EXECUTION_TIMEOUT));
    }
  }

  @Test
  public void shouldReturnSuccessOnEarlyTerminateInMergeMode() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadata1, RelDistribution.Type.SINGLETON)) {
      operator.earlyTerminate();
      assertTrue(operator.nextBlock().isSuccess());
      verify(_mailbox1).earlyTerminate();
    }
  }

  /// Early termination in production (`SortOperator` hitting its LIMIT) arrives *mid-merge*: the heap is
  /// live, mailboxes still hold undelivered blocks, and no handle has reached EOS. That is the state `drainToEos`
  /// was rewritten for — it must round-robin every remaining handle to EOS rather than head-of-line block on one while
  /// a sibling's sender sits on a full mailbox. Terminating before the first `nextBlock()` (as the other early
  /// termination tests do) leaves that path unexercised, because every handle is already exhausted on the first pass.
  @Test
  public void shouldDrainBothMailboxesWhenEarlyTerminatedMidMerge() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}),
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{3, 3}),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}),
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{4, 4}),
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{6, 6}),
        OperatorTestUtil.eosWithEmptyStats());
    // Block size 1 so the merge is primed and mid-flight (heap live, both mailboxes unexhausted) after two calls.
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED, DATA_SCHEMA, FIELD_COLLATIONS, Long.MAX_VALUE,
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true",
            CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE_BLOCK_SIZE, "1"))) {
      MseBlock first = operator.nextBlock();
      assertTrue(first.isData());
      assertRowsEqual(((MseBlock.Data) first).asRowHeap().getRows(), List.<Object[]>of(new Object[]{1, 1}));
      MseBlock second = operator.nextBlock();
      assertTrue(second.isData());
      assertRowsEqual(((MseBlock.Data) second).asRowHeap().getRows(), List.<Object[]>of(new Object[]{2, 2}));

      operator.earlyTerminate();
      assertTrue(operator.nextBlock().isSuccess());
      // Both senders must have been driven to EOS, not just the one the merge happened to be waiting on.
      verify(_mailbox1).earlyTerminate();
      verify(_mailbox2).earlyTerminate();
      verify(_mailbox1, atLeast(3)).poll();
      verify(_mailbox2, atLeast(4)).poll();
    }
  }

  /// The merge's whole correctness rests on each mailbox stream already being sorted, and nothing downstream re-sorts.
  /// If a sender violates that (a plan shape the fragmenter gate should have rejected, or a leaf concatenating two
  /// independently sorted runs), the merge must fail loudly rather than silently emit misordered rows.
  @Test
  public void shouldFailFastOnOutOfOrderSenderRows() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    // A single mailbox whose second block sorts before its first: two independently sorted runs concatenated.
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{5, 5}, new Object[]{9, 9}),
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}, new Object[]{2, 2}),
        OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadata1, RelDistribution.Type.SINGLETON,
        DATA_SCHEMA, FIELD_COLLATIONS, Long.MAX_VALUE,
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true"))) {
      // MultiStageOperator.nextBlock() converts the thrown IllegalStateException into an error block, so the query
      // fails with a diagnosable message instead of returning silently misordered rows.
      MseBlock block = operator.nextBlock();
      assertTrue(block.isError());
      assertTrue(((ErrorMseBlock) block).getErrorMessages().values().stream()
              .anyMatch(message -> message.contains("out-of-order")),
          "Expected an out-of-order error, got: " + ((ErrorMseBlock) block).getErrorMessages());
    }
  }

  @Test
  public void shouldPreserveStatsInMergeMode()
      throws Exception {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    List<DataBuffer> stats1 = new MultiStageQueryStats.Builder(1).addLast(
        open -> open.addLastOperator(MultiStageOperator.Type.MAILBOX_SEND,
                new StatMap<>(MailboxSendOperator.StatKey.class))
            .addLastOperator(MultiStageOperator.Type.LEAF, new StatMap<>(LeafOperator.StatKey.class))
            .close()).build().serialize();
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}),
        OperatorTestUtil.eosWithStats(stats1));
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    List<DataBuffer> stats2 = new MultiStageQueryStats.Builder(1).addLast(
        open -> open.addLastOperator(MultiStageOperator.Type.MAILBOX_SEND,
                new StatMap<>(MailboxSendOperator.StatKey.class))
            .addLastOperator(MultiStageOperator.Type.LEAF, new StatMap<>(LeafOperator.StatKey.class))
            .close()).build().serialize();
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}),
        OperatorTestUtil.eosWithStats(stats2));
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      while (!operator.nextBlock().isEos()) {
        // drain
      }
      MultiStageQueryStats stats = operator.calculateStats();
      assertNotNull(stats);
      // Both mailboxes carry identically-shaped upstream stats, so they merge cleanly into stage 1.
      MultiStageQueryStats.StageStats.Closed upstreamStats = stats.getUpstreamStageStats(1);
      assertNotNull(upstreamStats);
    }
  }

  @Test
  public void shouldMatchFullSortParityWithMerge() {
    Random random = new Random(42L);
    SortUtils.SortComparator comparator = new SortUtils.SortComparator(FIELD_COLLATIONS, false);
    // Use unique col0 (the only collation key) across both lists so the comparator has no ties; with ties the merge
    // order and a single List.sort order of equal-key rows are both valid but need not match.
    List<Integer> keys = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      keys.add(i);
    }
    Collections.shuffle(keys, random);
    List<Object[]> mb1 = sortedRowsFromKeys(keys.subList(0, 7), random, comparator);
    List<Object[]> mb2 = sortedRowsFromKeys(keys.subList(7, 16), random, comparator);

    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, mb1.toArray(new Object[0][])),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, mb2.toArray(new Object[0][])),
        OperatorTestUtil.eosWithEmptyStats());
    List<Object[]> mergeRows = new ArrayList<>();
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      MseBlock block = operator.nextBlock();
      while (block.isData()) {
        mergeRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
        block = operator.nextBlock();
      }
      assertTrue(block.isSuccess());
    }

    // Re-stub the same mailboxes with the same data and run the default accumulate-then-sort path.
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, mb1.toArray(new Object[0][])),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, mb2.toArray(new Object[0][])),
        OperatorTestUtil.eosWithEmptyStats());
    List<Object[]> defaultRows = new ArrayList<>();
    try (SortedMailboxReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      MseBlock block = operator.nextBlock();
      while (block.isData()) {
        defaultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
        block = operator.nextBlock();
      }
      assertTrue(block.isSuccess());
    }
    assertRowsEqual(mergeRows, defaultRows);
  }

  @Test
  public void shouldMatchMultiColumnParityWithMerge() {
    // Schema: key1 (STRING), key2 (INT), key3 (INT), payload (LONG)
    DataSchema schema = new DataSchema(
        new String[]{"key1", "key2", "key3", "payload"},
        new DataSchema.ColumnDataType[]{STRING, INT, INT, LONG});

    // Collation: key1 DESC NULLS_FIRST, key2 ASC NULLS_LAST, key3 DESC NULLS_LAST
    List<RelFieldCollation> collations = List.of(
        new RelFieldCollation(0, Direction.DESCENDING, NullDirection.FIRST),
        new RelFieldCollation(1, Direction.ASCENDING, NullDirection.LAST),
        new RelFieldCollation(2, Direction.DESCENDING, NullDirection.LAST));
    SortUtils.SortComparator comparator = new SortUtils.SortComparator(collations, false);

    // ~20 rows with: tied key1 values, tied key1+key2 pairs, nulls in key1,
    // all (key1, key2, key3) composite keys unique for deterministic ordering.
    List<Object[]> allRows = List.of(
        new Object[]{null, 5, 10, 100L},    // null key1 — exercises NULLS_FIRST in DESC
        new Object[]{null, 5, 20, 101L},    // null key1 — different key3 breaks tie
        new Object[]{null, 10, 30, 102L},   // null key1, different key2
        new Object[]{"delta", 1, 50, 103L},
        new Object[]{"delta", 1, 40, 104L}, // tied key1+key2, key3 breaks tie
        new Object[]{"delta", 2, 60, 105L},
        new Object[]{"delta", 3, 70, 106L},
        new Object[]{"charlie", 1, 10, 107L},
        new Object[]{"charlie", 1, 20, 108L},
        new Object[]{"charlie", 2, 30, 109L},
        new Object[]{"bravo", 5, 15, 110L},
        new Object[]{"bravo", 5, 25, 111L},
        new Object[]{"bravo", 10, 35, 112L},
        new Object[]{"alpha", 1, 100, 113L},
        new Object[]{"alpha", 1, 200, 114L},
        new Object[]{"alpha", 2, 50, 115L},
        new Object[]{"alpha", 3, 10, 116L},
        new Object[]{"alpha", 3, 20, 117L},
        new Object[]{"alpha", 4, 5, 118L},
        new Object[]{"alpha", 4, 15, 119L}
    );

    // Shuffle deterministically and split across 2 mailboxes
    Random random = new Random(99L);
    List<Object[]> shuffled = new ArrayList<>(allRows);
    Collections.shuffle(shuffled, random);
    List<Object[]> mb1 = new ArrayList<>(shuffled.subList(0, 10));
    List<Object[]> mb2 = new ArrayList<>(shuffled.subList(10, 20));
    mb1.sort(comparator);
    mb2.sort(comparator);

    // --- Merge path ---
    RelDistribution.Type distributionType = RelDistribution.Type.HASH_DISTRIBUTED;
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(schema, mb1.toArray(new Object[0][])),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(schema, mb2.toArray(new Object[0][])),
        OperatorTestUtil.eosWithEmptyStats());
    List<Object[]> mergeRows = new ArrayList<>();
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        distributionType, schema, collations, Long.MAX_VALUE,
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true"))) {
      MseBlock block = operator.nextBlock();
      while (block.isData()) {
        mergeRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
        block = operator.nextBlock();
      }
      assertTrue(block.isSuccess());
    }

    // --- Accumulate path (re-stub same data) ---
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(schema, mb1.toArray(new Object[0][])),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(schema, mb2.toArray(new Object[0][])),
        OperatorTestUtil.eosWithEmptyStats());
    List<Object[]> defaultRows = new ArrayList<>();
    try (SortedMailboxReceiveOperator operator = getOperator(_stageMetadataBoth,
        distributionType, schema, collations, Long.MAX_VALUE)) {
      MseBlock block = operator.nextBlock();
      while (block.isData()) {
        defaultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
        block = operator.nextBlock();
      }
      assertTrue(block.isSuccess());
    }

    assertRowsEqual(mergeRows, defaultRows);
  }

  @Test
  public void shouldAccumulateWhenHintTrueButNotSortedOnSender() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{3, 3}, new Object[]{1, 1}),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}),
        OperatorTestUtil.eosWithEmptyStats());
    // hint=true but isSortedOnSender=false => accumulate-then-sort (AND gate blocks the unsafe arm).
    // blockSize=2 proves accumulate: merge would emit 2 blocks of 2, but accumulate returns all 3 rows in one block.
    try (SortedMailboxReceiveOperator operator = getSenderSortedOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED, false, "2",
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true",
            CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE_BLOCK_SIZE, "2"))) {
      assertRowsEqual(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows(),
          List.of(new Object[]{1, 1}, new Object[]{2, 2}, new Object[]{3, 3}));
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldUseAccumulatePathWhenHintFalse() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{3, 3}, new Object[]{1, 1}),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}),
        OperatorTestUtil.eosWithEmptyStats());
    try (SortedMailboxReceiveOperator operator = getMergeOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED, DATA_SCHEMA, FIELD_COLLATIONS, Long.MAX_VALUE, Map.of(
            CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "false"))) {
      // Accumulate-then-sort path returns a single globally sorted data block.
      assertRowsEqual(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows(),
          List.of(new Object[]{1, 1}, new Object[]{2, 2}, new Object[]{3, 3}));
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldAccumulateWhenHintUnsetEvenIfSortedOnSender() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}, new Object[]{3, 3}),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}, new Object[]{4, 4}),
        OperatorTestUtil.eosWithEmptyStats());
    // No hint in opChainMetadata + isSortedOnSender()==true => accumulate-then-sort (AND gate requires explicit hint).
    // blockSize=2 proves accumulate: merge would emit 2 blocks of 2, but accumulate returns all 4 rows in one block.
    try (SortedMailboxReceiveOperator operator = getSenderSortedOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED, true, "2")) {
      assertRowsEqual(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows(),
          List.of(new Object[]{1, 1}, new Object[]{2, 2}, new Object[]{3, 3}, new Object[]{4, 4}));
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldAccumulateWhenHintUnsetAndNotSortedOnSender() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{3, 3}, new Object[]{1, 1}),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 2}),
        OperatorTestUtil.eosWithEmptyStats());
    // No hint + isSortedOnSender()==false => accumulate-then-sort path: one globally sorted block.
    try (SortedMailboxReceiveOperator operator = getSenderSortedOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED, false, null)) {
      assertRowsEqual(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows(),
          List.of(new Object[]{1, 1}, new Object[]{2, 2}, new Object[]{3, 3}));
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  @Test
  public void shouldReportKWayMergeStatWhenMergeUsed() {
    assertKWayMergeStat(true,
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true"), true);
  }

  @Test
  public void shouldNotReportKWayMergeStatWhenSenderNotSorted() {
    assertKWayMergeStat(false,
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true"), false);
  }

  @Test
  public void shouldNotReportKWayMergeStatWhenOptionOff() {
    assertKWayMergeStat(true, Map.of(), false);
  }

  /// The stat travels to the broker through [StatMap#serialize]/[StatMap#deserialize], which encode keys by
  /// ordinal. Asserting the round trip (and the ordinal position) here means a future reordering or an accidental
  /// change to the presence-based boolean encoding fails loudly rather than silently breaking cluster diagnosis and
  /// mixed-version decoding.
  @Test
  public void shouldRoundTripKWayMergeStatThroughSerialization()
      throws Exception {
    BaseMailboxReceiveOperator.StatKey[] keys = BaseMailboxReceiveOperator.StatKey.values();
    assertEquals(keys[keys.length - 1], BaseMailboxReceiveOperator.StatKey.K_WAY_MERGE_USED,
        "K_WAY_MERGE_USED must stay the last key: StatMap serializes by ordinal, so keys may only be appended");

    StatMap<BaseMailboxReceiveOperator.StatKey> merged =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    merged.merge(BaseMailboxReceiveOperator.StatKey.K_WAY_MERGE_USED, true);
    assertTrue(roundTrip(merged).getBoolean(BaseMailboxReceiveOperator.StatKey.K_WAY_MERGE_USED));

    // On the accumulate-then-sort path the key must not be written at all, which is what keeps the new ordinal off
    // the wire for peers that predate it.
    StatMap<BaseMailboxReceiveOperator.StatKey> notMerged =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    notMerged.merge(BaseMailboxReceiveOperator.StatKey.K_WAY_MERGE_USED, false);
    notMerged.merge(BaseMailboxReceiveOperator.StatKey.FAN_IN, 1);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      notMerged.serialize(out);
    }
    // Serialized form is: key count, then one (ordinal, value) pair per present key. Only FAN_IN is present.
    assertEquals(bytes.toByteArray()[0], (byte) 1);
    assertFalse(roundTrip(notMerged).getBoolean(BaseMailboxReceiveOperator.StatKey.K_WAY_MERGE_USED));
  }

  private static StatMap<BaseMailboxReceiveOperator.StatKey> roundTrip(
      StatMap<BaseMailboxReceiveOperator.StatKey> statMap)
      throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      statMap.serialize(out);
    }
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return StatMap.deserialize(in, BaseMailboxReceiveOperator.StatKey.class);
    }
  }

  /// Drives a single-mailbox receive to completion and asserts the `K_WAY_MERGE_USED` stat, both on the stat map
  /// and in the JSON that is rendered into the query response `stageStats`. Both paths must return the same rows,
  /// so the stat is the only thing that distinguishes them.
  private void assertKWayMergeStat(boolean sortedOnSender, Map<String, String> opChainMetadata, boolean expected) {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}, new Object[]{2, 2}),
        OperatorTestUtil.eosWithEmptyStats());
    List<Object[]> rows = new ArrayList<>();
    try (SortedMailboxReceiveOperator operator = getSenderSortedOperator(_stageMetadata1,
        RelDistribution.Type.SINGLETON, sortedOnSender, null, opChainMetadata)) {
      MseBlock block = operator.nextBlock();
      while (block.isData()) {
        rows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
        block = operator.nextBlock();
      }
      assertTrue(block.isSuccess());
      assertRowsEqual(rows, List.of(new Object[]{1, 1}, new Object[]{2, 2}));

      StatMap<BaseMailboxReceiveOperator.StatKey> statMap = operator.copyStatMaps();
      assertEquals(statMap.getBoolean(BaseMailboxReceiveOperator.StatKey.K_WAY_MERGE_USED), expected);
      // The stat must survive into the response stageStats, which is rendered from StatMap.asJson(). Reporting is
      // presence-based: rendered as true on the merge path, and absent (not "false") otherwise.
      JsonNode kWayMergeUsed = statMap.asJson().get(BaseMailboxReceiveOperator.StatKey.K_WAY_MERGE_USED.getStatName());
      if (expected) {
        assertNotNull(kWayMergeUsed, "kWayMergeUsed must be present in the stageStats JSON when the merge is used");
        assertTrue(kWayMergeUsed.booleanValue());
      } else {
        assertNull(kWayMergeUsed,
            "kWayMergeUsed must be absent from the stageStats JSON on the accumulate-then-sort path");
      }
    }
  }

  private SortedMailboxReceiveOperator getSenderSortedOperator(StageMetadata stageMetadata,
      RelDistribution.Type distributionType, boolean sortedOnSender, String blockSize) {
    Map<String, String> opChainMetadata = blockSize == null ? Map.of()
        : Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE_BLOCK_SIZE, blockSize);
    return getSenderSortedOperator(stageMetadata, distributionType, sortedOnSender, blockSize, opChainMetadata);
  }

  private SortedMailboxReceiveOperator getSenderSortedOperator(StageMetadata stageMetadata,
      RelDistribution.Type distributionType, boolean sortedOnSender, String blockSize,
      Map<String, String> opChainMetadata) {
    if (blockSize != null && !opChainMetadata
        .containsKey(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE_BLOCK_SIZE)) {
      Map<String, String> merged = new HashMap<>(opChainMetadata);
      merged.put(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE_BLOCK_SIZE, blockSize);
      opChainMetadata = merged;
    }
    OpChainExecutionContext context =
        OperatorTestUtil.getOpChainContext(_mailboxService, Long.MAX_VALUE, stageMetadata, opChainMetadata);
    MailboxReceiveNode node = mock(MailboxReceiveNode.class);
    when(node.getDistributionType()).thenReturn(distributionType);
    when(node.getSenderStageId()).thenReturn(1);
    when(node.getDataSchema()).thenReturn(DATA_SCHEMA);
    when(node.getCollations()).thenReturn(FIELD_COLLATIONS);
    when(node.isSortedOnSender()).thenReturn(sortedOnSender);
    return new SortedMailboxReceiveOperator(context, node);
  }

  private void assertRowsEqual(List<Object[]> actual, List<Object[]> expected) {
    assertEquals(actual.size(), expected.size());
    for (int i = 0; i < actual.size(); i++) {
      assertEquals(actual.get(i), expected.get(i));
    }
  }

  private List<Object[]> sortedRowsFromKeys(List<Integer> keys, Random random, SortUtils.SortComparator comparator) {
    List<Object[]> rows = new ArrayList<>(keys.size());
    for (int key : keys) {
      rows.add(new Object[]{key, random.nextInt(50)});
    }
    rows.sort(comparator);
    return rows;
  }

  private SortedMailboxReceiveOperator getMergeOperator(StageMetadata stageMetadata,
      RelDistribution.Type distributionType, DataSchema resultSchema, List<RelFieldCollation> collations,
      long deadlineMs, Map<String, String> opChainMetadata) {
    OpChainExecutionContext context =
        OperatorTestUtil.getOpChainContext(_mailboxService, deadlineMs, stageMetadata, opChainMetadata);
    MailboxReceiveNode node = mock(MailboxReceiveNode.class);
    when(node.getDistributionType()).thenReturn(distributionType);
    when(node.getSenderStageId()).thenReturn(1);
    when(node.getDataSchema()).thenReturn(resultSchema);
    when(node.getCollations()).thenReturn(collations);
    when(node.isSortedOnSender()).thenReturn(true);
    return new SortedMailboxReceiveOperator(context, node);
  }

  private SortedMailboxReceiveOperator getMergeOperator(StageMetadata stageMetadata,
      RelDistribution.Type distributionType) {
    return getMergeOperator(stageMetadata, distributionType, DATA_SCHEMA, FIELD_COLLATIONS, Long.MAX_VALUE,
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true"));
  }

  private SortedMailboxReceiveOperator getOperator(StageMetadata stageMetadata, RelDistribution.Type distributionType,
      DataSchema resultSchema, List<RelFieldCollation> collations, long deadlineMs) {
    OpChainExecutionContext context = OperatorTestUtil.getOpChainContext(_mailboxService, deadlineMs, stageMetadata);
    MailboxReceiveNode node = mock(MailboxReceiveNode.class);
    when(node.getDistributionType()).thenReturn(distributionType);
    when(node.getSenderStageId()).thenReturn(1);
    when(node.getDataSchema()).thenReturn(resultSchema);
    when(node.getCollations()).thenReturn(collations);
    return new SortedMailboxReceiveOperator(context, node);
  }

  private SortedMailboxReceiveOperator getOperator(StageMetadata stageMetadata, RelDistribution.Type distributionType) {
    return getOperator(stageMetadata, distributionType, DATA_SCHEMA, FIELD_COLLATIONS, Long.MAX_VALUE);
  }
}
