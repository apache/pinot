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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


@Test(singleThreaded = true)
public class SortedMailboxMergeReceiveOperatorTest {
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

  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = "Sender-side sorting must be enabled")
  public void shouldRequireSenderSideSorting() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    getOperator(_stageMetadata1, RelDistribution.Type.SINGLETON, DATA_SCHEMA, FIELD_COLLATIONS, Long.MAX_VALUE, true,
        false);
  }

  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = "Receiver-side sorting must be enabled")
  public void shouldRequireReceiverSideSorting() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    getOperator(_stageMetadata1, RelDistribution.Type.SINGLETON, DATA_SCHEMA, FIELD_COLLATIONS, Long.MAX_VALUE, false,
        true);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Field collations.*")
  public void shouldRequireCollations() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    getOperator(_stageMetadata1, RelDistribution.Type.SINGLETON, DATA_SCHEMA, List.of(), Long.MAX_VALUE, true, true);
  }

  @Test
  public void shouldMergeConfirmedSortedSenders() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{3, 1};
    Object[] row3 = new Object[]{5, 1};
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, row1, row2),
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, row3),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    Object[] row4 = new Object[]{-1, 2};
    Object[] row5 = new Object[]{2, 2};
    Object[] row6 = new Object[]{4, 2};
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, row4),
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, row5, row6),
        OperatorTestUtil.eosWithEmptyStats());

    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      assertEquals(drain(operator), List.of(row4, row1, row5, row2, row6, row3));
    }
  }

  @Test
  public void shouldMergeDescendingNullsFirstAndDuplicateKeys() {
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(0, Direction.DESCENDING, NullDirection.FIRST));
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA,
            new Object[]{null, 1}, new Object[]{5, 1}, new Object[]{3, 1}),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA,
            new Object[]{null, 2}, new Object[]{5, 2}, new Object[]{4, 2}, new Object[]{3, 2}),
        OperatorTestUtil.eosWithEmptyStats());

    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED, DATA_SCHEMA, collations, Long.MAX_VALUE, true, true)) {
      List<Object[]> rows = drain(operator);
      assertEquals(rows.stream().map(row -> row[0]).collect(Collectors.toList()),
          Arrays.asList(null, null, 5, 5, 4, 3, 3));
    }
  }

  @Test
  public void shouldPassThroughConfirmedSingleSenderBlock() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    MseBlock.Data dataBlock = mock(MseBlock.Data.class);
    when(dataBlock.getNumRows()).thenReturn(2);
    when(_mailbox1.poll()).thenReturn(
        new ReceivingMailbox.MseBlockWithStats(dataBlock, List.of(), true),
        OperatorTestUtil.eosWithEmptyStats());

    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadata1,
        RelDistribution.Type.SINGLETON)) {
      assertSame(operator.nextBlock(), dataBlock);
      verify(dataBlock, never()).asRowHeap();
      assertTrue(operator.nextBlock().isSuccess());
    }
  }

  /// The merge must produce a bounded block once every live sender has a head, without waiting for sender EOS.
  @Test(timeOut = 10_000)
  public void shouldEmitAtMostTenThousandRowsBeforeEos() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    Object[][] evenRows = new Object[6_000][];
    Object[][] oddRows = new Object[6_000][];
    for (int i = 0; i < 6_000; i++) {
      evenRows[i] = new Object[]{i * 2, 1};
      oddRows[i] = new Object[]{i * 2 + 1, 2};
    }
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, evenRows));
    when(_mailbox2.poll()).thenReturn(OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, oddRows));

    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
      assertEquals(rows.size(), 10_000);
      for (int i = 0; i < rows.size(); i++) {
        assertEquals(rows.get(i)[0], i);
      }
    }
  }

  @Test
  public void shouldFallBackBeforeOutputForLegacySender() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    Object[] row1 = new Object[]{5, 1};
    Object[] row2 = new Object[]{1, 1};
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row1, row2),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    Object[] row3 = new Object[]{2, 2};
    Object[] row4 = new Object[]{4, 2};
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, row3, row4),
        OperatorTestUtil.eosWithEmptyStats());

    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      assertEquals(drain(operator), List.of(row2, row3, row4, row1));
    }
  }

  @Test
  public void shouldPreserveTentativeRowsWhenFallingBack() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{4, 1};
    Object[] row3 = new Object[]{0, 1};
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, row1),
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, row2, row3),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    Object[] row4 = new Object[]{2, 2};
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, row4),
        OperatorTestUtil.eosWithEmptyStats());

    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      assertEquals(drain(operator), List.of(row3, row1, row4, row2));
    }
  }

  @Test
  public void shouldRejectMissingConfirmationAfterOutputStarts() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    Object[] confirmedRow = new Object[]{1, 1};
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, confirmedRow),
        OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{2, 1}),
        OperatorTestUtil.eosWithEmptyStats());

    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadata1,
        RelDistribution.Type.SINGLETON)) {
      assertEquals(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows(), List.<Object[]>of(confirmedRow));

      MseBlock block = operator.nextBlock();
      assertTrue(block.isError());
      assertTrue(((ErrorMseBlock) block).getErrorMessages().values().stream()
          .anyMatch(message -> message.contains("stopped confirming sorted data after merge output started")));
    }
  }

  @Test
  public void shouldIgnoreEmptySender() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    Object[] row1 = new Object[]{1, 2};
    Object[] row2 = new Object[]{2, 2};
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, row1, row2),
        OperatorTestUtil.eosWithEmptyStats());

    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      assertEquals(drain(operator), List.of(row1, row2));
    }
  }

  /// Consuming one sender's EOS is itself merge progress. The operator must not wait for another mailbox
  /// notification when the remaining sender already has rows buffered behind that ordering frontier.
  @Test(timeOut = 10_000)
  public void shouldEmitBufferedRowsWhenAnotherSenderFinishes() {
    MailboxService mailboxService = mock(MailboxService.class);
    ReceivingMailbox mailbox1 = mock(ReceivingMailbox.class);
    ReceivingMailbox mailbox2 = mock(ReceivingMailbox.class);
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(1234);
    when(mailbox1.getStatMap()).thenReturn(new StatMap<>(ReceivingMailbox.StatKey.class));
    when(mailbox2.getStatMap()).thenReturn(new StatMap<>(ReceivingMailbox.StatKey.class));
    when(mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(mailbox1);
    Object[] row = new Object[]{1, 1};
    when(mailbox1.poll()).thenReturn(OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, row)).thenReturn(null);
    when(mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(mailbox2);
    when(mailbox2.poll()).thenReturn(OperatorTestUtil.eosWithEmptyStats());

    try (SortedMailboxMergeReceiveOperator operator = getOperator(mailboxService, _stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      MseBlock block = operator.nextBlock();
      assertTrue(block.isData());
      assertEquals(((MseBlock.Data) block).asRowHeap().getRows(), List.<Object[]>of(row));
    }
  }

  @Test
  public void shouldPropagateSenderError() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    String errorMessage = "TEST ERROR";
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.errorWithEmptyStats(new RuntimeException(errorMessage)));
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, new Object[]{1, 2}),
        OperatorTestUtil.eosWithEmptyStats());

    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      MseBlock block = operator.nextBlock();
      assertTrue(block.isError());
      assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.UNKNOWN).contains(errorMessage));
    }
  }

  /// A fully consumed final cursor block must be released while another sender continues.
  @Test
  public void shouldReleaseDrainedFinalBlock() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    Object[] firstRow = new Object[]{0, 1};
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, firstRow),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    Object[][] continuingRows = sortedRows(1, 10_000, 2);
    Object[] nextRow = new Object[]{10_001, 2};
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, continuingRows),
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, nextRow));

    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
      assertEquals(rows.size(), 10_000);
      assertEquals(rows.get(0), firstRow);
      assertEquals(operator.getRetainedCursorRowCount(), 10_001L);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReleaseReadAheadAndPreserveStatsAfterEarlyTermination()
      throws IOException {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, sortedRows(0, 5_000, 1)),
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, sortedRows(5_000, 5_000, 1)),
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, sortedRows(10_000, 5_000, 1)),
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, new Object[]{15_000, 1}),
        OperatorTestUtil.eosWithStats(leafStats(1).serialize()));
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        null,
        null,
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, new Object[]{100_000, 2}),
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, new Object[]{100_001, 2}),
        OperatorTestUtil.eosWithStats(leafStats(2).serialize()));

    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadataBoth,
        RelDistribution.Type.HASH_DISTRIBUTED)) {
      assertEquals(((MseBlock.Data) operator.nextBlock()).getNumRows(), 10_000);
      assertEquals(operator.getRetainedCursorRowCount(), 5_001L);

      operator.earlyTerminate();

      assertEquals(operator.getRetainedCursorRowCount(), 0L);
      assertTrue(operator.nextBlock().isSuccess());
      MultiStageQueryStats.StageStats.Closed upstreamStats =
          operator.calculateUpstreamStats().getUpstreamStageStats(1);
      assertNotNull(upstreamStats);
      StatMap<LeafOperator.StatKey> mergedLeafStats =
          (StatMap<LeafOperator.StatKey>) upstreamStats.getLastOperatorStats();
      assertEquals(mergedLeafStats.getLong(LeafOperator.StatKey.EMITTED_ROWS), 3L);
      verify(_mailbox1).earlyTerminate();
      verify(_mailbox2).earlyTerminate();
    }
  }

  @Test
  public void shouldPropagateErrorAfterEarlyTermination() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    String errorMessage = "TEST ERROR AFTER EARLY TERMINATION";
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, new Object[]{1, 1}),
        OperatorTestUtil.sortedBlockWithStats(DATA_SCHEMA, new Object[]{2, 1}),
        OperatorTestUtil.errorWithEmptyStats(new RuntimeException(errorMessage)));

    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadata1,
        RelDistribution.Type.SINGLETON)) {
      assertEquals(((MseBlock.Data) operator.nextBlock()).getNumRows(), 1);
      operator.earlyTerminate();

      MseBlock block = operator.nextBlock();
      assertTrue(block.isError());
      assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.UNKNOWN).contains(errorMessage));
      verify(_mailbox1).earlyTerminate();
    }
  }

  @Test
  public void shouldTimeout() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    try (SortedMailboxMergeReceiveOperator operator = getOperator(_stageMetadata1,
        RelDistribution.Type.SINGLETON, DATA_SCHEMA, FIELD_COLLATIONS, System.currentTimeMillis() + 100L, true,
        true)) {
      MseBlock block = operator.nextBlock();
      assertTrue(block.isError());
      assertTrue(((ErrorMseBlock) block).getErrorMessages().containsKey(QueryErrorCode.EXECUTION_TIMEOUT));
    }
  }

  private static Object[][] sortedRows(int start, int count, int sender) {
    Object[][] rows = new Object[count][];
    for (int i = 0; i < count; i++) {
      rows[i] = new Object[]{start + i, sender};
    }
    return rows;
  }

  private static MultiStageQueryStats leafStats(long emittedRows) {
    MultiStageQueryStats stats = MultiStageQueryStats.emptyStats(1);
    stats.getCurrentStats().addLastOperator(MultiStageOperator.Type.LEAF,
        new StatMap<>(LeafOperator.StatKey.class).merge(LeafOperator.StatKey.EMITTED_ROWS, emittedRows));
    return stats;
  }

  private static List<Object[]> drain(SortedMailboxMergeReceiveOperator operator) {
    List<Object[]> rows = new ArrayList<>();
    MseBlock block = operator.nextBlock();
    while (block.isData()) {
      rows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }
    assertTrue(block.isSuccess());
    return rows;
  }

  private SortedMailboxMergeReceiveOperator getOperator(StageMetadata stageMetadata,
      RelDistribution.Type distributionType) {
    return getOperator(_mailboxService, stageMetadata, distributionType);
  }

  private SortedMailboxMergeReceiveOperator getOperator(MailboxService mailboxService, StageMetadata stageMetadata,
      RelDistribution.Type distributionType) {
    return getOperator(mailboxService, stageMetadata, distributionType, DATA_SCHEMA, FIELD_COLLATIONS, Long.MAX_VALUE,
        true, true);
  }

  private SortedMailboxMergeReceiveOperator getOperator(StageMetadata stageMetadata,
      RelDistribution.Type distributionType, DataSchema dataSchema, List<RelFieldCollation> collations,
      long deadlineMs, boolean sort, boolean sortedOnSender) {
    return getOperator(_mailboxService, stageMetadata, distributionType, dataSchema, collations, deadlineMs, sort,
        sortedOnSender);
  }

  private SortedMailboxMergeReceiveOperator getOperator(MailboxService mailboxService, StageMetadata stageMetadata,
      RelDistribution.Type distributionType, DataSchema dataSchema, List<RelFieldCollation> collations,
      long deadlineMs, boolean sort, boolean sortedOnSender) {
    OpChainExecutionContext context = OperatorTestUtil.getOpChainContext(mailboxService, deadlineMs, stageMetadata);
    MailboxReceiveNode node = mock(MailboxReceiveNode.class);
    when(node.getDistributionType()).thenReturn(distributionType);
    when(node.getSenderStageId()).thenReturn(1);
    when(node.getDataSchema()).thenReturn(dataSchema);
    when(node.getCollations()).thenReturn(collations);
    when(node.isSort()).thenReturn(sort);
    when(node.isSortedOnSender()).thenReturn(sortedOnSender);
    return new SortedMailboxMergeReceiveOperator(context, node);
  }
}
