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

import java.util.ArrayList;
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
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.SharedMailboxInfos;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.LONG;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class SortOperatorTest {
  private AutoCloseable _mocks;
  @Mock
  private MultiStageOperator _input;
  @Mock
  private VirtualServerAddress _serverAddress;
  @Mock
  private MailboxService _mailboxService;
  @Mock
  private ReceivingMailbox _mailbox1;
  @Mock
  private ReceivingMailbox _mailbox2;

  @BeforeMethod
  public void setUp() {
    _mocks = openMocks(this);
    when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldHandleUpstreamErrorBlock() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(ErrorMseBlock.fromException(new Exception("foo!")));
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    MseBlock block = operator.nextBlock();

    // Then:
    assertTrue(block.isError(), "expected error block to propagate");
  }

  @Test
  public void shouldCreateEmptyBlockOnUpstreamEOS() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    MseBlock block = operator.nextBlock();

    // Then:
    assertTrue(block.isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortInputOneBlockWithTwoRows() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSkipSortInputOneBlockWithTwoRowsInputSorted() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    _input = mock(SortedMailboxReceiveOperator.class);
    // Purposefully setting input as unsorted order for validation but 'isInputSorted' should only be true if actually
    // sorted
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2});
    assertEquals(resultRows.get(1), new Object[]{1});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortOnNonZeroIdxCollation() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"ignored", "sort"}, new DataSchema.ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1, 2}, new Object[]{2, 1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(1, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2, 1});
    assertEquals(resultRows.get(1), new Object[]{1, 2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortInputOneBlockWithTwoRowsNonNumeric() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{STRING});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{"b"}, new Object[]{"a"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{"a"});
    assertEquals(resultRows.get(1), new Object[]{"b"});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortDescending() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.DESCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2});
    assertEquals(resultRows.get(1), new Object[]{1});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldOffsetSortInputOneBlockWithThreeRows() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations, 10, 1);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2});
    assertEquals(resultRows.get(1), new Object[]{3});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldOffsetSortInputOneBlockWithThreeRowsInputSorted() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    _input = mock(SortedMailboxReceiveOperator.class);
    // Set input rows as sorted since input is expected to be sorted
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1}, new Object[]{2}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations, 10, 1);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2});
    assertEquals(resultRows.get(1), new Object[]{3});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldOffsetLimitSortInputOneBlockWithThreeRows() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations, 1, 1);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldOffsetLimitSortInputOneBlockWithThreeRowsInputSorted() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    _input = mock(SortedMailboxReceiveOperator.class);
    // Set input rows as sorted since input is expected to be sorted
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1}, new Object[]{2}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations, 1, 1);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldRespectDefaultLimit() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = new SortOperator(OperatorTestUtil.getTracingContext(), _input,
        new SortNode(-1, schema, PlanNode.NodeHint.EMPTY, List.of(), collations, -1, 0), 10, 1);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 1, "expected 1 element even though fetch is 2 because of max limit");
    assertEquals(resultRows.get(0), new Object[]{1});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldFetchAllWithNegativeFetch() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations, -1, 0);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertEquals(resultRows.get(2), new Object[]{3});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortTwoInputBlocksWithOneRowEach() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2})).thenReturn(block(schema, new Object[]{1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortTwoInputBlocksWithOneRowEachInputSorted() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    _input = mock(SortedMailboxReceiveOperator.class);
    // Set input rows as sorted since input is expected to be sorted
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1})).thenReturn(block(schema, new Object[]{2}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldBreakTiesUsingSecondCollationKey() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"first", "second"}, new DataSchema.ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1, 2}, new Object[]{1, 1}, new Object[]{1, 3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST),
        new RelFieldCollation(1, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, 1});
    assertEquals(resultRows.get(1), new Object[]{1, 2});
    assertEquals(resultRows.get(2), new Object[]{1, 3});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldBreakTiesUsingSecondCollationKeyWithDifferentDirection() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"first", "second"}, new DataSchema.ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1, 2}, new Object[]{1, 1}, new Object[]{1, 3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST),
        new RelFieldCollation(1, Direction.DESCENDING, NullDirection.FIRST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, 3});
    assertEquals(resultRows.get(1), new Object[]{1, 2});
    assertEquals(resultRows.get(2), new Object[]{1, 1});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHaveNullAtLast() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{null}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertEquals(resultRows.get(2), new Object[]{null});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHaveNullAtFirst() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{null}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.FIRST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{null});
    assertEquals(resultRows.get(1), new Object[]{1});
    assertEquals(resultRows.get(2), new Object[]{2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHaveNullAtLastWhenUnspecified() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{null}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.UNSPECIFIED));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertEquals(resultRows.get(2), new Object[]{null});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHandleMultipleCollationKeysWithNulls() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"first", "second"}, new DataSchema.ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1, 1}, new Object[]{1, null}, new Object[]{null, 1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.FIRST),
        new RelFieldCollation(1, Direction.DESCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{null, 1});
    assertEquals(resultRows.get(1), new Object[]{1, 1});
    assertEquals(resultRows.get(2), new Object[]{1, null});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldPreservePrecision() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{LONG});
    // Insert 3 consecutive large numbers that are represented by the same double value if converted to double due to
    // precision loss.
    long largeValue = 1L << 60;
    //noinspection ConstantValue
    assert (double) largeValue == (double) (largeValue + 1) && (double) largeValue == (double) (largeValue + 2);
    when(_input.nextBlock())
        .thenReturn(block(schema, new Object[]{largeValue + 2}, new Object[]{largeValue}, new Object[]{largeValue + 1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{largeValue});
    assertEquals(resultRows.get(1), new Object[]{largeValue + 1});
    assertEquals(resultRows.get(2), new Object[]{largeValue + 2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  /**
   * End-to-end fast-path test: feed a real {@link SortedMailboxReceiveOperator} running in k-way MERGE mode (block
   * size = 2, so it emits MULTIPLE bounded data blocks) into a {@link SortOperator} with a LIMIT. Because the input is
   * a {@code SortedMailboxReceiveOperator}, the SortOperator skips re-sorting and just slices to the limit. The final
   * output must be globally sorted and honor the limit across the bounded blocks.
   */
  @Test
  public void shouldSliceLimitOverMergedBoundedBlocksWithoutResorting() {
    DataSchema schema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    // Two pre-sorted sender streams; the k-way merge produces a globally sorted stream 1..6.
    String mailboxId1 = MailboxIdUtils.toMailboxId(0, 1, 0, 0, 0);
    String mailboxId2 = MailboxIdUtils.toMailboxId(0, 1, 1, 0, 0);
    when(_mailboxService.getHostname()).thenReturn("localhost");
    when(_mailboxService.getPort()).thenReturn(1234);
    when(_mailbox1.getStatMap()).thenReturn(new StatMap<>(ReceivingMailbox.StatKey.class));
    when(_mailbox2.getStatMap()).thenReturn(new StatMap<>(ReceivingMailbox.StatKey.class));
    when(_mailboxService.getReceivingMailbox(eq(mailboxId1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(schema, new Object[]{1, 1}, new Object[]{3, 3}, new Object[]{5, 5}),
        OperatorTestUtil.eosWithEmptyStats());
    when(_mailboxService.getReceivingMailbox(eq(mailboxId2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(
        OperatorTestUtil.blockWithStats(schema, new Object[]{2, 2}, new Object[]{4, 4}, new Object[]{6, 6}),
        OperatorTestUtil.eosWithEmptyStats());

    MailboxInfos mailboxInfos = new SharedMailboxInfos(new MailboxInfo("localhost", 1234, List.of(0, 1)));
    StageMetadata stageMetadata = new StageMetadata(0,
        Stream.of(0, 1).map(workerId -> new WorkerMetadata(workerId, Map.of(1, mailboxInfos), Map.of()))
            .collect(Collectors.toList()), Map.of());
    Map<String, String> opChainMetadata = Map.of(
        CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE, "true",
        CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE_BLOCK_SIZE, "2");
    OpChainExecutionContext receiveContext =
        OperatorTestUtil.getOpChainContext(_mailboxService, Long.MAX_VALUE, stageMetadata, opChainMetadata);
    MailboxReceiveNode receiveNode = mock(MailboxReceiveNode.class);
    when(receiveNode.getDistributionType()).thenReturn(RelDistribution.Type.HASH_DISTRIBUTED);
    when(receiveNode.getSenderStageId()).thenReturn(1);
    when(receiveNode.getDataSchema()).thenReturn(schema);
    when(receiveNode.getCollations()).thenReturn(collations);
    when(receiveNode.isSortedOnSender()).thenReturn(true);

    try (SortedMailboxReceiveOperator receiveOperator = new SortedMailboxReceiveOperator(receiveContext, receiveNode)) {
      // fetch = 4, offset = 1 => keep merged rows at indices 1..4 (values 2, 3, 4, 5).
      SortOperator operator = new SortOperator(OperatorTestUtil.getTracingContext(), receiveOperator,
          new SortNode(-1, schema, PlanNode.NodeHint.EMPTY, List.of(), collations, 4, 1));

      List<Object[]> resultRows = new ArrayList<>();
      MseBlock block = operator.nextBlock();
      while (block.isData()) {
        resultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
        block = operator.nextBlock();
      }
      assertTrue(block.isSuccess(), "expected EOS block to propagate");
      assertEquals(resultRows.size(), 4, "limit (fetch=4) must be honored across bounded blocks");
      assertEquals(resultRows.get(0), new Object[]{2, 2});
      assertEquals(resultRows.get(1), new Object[]{3, 3});
      assertEquals(resultRows.get(2), new Object[]{4, 4});
      assertEquals(resultRows.get(3), new Object[]{5, 5});
      // Prove the fast-path was taken: the SortOperator must NOT have built a priority queue (no re-sort) because the
      // input is a SortedMailboxReceiveOperator. REQUIRE_SORT reflects (_priorityQueue != null).
      assertFalse(operator.copyStatMaps().getBoolean(SortOperator.StatKey.REQUIRE_SORT),
          "SortOperator must skip re-sorting when input is a SortedMailboxReceiveOperator");
    }
  }

  private SortOperator getOperator(DataSchema schema, List<RelFieldCollation> collations, int fetch, int offset) {
    return new SortOperator(OperatorTestUtil.getTracingContext(), _input,
        new SortNode(-1, schema, PlanNode.NodeHint.EMPTY, List.of(), collations, fetch, offset));
  }

  private SortOperator getOperator(DataSchema schema, List<RelFieldCollation> collations) {
    return getOperator(schema, collations, 10, 0);
  }

  private static RowHeapDataBlock block(DataSchema schema, Object[]... rows) {
    return new RowHeapDataBlock(List.of(rows), schema);
  }
}
