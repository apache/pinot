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
package org.apache.pinot.query.runtime.operator.exchange;

import java.util.List;
import java.util.PriorityQueue;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEvent;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class BroadcastExchangeTest {
  private static final DataSchema OBJECT_SCHEMA = new DataSchema(new String[]{"key", "funnel"},
      new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.OBJECT});

  private AutoCloseable _mocks;

  @Mock
  private SendingMailbox _mailbox1;
  @Mock
  private SendingMailbox _mailbox2;

  @SuppressWarnings("rawtypes")
  private AggregationFunction _aggFunction;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    Mockito.when(_mailbox1.isLocal()).thenReturn(true);
    Mockito.when(_mailbox2.isLocal()).thenReturn(true);
    _aggFunction = mockFunnelAggFunction();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldBroadcast() {
    RowHeapDataBlock block = new RowHeapDataBlock(List.<Object[]>of(new Object[]{"something", 1, 2}),
        DataSchema.EXPLAIN_RESULT_SCHEMA);

    route(block, _mailbox1, _mailbox2);

    // Blocks without OBJECT columns are shared by reference with all the destinations
    assertSame(capturedBlock(_mailbox1), block);
    assertSame(capturedBlock(_mailbox2), block);
  }

  // getAggFunctions() is deprecated, but the copy must preserve it for downstream serialization, so assert on it
  @SuppressWarnings("deprecation")
  @Test
  public void shouldCopyBlocksWithObjectColumnsForAllLocalDestinationsButOne() {
    // Given:
    PriorityQueue<FunnelStepEvent> stepEvents = stepEvents(new FunnelStepEvent(1000L, 0),
        new FunnelStepEvent(2000L, 1));
    RowHeapDataBlock block = new RowHeapDataBlock(
        List.of(new Object[]{"user0", stepEvents}, new Object[]{"user1", null}), OBJECT_SCHEMA,
        new AggregationFunction[]{_aggFunction});

    // When:
    route(block, _mailbox1, _mailbox2);

    // Then: the first destination receives the original block, the second one receives a copy that shares no OBJECT
    // cells with the original
    assertSame(capturedBlock(_mailbox1), block);
    MseBlock.Data copiedBlock = capturedBlock(_mailbox2);
    assertNotSame(copiedBlock, block);
    assertTrue(copiedBlock.isRowHeap());
    RowHeapDataBlock copiedRowHeapBlock = (RowHeapDataBlock) copiedBlock;
    assertSame(copiedRowHeapBlock.getDataSchema(), OBJECT_SCHEMA);
    assertEquals(copiedRowHeapBlock.getAggFunctions(), new AggregationFunction[]{_aggFunction});
    Object[] copiedRow = copiedRowHeapBlock.getRows().get(0);
    assertEquals(copiedRow[0], "user0");
    assertNotSame(copiedRow[1], stepEvents);
    @SuppressWarnings("unchecked")
    PriorityQueue<FunnelStepEvent> copiedStepEvents = (PriorityQueue<FunnelStepEvent>) copiedRow[1];
    assertEquals(copiedStepEvents.size(), 2);
    // Draining the copy (like extracting the final result does) must not affect the original
    assertEquals(copiedStepEvents.poll(), new FunnelStepEvent(1000L, 0));
    assertEquals(copiedStepEvents.poll(), new FunnelStepEvent(2000L, 1));
    assertEquals(stepEvents.size(), 2);
    // Null intermediate results stay null in the copy
    assertEquals(copiedRowHeapBlock.getRows().get(1), new Object[]{"user1", null});
  }

  @Test
  public void shouldSendOriginalBlockToRemoteDestinationsBeforeTheLocalOne() {
    // Given: a remote destination between two local ones
    SendingMailbox remoteMailbox = Mockito.mock(SendingMailbox.class);
    RowHeapDataBlock block = funnelBlock(stepEvents(new FunnelStepEvent(1000L, 0)));

    // When:
    route(block, remoteMailbox, _mailbox1, _mailbox2);

    // Then: the remote destination serializes the original, the first local destination receives the original by
    // reference after all other reads of it, and the extra local destination receives a copy
    assertSame(capturedBlock(remoteMailbox), block);
    assertSame(capturedBlock(_mailbox1), block);
    assertNotSame(capturedBlock(_mailbox2), block);
    InOrder inOrder = Mockito.inOrder(remoteMailbox, _mailbox2, _mailbox1);
    inOrder.verify(remoteMailbox).send(Mockito.any(MseBlock.Data.class));
    inOrder.verify(_mailbox2).send(Mockito.any(MseBlock.Data.class));
    inOrder.verify(_mailbox1).send(Mockito.any(MseBlock.Data.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldShareBlocksWithObjectColumnsWithRemoteOnlyDestinations() {
    // Given: only remote destinations, which serialize the block instead of delivering it by reference
    SendingMailbox remoteMailbox1 = Mockito.mock(SendingMailbox.class);
    SendingMailbox remoteMailbox2 = Mockito.mock(SendingMailbox.class);
    RowHeapDataBlock block = funnelBlock(stepEvents(new FunnelStepEvent(1000L, 0)));

    // When:
    route(block, remoteMailbox1, remoteMailbox2);

    // Then: no copies are made and both destinations receive the original block
    assertSame(capturedBlock(remoteMailbox1), block);
    assertSame(capturedBlock(remoteMailbox2), block);
    Mockito.verify(_aggFunction, Mockito.never()).serializeIntermediateResult(Mockito.any());
  }

  @Test
  public void shouldSendOriginalBlockToFirstActiveDestination() {
    RowHeapDataBlock block = funnelBlock(stepEvents(new FunnelStepEvent(1000L, 0)));
    Mockito.when(_mailbox1.isEarlyTerminated()).thenReturn(true);

    route(block, _mailbox1, _mailbox2);

    // The early-terminated destination is skipped and the first active one receives the original without a copy
    Mockito.verify(_mailbox1, Mockito.never()).send(Mockito.any(MseBlock.Data.class));
    assertSame(capturedBlock(_mailbox2), block);
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void shouldCopyEachObjectColumnWithItsOwnAggFunction() {
    // Given: a block with one key column and two OBJECT columns, each backed by its own aggregation function
    DataSchema schema = new DataSchema(new String[]{"key", "funnel1", "funnel2"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.OBJECT, ColumnDataType.OBJECT});
    AggregationFunction otherAggFunction = mockFunnelAggFunction();
    PriorityQueue<FunnelStepEvent> stepEvents1 = stepEvents(new FunnelStepEvent(1000L, 0));
    PriorityQueue<FunnelStepEvent> stepEvents2 = stepEvents(new FunnelStepEvent(2000L, 1));
    RowHeapDataBlock block = new RowHeapDataBlock(List.<Object[]>of(new Object[]{"user0", stepEvents1, stepEvents2}),
        schema, new AggregationFunction[]{_aggFunction, otherAggFunction});

    // When:
    route(block, _mailbox1, _mailbox2);

    // Then: each OBJECT column is copied through the aggregation function at the matching position
    Mockito.verify(_aggFunction, Mockito.times(1)).serializeIntermediateResult(Mockito.same(stepEvents1));
    Mockito.verify(otherAggFunction, Mockito.times(1)).serializeIntermediateResult(Mockito.same(stepEvents2));
    Object[] copiedRow = ((RowHeapDataBlock) capturedBlock(_mailbox2)).getRows().get(0);
    assertNotSame(copiedRow[1], stepEvents1);
    assertNotSame(copiedRow[2], stepEvents2);
    assertEquals(((PriorityQueue<FunnelStepEvent>) copiedRow[1]).peek(), new FunnelStepEvent(1000L, 0));
    assertEquals(((PriorityQueue<FunnelStepEvent>) copiedRow[2]).peek(), new FunnelStepEvent(2000L, 1));
  }

  @Test
  public void shouldShareSerializedBlocks() {
    // Serialized blocks are read-only and every receiver deserializes its own copy of the data
    MseBlock.Data serializedBlock = Mockito.mock(MseBlock.Data.class);
    Mockito.when(serializedBlock.isRowHeap()).thenReturn(false);

    route(serializedBlock, _mailbox1, _mailbox2);

    assertSame(capturedBlock(_mailbox1), serializedBlock);
    assertSame(capturedBlock(_mailbox2), serializedBlock);
  }

  @Test
  public void shouldFailToCopyBlocksWithObjectColumnsWithoutAggFunctions() {
    RowHeapDataBlock block = new RowHeapDataBlock(
        List.<Object[]>of(new Object[]{"user0", stepEvents(new FunnelStepEvent(1000L, 0))}), OBJECT_SCHEMA);
    assertThrows(IllegalStateException.class, block::copyObjectColumns);
  }

  @Test
  public void shouldShareBlocksWithObjectColumnsWithSingleDestination() {
    RowHeapDataBlock block = funnelBlock(stepEvents(new FunnelStepEvent(1000L, 0)));

    route(block, _mailbox1);

    // With a single destination there is no sharing across destinations, so no copy is needed
    assertSame(capturedBlock(_mailbox1), block);
  }

  /// Mocks an aggregation function whose intermediate result serde is the funnel step event accumulator serde.
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static AggregationFunction mockFunnelAggFunction() {
    AggregationFunction aggFunction = Mockito.mock(AggregationFunction.class);
    Mockito.when(aggFunction.serializeIntermediateResult(Mockito.any())).thenAnswer(
        invocation -> new AggregationFunction.SerializedIntermediateResult(
            ObjectSerDeUtils.ObjectType.FunnelStepEventAccumulator.getValue(),
            ObjectSerDeUtils.FUNNEL_STEP_EVENT_ACCUMULATOR_SER_DE.serialize(invocation.getArgument(0))));
    Mockito.when(aggFunction.deserializeIntermediateResult(Mockito.any())).thenAnswer(
        invocation -> ObjectSerDeUtils.FUNNEL_STEP_EVENT_ACCUMULATOR_SER_DE.deserialize(
            ((CustomObject) invocation.getArgument(0)).getBuffer()));
    return aggFunction;
  }

  private static PriorityQueue<FunnelStepEvent> stepEvents(FunnelStepEvent... events) {
    return new PriorityQueue<>(List.of(events));
  }

  @SuppressWarnings("rawtypes")
  private RowHeapDataBlock funnelBlock(PriorityQueue<FunnelStepEvent> stepEvents) {
    return new RowHeapDataBlock(List.<Object[]>of(new Object[]{"user0", stepEvents}), OBJECT_SCHEMA,
        new AggregationFunction[]{_aggFunction});
  }

  private static void route(MseBlock.Data block, SendingMailbox... destinations) {
    List<SendingMailbox> destinationList = List.of(destinations);
    new BroadcastExchange(destinationList, BlockSplitter.NO_OP).route(destinationList, block);
  }

  private static MseBlock.Data capturedBlock(SendingMailbox mailbox) {
    ArgumentCaptor<MseBlock.Data> captor = ArgumentCaptor.forClass(MseBlock.Data.class);
    Mockito.verify(mailbox, Mockito.times(1)).send(captor.capture());
    return captor.getValue();
  }
}
