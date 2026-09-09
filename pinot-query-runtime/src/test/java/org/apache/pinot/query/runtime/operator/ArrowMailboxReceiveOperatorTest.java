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

import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.SharedMailboxInfos;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlockConverter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.memory.ArrowBuffers;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/**
 * Receive-side Arrow origination is capability-aware and never transfers native ownership to a legacy consumer.
 */
public class ArrowMailboxReceiveOperatorTest {
  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"key", "payload"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
  private ArrowBuffers _buffers;
  private MailboxService _mailboxService;
  private ReceivingMailbox _mailbox;
  private OpChainExecutionContext _context;
  private MailboxReceiveOperator _operator;

  @BeforeMethod
  public void setUp() {
    _buffers = new ArrowBuffers(true, new RootAllocator(32L * 1024 * 1024), 0, 32L * 1024 * 1024);
    _mailboxService = mock(MailboxService.class);
    when(_mailboxService.isArrowEnabled()).thenReturn(true);
    when(_mailboxService.getArrowBuffers()).thenReturn(_buffers);
    when(_mailboxService.getHostname()).thenReturn("localhost");
    when(_mailboxService.getPort()).thenReturn(1234);
    _mailbox = mock(ReceivingMailbox.class);
    when(_mailbox.getStatMap()).thenReturn(new StatMap<>(ReceivingMailbox.StatKey.class));
    when(_mailboxService.getReceivingMailbox(MailboxIdUtils.toMailboxId(0, 1, 0, 0, 0))).thenReturn(_mailbox);
    WorkerMetadata worker = new WorkerMetadata(0,
        Map.of(1, new SharedMailboxInfos(new MailboxInfo("localhost", 1234, List.of(0)))), Map.of());
    _context = spy(OperatorTestUtil.getOpChainContext(
        _mailboxService, Long.MAX_VALUE, new StageMetadata(0, List.of(worker), Map.of())));
    MailboxReceiveNode node = mock(MailboxReceiveNode.class);
    when(node.getDistributionType()).thenReturn(RelDistribution.Type.SINGLETON);
    when(node.getSenderStageId()).thenReturn(1);
    when(node.getDataSchema()).thenReturn(SCHEMA);
    _operator = new MailboxReceiveOperator(_context, node);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    try {
      _operator.close();
      assertEquals(_buffers.getAllocatedMemory(), 0L, "Receive must not strand buffers until the query-end sweep");
    } finally {
      _context.closeArrowResources();
      _buffers.close();
    }
  }

  @Test
  public void testDefaultReceiveStaysLegacy() {
    RowHeapDataBlock input = OperatorTestUtil.block(SCHEMA, new Object[]{1, "row"});
    when(_mailbox.poll()).thenReturn(new ReceivingMailbox.MseBlockWithStats(input, List.of()),
        OperatorTestUtil.eosWithEmptyStats());
    assertSame(_operator.nextBlock(), input);
    assertTrue(_operator.nextBlock().isSuccess());
    verify(_mailboxService, never()).getArrowBuffers();
  }

  @Test
  public void testPlanningDoesNotAcquireArrowContext() {
    _operator.enableArrowOutput();
    verify(_context, never()).getOrCreateArrowContext();
    verify(_mailboxService, never()).getArrowBuffers();
  }

  @Test
  public void testNativeConsumerEnablesReceiveOrigination() {
    RowHeapDataBlock input = OperatorTestUtil.block(SCHEMA, new Object[]{1, "row"}, new Object[]{null, null});
    when(_mailbox.poll()).thenReturn(new ReceivingMailbox.MseBlockWithStats(input, List.of()),
        OperatorTestUtil.eosWithEmptyStats());
    _operator.enableArrowOutput();
    ArrowBlock result = (ArrowBlock) _operator.nextBlock();
    try {
      assertEquals(_context.getOrCreateArrowContext().getLiveBlockCount(), 1);
      assertEquals(result.asRowHeap().getRows().get(0), new Object[]{1, "row"});
      assertEquals(result.asRowHeap().getRows().get(1), new Object[]{null, null});
      assertTrue(_operator.nextBlock().isSuccess());
    } finally {
      result.release();
    }
  }

  @Test
  public void testUnsupportedReceiveSchemaRemainsLegacy() {
    DataSchema schema = new DataSchema(new String[]{"key", "array"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT_ARRAY});
    RowHeapDataBlock input = OperatorTestUtil.block(schema, new Object[]{1, new int[]{1, 2}});
    when(_mailbox.poll()).thenReturn(new ReceivingMailbox.MseBlockWithStats(input, List.of()));
    _operator.enableArrowOutput();
    assertSame(_operator.nextBlock(), input);
  }

  @Test
  public void testReceivedArrowIsMaterializedAndReleasedAtLegacyEdge() {
    ArrowBlock input = arrow(1);
    when(_mailbox.poll()).thenReturn(new ReceivingMailbox.MseBlockWithStats(input, List.of()));
    MseBlock.Data result = (MseBlock.Data) _operator.nextBlock();
    assertTrue(result.isRowHeap());
    assertEquals(_context.getOrCreateArrowContext().getLiveBlockCount(), 0);
    assertEquals(result.asRowHeap().getRows().get(0), new Object[]{1, "row"});
  }

  @Test
  public void testReceivedArrowTransfersOneReferenceToNativeConsumer() {
    ArrowBlock input = arrow(1);
    when(_mailbox.poll()).thenReturn(new ReceivingMailbox.MseBlockWithStats(input, List.of()));
    _operator.enableArrowOutput();
    ArrowBlock result = (ArrowBlock) _operator.nextBlock();
    try {
      assertSame(result, input);
    } finally {
      result.release();
    }
    assertEquals(_context.getOrCreateArrowContext().getLiveBlockCount(), 0);
  }

  @Test
  public void testEarlyTerminationDropsAndReleasesBufferedArrow() {
    ArrowBlock first = arrow(1);
    ArrowBlock second = arrow(2);
    when(_mailbox.poll()).thenReturn(new ReceivingMailbox.MseBlockWithStats(first, List.of()),
        new ReceivingMailbox.MseBlockWithStats(second, List.of()), OperatorTestUtil.eosWithEmptyStats());
    _operator.enableArrowOutput();
    _operator.earlyTerminate();
    assertTrue(_operator.nextBlock().isSuccess());
    assertEquals(_context.getOrCreateArrowContext().getLiveBlockCount(), 0);
  }

  @Test
  public void testDeadlineAfterReceiveReleasesArrow() {
    ArrowBlock input = arrow(1);
    when(_mailbox.poll()).thenAnswer(invocation -> {
      doReturn(1L).when(_context).getPassiveDeadlineMs();
      return new ReceivingMailbox.MseBlockWithStats(input, List.of());
    });
    _operator.enableArrowOutput();
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      assertTrue(_operator.nextBlock().isError());
    }
    assertEquals(_context.getOrCreateArrowContext().getLiveBlockCount(), 0);
  }

  private ArrowBlock arrow(int key) {
    return ArrowBlockConverter.toArrowBlock(OperatorTestUtil.block(SCHEMA, new Object[]{key, "row"}),
        _context.getOrCreateArrowContext());
  }
}
