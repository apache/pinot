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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.RootAllocator;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlockConverter;
import org.apache.pinot.query.runtime.blocks.HeldBlocks;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.memory.ArrowBuffers;
import org.apache.pinot.query.runtime.memory.ArrowQueryContext;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/** Tests lazy attempt ownership and the single post-operator-close cleanup boundary. */
public class OpChainArrowLifecycleTest {
  private RootAllocator _root;
  private ArrowBuffers _buffers;
  private MailboxService _mailbox;
  private OpChainExecutionContext _context;

  @BeforeMethod
  public void setUp() {
    _root = new RootAllocator(16L * 1024 * 1024);
    _buffers = new ArrowBuffers(true, _root, 0, 16L * 1024 * 1024);
    _mailbox = mock(MailboxService.class);
    when(_mailbox.getHostname()).thenReturn("localhost");
    when(_mailbox.isArrowEnabled()).thenReturn(true);
    when(_mailbox.getArrowBuffers()).thenReturn(_buffers);
    _context = context();
  }

  @AfterMethod
  public void tearDown() {
    try {
      _context.closeArrowResources();
    } finally {
      _buffers.close();
    }
  }

  @Test
  public void testEnabledContextAllocatesLazilyOnce() {
    assertTrue(_context.isArrowEnabled());
    assertTrue(_root.getChildAllocators().isEmpty());
    ArrowQueryContext arrow = _context.getOrCreateArrowContext();
    assertSame(_context.getOrCreateArrowContext(), arrow);
    assertEquals(_root.getChildAllocators().size(), 1);
    _context.closeArrowResources();
    _context.closeArrowResources();
    assertTrue(_root.getChildAllocators().isEmpty());
    expectThrows(IllegalStateException.class, _context::getOrCreateArrowContext);
  }

  @Test
  public void testDisabledContextNeverAllocates() {
    when(_mailbox.isArrowEnabled()).thenReturn(false);
    OpChainExecutionContext disabled = context();
    assertFalse(disabled.isArrowEnabled());
    expectThrows(IllegalStateException.class, disabled::getOrCreateArrowContext);
    disabled.closeArrowResources();
    disabled.closeArrowResources();
    assertTrue(_root.getChildAllocators().isEmpty());
    verify(_mailbox, never()).getArrowBuffers();
  }

  @Test
  public void testClosingUnusedContextPreventsLateInitialization() {
    _context.closeArrowResources();
    expectThrows(IllegalStateException.class, _context::getOrCreateArrowContext);
    assertTrue(_root.getChildAllocators().isEmpty());
    verify(_mailbox, never()).getArrowBuffers();
  }

  @Test
  public void testOperatorStateClosesBeforeAllocatorAndCallback() {
    ArrowBlock block = block();
    MultiStageOperator operator = mock(MultiStageOperator.class);
    doAnswer(invocation -> {
      assertTrue(_buffers.getAllocatedMemory() > 0);
      block.release();
      return null;
    }).when(operator).close();
    AtomicInteger callbacks = new AtomicInteger();
    OpChain chain = new OpChain(_context, operator, id -> {
      assertEquals(_buffers.getAllocatedMemory(), 0L);
      assertTrue(_root.getChildAllocators().isEmpty());
      callbacks.incrementAndGet();
    });
    chain.close();
    chain.close();
    chain.cancel(new IllegalStateException("late cancel"));
    verify(operator, times(1)).close();
    verify(operator, never()).cancel(any());
    assertEquals(callbacks.get(), 1);
  }

  @Test
  public void testCancelDoesNotSweepUntilClose() {
    ArrowBlock block = block();
    block.retain();
    MultiStageOperator operator = mock(MultiStageOperator.class);
    AtomicInteger callbacks = new AtomicInteger();
    OpChain chain = new OpChain(_context, operator, id -> callbacks.incrementAndGet());
    IllegalStateException failure = new IllegalStateException("cancel");
    chain.cancel(failure);
    chain.cancel(failure);
    assertEquals(callbacks.get(), 0);
    assertEquals(block.getDataBlock().getInt(0, 0), 7);
    assertTrue(_buffers.getAllocatedMemory() > 0);

    chain.close();
    chain.close();
    verify(operator, times(1)).cancel(failure);
    verify(operator, times(1)).close();
    assertEquals(callbacks.get(), 1);
    assertEquals(_buffers.getAllocatedMemory(), 0L);
    expectThrows(IllegalStateException.class, block::release);
  }

  @Test
  public void testOperatorCloseFailureStillSweepsAndSignalsOnce() {
    block();
    MultiStageOperator operator = mock(MultiStageOperator.class);
    IllegalStateException failure = new IllegalStateException("close");
    doThrow(failure).when(operator).close();
    AtomicInteger callbacks = new AtomicInteger();
    OpChain chain = new OpChain(_context, operator, id -> callbacks.incrementAndGet());
    assertSame(expectThrows(IllegalStateException.class, chain::close), failure);
    chain.close();
    verify(operator, times(1)).close();
    assertEquals(callbacks.get(), 1);
    assertEquals(_buffers.getAllocatedMemory(), 0L);
    assertTrue(_root.getChildAllocators().isEmpty());
  }

  @Test
  public void testEarlyTerminationKeepsStateUntilClose() {
    EarlyTerminatingOperator operator = new EarlyTerminatingOperator(_context);
    AtomicInteger callbacks = new AtomicInteger();
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest();
        OpChain chain = new OpChain(_context, operator, id -> callbacks.incrementAndGet())) {
      assertTrue(operator.nextBlock().isData());
      operator.earlyTerminate();
      assertTrue(operator.nextBlock().isSuccess());
      assertTrue(_buffers.getAllocatedMemory() > 0, "State belongs to close(), not earlyTerminate()");
      assertEquals(callbacks.get(), 0);
    }
    assertEquals(callbacks.get(), 1);
    assertEquals(_buffers.getAllocatedMemory(), 0L);
    assertTrue(_root.getChildAllocators().isEmpty());
  }

  private OpChainExecutionContext context() {
    WorkerMetadata worker = new WorkerMetadata(0, Map.of(), Map.of());
    return OpChainExecutionContext.fromQueryContext(_mailbox, Map.of(),
        new StageMetadata(0, List.of(worker), Map.of()), worker, null, false, false,
        QueryExecutionContext.forMseTest());
  }

  private ArrowBlock block() {
    DataSchema schema = new DataSchema(new String[]{"i"}, new ColumnDataType[]{ColumnDataType.INT});
    return ArrowBlockConverter.toArrowBlock(new RowHeapDataBlock(List.<Object[]>of(new Object[]{7}), schema),
        _context.getOrCreateArrowContext());
  }

  /** Single-threaded stateful test operator exercising the real early-termination path. */
  private static final class EarlyTerminatingOperator extends MultiStageOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(EarlyTerminatingOperator.class);
    private final HeldBlocks _held = new HeldBlocks();
    private final StatMap<LiteralValueOperator.StatKey> _stats = new StatMap<>(LiteralValueOperator.StatKey.class);

    private EarlyTerminatingOperator(OpChainExecutionContext context) {
      super(context);
    }

    @Override
    protected MseBlock getNextBlock() {
      if (_isEarlyTerminated) {
        return SuccessMseBlock.INSTANCE;
      }
      DataSchema schema = new DataSchema(new String[]{"i"}, new ColumnDataType[]{ColumnDataType.INT});
      RowHeapDataBlock rows = new RowHeapDataBlock(List.<Object[]>of(new Object[]{7}), schema);
      _held.holdTransferred(ArrowBlockConverter.toArrowBlock(rows, _context.getOrCreateArrowContext()));
      return rows;
    }

    @Override
    public void close() {
      _held.releaseAll();
      super.close();
    }

    @Override
    protected Logger logger() {
      return LOGGER;
    }

    @Override
    public Type getOperatorType() {
      return Type.LITERAL;
    }

    @Override
    public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
      _stats.merge(LiteralValueOperator.StatKey.EXECUTION_TIME_MS, time);
      _stats.merge(LiteralValueOperator.StatKey.EMITTED_ROWS, numRows);
    }

    @Override
    public StatMap<LiteralValueOperator.StatKey> copyStatMaps() {
      return new StatMap<>(_stats);
    }

    @Override
    public List<MultiStageOperator> getChildOperators() {
      return List.of();
    }

    @Override
    public String toExplainString() {
      return "early-termination-test";
    }
  }
}
