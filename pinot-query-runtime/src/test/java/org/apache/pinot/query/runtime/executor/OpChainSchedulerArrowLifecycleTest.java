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
package org.apache.pinot.query.runtime.executor;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlockConverter;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.executor.ExecutorServiceUtils;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/** Tests exactly-once cleanup for scheduled, rejected and cancelled attempts, with explicit quiescence barriers. */
public class OpChainSchedulerArrowLifecycleTest {
  private static final long REQUEST_ID = 123L;
  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"i"}, new ColumnDataType[]{ColumnDataType.INT});

  private final List<OpChainExecutionContext> _contexts = new ArrayList<>();
  private ExecutorService _rawExecutor;
  private ExecutorService _executor;
  private MailboxService _mailbox;
  private OpChainSchedulerService _scheduler;
  private AtomicInteger _finishCalls;
  @Nullable
  private OpChainExecutionContext _context;

  @BeforeMethod
  public void setUp() {
    _rawExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("arrow-lifecycle"));
    _executor = QueryThreadContext.contextAwareExecutorService(_rawExecutor);
    _scheduler = new OpChainSchedulerService(_executor);
    _mailbox = new MailboxService("localhost", 0, InstanceType.CONTROLLER,
        new PinotConfiguration(Map.of(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW, true)));
    _finishCalls = new AtomicInteger();
    _context = null;
    _contexts.clear();
  }

  @AfterMethod
  public void tearDown() {
    try {
      ExecutorServiceUtils.close(_executor);
      for (OpChainExecutionContext context : _contexts) {
        context.closeArrowResources();
      }
    } finally {
      _mailbox.shutdown();
    }
  }

  @DataProvider
  public Object[][] terminalOutcomes() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "terminalOutcomes")
  public void testNormalAndErrorOutcomesReleaseOnce(boolean error)
      throws InterruptedException {
    MultiStageOperator operator = operator();
    CountDownLatch finished = new CountDownLatch(1);
    OpChain chain;
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      chain = chain(operator, finished);
      ArrowBlock state = block();
      doAnswer(invocation -> {
        state.release();
        return null;
      }).when(operator).close();
      when(operator.nextBlock()).thenReturn(error
          ? ErrorMseBlock.fromException(new IllegalStateException("operator error")) : SuccessMseBlock.INSTANCE);
      _scheduler.register(chain);
    }

    await(finished);
    assertClean(operator);
    if (error) {
      verify(operator, times(1)).cancel(any());
    } else {
      verify(operator, never()).cancel(any());
    }
    chain.close();
    assertEquals(_finishCalls.get(), 1);
  }

  @Test
  public void testBuildFailureSweepsDroppedAndRetainedBlocks()
      throws InterruptedException {
    MultiStageOperator operator = operator();
    CountDownLatch finished = new CountDownLatch(1);
    when(operator.nextBlock()).thenAnswer(invocation -> {
      block();
      block().retain();
      throw new IllegalStateException("build failed");
    });
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      _scheduler.register(chain(operator, finished));
    }

    await(finished);
    assertClean(operator);
    verify(operator, times(1)).cancel(any());
  }

  @Test
  public void testCancellationFailureStillCloses()
      throws InterruptedException {
    MultiStageOperator operator = operator();
    CountDownLatch finished = new CountDownLatch(1);
    when(operator.nextBlock()).thenThrow(new IllegalStateException("execution failed"));
    doThrow(new IllegalStateException("cancel failed")).when(operator).cancel(any());
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      OpChain chain = chain(operator, finished);
      block();
      _scheduler.register(chain);
    }

    await(finished);
    assertClean(operator);
    verify(operator, times(1)).cancel(any());
  }

  @Test
  public void testMetricsFailureStillCloses()
      throws InterruptedException {
    MultiStageOperator operator = operator();
    CountDownLatch finished = new CountDownLatch(1);
    when(operator.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    when(operator.copyStatMaps()).thenThrow(new IllegalStateException("metrics failed"));
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      OpChain chain = chain(operator, finished);
      block();
      _scheduler.register(chain);
    }

    await(finished);
    assertClean(operator);
  }

  @Test
  public void testExecutorRejectionClosesUnscheduledAttempt() {
    _rawExecutor.shutdown();
    MultiStageOperator operator = operator();
    CountDownLatch finished = new CountDownLatch(1);
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      OpChain chain = chain(operator, finished);
      block();
      expectThrows(RejectedExecutionException.class, () -> _scheduler.register(chain));
      assertClean(operator);
      assertEquals(finished.getCount(), 0L);
      chain.close();
    }

    assertEquals(finished.getCount(), 0L);
    assertClean(operator);
    verify(operator, never()).nextBlock();
    verify(operator, times(1)).cancel(any());
  }

  @DataProvider
  public Object[][] preSchedulingTermination() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "preSchedulingTermination")
  public void testTerminationBeforeRegistrationClosesAttempt(boolean cachedCancellation) {
    MultiStageOperator operator = operator();
    CountDownLatch finished = new CountDownLatch(1);
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      OpChain chain = chain(operator, finished);
      block();
      if (cachedCancellation) {
        _scheduler.cancel(REQUEST_ID);
      } else {
        QueryThreadContext.get().getExecutionContext().terminate(QueryErrorCode.QUERY_CANCELLATION, "cancel");
      }
      expectThrows(QueryCancelledException.class, () -> _scheduler.register(chain));
      assertClean(operator);
      assertEquals(finished.getCount(), 0L);
      chain.close();
    }

    assertEquals(finished.getCount(), 0L);
    assertClean(operator);
    verify(operator, never()).nextBlock();
    verify(operator, times(1)).cancel(any());
  }

  @Test
  public void testQueuedCancellationDoesNotRequireWorkerExecution()
      throws Exception {
    CountDownLatch workerOccupied = new CountDownLatch(1);
    CountDownLatch unblockWorker = new CountDownLatch(1);
    Future<?> occupying = _rawExecutor.submit(() -> {
      workerOccupied.countDown();
      Uninterruptibles.awaitUninterruptibly(unblockWorker);
    });
    MultiStageOperator operator = operator();
    CountDownLatch finished = new CountDownLatch(1);
    try {
      await(workerOccupied);
      try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
        OpChain chain = chain(operator, finished);
        block();
        _scheduler.register(chain);
      }
      _scheduler.cancel(REQUEST_ID);
      await(finished);
      assertClean(operator);
      verify(operator, never()).nextBlock();
      verify(operator, times(1)).cancel(any());
    } finally {
      unblockWorker.countDown();
      occupying.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testQueuedCleanupFailureDoesNotInterruptQueryCancellation()
      throws Exception {
    CountDownLatch workerOccupied = new CountDownLatch(1);
    CountDownLatch unblockWorker = new CountDownLatch(1);
    Future<?> occupying = _rawExecutor.submit(() -> {
      workerOccupied.countDown();
      Uninterruptibles.awaitUninterruptibly(unblockWorker);
    });
    MultiStageOperator first = operator();
    MultiStageOperator second = operator();
    doThrow(new IllegalStateException("cancel failed")).when(first).cancel(any());
    CountDownLatch finished = new CountDownLatch(2);
    FutureTask<Void> laterTask = new FutureTask<>(() -> null);
    try {
      await(workerOccupied);
      try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
        OpChain firstChain = chain(first, finished);
        block();
        _scheduler.register(firstChain);
        OpChain secondChain = chain(second, finished, 1);
        block();
        _scheduler.register(secondChain);
        QueryThreadContext.get().getExecutionContext().addTask(laterTask);
      }
      _scheduler.cancel(REQUEST_ID);
      await(finished);
      assertTrue(laterTask.isCancelled(), "Cleanup failure must not stop cancellation of subsequent tasks");
      assertEquals(_finishCalls.get(), 2);
      assertEquals(_scheduler.activeRequestCount(), 0);
      assertEquals(_mailbox.getArrowBuffers().getAllocatedMemory(), 0L);
      verify(first, times(1)).close();
      verify(second, times(1)).close();
      verify(first, never()).nextBlock();
      verify(second, never()).nextBlock();
    } finally {
      unblockWorker.countDown();
      occupying.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testRunningCancellationWaitsForOperatorToExit()
      throws InterruptedException {
    MultiStageOperator operator = operator();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    CountDownLatch mayExit = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(1);
    AtomicBoolean executing = new AtomicBoolean();
    AtomicBoolean closedWhileExecuting = new AtomicBoolean();
    AtomicInteger observed = new AtomicInteger();
    doAnswer(invocation -> {
      closedWhileExecuting.set(executing.get());
      return null;
    }).when(operator).close();

    try {
      try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
        OpChain chain = chain(operator, finished);
        ArrowBlock block = block();
        when(operator.nextBlock()).thenAnswer(invocation -> {
          executing.set(true);
          entered.countDown();
          try {
            try {
              new CountDownLatch(1).await();
            } catch (InterruptedException expected) {
              interrupted.countDown();
              Uninterruptibles.awaitUninterruptibly(mayExit);
            }
            observed.set(block.getDataBlock().getInt(0, 0));
            return SuccessMseBlock.INSTANCE;
          } finally {
            executing.set(false);
          }
        });
        _scheduler.register(chain);
      }

      await(entered);
      _scheduler.cancel(REQUEST_ID);
      await(interrupted);
      assertEquals(finished.getCount(), 1L, "Cancellation is not proof of execution quiescence");
      assertEquals(_context.getOrCreateArrowContext().getLiveBlockCount(), 1);
      assertTrue(_mailbox.getArrowBuffers().getAllocatedMemory() > 0);
      mayExit.countDown();
      await(finished);
      assertFalse(closedWhileExecuting.get());
      assertEquals(observed.get(), 7);
      assertClean(operator);
      verify(operator, times(1)).cancel(any());
    } finally {
      _scheduler.cancel(REQUEST_ID);
      mayExit.countDown();
    }
  }

  private MultiStageOperator operator() {
    MultiStageOperator operator = mock(MultiStageOperator.class);
    when(operator.getChildOperators()).thenReturn(List.of());
    when(operator.copyStatMaps()).thenAnswer(invocation -> new StatMap<>(MailboxSendOperator.StatKey.class));
    when(operator.calculateStats()).thenReturn(MultiStageQueryStats.emptyStats(0));
    return operator;
  }

  private OpChain chain(MultiStageOperator operator, CountDownLatch finished) {
    return chain(operator, finished, 0);
  }

  private OpChain chain(MultiStageOperator operator, CountDownLatch finished, int stageId) {
    WorkerMetadata worker = new WorkerMetadata(0, Map.of(), Map.of());
    _context = OpChainExecutionContext.fromQueryContext(_mailbox, Map.of(),
        new StageMetadata(stageId, List.of(worker), Map.of()), worker, null, false, false);
    _contexts.add(_context);
    return new OpChain(_context, operator, id -> {
      _finishCalls.incrementAndGet();
      finished.countDown();
    });
  }

  private ArrowBlock block() {
    return ArrowBlockConverter.toArrowBlock(new RowHeapDataBlock(List.<Object[]>of(new Object[]{7}), SCHEMA),
        _context.getOrCreateArrowContext());
  }

  private void assertClean(MultiStageOperator operator) {
    assertEquals(_mailbox.getArrowBuffers().getAllocatedMemory(), 0L);
    assertEquals(_scheduler.activeRequestCount(), 0);
    assertEquals(_finishCalls.get(), 1);
    verify(operator, times(1)).close();
  }

  private static void await(CountDownLatch latch)
      throws InterruptedException {
    assertTrue(latch.await(10, TimeUnit.SECONDS), "Expected lifecycle transition within 10 seconds");
  }
}
