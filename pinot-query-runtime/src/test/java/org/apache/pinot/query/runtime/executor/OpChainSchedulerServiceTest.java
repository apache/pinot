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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.executor.DecoratorExecutorService;
import org.apache.pinot.spi.executor.ExecutorServiceUtils;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class OpChainSchedulerServiceTest {
  private AutoCloseable _mocks;
  private ExecutorService _executor;
  private MultiStageOperator _operatorA;

  @BeforeClass
  public void beforeClass() {
    _mocks = MockitoAnnotations.openMocks(this);
    _executor = QueryThreadContext.contextAwareExecutorService(
        Executors.newCachedThreadPool(new NamedThreadFactory("worker_on_" + getClass().getSimpleName())));
  }

  @AfterClass
  public void afterClass()
      throws Exception {
    _mocks.close();
    ExecutorServiceUtils.close(_executor);
  }

  @BeforeMethod
  public void beforeMethod() {
    _operatorA = Mockito.mock(MultiStageOperator.class);
    clearInvocations(_operatorA);
    Mockito.when(_operatorA.copyStatMaps()).thenAnswer(inv -> new StatMap<>(MailboxSendOperator.StatKey.class));
  }

  private OpChain getChain(MultiStageOperator operator) {
    return getChain(operator, 0);
  }

  private OpChain getChain(MultiStageOperator operator, int stageId) {
    MailboxService mailboxService = mock(MailboxService.class);
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(1234);
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of(), Map.of());
    OpChainExecutionContext context = OpChainExecutionContext.fromQueryContext(mailboxService, Map.of(),
        new StageMetadata(stageId, List.of(workerMetadata), Map.of()), workerMetadata, null, true, true);
    return new OpChain(context, operator);
  }

  @Test
  public void shouldScheduleSingleOpChainRegisteredAfterStart()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      latch.countDown();
      return SuccessMseBlock.INSTANCE;
    });

    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(getChain(_operatorA));
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldScheduleSingleOpChainRegisteredBeforeStart()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      latch.countDown();
      return SuccessMseBlock.INSTANCE;
    });

    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(getChain(_operatorA));
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldCallCloseOnOperatorsThatFinishSuccessfully()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    Mockito.doAnswer(inv -> {
      latch.countDown();
      return null;
    }).when(_operatorA).close();

    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(getChain(_operatorA));
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldCallCancelOnOperatorsThatReturnErrorBlock()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(ErrorMseBlock.fromException(new RuntimeException("foo")));
    Mockito.doAnswer(inv -> {
      latch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());

    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(getChain(_operatorA));
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS), "expected await to be called in less than 10 seconds");
  }

  @Test
  public void shouldCallCancelOnOpChainsWhenItIsCancelledByDispatch()
      throws InterruptedException {
    CountDownLatch opChainStarted = new CountDownLatch(1);
    Mockito.doAnswer(inv -> {
      opChainStarted.countDown();
      while (true) {
        Thread.sleep(1000);
      }
    }).when(_operatorA).nextBlock();

    CountDownLatch cancelLatch = new CountDownLatch(1);
    Mockito.doAnswer(inv -> {
      cancelLatch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());
    Mockito.doAnswer(inv -> MultiStageQueryStats.emptyStats(1)).when(_operatorA).calculateStats();

    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(getChain(_operatorA));
    }

    assertTrue(opChainStarted.await(10, TimeUnit.SECONDS), "op chain doesn't seem to be started");

    // now cancel the request.
    schedulerService.cancel(123L);
    // The eviction inside cancel() happens under the write lock (before context.terminate()), so the context
    // map must be empty as soon as cancel() returns — no need to wait for the opchain to finish.
    assertEquals(schedulerService.activeRequestCount(), 0,
        "context map should be empty immediately after cancel() returns");

    assertTrue(cancelLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be cancelled");
    Mockito.verify(_operatorA, Mockito.times(1)).cancel(Mockito.any());
  }

  /// An op chain that is still waiting for a thread when the query is terminated must still fail with the termination
  /// reason. Otherwise it never sends an EOS or an error to its receivers, and they wait until the query deadline.
  @Test
  public void shouldCancelOpChainWhenQueryIsTerminatedBeforeItStarts()
      throws InterruptedException {
    ExecutorService singleThreadExecutor =
        Executors.newSingleThreadExecutor(new NamedThreadFactory("single_worker_on_" + getClass().getSimpleName()));
    try {
      // Keep the only thread busy, so that the op chain waits in the executor queue until the query is terminated.
      CountDownLatch releaseThread = new CountDownLatch(1);
      singleThreadExecutor.execute(() -> {
        try {
          releaseThread.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      CountDownLatch cancelLatch = new CountDownLatch(1);
      AtomicReference<Throwable> cancelCause = new AtomicReference<>();
      Mockito.doAnswer(inv -> {
        cancelCause.set(inv.getArgument(0));
        cancelLatch.countDown();
        return null;
      }).when(_operatorA).cancel(Mockito.any());

      OpChainSchedulerService schedulerService =
          new OpChainSchedulerService(QueryThreadContext.contextAwareExecutorService(singleThreadExecutor));
      try (QueryThreadContext threadContext = QueryThreadContext.openForMseTest()) {
        schedulerService.register(getChain(_operatorA));
        threadContext.getExecutionContext()
            .terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, "OOM killed on SERVER");
      }
      releaseThread.countDown();

      assertTrue(cancelLatch.await(10, TimeUnit.SECONDS), "expected the op chain to be cancelled");
      assertEquals(QueryErrorCode.fromThrowable(cancelCause.get(), QueryErrorCode.UNKNOWN),
          QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED);
      assertEquals(cancelCause.get().getMessage(), "OOM killed on SERVER");
      Mockito.verify(_operatorA, Mockito.timeout(10_000)).close();
      Mockito.verify(_operatorA, Mockito.never()).nextBlock();
    } finally {
      singleThreadExecutor.shutdownNow();
    }
  }

  /// An op chain registered after its query is terminated does not run, and must not be dropped either. It is
  /// cancelled with the termination error before register() returns, without going through the executor, so that its
  /// receivers get that error.
  @Test
  public void shouldCancelOpChainRegisteredAfterQueryIsTerminated() {
    AtomicReference<Throwable> listenerError = new AtomicReference<>();
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    schedulerService.registerCompletionListener(123L, (id, root, stats, ctx, error) -> listenerError.set(error));
    try (QueryThreadContext threadContext = QueryThreadContext.openForMseTest()) {
      threadContext.getExecutionContext()
          .terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, "OOM killed on SERVER");
      schedulerService.register(getChain(_operatorA));
    }

    ArgumentCaptor<Throwable> cancelCause = ArgumentCaptor.forClass(Throwable.class);
    Mockito.verify(_operatorA).cancel(cancelCause.capture());
    assertEquals(QueryErrorCode.fromThrowable(cancelCause.getValue(), QueryErrorCode.UNKNOWN),
        QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED);
    assertEquals(cancelCause.getValue().getMessage(), "OOM killed on SERVER");
    Mockito.verify(_operatorA).close();
    Mockito.verify(_operatorA, Mockito.never()).nextBlock();
    assertSame(listenerError.get(), cancelCause.getValue());
    assertEquals(schedulerService.activeRequestCount(), 0);
  }

  /// Terminating the query interrupts the thread that runs the op chain. The interrupt must not outlive the op chain,
  /// or it would hit the next task of that thread.
  @Test
  public void shouldNotLeakInterruptAfterOpChainCompletes()
      throws InterruptedException {
    CountDownLatch registered = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      registered.await();
      return SuccessMseBlock.INSTANCE;
    });
    Mockito.doAnswer(inv -> MultiStageQueryStats.emptyStats(0)).when(_operatorA).calculateStats();
    // Records whether the thread is still interrupted when the task returns, which the next task of a pooled thread
    // would see
    AtomicBoolean interruptedAfterTask = new AtomicBoolean();
    CountDownLatch taskDone = new CountDownLatch(1);
    ExecutorService threadPool = Executors.newCachedThreadPool();
    ExecutorService recordingExecutor = new DecoratorExecutorService(threadPool) {
      @Override
      protected <T> Callable<T> decorate(Callable<T> task) {
        return task;
      }

      @Override
      protected Runnable decorate(Runnable task) {
        return () -> {
          task.run();
          interruptedAfterTask.set(Thread.currentThread().isInterrupted());
          taskDone.countDown();
        };
      }
    };
    try {
      OpChainSchedulerService schedulerService =
          new OpChainSchedulerService(QueryThreadContext.contextAwareExecutorService(recordingExecutor));
      try (QueryThreadContext threadContext = QueryThreadContext.openForMseTest()) {
        // Terminate the query while the op chain closes, once it no longer checks for interrupts
        Mockito.doAnswer(inv -> threadContext.getExecutionContext()
                .terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, "OOM killed on SERVER"))
            .when(_operatorA).close();
        schedulerService.register(getChain(_operatorA));
      }
      registered.countDown();

      assertTrue(taskDone.await(10, TimeUnit.SECONDS), "expected the op chain to complete");
      Mockito.verify(_operatorA).close();
      assertFalse(interruptedAfterTask.get(), "The interrupt of the op chain leaked into its thread");
    } finally {
      threadPool.shutdownNow();
    }
  }

  @Test
  public void shouldFireCompletionListenerOnSuccess()
      throws InterruptedException {
    CountDownLatch completed = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    Mockito.doAnswer(inv -> MultiStageQueryStats.emptyStats(0)).when(_operatorA).calculateStats();

    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    AtomicBoolean errorWasNull = new AtomicBoolean(false);
    AtomicReference<MultiStageQueryStats> seenStats =
        new AtomicReference<>();
    schedulerService.registerCompletionListener(123L, (id, root, stats, ctx, error) -> {
      errorWasNull.set(error == null);
      seenStats.set(stats);
      completed.countDown();
    });
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(getChain(_operatorA));
    }

    assertTrue(completed.await(10, TimeUnit.SECONDS), "expected completion listener to fire");
    assertTrue(errorWasNull.get(), "expected error to be null on successful opchain");
    assertTrue(seenStats.get() != null, "expected stats to be passed to listener");
  }

  @Test
  public void shouldFireCompletionListenerOnError()
      throws InterruptedException {
    CountDownLatch completed = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenThrow(new RuntimeException("boom"));

    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    AtomicReference<Throwable> seenError =
        new AtomicReference<>();
    schedulerService.registerCompletionListener(123L, (id, root, stats, ctx, error) -> {
      seenError.set(error);
      completed.countDown();
    });
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(getChain(_operatorA));
    }

    assertTrue(completed.await(10, TimeUnit.SECONDS), "expected completion listener to fire on error");
    assertTrue(seenError.get() != null, "expected error to be propagated to listener");
  }

  @Test
  public void shouldNotFireCompletionListenerAfterUnregister()
      throws InterruptedException {
    CountDownLatch opChainCloseLatch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    Mockito.doAnswer(inv -> MultiStageQueryStats.emptyStats(0)).when(_operatorA).calculateStats();
    Mockito.doAnswer(inv -> {
      opChainCloseLatch.countDown();
      return null;
    }).when(_operatorA).close();

    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    AtomicInteger fired = new AtomicInteger();
    OpChainCompletionListener listener = (id, root, stats, ctx, error) -> fired.incrementAndGet();
    schedulerService.registerCompletionListener(123L, listener);
    schedulerService.unregisterCompletionListener(123L);

    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(getChain(_operatorA));
    }

    assertTrue(opChainCloseLatch.await(10, TimeUnit.SECONDS), "expected opchain to finish");
    // Listener was unregistered before the opchain ran, so it must not fire.
    assertEquals(fired.get(), 0, "listener should not fire after unregister");
  }

  @Test
  public void shouldCallCancelOnOpChainsThatThrow()
      throws InterruptedException {
    CountDownLatch cancelLatch = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenThrow(new RuntimeException("foo"));
    Mockito.doAnswer(inv -> {
      cancelLatch.countDown();
      return null;
    }).when(_operatorA).cancel(Mockito.any());

    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(getChain(_operatorA));
    }

    assertTrue(cancelLatch.await(10, TimeUnit.SECONDS), "expected OpChain to be cancelled");
    Mockito.verify(_operatorA, Mockito.times(1)).cancel(Mockito.any());
  }

  /// The cancel of a query can reach the server before its stages. The op chains registered after that do not run:
  /// they are cancelled with the cancellation error.
  @Test
  public void shouldCancelOpChainRegisteredAfterQueryCancellation() {
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    // No op chain of the query is registered yet, so only the cancelled-query cache keeps the cancellation
    schedulerService.cancel(123L);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(getChain(_operatorA));
    }

    ArgumentCaptor<Throwable> cancelCause = ArgumentCaptor.forClass(Throwable.class);
    Mockito.verify(_operatorA).cancel(cancelCause.capture());
    assertEquals(QueryErrorCode.fromThrowable(cancelCause.getValue(), QueryErrorCode.UNKNOWN),
        QueryErrorCode.QUERY_CANCELLATION);
    Mockito.verify(_operatorA).close();
    Mockito.verify(_operatorA, Mockito.never()).nextBlock();
  }

  /// Registers two opchains for the same request, waits for both to complete, and verifies that the per-request
  /// context map entry is removed once the last opchain finishes. Regression coverage for the TOCTOU race in
  /// decrementActiveOpChains that could leave a stale entry in \_executionContextByRequest.
  @Test
  public void shouldCleanUpContextAfterAllOpChainsComplete()
      throws InterruptedException {
    CountDownLatch allClosed = new CountDownLatch(2);
    MultiStageOperator operatorB = Mockito.mock(MultiStageOperator.class);
    Mockito.when(operatorB.copyStatMaps()).thenAnswer(inv -> new StatMap<>(MailboxSendOperator.StatKey.class));

    Mockito.when(_operatorA.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    Mockito.doAnswer(inv -> MultiStageQueryStats.emptyStats(0)).when(_operatorA).calculateStats();
    Mockito.doAnswer(inv -> {
      allClosed.countDown();
      return null;
    }).when(_operatorA).close();

    Mockito.when(operatorB.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    Mockito.doAnswer(inv -> MultiStageQueryStats.emptyStats(1)).when(operatorB).calculateStats();
    Mockito.doAnswer(inv -> {
      allClosed.countDown();
      return null;
    }).when(operatorB).close();

    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(getChain(_operatorA, 0));
      schedulerService.register(getChain(operatorB, 1));
    }

    assertTrue(allClosed.await(10, TimeUnit.SECONDS), "expected both opchains to complete within 10 s");
    assertEquals(schedulerService.activeRequestCount(), 0,
        "context map should be empty after all opchains for a request complete");
  }

  @Test
  public void shouldHandleConcurrentCancellationAndRegistration()
      throws InterruptedException {
    CountDownLatch registrationStarted = new CountDownLatch(1);
    CountDownLatch cancellationCanProceed = new CountDownLatch(1);
    Mockito.when(_operatorA.nextBlock()).thenAnswer(inv -> {
      registrationStarted.countDown();
      cancellationCanProceed.await();
      return SuccessMseBlock.INSTANCE;
    });
    Mockito.doAnswer(inv -> MultiStageQueryStats.emptyStats(1)).when(_operatorA).calculateStats();

    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      // Start registration in a separate thread
      CountDownLatch registrationCompleted = new CountDownLatch(1);
      boolean[] registrationSucceeded = {false};
      _executor.submit(() -> {
        try {
          schedulerService.register(getChain(_operatorA));
          registrationSucceeded[0] = true;
        } finally {
          registrationCompleted.countDown();
        }
      });

      // Wait for registration to start
      assertTrue(registrationStarted.await(10, TimeUnit.SECONDS), "Registration should have started");

      // Now cancel the query while it's running
      schedulerService.cancel(123L);

      // Allow the opchain execution to complete
      cancellationCanProceed.countDown();

      // Wait for registration thread to complete
      assertTrue(registrationCompleted.await(10, TimeUnit.SECONDS), "Registration thread should complete");

      // The registration should have succeeded since it started before cancellation
      assertTrue(registrationSucceeded[0], "Registration should have succeeded");

      // An op chain registered for the same query after the cancellation does not run, but is cancelled
      MultiStageOperator operatorB = Mockito.mock(MultiStageOperator.class);
      Mockito.when(operatorB.copyStatMaps()).thenAnswer(inv -> new StatMap<>(MailboxSendOperator.StatKey.class));
      schedulerService.register(getChain(operatorB));
      Mockito.verify(operatorB, Mockito.timeout(10_000)).cancel(Mockito.argThat(
          t -> QueryErrorCode.fromThrowable(t, QueryErrorCode.UNKNOWN) == QueryErrorCode.QUERY_CANCELLATION));
      Mockito.verify(operatorB, Mockito.never()).nextBlock();
    }
  }
}
