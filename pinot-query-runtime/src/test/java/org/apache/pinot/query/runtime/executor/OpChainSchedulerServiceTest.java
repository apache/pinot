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
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.executor.ExecutorServiceUtils;
import org.apache.pinot.spi.query.QueryThreadContext;
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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


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

  @Test
  public void shouldThrowQueryCancelledExceptionWhenRegisteringOpChainAfterQueryCancellation() {
    Mockito.when(_operatorA.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    Mockito.doAnswer(inv -> MultiStageQueryStats.emptyStats(1)).when(_operatorA).calculateStats();

    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(getChain(_operatorA));

      // First cancel the query with the same requestId (123L as defined in getChain method)
      schedulerService.cancel(123L);

      // Now try to register an OpChain for the same query - should throw QueryCancelledException
      try {
        schedulerService.register(getChain(_operatorA));
        fail("Expected QueryCancelledException to be thrown when registering OpChain after query cancellation");
      } catch (QueryCancelledException e) {
        String message = e.getMessage();
        assertTrue(message.contains("Query has been cancelled before op-chain"));
        assertTrue(message.contains("being scheduled"));
      }
    }
  }

  /**
   * Registers two opchains for the same request, waits for both to complete, and verifies that the per-request
   * context map entry is removed once the last opchain finishes. Regression coverage for the TOCTOU race in
   * decrementActiveOpChains that could leave a stale entry in _executionContextByRequest.
   */
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
        } catch (QueryCancelledException e) {
          // Expected if cancellation happens before registration
          registrationSucceeded[0] = false;
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

      // Now try to register another OpChain with the same requestId - should fail
      try {
        schedulerService.register(getChain(_operatorA));
        fail("Expected QueryCancelledException for subsequent registration after cancellation");
      } catch (QueryCancelledException e) {
        assertTrue(e.getMessage().contains("Query has been cancelled before op-chain"));
      }
    }
  }
}
