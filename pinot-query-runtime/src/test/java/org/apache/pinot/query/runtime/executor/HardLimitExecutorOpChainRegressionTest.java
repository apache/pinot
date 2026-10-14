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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.executor.HardLimitExecutor;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.query.QueryThreadExceedStrategy;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Regression test for the opchain leak that [HardLimitExecutor] used to cause when it re-checked the limit at run
/// time. The second check could reject a task that passed the submit-time check, throwing before the
/// [OpChainSchedulerService] `ListenableFutureTask` ran. The completion callback that closes the opchain and
/// decrements the per-request counter never fired, and the exception was swallowed by the `FutureTask` created by
/// `AbstractExecutorService.submit`, so nothing was logged either.
///
/// Now the limit is checked only once, on submit. A task that passes that check always runs.
public class HardLimitExecutorOpChainRegressionTest {

  /// Executor that queues tasks instead of running them so the test controls when each one starts.
  private static class DeferredExecutor extends AbstractExecutorService {
    final List<Runnable> _queued = new ArrayList<>();

    @Override
    public void execute(Runnable command) {
      _queued.add(command);
    }

    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
      return List.of();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return true;
    }
  }

  private static OpChain getChain(MultiStageOperator operator) {
    MailboxService mailboxService = mock(MailboxService.class);
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(1234);
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of(), Map.of());
    OpChainExecutionContext context = OpChainExecutionContext.fromQueryContext(mailboxService, Map.of(),
        new StageMetadata(0, List.of(workerMetadata), Map.of()), workerMetadata, null, true, true);
    return new OpChain(context, operator);
  }

  private static MultiStageOperator mockOperator() {
    MultiStageOperator operator = Mockito.mock(MultiStageOperator.class);
    when(operator.copyStatMaps()).thenAnswer(inv -> new StatMap<>(MailboxSendOperator.StatKey.class));
    return operator;
  }

  @Test
  public void acceptedOpChainAlwaysRunsAndCompletes()
      throws Exception {
    DeferredExecutor deferred = new DeferredExecutor();
    ExecutorService limited = new HardLimitExecutor(1, QueryThreadContext.contextAwareExecutorService(deferred),
        QueryThreadExceedStrategy.ERROR);
    OpChainSchedulerService scheduler = new OpChainSchedulerService(limited);

    CountDownLatch aStarted = new CountDownLatch(1);
    CountDownLatch aRelease = new CountDownLatch(1);
    MultiStageOperator operatorA = mockOperator();
    when(operatorA.nextBlock()).thenAnswer(inv -> {
      aStarted.countDown();
      aRelease.await();
      return SuccessMseBlock.INSTANCE;
    });
    MultiStageOperator operatorB = mockOperator();
    when(operatorB.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);

    // Both registrations pass the synchronous check: nothing has started yet, so _running == 0.
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      scheduler.register(getChain(operatorA));
      scheduler.register(getChain(operatorB));
    }
    assertEquals(deferred._queued.size(), 2, "both opchains must have been accepted by the executor");
    assertEquals(scheduler.activeRequestCount(), 1);

    // Start A: the HardLimitExecutor wrapper increments _running to 1 and A blocks inside nextBlock().
    Thread threadA = new Thread(deferred._queued.get(0), "opchain-a");
    threadA.start();
    assertTrue(aStarted.await(10, TimeUnit.SECONDS), "A did not start");

    // Start B: it passed the submit-time check, so it must run even though A pushed the counter to the limit.
    Runnable taskB = deferred._queued.get(1);
    taskB.run();
    assertTrue(taskB instanceof Future, "AbstractExecutorService.submit wraps the task in a FutureTask");
    ((Future<?>) taskB).get(10, TimeUnit.SECONDS);
    verify(operatorB).nextBlock();
    verify(operatorB).close();

    // Let A finish normally.
    aRelease.countDown();
    threadA.join(TimeUnit.SECONDS.toMillis(10));
    verify(operatorA).close();

    assertEquals(scheduler.activeRequestCount(), 0, "every opchain completed, so no request may stay active");
  }
}
