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
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.metrics.MseMetricsEmitter;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerMetricsEmitter;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.executor.ExecutorServiceUtils;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Regression test for the NOOP-binding fix in {@link OpChainSchedulerService}'s inner {@code Metrics}
 * class.
 *
 * <p>Before the fix, the inner class cached {@link org.apache.pinot.spi.metrics.PinotMeter} handles at
 * construction by calling {@code ServerMeter.MSE_OPCHAINS_STARTED.getGlobalMeter()}. When the scheduler
 * was constructed before {@code ServerMetrics.register(...)} was invoked, the captured meter was
 * permanently bound to the NOOP registry and emitted zero for the lifetime of the JVM.
 *
 * <p>The fix routes emissions through {@link MseMetricsEmitter}, which resolves the active registry
 * at call time. This test:
 *
 * <ol>
 *   <li>Deregisters {@code ServerMetrics} and {@code MseMetricsEmitter}.</li>
 *   <li>Constructs an {@link OpChainSchedulerService} (the moment at which the legacy code captured
 *       its meter handles).</li>
 *   <li>Registers a fresh {@code ServerMetrics} + {@link ServerMetricsEmitter}.</li>
 *   <li>Schedules an op-chain through the scheduler and waits for completion.</li>
 *   <li>Asserts that {@code ServerMeter.MSE_OPCHAINS_STARTED} was incremented on the freshly
 *       registered {@code ServerMetrics}.</li>
 * </ol>
 */
public class OpChainSchedulerServiceMetricsTest {

  private AutoCloseable _mocks;
  private ExecutorService _executor;
  private MultiStageOperator _operator;

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
    // Critical: clear any prior registration so the scheduler is constructed against the NOOP.
    MseMetricsEmitter.deregister();
    ServerMetrics.deregister();
    _operator = Mockito.mock(MultiStageOperator.class);
    Mockito.when(_operator.copyStatMaps()).thenAnswer(inv -> new StatMap<>(MailboxSendOperator.StatKey.class));
  }

  @AfterMethod
  public void afterMethod() {
    MseMetricsEmitter.deregister();
    ServerMetrics.deregister();
  }

  @Test
  public void testOpChainMetricsEmittedAfterLateRegistration()
      throws InterruptedException {
    // Step 2: construct the scheduler BEFORE registering metrics. With the buggy code path this
    // would have bound the meter handles to the NOOP registry permanently.
    OpChainSchedulerService schedulerService = new OpChainSchedulerService(_executor);

    // Step 3: register a fresh ServerMetrics + ServerMetricsEmitter AFTER construction.
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    ServerMetrics.register(serverMetrics);
    MseMetricsEmitter.register(new ServerMetricsEmitter());

    // Snapshot baseline counts on the live registry.
    long startedBefore = ServerMetrics.get().getMeteredValue(ServerMeter.MSE_OPCHAINS_STARTED).count();
    long completedBefore = ServerMetrics.get().getMeteredValue(ServerMeter.MSE_OPCHAINS_COMPLETED).count();

    // Step 4: run an op-chain to completion.
    CountDownLatch closeLatch = new CountDownLatch(1);
    when(_operator.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    Mockito.doAnswer(inv -> {
      closeLatch.countDown();
      return null;
    }).when(_operator).close();

    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      schedulerService.register(buildOpChain(_operator));
    }
    assertTrue(closeLatch.await(10, TimeUnit.SECONDS), "op-chain did not complete within 10 seconds");

    // Step 5: assert that BOTH the started and completed counters were incremented on the live
    // ServerMetrics. If the NOOP-binding bug were present, both deltas would be zero.
    waitForCountToReach(ServerMeter.MSE_OPCHAINS_STARTED, startedBefore + 1L,
        "MSE_OPCHAINS_STARTED was not incremented — NOOP-binding regression detected");
    waitForCountToReach(ServerMeter.MSE_OPCHAINS_COMPLETED, completedBefore + 1L,
        "MSE_OPCHAINS_COMPLETED was not incremented — NOOP-binding regression detected");
  }

  private static OpChain buildOpChain(MultiStageOperator operator) {
    MailboxService mailboxService = mock(MailboxService.class);
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(1234);
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of(), Map.of());
    OpChainExecutionContext context = OpChainExecutionContext.fromQueryContext(mailboxService, Map.of(),
        new StageMetadata(0, List.of(workerMetadata), Map.of()), workerMetadata, null, true, true);
    return new OpChain(context, operator);
  }

  private static void waitForCountToReach(ServerMeter meter, long target, String failureMessage)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + 5_000L;
    while (System.currentTimeMillis() < deadline) {
      long current = ServerMetrics.get().getMeteredValue(meter).count();
      if (current >= target) {
        return;
      }
      Thread.sleep(25L);
    }
    fail(failureMessage + " (target=" + target + ", actual="
        + ServerMetrics.get().getMeteredValue(meter).count() + ")");
  }
}
