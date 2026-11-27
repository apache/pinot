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
package org.apache.pinot.core.accounting;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.accounting.ExternalExecutionSampler;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.accounting.WorkloadBudgetManagerFactory;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.apache.pinot.spi.utils.ResourceUsageUtils;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises query-thread attribution and pause controls without cooperative Java samples on the query thread.
public class ExternalThreadAccountingTest {
  private static volatile Object _allocationSink;

  @Test(timeOut = 15000)
  public void testSamplesOriginalThreadAndPreservesFinalAccounting() throws Exception {
    boolean registeredWorkloadManager = WorkloadBudgetManagerFactory.get() == null;
    if (registeredWorkloadManager) {
      WorkloadBudgetManagerFactory.register(new PinotConfiguration());
    }
    boolean cpuEnabled = ThreadResourceUsageProvider.isThreadCpuTimeMeasurementEnabled();
    boolean memoryEnabled = ThreadResourceUsageProvider.isThreadMemoryMeasurementEnabled();
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    try {
      if (!ThreadResourceUsageProvider.isCrossThreadCpuTimeMeasurementEnabled()
          || !ThreadResourceUsageProvider.isCrossThreadMemoryMeasurementEnabled()) {
        throw new SkipException("Platform does not support cross-thread CPU and heap measurements");
      }
      verifyAccounting(
          new ResourceUsageAccountantFactory.ResourceUsageAccountant(config(), "test", InstanceType.SERVER));
      verifyAccounting(new PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant(config(), "test",
          InstanceType.SERVER));
    } finally {
      ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(cpuEnabled);
      ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(memoryEnabled);
      if (registeredWorkloadManager) {
        WorkloadBudgetManagerFactory.unregister();
      }
    }
  }

  private static PinotConfiguration config() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Accounting.Keys.ENABLE_THREAD_CPU_SAMPLING, true);
    config.setProperty(Accounting.Keys.ENABLE_THREAD_MEMORY_SAMPLING, true);
    return config;
  }

  private static void verifyAccounting(ThreadAccountant accountant) throws Exception {
    AtomicReference<ExternalExecutionSampler> captured = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicReference<QueryThreadContext> expectedContext = new AtomicReference<>();
    CountDownLatch ready = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    QueryExecutionContext execution = QueryExecutionContext.forSseTest();
    Thread owner = new Thread(() -> {
      try (QueryThreadContext context = QueryThreadContext.open(execution, accountant)) {
        expectedContext.set(context);
        try (ExternalExecutionSampler sampler = accountant.captureExternalExecutionSampler()) {
          assertNotNull(sampler);
          captured.set(sampler);
          for (int i = 0; i < 512; i++) {
            _allocationSink = new byte[8192];
          }
          ready.countDown();
          assertTrue(finish.await(5, TimeUnit.SECONDS));
          sampler.sampleUsage();
        }
      } catch (Throwable t) {
        failure.set(t);
        ready.countDown();
      }
    });
    owner.start();
    try {
      assertTrue(ready.await(5, TimeUnit.SECONDS));
      if (failure.get() != null) {
        throw new AssertionError(failure.get());
      }
      ExternalExecutionSampler sampler = captured.get();
      sampler.sampleUsage();
      var tracker = accountant.getThreadResources().iterator().next();
      assertSame(tracker.getThreadContext(), expectedContext.get());
      assertTrue(tracker.getCpuTimeNs() > 0);
      assertTrue(tracker.getAllocatedBytes() >= 512L * 8192);
      assertTrue(tracker.getCpuTimeNs() <= ThreadResourceUsageProvider.getThreadCpuTime(owner.threadId()));
      assertEquals(accountant.getQueryResources().get(execution.getCid()).getCpuTimeNs(), tracker.getCpuTimeNs());
      assertEquals(accountant.getQueryResources().get(execution.getCid()).getAllocatedBytes(),
          tracker.getAllocatedBytes());
      assertFalse(sampler.isPaused());
      finish.countDown();
      owner.join(5000);
      assertFalse(owner.isAlive());
      if (failure.get() != null) {
        throw new AssertionError(failure.get());
      }
      // A stale timer snapshot after unregister is harmless and cannot overwrite the next query's tracker.
      sampler.sampleUsage();
      assertNull(tracker.getThreadContext());
      assertEquals(tracker.getCpuTimeNs(), 0L);
      assertEquals(tracker.getAllocatedBytes(), 0L);
    } finally {
      finish.countDown();
      owner.join(5000);
      accountant.stopWatcherTask();
    }
  }

  @Test
  public void testRealPauseStateRemainsObservableWithoutBlockingSampler() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Accounting.Keys.OOM_PRE_QUERY_KILL_PAUSE_DURATION_MS, 5000L);
    config.setProperty(Accounting.Keys.CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0f);
    config.setProperty(Accounting.Keys.PANIC_LEVEL_HEAP_USAGE_RATIO, 1.1f);
    config.setProperty(Accounting.Keys.OOM_PROTECTION_KILLING_QUERY, true);
    config.setProperty(Accounting.Keys.ALARMING_LEVEL_HEAP_USAGE_RATIO, 0f);
    QueryResourceAggregator aggregator = new QueryResourceAggregator("test", InstanceType.SERVER, false, true,
        new AtomicReference<>(new QueryMonitorConfig(config, ResourceUsageUtils.getMaxHeapSize())));
    try (QueryThreadContext context = QueryThreadContext.openForSseTest()) {
      ThreadResourceTrackerImpl tracker = new ThreadResourceTrackerImpl();
      tracker.setThreadContext(context);
      try (ExternalExecutionSampler sampler = tracker.captureExternalExecutionSampler(false, false,
          aggregator::isPauseActive)) {
        assertNotNull(sampler);
        assertFalse(sampler.isPaused());
        aggregator.preAggregate(List.of(tracker));
        aggregator.postAggregate();
        assertTrue(sampler.isPaused());
        sampler.sampleUsage();
        aggregator.clearPause();
        assertFalse(sampler.isPaused());
      } finally {
        aggregator.clearPause();
        tracker.clear();
      }
    }
  }

  @Test
  public void testCustomAccountantMustExplicitlyPreserveItsPolicies() {
    ThreadAccountant custom =
        new PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant(new PinotConfiguration(), "test",
            InstanceType.SERVER) {
          @Override
          public boolean waitIfPaused() {
            return true;
          }
        };
    try (QueryThreadContext ignored = QueryThreadContext.open(QueryExecutionContext.forSseTest(), custom)) {
      assertNull(custom.captureExternalExecutionSampler());
    } finally {
      custom.stopWatcherTask();
    }
  }

  @Test
  public void testRejectsMissingContextAndContextReuse() {
    ThreadResourceTrackerImpl tracker = new ThreadResourceTrackerImpl();
    assertNull(tracker.captureExternalExecutionSampler(false, false, () -> false));
    try (QueryThreadContext context = QueryThreadContext.openForSseTest()) {
      tracker.setThreadContext(context);
      try (ExternalExecutionSampler sampler = tracker.captureExternalExecutionSampler(false, false, () -> false)) {
        assertNotNull(sampler);
        tracker.clear();
        assertThrows(IllegalStateException.class, sampler::sampleUsage);
      }
    }
  }

  @Test
  public void testVirtualThreadCannotBorrowPlatformAccounting() throws Exception {
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread thread = Thread.ofVirtual().start(() -> {
      try (QueryThreadContext context = QueryThreadContext.openForSseTest()) {
        ThreadResourceTrackerImpl tracker = new ThreadResourceTrackerImpl();
        tracker.setThreadContext(context);
        assertNull(tracker.captureExternalExecutionSampler(false, false, () -> false));
      } catch (Throwable t) {
        failure.set(t);
      }
    });
    thread.join();
    if (failure.get() != null) {
      throw new AssertionError(failure.get());
    }
  }

  @Test
  public void testControlThreadCannotCaptureAnotherThreadsAccountingContext() throws Exception {
    ThreadResourceTrackerImpl tracker = new ThreadResourceTrackerImpl();
    try (QueryThreadContext context = QueryThreadContext.openForSseTest()) {
      tracker.setThreadContext(context);
      assertNull(CompletableFuture.supplyAsync(
          () -> tracker.captureExternalExecutionSampler(false, false, () -> false)).get(5, TimeUnit.SECONDS));
    } finally {
      tracker.clear();
    }
  }
}
