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
package org.apache.pinot.core.query.executor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.executor.ThrottleOnCriticalHeapUsageExecutor;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ThrottleOnCriticalHeapUsageExecutorTest {
  private ExecutorService _base;

  @BeforeMethod
  public void setUp() {
    _base = Executors.newFixedThreadPool(8);
    ServerMetrics.deregister();
    ServerMetrics.register(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
  }

  @AfterMethod
  public void tearDown() {
    if (_base != null) {
      _base.shutdownNow();
    }
    ServerMetrics.deregister();
  }

  @Test
  public void testMultiThreadThrottlingQueueAndRecovery()
      throws Exception {
    // Custom accountant toggling throttle
    AtomicBoolean throttle = new AtomicBoolean(true);
    ThreadAccountant accountant = Mockito.mock(ThreadAccountant.class);
    Mockito.when(accountant.throttleQuerySubmission()).thenAnswer(inv -> throttle.get());

    // Open a query context for submitting tasks
    try (QueryThreadContext ignored = QueryThreadContext.open(QueryExecutionContext.forMseTest(), accountant)) {
      ThrottleOnCriticalHeapUsageExecutor ex = new ThrottleOnCriticalHeapUsageExecutor(
          _base, /*maxQueueSize*/ 100, /*timeout*/ TimeUnit.SECONDS.toMillis(5), /*monitor*/ 50);

      int numTasks = 50;
      AtomicInteger ranCount = new AtomicInteger(0);

      // Submit multiple tasks concurrently under throttling; they should queue
      for (int i = 0; i < numTasks; i++) {
        ex.execute(() -> {
          ranCount.incrementAndGet();
        });
      }

      // Ensure tasks are queued (not run yet)
      Thread.sleep(100);
      assertEquals(ranCount.get(), 0);
      assertEquals(ex.getQueueSize(), numTasks);

      // Release throttling and wait for background monitor to drain
      throttle.set(false);

      // Wait up to a few seconds for all tasks to run
      long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
      while (System.currentTimeMillis() < deadline && ranCount.get() < numTasks) {
        Thread.sleep(20);
      }

      assertEquals(ranCount.get(), numTasks, "All queued tasks should run after recovery");

      // Metrics should reflect queued and processed counts
      // Note: Using getMeteredValue increments is intrusive; validate no exceptions and queue drained
      assertEquals(ex.getQueueSize(), 0);

      ex.shutdown();
    }
  }
}
