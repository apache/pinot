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
package org.apache.pinot.spi.query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class QueryExecutionContextTest {
  private ExecutorService _baseExecutorService;
  private ExecutorService _executorService;

  @BeforeClass
  public void setUp() {
    _baseExecutorService = Executors.newCachedThreadPool();
    _executorService = QueryThreadContext.contextAwareExecutorService(_baseExecutorService);
  }

  @AfterClass
  public void tearDown() {
    _baseExecutorService.shutdownNow();
  }

  /// A done task keeps its result, and the execution context can stay reachable after the query ends (every thread of
  /// the query references it). So the context must not keep the tasks that are done.
  @Test
  public void testDoesNotKeepDoneTasks()
      throws Exception {
    try (QueryThreadContext threadContext = QueryThreadContext.openForMseTest()) {
      QueryExecutionContext executionContext = threadContext.getExecutionContext();
      List<Future<?>> futures = List.of(
          _executorService.submit(() -> new long[1000]),
          _executorService.submit(() -> {
            throw new IllegalStateException("test");
          }),
          _executorService.submit(() -> {
          }),
          _executorService.submit(() -> {
          }, "result"));
      for (Future<?> future : futures) {
        try {
          future.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
          assertTrue(e.getCause() instanceof IllegalStateException);
        }
      }
      // A task removes itself right after its result is set, so get() can return a little earlier
      waitForNoTasks(executionContext);
    }
  }

  @Test
  public void testDoesNotAddDoneTask() {
    QueryExecutionContext executionContext = QueryExecutionContext.forMseTest();
    executionContext.addTask(CompletableFuture.completedFuture("result"));
    assertEquals(executionContext.getNumTasks(), 0);
  }

  @Test
  public void testDoesNotRegisterTasksWhenNotAsked()
      throws Exception {
    ExecutorService executorService = QueryThreadContext.contextAwareExecutorService(_baseExecutorService, false);
    try (QueryThreadContext threadContext = QueryThreadContext.openForMseTest()) {
      CountDownLatch release = new CountDownLatch(1);
      Future<?> future = executorService.submit(() -> release.await(10, TimeUnit.SECONDS));
      assertEquals(threadContext.getExecutionContext().getNumTasks(), 0);
      release.countDown();
      future.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testTerminateCancelsRunningTasks()
      throws Exception {
    try (QueryThreadContext threadContext = QueryThreadContext.openForMseTest()) {
      QueryExecutionContext executionContext = threadContext.getExecutionContext();
      // Two tasks, because each one removes itself from the context while terminate() cancels them
      int numTasks = 2;
      CountDownLatch started = new CountDownLatch(numTasks);
      List<Future<?>> running = new ArrayList<>();
      for (int i = 0; i < numTasks; i++) {
        running.add(_executorService.submit(() -> {
          started.countDown();
          Thread.sleep(60_000L);
          return null;
        }));
      }
      assertTrue(started.await(10, TimeUnit.SECONDS));
      assertEquals(executionContext.getNumTasks(), numTasks);

      assertTrue(executionContext.terminate(QueryErrorCode.QUERY_CANCELLATION, "test"));
      for (Future<?> future : running) {
        assertTrue(future.isCancelled());
      }
      assertEquals(executionContext.getNumTasks(), 0);

      // A task submitted after the termination is cancelled and does not run
      AtomicBoolean ran = new AtomicBoolean();
      Future<?> late = _executorService.submit(() -> ran.set(true));
      assertTrue(late.isCancelled());
      assertThrows(CancellationException.class, late::get);
      assertFalse(ran.get());
      assertEquals(executionContext.getNumTasks(), 0);
    }
  }

  private static void waitForNoTasks(QueryExecutionContext executionContext)
      throws InterruptedException {
    long deadlineMs = System.currentTimeMillis() + 10_000L;
    while (executionContext.getNumTasks() > 0 && System.currentTimeMillis() < deadlineMs) {
      Thread.sleep(10L);
    }
    assertEquals(executionContext.getNumTasks(), 0);
  }
}
