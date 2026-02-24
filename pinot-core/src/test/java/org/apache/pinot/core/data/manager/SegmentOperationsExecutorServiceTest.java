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
package org.apache.pinot.core.data.manager;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SegmentOperationsExecutorServiceTest {
  private ExecutorService _executorService;

  @AfterMethod
  public void tearDown() {
    if (_executorService != null) {
      _executorService.shutdown();
    }
  }

  @Test
  public void testRefreshOrReloadContextForCallable() throws Exception {
    // Test that REFRESH_OR_RELOAD tasks execute with proper context
    ExecutorService delegate = Executors.newFixedThreadPool(2);
    _executorService = new SegmentOperationsExecutorService(delegate,
        SegmentOperationsTaskType.REFRESH_OR_RELOAD, "testTable_OFFLINE");

    AtomicReference<SegmentOperationsTaskType> capturedTaskType = new AtomicReference<>();
    AtomicReference<String> capturedTableName = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    Callable<String> task = () -> {
      capturedTaskType.set(SegmentOperationsTaskContext.getTaskType());
      capturedTableName.set(SegmentOperationsTaskContext.getTableNameWithType());
      latch.countDown();
      return "success";
    };

    String result = _executorService.submit(task).get(5, TimeUnit.SECONDS);
    assertTrue(latch.await(5, TimeUnit.SECONDS));

    assertEquals(result, "success");
    assertEquals(capturedTaskType.get(), SegmentOperationsTaskType.REFRESH_OR_RELOAD,
        "Task type should be REFRESH_OR_RELOAD");
    assertEquals(capturedTableName.get(), "testTable_OFFLINE",
        "Table name should match");

    // Verify context is cleared after execution
    assertNull(SegmentOperationsTaskContext.getTaskType());
    assertNull(SegmentOperationsTaskContext.getTableNameWithType());
  }

  @Test
  public void testRefreshOrReloadContextForRunnable() throws Exception {
    // Test that REFRESH_OR_RELOAD Runnable tasks execute with proper context
    ExecutorService delegate = Executors.newFixedThreadPool(2);
    _executorService = new SegmentOperationsExecutorService(delegate,
        SegmentOperationsTaskType.REFRESH_OR_RELOAD, "testTable_REALTIME");

    AtomicReference<SegmentOperationsTaskType> capturedTaskType = new AtomicReference<>();
    AtomicReference<String> capturedTableName = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    Runnable task = () -> {
      capturedTaskType.set(SegmentOperationsTaskContext.getTaskType());
      capturedTableName.set(SegmentOperationsTaskContext.getTableNameWithType());
      latch.countDown();
    };

    _executorService.submit(task).get(5, TimeUnit.SECONDS);
    assertTrue(latch.await(5, TimeUnit.SECONDS));

    assertEquals(capturedTaskType.get(), SegmentOperationsTaskType.REFRESH_OR_RELOAD);
    assertEquals(capturedTableName.get(), "testTable_REALTIME");

    // Verify context is cleared
    assertNull(SegmentOperationsTaskContext.getTaskType());
    assertNull(SegmentOperationsTaskContext.getTableNameWithType());
  }

  @Test
  public void testPreloadContext() throws Exception {
    // Test that PRELOAD tasks execute with proper context
    ExecutorService delegate = Executors.newFixedThreadPool(2);
    _executorService = new SegmentOperationsExecutorService(delegate,
        SegmentOperationsTaskType.PRELOAD, "preloadTable_OFFLINE");

    AtomicReference<SegmentOperationsTaskType> capturedTaskType = new AtomicReference<>();
    AtomicReference<String> capturedTableName = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    Callable<String> task = () -> {
      capturedTaskType.set(SegmentOperationsTaskContext.getTaskType());
      capturedTableName.set(SegmentOperationsTaskContext.getTableNameWithType());
      latch.countDown();
      return "preload-success";
    };

    String result = _executorService.submit(task).get(5, TimeUnit.SECONDS);
    assertTrue(latch.await(5, TimeUnit.SECONDS));

    assertEquals(result, "preload-success");
    assertEquals(capturedTaskType.get(), SegmentOperationsTaskType.PRELOAD,
        "Task type should be PRELOAD");
    assertEquals(capturedTableName.get(), "preloadTable_OFFLINE",
        "Table name should match");

    // Verify context cleanup
    assertNull(SegmentOperationsTaskContext.getTaskType());
    assertNull(SegmentOperationsTaskContext.getTableNameWithType());
  }

  @Test
  public void testContextIsolationBetweenDifferentTaskTypes() throws Exception {
    // Test that different executor services maintain separate contexts
    ExecutorService delegate1 = Executors.newFixedThreadPool(2);
    ExecutorService delegate2 = Executors.newFixedThreadPool(2);

    ExecutorService refreshExecutor = new SegmentOperationsExecutorService(delegate1,
        SegmentOperationsTaskType.REFRESH_OR_RELOAD, "table1_OFFLINE");
    ExecutorService preloadExecutor = new SegmentOperationsExecutorService(delegate2,
        SegmentOperationsTaskType.PRELOAD, "table2_OFFLINE");

    CountDownLatch startLatch = new CountDownLatch(2);
    CountDownLatch doneLatch = new CountDownLatch(2);
    AtomicReference<Throwable> error = new AtomicReference<>();

    // Submit refresh task
    refreshExecutor.submit(() -> {
      try {
        startLatch.countDown();
        startLatch.await(5, TimeUnit.SECONDS);

        SegmentOperationsTaskType taskType = SegmentOperationsTaskContext.getTaskType();
        String tableName = SegmentOperationsTaskContext.getTableNameWithType();

        if (taskType != SegmentOperationsTaskType.REFRESH_OR_RELOAD) {
          error.set(new AssertionError("Expected REFRESH_OR_RELOAD but got: " + taskType));
        }
        if (!"table1_OFFLINE".equals(tableName)) {
          error.set(new AssertionError("Expected table1_OFFLINE but got: " + tableName));
        }

        Thread.sleep(10);

        // Verify context hasn't changed
        if (SegmentOperationsTaskContext.getTaskType() != SegmentOperationsTaskType.REFRESH_OR_RELOAD) {
          error.set(new AssertionError("Context changed during execution"));
        }
      } catch (Throwable e) {
        error.set(e);
      } finally {
        doneLatch.countDown();
      }
      return null;
    });

    // Submit preload task
    preloadExecutor.submit(() -> {
      try {
        startLatch.countDown();
        startLatch.await(5, TimeUnit.SECONDS);

        SegmentOperationsTaskType taskType = SegmentOperationsTaskContext.getTaskType();
        String tableName = SegmentOperationsTaskContext.getTableNameWithType();

        if (taskType != SegmentOperationsTaskType.PRELOAD) {
          error.set(new AssertionError("Expected PRELOAD but got: " + taskType));
        }
        if (!"table2_OFFLINE".equals(tableName)) {
          error.set(new AssertionError("Expected table2_OFFLINE but got: " + tableName));
        }

        Thread.sleep(10);

        // Verify context hasn't changed
        if (SegmentOperationsTaskContext.getTaskType() != SegmentOperationsTaskType.PRELOAD) {
          error.set(new AssertionError("Context changed during execution"));
        }
      } catch (Throwable e) {
        error.set(e);
      } finally {
        doneLatch.countDown();
      }
      return null;
    });

    assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "Both tasks should complete");
    assertNull(error.get(), "Contexts should be properly isolated");

    refreshExecutor.shutdown();
    preloadExecutor.shutdown();
  }

  @Test
  public void testContextClearedAfterException() throws Exception {
    // Test that context is cleared even when task throws exception
    ExecutorService delegate = Executors.newFixedThreadPool(2);
    _executorService = new SegmentOperationsExecutorService(delegate,
        SegmentOperationsTaskType.REFRESH_OR_RELOAD, "testTable_OFFLINE");

    AtomicReference<SegmentOperationsTaskType> capturedBeforeException = new AtomicReference<>();
    AtomicReference<String> capturedTableBeforeException = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    Callable<String> task = () -> {
      capturedBeforeException.set(SegmentOperationsTaskContext.getTaskType());
      capturedTableBeforeException.set(SegmentOperationsTaskContext.getTableNameWithType());
      latch.countDown();
      throw new RuntimeException("Test exception");
    };

    try {
      _executorService.submit(task).get(5, TimeUnit.SECONDS);
      fail("Should have thrown exception");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertEquals(e.getCause().getMessage(), "Test exception");
    }

    assertTrue(latch.await(5, TimeUnit.SECONDS));

    // Verify context was set before exception
    assertEquals(capturedBeforeException.get(), SegmentOperationsTaskType.REFRESH_OR_RELOAD);
    assertEquals(capturedTableBeforeException.get(), "testTable_OFFLINE");

    // Submit another task to verify context was cleaned up
    AtomicReference<SegmentOperationsTaskType> capturedInNextTask = new AtomicReference<>();
    AtomicReference<String> capturedTableInNextTask = new AtomicReference<>();

    Callable<String> nextTask = () -> {
      capturedInNextTask.set(SegmentOperationsTaskContext.getTaskType());
      capturedTableInNextTask.set(SegmentOperationsTaskContext.getTableNameWithType());
      return "success";
    };

    String result = _executorService.submit(nextTask).get(5, TimeUnit.SECONDS);
    assertEquals(result, "success");

    // Verify fresh context
    assertEquals(capturedInNextTask.get(), SegmentOperationsTaskType.REFRESH_OR_RELOAD);
    assertEquals(capturedTableInNextTask.get(), "testTable_OFFLINE");

    // Verify cleanup
    assertNull(SegmentOperationsTaskContext.getTaskType());
    assertNull(SegmentOperationsTaskContext.getTableNameWithType());
  }
}
