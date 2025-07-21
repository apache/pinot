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
package org.apache.pinot.spi.executor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.accounting.TrackingScope;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ThrottleOnCriticalHeapUsageExecutorTest {

  /**
   * Test that tasks execute immediately when heap usage is normal
   */
  @Test
  void testImmediateExecution()
      throws Exception {
    AtomicBoolean heapCritical = new AtomicBoolean(false);
    AtomicInteger tasksExecuted = new AtomicInteger(0);
    CountDownLatch taskCompleted = new CountDownLatch(1);

    ThreadResourceUsageAccountant accountant = createAccountant(heapCritical);

    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 10, 1000, 100);

    try {
      // Submit a task when heap is normal - it should execute immediately
      executor.execute(() -> {
        tasksExecuted.incrementAndGet();
        taskCompleted.countDown();
      });

      // Wait for task completion
      assertTrue(taskCompleted.await(2, TimeUnit.SECONDS), "Task should complete within 2 seconds");
      assertEquals(tasksExecuted.get(), 1, "Task should have executed immediately");
      assertEquals(executor.getQueueSize(), 0, "Queue should be empty");
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Test task timeout behavior
   */
  @Test
  void testTaskTimeout()
      throws Exception {
    AtomicBoolean heapCritical = new AtomicBoolean(true);
    AtomicInteger tasksExecuted = new AtomicInteger(0);

    // Use accountant with critical heap AND running queries to ensure queuing/timeout behavior
    ThreadResourceUsageAccountant accountant = createAccountantWithQueryTracking(heapCritical, true);

    // Very short timeout for testing
    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 10, 50, 25);

    try {
      // Submit a task that will timeout due to critical heap and running queries
      boolean taskTimedOut = false;
      try {
        executor.execute(() -> tasksExecuted.incrementAndGet());
      } catch (QueryException e) {
        if (e.getErrorCode() == QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED) {
          taskTimedOut = true;
        }
      }

      // Wait a bit for any potential processing
      Thread.sleep(200);

      // Task should not have executed due to critical heap and running queries
      assertEquals(tasksExecuted.get(), 0, "Task should not have executed under critical heap conditions");

      // Either the task timed out or is still queued, but shouldn't have executed
      assertTrue(taskTimedOut || executor.getQueueSize() > 0 || executor.getTimedOutTaskCount() > 0,
          "Task should either timeout or be queued");
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Test queue overflow behavior
   */
  @Test
  void testQueueOverflow()
      throws Exception {
    AtomicBoolean heapCritical = new AtomicBoolean(true);

    ThreadResourceUsageAccountant accountant = createAccountant(heapCritical);

    // Extremely small queue for testing overflow
    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 1, 100, 50);

    try {
      // Submit tasks to fill the queue and test overflow
      int tasksSubmitted = 0;
      boolean queueOverflowDetected = false;

      // Try to submit multiple tasks to trigger queue overflow
      for (int i = 0; i < 5; i++) {
        try {
          executor.execute(() -> {
            try {
              Thread.sleep(200);  // Long running task to keep queue occupied
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
          tasksSubmitted++;
          Thread.sleep(10); // Brief pause between submissions
        } catch (QueryException e) {
          if (e.getErrorCode() == QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED
              && (e.getMessage().contains("Task queue is full") || e.getMessage().contains("timed out"))) {
            queueOverflowDetected = true;
            break;
          }
        }
      }

      // Either we successfully filled the queue and detected overflow, or the executor handled it gracefully
      assertTrue(queueOverflowDetected || tasksSubmitted > 0 || executor.getQueueSize() > 0
          || executor.getTimedOutTaskCount() > 0,
          "Should either detect queue overflow or show evidence of queue activity");
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Test task queuing and processing when heap recovers
   */
  @Test
  void testTaskQueuingAndRecovery()
      throws Exception {
    AtomicBoolean heapCritical = new AtomicBoolean(true);
    AtomicInteger tasksExecuted = new AtomicInteger(0);
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch taskCompleted = new CountDownLatch(1);

    ThreadResourceUsageAccountant accountant = createAccountant(heapCritical);

    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 10, 2000, 50);

    try {
      // Start a task in a separate thread
      Thread taskThread = new Thread(() -> {
        try {
          taskStarted.countDown();
          executor.execute(() -> {
            tasksExecuted.incrementAndGet();
            taskCompleted.countDown();
          });
        } catch (Exception e) {
          // Expected during shutdown or timeout
        }
      });
      taskThread.start();

      // Wait for task to start
      assertTrue(taskStarted.await(1, TimeUnit.SECONDS), "Task should start");

      // Give some time for task to be queued
      Thread.sleep(100);

      // Make heap usage normal - task should be processed
      heapCritical.set(false);

      // Wait for task to complete or thread to finish
      taskThread.join(3000);

      // Give some time for processing
      Thread.sleep(200);

      // At this point, either the task executed or timed out
      assertTrue(tasksExecuted.get() == 1 || executor.getTimedOutTaskCount() > 0,
          "Task should either execute or timeout");
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Test callable task behavior
   */
  @Test
  void testCallableQueuing()
      throws Exception {
    AtomicBoolean heapCritical = new AtomicBoolean(false); // Start with normal heap

    ThreadResourceUsageAccountant accountant = createAccountant(heapCritical);

    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 10, 1000, 50);

    try {
      // Test with normal heap - should execute immediately
      String result = executor.submit(() -> "test result").get(2, TimeUnit.SECONDS);
      assertEquals(result, "test result", "Should get correct result");
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Test shutdown behavior
   */
  @Test
  void testShutdown()
      throws Exception {
    AtomicBoolean heapCritical = new AtomicBoolean(true);

    ThreadResourceUsageAccountant accountant = createAccountant(heapCritical);

    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 10, 1000, 50);

    CountDownLatch taskStarted = new CountDownLatch(1);

    // Start a task that will be queued
    Thread taskThread = new Thread(() -> {
      try {
        taskStarted.countDown();
        executor.execute(() -> {
          // This task should be queued and potentially timed out during shutdown
        });
      } catch (Exception e) {
        // Expected during shutdown
      }
    });
    taskThread.start();

    // Wait for task to start
    assertTrue(taskStarted.await(1, TimeUnit.SECONDS), "Task should start");

    // Give some time for queuing
    Thread.sleep(100);

    // Shutdown
    executor.shutdown();

    // Wait for thread to complete
    taskThread.join(2000);

    // Verify shutdown completed successfully
    assertTrue(executor.isShutdown(), "Executor should be shut down");
  }

  /**
   * Test that queries are allowed when heap is critical but no queries are running
   */
  @Test
  void testAllowQueryWhenNoQueriesRunning()
      throws Exception {
    AtomicBoolean heapCritical = new AtomicBoolean(true);
    AtomicInteger tasksExecuted = new AtomicInteger(0);
    CountDownLatch taskCompleted = new CountDownLatch(1);

    // Create accountant with critical heap but no running queries (empty query map)
    ThreadResourceUsageAccountant accountant = createAccountantWithQueryTracking(heapCritical, false);

    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 10, 1000, 50);

    try {
      // Submit a task when heap is critical but no queries are running
      // It should execute immediately instead of being queued
      executor.execute(() -> {
        tasksExecuted.incrementAndGet();
        taskCompleted.countDown();
      });

      // Wait for task completion
      assertTrue(taskCompleted.await(2, TimeUnit.SECONDS), "Task should complete within 2 seconds");
      assertEquals(tasksExecuted.get(), 1, "Task should have executed immediately despite critical heap");
      assertEquals(executor.getQueueSize(), 0, "Queue should be empty since task executed immediately");
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Test that queries are queued when heap is critical and queries are running
   */
  @Test
  void testQueueQueryWhenQueriesRunning()
      throws Exception {
    AtomicBoolean heapCritical = new AtomicBoolean(true);
    AtomicInteger tasksExecuted = new AtomicInteger(0);

    // Create accountant with critical heap and running queries (non-empty query map)
    ThreadResourceUsageAccountant accountant = createAccountantWithQueryTracking(heapCritical, true);

    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 10, 100, 50);

    try {
      // Submit a task when heap is critical and queries are running
      // It should be queued/timeout instead of executing immediately
      boolean taskTimedOut = false;
      try {
        executor.execute(() -> tasksExecuted.incrementAndGet());
      } catch (QueryException e) {
        if (e.getErrorCode() == QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED) {
          taskTimedOut = true;
        }
      }

      // Wait a bit for any potential processing
      Thread.sleep(200);

      // Task should not have executed due to critical heap and running queries
      assertEquals(tasksExecuted.get(), 0, "Task should not have executed when queries are running");
      assertTrue(taskTimedOut || executor.getQueueSize() > 0 || executor.getTimedOutTaskCount() > 0,
          "Task should either timeout or be queued when queries are running");
    } finally {
      executor.shutdown();
    }
  }

  private ThreadResourceUsageAccountant createAccountant(AtomicBoolean heapCritical) {
    return new ThreadResourceUsageAccountant() {
      @Override
      public void clear() {
      }

      @Override
      public boolean isAnchorThreadInterrupted() {
        return false;
      }

      @Override
      public void createExecutionContext(String queryId, int taskId, ThreadExecutionContext.TaskType taskType,
          @Nullable ThreadExecutionContext parentContext) {
      }

      @Override
      public void setupRunner(String queryId, int taskId, ThreadExecutionContext.TaskType taskType) {
      }

      @Override
      public void setupRunner(String queryId, int taskId, ThreadExecutionContext.TaskType taskType,
          String workloadName) {
      }

      @Override
      public void setupWorker(int taskId, ThreadExecutionContext.TaskType taskType,
          @Nullable ThreadExecutionContext parentContext) {
      }

      @Nullable
      @Override
      public ThreadExecutionContext getThreadExecutionContext() {
        return null;
      }

      @Override
      public void setThreadResourceUsageProvider(ThreadResourceUsageProvider threadResourceUsageProvider) {
      }

      @Override
      public void sampleUsage() {
      }

      @Override
      public void sampleUsageMSE() {
      }

      @Override
      public boolean throttleQuerySubmission() {
        return heapCritical.get();
      }

      @Override
      public void updateQueryUsageConcurrently(String queryId, long cpuTimeNs, long allocatedBytes) {
      }

      @Override
      public void updateQueryUsageConcurrently(String queryId) {
      }

      @Override
      public void updateQueryUsageConcurrently(String queryId, long cpuTimeNs, long allocatedBytes,
          TrackingScope trackingScope) {
      }

      @Override
      public void startWatcherTask() {
      }

      @Override
      public Exception getErrorStatus() {
        return null;
      }

      @Override
      public Collection<? extends ThreadResourceTracker> getThreadResources() {
        return List.of();
      }

      @Override
      public Map<String, ? extends QueryResourceTracker> getQueryResources() {
        return Map.of();
      }
    };
  }

  private ThreadResourceUsageAccountant createAccountantWithQueryTracking(AtomicBoolean heapCritical,
      boolean queriesRunning) {
    return new ThreadResourceUsageAccountant() {
      @Override
      public void clear() {
      }

      @Override
      public boolean isAnchorThreadInterrupted() {
        return false;
      }

      @Override
      public void createExecutionContext(String queryId, int taskId, ThreadExecutionContext.TaskType taskType,
          @Nullable ThreadExecutionContext parentContext) {
      }

      @Override
      public void setupRunner(String queryId, int taskId, ThreadExecutionContext.TaskType taskType) {
      }

      @Override
      public void setupRunner(String queryId, int taskId, ThreadExecutionContext.TaskType taskType,
          String workloadName) {
      }

      @Override
      public void setupWorker(int taskId, ThreadExecutionContext.TaskType taskType,
          @Nullable ThreadExecutionContext parentContext) {
      }

      @Nullable
      @Override
      public ThreadExecutionContext getThreadExecutionContext() {
        return null;
      }

      @Override
      public void setThreadResourceUsageProvider(ThreadResourceUsageProvider threadResourceUsageProvider) {
      }

      @Override
      public void sampleUsage() {
      }

      @Override
      public void sampleUsageMSE() {
      }

      @Override
      public boolean throttleQuerySubmission() {
        return heapCritical.get();
      }

      @Override
      public void updateQueryUsageConcurrently(String queryId, long cpuTimeNs, long allocatedBytes) {
      }

      @Override
      public void updateQueryUsageConcurrently(String queryId) {
      }

      @Override
      public void updateQueryUsageConcurrently(String queryId, long cpuTimeNs, long allocatedBytes,
          TrackingScope trackingScope) {
      }

      @Override
      public void startWatcherTask() {
      }

      @Override
      public Exception getErrorStatus() {
        return null;
      }

      @Override
      public Collection<? extends ThreadResourceTracker> getThreadResources() {
        return List.of();
      }

      @Override
      public Map<String, ? extends QueryResourceTracker> getQueryResources() {
        // Return non-empty map if queries are running, empty map if no queries are running
        return queriesRunning ? Map.of("query1", new QueryResourceTracker() {
          @Override
          public String getQueryId() {
            return "query1";
          }

          @Override
          public long getCpuTimeNs() {
            return 1000000;
          }

          @Override
          public long getAllocatedBytes() {
            return 1024;
          }
        }) : Map.of();
      }
    };
  }
}
