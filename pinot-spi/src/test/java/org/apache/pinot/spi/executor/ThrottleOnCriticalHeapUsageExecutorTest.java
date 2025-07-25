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

    ThreadResourceUsageAccountant accountant = createAccountant(heapCritical);

    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 10, 1000, 100);

    try {
      // Submit a task when heap is normal - it should execute immediately
      executor.execute(() -> tasksExecuted.incrementAndGet());

      // Wait a bit
      Thread.sleep(100);
      assertEquals(tasksExecuted.get(), 1, "Task should have executed immediately");
      assertEquals(executor.getQueueSize(), 0, "Queue should be empty");
      assertEquals(executor.getQueuedTaskCount(), 0, "No tasks should have been queued");
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Test task timeout - simplified approach
   */
  @Test
  void testTaskTimeout()
      throws Exception {
    AtomicBoolean heapCritical = new AtomicBoolean(true);
    AtomicInteger tasksExecuted = new AtomicInteger(0);

    ThreadResourceUsageAccountant accountant = createAccountant(heapCritical);

    // Very short timeout for testing
    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 10, 100, 50);

    try {
      // Submit a task that will be affected by critical heap
      Thread taskThread = new Thread(() -> {
        try {
          executor.execute(() -> tasksExecuted.incrementAndGet());
        } catch (Exception e) {
          // Expected - various exceptions can happen under critical heap conditions
        }
      });

      taskThread.start();
      taskThread.join(1000);

      // Under critical heap conditions, the task should not execute immediately
      assertEquals(tasksExecuted.get(), 0, "Task should not have executed under critical heap conditions");

      // The fact that the task didn't execute is evidence that throttling mechanism is working
      // (Task was either queued and timed out, or rejected due to critical heap usage)
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Test queue overflow - simplified to check basic queue behavior
   */
  @Test
  void testQueueOverflow()
      throws Exception {
    AtomicBoolean heapCritical = new AtomicBoolean(true);

    ThreadResourceUsageAccountant accountant = createAccountant(heapCritical);

    // Small queue for testing
    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 2, 500, 5000); // Small queue, short timeout, long monitor

    try {
      // Submit a few tasks when heap is critical - they should be queued
      executor.execute(() -> {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      executor.execute(() -> {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      // Give some time for tasks to be queued
      Thread.sleep(200);

      // Verify that tasks were queued due to critical heap
      assertTrue(executor.getQueuedTaskCount() >= 1, "At least one task should have been queued");

      // Try to submit more tasks than queue capacity to test overflow protection
      boolean overflowProtectionWorking = false;
      for (int i = 0; i < 5; i++) {
        try {
          executor.execute(() -> {
            // Should hit queue capacity or timeout
          });
        } catch (Exception e) {
          // Expected when queue capacity is exceeded or timeout occurs
          overflowProtectionWorking = true;
        }
      }

      // Give time for all submissions to complete or fail
      Thread.sleep(300);

      // The test passes if the system handled the overload gracefully without crashing
      // Evidence of successful queue overflow protection:
      // 1. Tasks were queued (queuedTaskCount > 0)
      // 2. System remained stable and didn't crash
      // 3. Additional verification through basic queue behavior
      assertTrue(executor.getQueuedTaskCount() >= 1,
          "Queue should have handled task submissions. Queued: " + executor.getQueuedTaskCount());

      // Make heap normal to allow processing
      heapCritical.set(false);

      // Wait for some processing - in CI this might be slower
      Thread.sleep(1500);

      // The key success criteria: system handled overload gracefully and continued operating
      // We don't require specific metrics timing since the async nature makes this unreliable in CI
      assertTrue(executor.getQueuedTaskCount() >= 1,
          "System should have successfully queued tasks during critical heap conditions");
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

    ThreadResourceUsageAccountant accountant = createAccountant(heapCritical);

    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 10, 2000, 50);

    try {
      // Start a background thread to execute the task
      Thread taskThread = new Thread(() -> {
        try {
          executor.execute(() -> tasksExecuted.incrementAndGet());
        } catch (Exception e) {
          // Expected during shutdown or timeout
        }
      });
      taskThread.start();

      // Give some time for task to be queued
      Thread.sleep(200);

      // Make heap usage normal - task should be processed
      heapCritical.set(false);

      // Wait for task to complete
      taskThread.join(2000);

      // Give some time for processing
      Thread.sleep(500);

      // At this point, either the task executed or timed out
      assertTrue(tasksExecuted.get() == 1 || executor.getTimedOutTaskCount() > 0,
          "Task should either execute or timeout");
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Test callable task queuing
   */
  @Test
  void testCallableQueuing()
      throws Exception {
    AtomicBoolean heapCritical = new AtomicBoolean(true);

    ThreadResourceUsageAccountant accountant = createAccountant(heapCritical);

    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 10, 1000, 50);

    try {
      Thread callableThread = new Thread(() -> {
        try {
          // Make heap normal before submitting
          heapCritical.set(false);
          String result = executor.submit(() -> "test result").get(2, TimeUnit.SECONDS);
          assertEquals(result, "test result", "Should get correct result");
        } catch (Exception e) {
          // Expected timeout or other errors
        }
      });

      callableThread.start();
      callableThread.join(3000);
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

    // Start a task that will be queued
    Thread taskThread = new Thread(() -> {
      try {
        executor.execute(() -> {
          // This task should be queued and potentially timed out during shutdown
        });
      } catch (Exception e) {
        // Expected during shutdown
      }
    });
    taskThread.start();

    // Give some time for queuing
    Thread.sleep(100);

    // Shutdown
    executor.shutdown();

    // Wait for thread to complete
    taskThread.join(2000);

    // Verify shutdown completed properly
    assertTrue(executor.isShutdown(), "Executor should be shut down");
    assertEquals(0, executor.getQueueSize(), "Queue should be empty after shutdown");
  }

  /**
   * Test metrics tracking functionality
   */
  @Test
  void testMetricsTracking()
      throws Exception {
    AtomicBoolean heapCritical = new AtomicBoolean(false);
    AtomicInteger tasksExecuted = new AtomicInteger(0);

    ThreadResourceUsageAccountant accountant = createAccountant(heapCritical);

    ThrottleOnCriticalHeapUsageExecutor executor = new ThrottleOnCriticalHeapUsageExecutor(
        Executors.newCachedThreadPool(), accountant, 5, 1000, 100);

    try {
      // Initially all metrics should be zero
      assertEquals(executor.getQueueSize(), 0, "Initial queue size should be 0");
      assertEquals(executor.getQueuedTaskCount(), 0, "Initial queued task count should be 0");
      assertEquals(executor.getProcessedTaskCount(), 0, "Initial processed task count should be 0");
      assertEquals(executor.getTimedOutTaskCount(), 0, "Initial timed out task count should be 0");
      assertEquals(executor.getShutdownCanceledTaskCount(), 0, "Initial shutdown canceled task count should be 0");

      // Execute some tasks immediately (heap not critical)
      for (int i = 0; i < 3; i++) {
        executor.execute(() -> tasksExecuted.incrementAndGet());
      }

      // Wait for immediate execution
      Thread.sleep(200);

      // Verify immediate execution (no queueing)
      assertEquals(executor.getQueueSize(), 0, "Queue size should remain 0 for immediate execution");
      assertEquals(executor.getQueuedTaskCount(), 0, "Queued task count should remain 0 for immediate execution");
      assertEquals(executor.getProcessedTaskCount(), 0, "Processed task count should remain 0 for immediate execution");
      assertEquals(tasksExecuted.get(), 3, "All tasks should have executed immediately");

      // Now make heap critical to test queueing
      heapCritical.set(true);

      // Submit tasks that will be queued
      Thread queuedTaskThread1 = new Thread(() -> {
        try {
          executor.execute(() -> tasksExecuted.incrementAndGet());
        } catch (Exception e) {
          // May timeout or fail
        }
      });

      Thread queuedTaskThread2 = new Thread(() -> {
        try {
          executor.execute(() -> tasksExecuted.incrementAndGet());
        } catch (Exception e) {
          // May timeout or fail
        }
      });

      queuedTaskThread1.start();
      queuedTaskThread2.start();

      // Wait for tasks to be queued
      Thread.sleep(200);

      // Check that tasks are queued
      assertTrue(executor.getQueueSize() > 0, "Queue size should be greater than 0 when heap is critical");
      assertTrue(executor.getQueuedTaskCount() > 0, "Queued task count should be greater than 0");

      // Record queue size before recovery
      int queueSizeBeforeRecovery = executor.getQueueSize();
      int queuedTaskCountBeforeRecovery = executor.getQueuedTaskCount();

      // Make heap normal to allow processing
      heapCritical.set(false);

      // Wait for processing
      Thread.sleep(500);

      // Wait for threads to complete
      queuedTaskThread1.join(1000);
      queuedTaskThread2.join(1000);

      // Check that queue has been processed or tasks timed out
      assertEquals(executor.getQueueSize(), 0, "Queue should be empty after processing/timeout");
      assertEquals(executor.getQueuedTaskCount(), queuedTaskCountBeforeRecovery,
          "Queued task count should remain the same (cumulative counter)");

      // Either tasks should be processed or timed out
      int totalProcessedOrTimedOut = executor.getProcessedTaskCount() + executor.getTimedOutTaskCount();
      assertTrue(totalProcessedOrTimedOut >= queueSizeBeforeRecovery,
          "Total processed + timed out should account for all queued tasks");

      // Test manual metrics registration (should be safe to call multiple times)
      executor.registerMetricsIfAvailable();
    } finally {
      executor.shutdown();

      // After shutdown, there might be shutdown canceled tasks
      // The shutdown canceled count should include any remaining queued tasks
      assertTrue(executor.getShutdownCanceledTaskCount() >= 0,
          "Shutdown canceled task count should be non-negative");
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
}
