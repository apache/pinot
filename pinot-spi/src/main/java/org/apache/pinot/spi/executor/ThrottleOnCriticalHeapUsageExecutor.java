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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An Executor that queues tasks when the heap usage is critical instead of rejecting them.
 * Heap Usage level is obtained from {@link ThreadResourceUsageAccountant#throttleQuerySubmission()}.
 *
 * Features:
 * - Tasks are queued when heap usage is critical
 * - Queued tasks are processed when heap usage drops below critical level
 * - Configurable queue size and timeout
 * - Background monitoring of heap usage to process queued tasks
 */
public class ThrottleOnCriticalHeapUsageExecutor extends DecoratorExecutorService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThrottleOnCriticalHeapUsageExecutor.class);

  // Default configuration values
  private static final int DEFAULT_QUEUE_SIZE = 1000;
  private static final long DEFAULT_QUEUE_TIMEOUT_MS = 30000; // 30 seconds
  private static final long DEFAULT_MONITOR_INTERVAL_MS = 1000; // 1 second

  private final ThreadResourceUsageAccountant _threadResourceUsageAccountant;
  private final BlockingQueue<QueuedTask> _taskQueue;
  private final int _maxQueueSize;
  private final long _queueTimeoutMs;
  private final ScheduledExecutorService _monitorExecutor;
  private final AtomicBoolean _isShutdown = new AtomicBoolean(false);
  private final AtomicInteger _queuedTaskCount = new AtomicInteger(0);
  private final AtomicInteger _processedTaskCount = new AtomicInteger(0);
  private final AtomicInteger _timedOutTaskCount = new AtomicInteger(0);

  public ThrottleOnCriticalHeapUsageExecutor(ExecutorService executorService,
      ThreadResourceUsageAccountant threadResourceUsageAccountant) {
    this(executorService, threadResourceUsageAccountant, DEFAULT_QUEUE_SIZE,
        DEFAULT_QUEUE_TIMEOUT_MS, DEFAULT_MONITOR_INTERVAL_MS);
  }

  public ThrottleOnCriticalHeapUsageExecutor(ExecutorService executorService,
      ThreadResourceUsageAccountant threadResourceUsageAccountant,
      int maxQueueSize, long queueTimeoutMs, long monitorIntervalMs) {
    super(executorService);
    _threadResourceUsageAccountant = threadResourceUsageAccountant;
    _maxQueueSize = maxQueueSize;
    _queueTimeoutMs = queueTimeoutMs;
    _taskQueue = new LinkedBlockingQueue<>(maxQueueSize);

    // Create a single-threaded scheduler for monitoring heap usage
    _monitorExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "throttle-heap-monitor");
      t.setDaemon(true);
      return t;
    });

    // Start the monitoring task
    _monitorExecutor.scheduleWithFixedDelay(this::processQueuedTasks,
        monitorIntervalMs, monitorIntervalMs, TimeUnit.MILLISECONDS);

    LOGGER.info(
        "ThrottleOnCriticalHeapUsageExecutor initialized with queue size: {}, timeout: {}ms, monitor interval: {}ms",
        maxQueueSize, queueTimeoutMs, monitorIntervalMs);
  }

  /**
   * Check if a task should be queued due to critical heap usage
   * @return true if the task should be queued, false if it can be executed immediately
   */
  protected boolean shouldQueueTask() {
    return _threadResourceUsageAccountant.throttleQuerySubmission();
  }

  /**
   * Process queued tasks when heap usage is below critical level
   */
  private void processQueuedTasks() {
    if (_isShutdown.get()) {
      return;
    }

    try {
      // Process tasks while heap usage is not critical and queue is not empty
      while (!shouldQueueTask() && !_taskQueue.isEmpty()) {
        QueuedTask queuedTask = _taskQueue.poll();
        if (queuedTask != null) {
          long queueTime = System.currentTimeMillis() - queuedTask.getQueueTime();

          if (queueTime > _queueTimeoutMs) {
            // Task has timed out in queue
            queuedTask.timeout();
            _timedOutTaskCount.incrementAndGet();
            LOGGER.warn("Task timed out after {}ms in queue", queueTime);
          } else {
            // Submit the task for execution
            try {
              queuedTask.execute();
              _processedTaskCount.incrementAndGet();
              LOGGER.debug("Processed queued task after {}ms in queue", queueTime);
            } catch (Exception e) {
              LOGGER.error("Error executing queued task", e);
              queuedTask.fail(e);
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error in processQueuedTasks", e);
    }
  }

  @Override
  protected <T> Callable<T> decorate(Callable<T> task) {
    return () -> {
      if (shouldQueueTask()) {
        // Queue the task if heap usage is critical
        return queueCallableTask(task);
      } else {
        // Execute immediately if heap usage is normal
        return task.call();
      }
    };
  }

  @Override
  protected Runnable decorate(Runnable task) {
    return () -> {
      if (shouldQueueTask()) {
        // Queue the task if heap usage is critical
        queueRunnableTask(task);
      } else {
        // Execute immediately if heap usage is normal
        task.run();
      }
    };
  }

  /**
   * Queue a callable task and wait for its execution
   */
  private <T> T queueCallableTask(Callable<T> task)
      throws Exception {
    QueuedCallableTask<T> queuedTask = new QueuedCallableTask<>(task);

    if (!_taskQueue.offer(queuedTask)) {
      // Queue is full
      throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
          "Task queue is full (size: " + _maxQueueSize + ") due to high heap usage.");
    }

    _queuedTaskCount.incrementAndGet();
    LOGGER.debug("Queued callable task, queue size: {}", _taskQueue.size());

    // Wait for the task to complete or timeout
    return queuedTask.get(_queueTimeoutMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Queue a runnable task and wait for its execution
   */
  private void queueRunnableTask(Runnable task) {
    QueuedRunnableTask queuedTask = new QueuedRunnableTask(task);

    if (!_taskQueue.offer(queuedTask)) {
      // Queue is full
      throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
          "Task queue is full (size: " + _maxQueueSize + ") due to high heap usage.");
    }

    _queuedTaskCount.incrementAndGet();
    LOGGER.debug("Queued runnable task, queue size: {}", _taskQueue.size());

    try {
      // Wait for the task to complete or timeout
      queuedTask.get(_queueTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      } else {
        throw new RuntimeException("Error executing queued task", e);
      }
    }
  }

  @Override
  public void shutdown() {
    _isShutdown.set(true);
    _monitorExecutor.shutdown();

    // Process any remaining tasks in the queue
    while (!_taskQueue.isEmpty()) {
      QueuedTask task = _taskQueue.poll();
      if (task != null) {
        task.timeout();
      }
    }

    super.shutdown();

    LOGGER.info("ThrottleOnCriticalHeapUsageExecutor shutdown. Stats - Queued: {}, Processed: {}, Timed out: {}",
        _queuedTaskCount.get(), _processedTaskCount.get(), _timedOutTaskCount.get());
  }

  /**
   * Get current queue size
   */
  public int getQueueSize() {
    return _taskQueue.size();
  }

  /**
   * Get total number of tasks queued
   */
  public int getQueuedTaskCount() {
    return _queuedTaskCount.get();
  }

  /**
   * Get total number of tasks processed from queue
   */
  public int getProcessedTaskCount() {
    return _processedTaskCount.get();
  }

  /**
   * Get total number of tasks that timed out in queue
   */
  public int getTimedOutTaskCount() {
    return _timedOutTaskCount.get();
  }

  /**
   * Base class for queued tasks
   */
  private abstract static class QueuedTask {
    private final long _queueTime;

    protected QueuedTask() {
      _queueTime = System.currentTimeMillis();
    }

    public long getQueueTime() {
      return _queueTime;
    }

    public abstract void execute()
        throws Exception;

    public abstract void timeout();

    public abstract void fail(Exception e);
  }

  /**
   * Wrapper for callable tasks that can be queued
   */
  private class QueuedCallableTask<T> extends QueuedTask {
    private final Callable<T> _task;
    private final Future<T> _future;
    private volatile T _result;
    private volatile Exception _exception;
    private volatile boolean _completed = false;
    private final Object _lock = new Object();

    public QueuedCallableTask(Callable<T> task) {
      super();
      _task = task;
      _future = null;
    }

    @Override
    public void execute()
        throws Exception {
      try {
        T result = _task.call();
        synchronized (_lock) {
          _result = result;
          _completed = true;
          _lock.notifyAll();
        }
      } catch (Exception e) {
        fail(e);
        throw e;
      }
    }

    @Override
    public void timeout() {
      synchronized (_lock) {
        _exception = QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
            "Task timed out in queue after " + _queueTimeoutMs + "ms due to high heap usage.");
        _completed = true;
        _lock.notifyAll();
      }
    }

    @Override
    public void fail(Exception e) {
      synchronized (_lock) {
        _exception = e;
        _completed = true;
        _lock.notifyAll();
      }
    }

    public T get(long timeout, TimeUnit unit)
        throws Exception {
      long timeoutMs = unit.toMillis(timeout);
      long startTime = System.currentTimeMillis();

      synchronized (_lock) {
        while (!_completed && (System.currentTimeMillis() - startTime) < timeoutMs) {
          try {
            _lock.wait(Math.max(1, timeoutMs - (System.currentTimeMillis() - startTime)));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for task completion", e);
          }
        }

        if (!_completed) {
          throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
              "Task timed out after " + timeoutMs + "ms waiting in queue.");
        }

        if (_exception != null) {
          throw _exception;
        }

        return _result;
      }
    }
  }

  /**
   * Wrapper for runnable tasks that can be queued
   */
  private class QueuedRunnableTask extends QueuedTask {
    private final Runnable _task;
    private volatile Exception _exception;
    private volatile boolean _completed = false;
    private final Object _lock = new Object();

    public QueuedRunnableTask(Runnable task) {
      super();
      _task = task;
    }

    @Override
    public void execute()
        throws Exception {
      try {
        _task.run();
        synchronized (_lock) {
          _completed = true;
          _lock.notifyAll();
        }
      } catch (Exception e) {
        fail(e);
        throw e;
      }
    }

    @Override
    public void timeout() {
      synchronized (_lock) {
        _exception = QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
            "Task timed out in queue after " + _queueTimeoutMs + "ms due to high heap usage.");
        _completed = true;
        _lock.notifyAll();
      }
    }

    @Override
    public void fail(Exception e) {
      synchronized (_lock) {
        _exception = e;
        _completed = true;
        _lock.notifyAll();
      }
    }

    public void get(long timeout, TimeUnit unit)
        throws Exception {
      long timeoutMs = unit.toMillis(timeout);
      long startTime = System.currentTimeMillis();

      synchronized (_lock) {
        while (!_completed && (System.currentTimeMillis() - startTime) < timeoutMs) {
          try {
            _lock.wait(Math.max(1, timeoutMs - (System.currentTimeMillis() - startTime)));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for task completion", e);
          }
        }

        if (!_completed) {
          throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
              "Task timed out after " + timeoutMs + "ms waiting in queue.");
        }

        if (_exception != null) {
          throw _exception;
        }
      }
    }
  }
}
