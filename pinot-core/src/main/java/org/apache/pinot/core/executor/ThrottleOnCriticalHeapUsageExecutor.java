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
package org.apache.pinot.core.executor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.executor.DecoratorExecutorService;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An Executor that queues tasks when the heap usage is critical instead of rejecting them.
 * Heap Usage level is obtained from {@link ThreadAccountant#throttleQuerySubmission()}.
 *
 * Features:
 * - Tasks are queued when heap usage is critical
 * - Queued tasks are processed when heap usage drops below critical level
 * - Configurable queue size and timeout (global default or per-task)
 * - Background monitoring of heap usage to process queued tasks
 */
public class ThrottleOnCriticalHeapUsageExecutor extends DecoratorExecutorService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThrottleOnCriticalHeapUsageExecutor.class);

  // Default configuration values
  // Defaults are kept near config in CommonConstants; no local defaults needed here
  private static final long LOG_THROTTLE_INTERVAL_MS = 30000; // Log queue status every 30 seconds

  private final BlockingQueue<QueuedTask<?>> _taskQueue;
  private final int _maxQueueSize;
  private final long _defaultQueueTimeoutMs;
  private final ScheduledExecutorService _monitorExecutor;
  private final ServerMetrics _serverMetrics;
  private final AtomicBoolean _isShutdown = new AtomicBoolean(false);
  private long _lastQueueStatusLogTime = 0;

  public ThrottleOnCriticalHeapUsageExecutor(ExecutorService executorService,
      int maxQueueSize, long defaultQueueTimeoutMs, long monitorIntervalMs) {
    super(executorService);
    _maxQueueSize = maxQueueSize;
    _defaultQueueTimeoutMs = defaultQueueTimeoutMs;
    // If maxQueueSize <= 0, use an unbounded queue; otherwise, use bounded queue
    _taskQueue = (maxQueueSize <= 0) ? new LinkedBlockingQueue<>() : new LinkedBlockingQueue<>(maxQueueSize);

    // Create a single-threaded scheduler for monitoring heap usage
    _monitorExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "throttle-heap-monitor");
      t.setDaemon(true);
      return t;
    });

    // Start the monitoring task
    _monitorExecutor.scheduleWithFixedDelay(this::processQueuedTasks,
        monitorIntervalMs, monitorIntervalMs, TimeUnit.MILLISECONDS);

    // Cache metrics and register gauge for queue activity tracking
    _serverMetrics = ServerMetrics.get();
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.THROTTLE_EXECUTOR_QUEUE_SIZE, () -> (long) _taskQueue.size());

    LOGGER.info(
        "ThrottleOnCriticalHeapUsageExecutor initialized with queue size: {}, default timeout: {}ms, "
            + "monitor interval: {}ms (maxQueueSize<=0 => unbounded)", maxQueueSize, defaultQueueTimeoutMs,
        monitorIntervalMs);
  }

  /**
   * Check if a task should be queued due to critical heap usage
   * @return true if the task should be queued, false if it can be executed immediately
   */
  protected boolean shouldQueueTask() {
    QueryThreadContext ctx = QueryThreadContext.getIfAvailable();
    if (ctx == null) {
      // No context available on this thread (e.g., monitor thread); do not throttle
      return false;
    }
    return ctx.getAccountant().throttleQuerySubmission();
  }

  /**
   * Process queued tasks when heap usage is below critical level
   */
  private void processQueuedTasks() {
    if (_isShutdown.get()) {
      return;
    }

    try {
      int initialQueueSize = _taskQueue.size();
      long currentTime = System.currentTimeMillis();

      // Log queue size for monitoring if there are queued tasks (throttled to prevent log flooding)
      if (initialQueueSize > 0 && (currentTime - _lastQueueStatusLogTime) > LOG_THROTTLE_INTERVAL_MS) {
        LOGGER.info("Processing queued tasks. Current queue size: {}", initialQueueSize);
        _lastQueueStatusLogTime = currentTime;
        // Metrics are exported via ServerMetrics global gauges registered by callers
      }

      // Process tasks while the queue head is allowed to run (i.e., its submitting accountant is not throttling)
      while (!_taskQueue.isEmpty()) {
        QueuedTask<?> headTask = _taskQueue.peek();
        if (headTask == null) {
          break;
        }

        if (headTask.isThrottled()) {
          // Still throttled for the submitting context; stop processing for now
          break;
        }

        // Safe to process this task
        QueuedTask<?> polledTask = _taskQueue.poll();
        if (polledTask != null) {
          long queueTime = System.currentTimeMillis() - polledTask.getQueueTime();

          if (queueTime > polledTask.getTimeoutMs()) {
            // Task has timed out in queue
            polledTask.timeout();
            _serverMetrics.addMeteredGlobalValue(ServerMeter.THROTTLE_EXECUTOR_TIMED_OUT_TASKS, 1);
            LOGGER.warn("Task timed out after {}ms in queue (timeout: {}ms)", queueTime, polledTask.getTimeoutMs());
          } else {
            // Submit the task for execution on the underlying executor (avoid double decoration)
            QueuedTask<?> submittedTask = polledTask;
            _executorService.execute(new FromQueueRunnable(() -> {
              try {
                submittedTask.execute();
                _serverMetrics.addMeteredGlobalValue(
                    ServerMeter.THROTTLE_EXECUTOR_PROCESSED_TASKS, 1);
                LOGGER.debug("Processed queued task after {}ms in queue", queueTime);
              } catch (Exception e) {
                LOGGER.error("Error executing queued task", e);
                submittedTask.fail(e);
              }
            }));
          }
        }
      }

      // Log completion only for significant processing (5+ tasks) to avoid log spam
      int finalQueueSize = _taskQueue.size();
      if (initialQueueSize > 0 && initialQueueSize != finalQueueSize) {
        int processedThisCycle = initialQueueSize - finalQueueSize;
        if (processedThisCycle >= 5) {
          LOGGER.info("Completed processing cycle. Processed {} tasks, remaining queue size: {}",
              processedThisCycle, finalQueueSize);
        } else {
          LOGGER.debug("Completed processing cycle. Processed {} tasks, remaining queue size: {}",
              processedThisCycle, finalQueueSize);
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
        long timeoutMs = computePerTaskTimeoutMs(_defaultQueueTimeoutMs);
        return queueCallableTask(task, timeoutMs);
      } else {
        // Execute immediately if heap usage is normal
        return task.call();
      }
    };
  }

  @Override
  protected Runnable decorate(Runnable task) {
    // If the task comes from this executor's internal queue, avoid re-applying queue logic
    if (task instanceof FromQueueRunnable) {
      return ((FromQueueRunnable) task).getDelegate();
    }

    // IMPORTANT: Decide throttling at submission time so that queuing is visible immediately to callers.
    // This ensures tests (and metrics) observe the full number of queued tasks, not just the ones picked by
    // the underlying thread pool yet.
    if (shouldQueueTask()) {
      long timeoutMs = computePerTaskTimeoutMs(_defaultQueueTimeoutMs);
      enqueueRunnableTask(task, timeoutMs);
      // Return a no-op runnable because the real task will be executed later by the monitor when dequeued
      return () -> {
      };
    }

    // No throttling: run the task as-is
    return task;
  }

  private long computePerTaskTimeoutMs(long configuredTimeoutMs) {
    try {
      if (QueryThreadContext.getIfAvailable() != null && QueryThreadContext.get().getExecutionContext() != null) {
        long remaining = QueryThreadContext.get().getExecutionContext().getActiveDeadlineMs()
            - System.currentTimeMillis();
        if (remaining <= 0) {
          return 0L;
        }
        // Do not wait in the queue longer than the query's remaining active deadline
        return Math.min(configuredTimeoutMs, remaining);
      }
    } catch (Throwable t) {
      // Be conservative and use configured timeout if context is not available
      LOGGER.debug("Failed to compute per-task timeout from thread context; using configured timeout.", t);
    }
    return configuredTimeoutMs;
  }

  /**
   * Queue a callable task and wait for its execution
   */
  private <T> T queueCallableTask(Callable<T> task, long timeoutMs)
      throws Exception {
    QueuedCallableTask<T> queuedTask = new QueuedCallableTask<>(task, timeoutMs);

    if (_maxQueueSize > 0 && !_taskQueue.offer(queuedTask)) {
      // Queue is full
      throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
          "Task queue is full (size: " + _maxQueueSize + ") due to high heap usage.");
    }

    _serverMetrics.addMeteredGlobalValue(ServerMeter.THROTTLE_EXECUTOR_QUEUED_TASKS, 1);
    LOGGER.debug("Queued callable task, queue size: {}", _taskQueue.size());

    // Wait for the task to complete or timeout
    return queuedTask.get(timeoutMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Queue a runnable task and wait for its execution
   */
  private void queueRunnableTask(Runnable task, long timeoutMs) {
    QueuedRunnableTask queuedTask = new QueuedRunnableTask(task, timeoutMs);

    if (_maxQueueSize > 0 && !_taskQueue.offer(queuedTask)) {
      // Queue is full
      throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
          "Task queue is full (size: " + _maxQueueSize + ") due to high heap usage.");
    }

    _serverMetrics.addMeteredGlobalValue(ServerMeter.THROTTLE_EXECUTOR_QUEUED_TASKS, 1);
    LOGGER.debug("Queued runnable task, queue size: {}", _taskQueue.size());

    try {
      // Wait for the task to complete or timeout
      queuedTask.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      } else {
        throw new RuntimeException("Error executing queued task", e);
      }
    }
  }

  /**
   * Enqueue a runnable task without waiting for its completion. Used when queuing at submission time.
   */
  private void enqueueRunnableTask(Runnable task, long timeoutMs) {
    QueuedRunnableTask queuedTask = new QueuedRunnableTask(task, timeoutMs);

    if (_maxQueueSize > 0 && !_taskQueue.offer(queuedTask)) {
      throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
          "Task queue is full (size: " + _maxQueueSize + ") due to high heap usage.");
    }

    _serverMetrics.addMeteredGlobalValue(ServerMeter.THROTTLE_EXECUTOR_QUEUED_TASKS, 1);
    LOGGER.debug("Enqueued runnable task (non-blocking), queue size: {}", _taskQueue.size());
  }

  @Override
  public void shutdown() {
    _isShutdown.set(true);
    _monitorExecutor.shutdownNow();

    // Allow the monitor thread to complete current processing
    try {
      if (!_monitorExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        LOGGER.warn("Monitor executor did not terminate within the timeout period.");
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupted while waiting for monitor executor to terminate.", e);
      Thread.currentThread().interrupt();
    }

    // Cancel remaining tasks in the queue - fail them with shutdown exception
    int remainingTasks = 0; // keep local counter for logging
    while (!_taskQueue.isEmpty()) {
      QueuedTask<?> task = _taskQueue.poll();
      if (task != null) {
        remainingTasks++;
        task.fail(new IllegalStateException("Executor is shutting down"));
        _serverMetrics.addMeteredGlobalValue(
            ServerMeter.THROTTLE_EXECUTOR_SHUTDOWN_CANCELED_TASKS, 1);
      }
    }

    super.shutdown();

    LOGGER.info("ThrottleOnCriticalHeapUsageExecutor shutdown. Remaining queued tasks: {}", remainingTasks);
  }

  public int getQueueSize() {
    return _taskQueue.size();
  }

  /**
   * Base class for queued tasks.
   *
   * <p>The {@code QueuedTask} class represents a task that can be queued for execution when the system is under
   * critical heap usage. It provides a common interface for handling task execution, timeouts, and failures.</p>
   *
   * <p>Lifecycle:
   * <ul>
   *   <li>Tasks are created and added to the queue when heap usage is critical.</li>
   *   <li>When heap usage drops below the critical level, tasks are dequeued and executed.</li>
   *   <li>If a task remains in the queue beyond a configured timeout, the {@code timeout()} method is invoked.</li>
   *   <li>If an exception occurs during execution, the {@code fail(Exception e)} method is invoked.</li>
   * </ul>
   * </p>
   *
   * <p>Thread-safety:
   * <ul>
   *   <li>Instances of {@code QueuedTask} are not inherently thread-safe and should be accessed in a thread-safe
   *       manner by the enclosing executor.</li>
   *   <li>The enclosing {@code ThrottleOnCriticalHeapUsageExecutor} ensures proper synchronization when accessing
   *       and modifying the queue.</li>
   * </ul>
   * </p>
   */
  private abstract static class QueuedTask<T> {
    private final long _queueTime;
    private final long _timeoutMs;
    private final Object _lock = new Object();
    private final ThreadAccountant _accountant; // submitting thread's accountant (may be null)
    private T _result;
    private Exception _exception;
    private boolean _completed = false;

    protected QueuedTask(long timeoutMs) {
      _queueTime = System.currentTimeMillis();
      _timeoutMs = timeoutMs;
      QueryThreadContext ctx = QueryThreadContext.getIfAvailable();
      _accountant = (ctx != null) ? ctx.getAccountant() : null;
    }

    public long getQueueTime() {
      return _queueTime;
    }

    public long getTimeoutMs() {
      return _timeoutMs;
    }

    /** Returns true if the originating context is still throttling submissions. */
    public boolean isThrottled() {
      return _accountant != null && _accountant.throttleQuerySubmission();
    }

    protected final void completeWithResult(T result) {
      synchronized (_lock) {
        _result = result;
        _completed = true;
        _lock.notifyAll();
      }
    }

    public abstract void execute();

    public void timeout() {
      synchronized (_lock) {
        _exception = QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
            "Task timed out in queue after " + getTimeoutMs() + "ms due to high heap usage.");
        _completed = true;
        _lock.notifyAll();
      }
    }

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
        while (!_completed) {
          long elapsedTime = System.currentTimeMillis() - startTime;
          long remainingTime = timeoutMs - elapsedTime;
          if (remainingTime <= 0) {
            break;
          }
          try {
            _lock.wait(remainingTime);
          } catch (InterruptedException e) {
            // Preserve interrupt status for calling code
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
   * Wrapper for callable tasks that can be queued
   */
  private static class QueuedCallableTask<T> extends QueuedTask<T> {
    private final Callable<T> _task;

    public QueuedCallableTask(Callable<T> task, long timeoutMs) {
      super(timeoutMs);
      _task = task;
    }

    @Override
    public void execute() {
      try {
        T result = _task.call();
        completeWithResult(result);
      } catch (Exception e) {
        fail(e);
      }
    }
  }

  /**
   * Wrapper for runnable tasks that can be queued
   */
  private static class QueuedRunnableTask extends QueuedTask<Void> {
    private final Runnable _task;

    public QueuedRunnableTask(Runnable task, long timeoutMs) {
      super(timeoutMs);
      _task = task;
    }

    @Override
    public void execute() {
      try {
        _task.run();
        completeWithResult(null);
      } catch (Exception e) {
        fail(e);
      }
    }
  }

  /** Marker wrapper for tasks dispatched from the internal queue to avoid re-decoration. */
  private static final class FromQueueRunnable implements Runnable {
    private final Runnable _delegate;

    FromQueueRunnable(Runnable delegate) {
      _delegate = delegate;
    }

    @Override
    public void run() {
      _delegate.run();
    }

    public Runnable getDelegate() {
      return _delegate;
    }
  }

  /**
   * Factory to conditionally wrap an executor with queue-based throttling based on config.
   * Also handles config parsing and logging as requested in review.
   */
  public static ExecutorService maybeWrap(ExecutorService base, PinotConfiguration serverConf, String logContext) {
    boolean enabled = serverConf.getProperty(
        CommonConstants.Server.CONFIG_OF_ENABLE_QUERY_SCHEDULER_THROTTLING_ON_HEAP_USAGE,
        CommonConstants.Server.DEFAULT_ENABLE_QUERY_SCHEDULER_THROTTLING_ON_HEAP_USAGE);
    if (!enabled) {
      return base;
    }

    int maxSize = serverConf.getProperty(CommonConstants.Server.CONFIG_OF_HEAP_USAGE_THROTTLE_QUEUE_MAX_SIZE,
        CommonConstants.Server.DEFAULT_HEAP_USAGE_THROTTLE_QUEUE_MAX_SIZE);
    long timeoutMs = serverConf.getProperty(CommonConstants.Server.CONFIG_OF_HEAP_USAGE_THROTTLE_QUEUE_TIMEOUT_MS,
        CommonConstants.Server.DEFAULT_HEAP_USAGE_THROTTLE_QUEUE_TIMEOUT_MS);
    long monitorIntervalMs =
        serverConf.getProperty(CommonConstants.Server.CONFIG_OF_HEAP_USAGE_THROTTLE_MONITOR_INTERVAL_MS,
            CommonConstants.Server.DEFAULT_HEAP_USAGE_THROTTLE_MONITOR_INTERVAL_MS);
    LOGGER.info(
        "Enable heap usage throttling for {}. maxSize={}, timeoutMs={}, monitorIntervalMs={} (maxSize<=0 => unbounded)",
        logContext, maxSize, timeoutMs, monitorIntervalMs);
    return new ThrottleOnCriticalHeapUsageExecutor(base, maxSize, timeoutMs, monitorIntervalMs);
  }
}
