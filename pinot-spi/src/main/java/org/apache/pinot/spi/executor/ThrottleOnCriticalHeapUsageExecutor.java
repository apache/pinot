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
 * - Configurable queue size and timeout (global default or per-task)
 * - Background monitoring of heap usage to process queued tasks
 * - Comprehensive metrics publishing for monitoring and alerting:
 *   * Current queue size (real-time)
 *   * Total tasks queued (cumulative counter)
 *   * Total tasks processed from queue (cumulative counter)
 *   * Total tasks timed out in queue (cumulative counter)
 *   * Total tasks canceled during shutdown (cumulative counter)
 *
 * Metrics Integration:
 * - Automatically registers gauges with ServerMetrics when available
 * - Uses reflection to avoid hard dependencies on metrics framework
 * - Gracefully degrades if ServerMetrics is not available
 * - Provides manual registration method for delayed initialization
 * - Cleans up metrics during shutdown
 */
public class ThrottleOnCriticalHeapUsageExecutor extends DecoratorExecutorService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThrottleOnCriticalHeapUsageExecutor.class);

  // Default configuration values
  private static final int DEFAULT_QUEUE_SIZE = 1000;
  private static final long DEFAULT_QUEUE_TIMEOUT_MS = 30000; // 30 seconds
  private static final long DEFAULT_MONITOR_INTERVAL_MS = 1000; // 1 second
  private static final long LOG_THROTTLE_INTERVAL_MS = 30000; // Log queue status every 30 seconds

  private final ThreadResourceUsageAccountant _threadResourceUsageAccountant;
  private final BlockingQueue<QueuedTask> _taskQueue;
  private final int _maxQueueSize;
  private final long _defaultQueueTimeoutMs;
  private final ScheduledExecutorService _monitorExecutor;
  private final AtomicBoolean _isShutdown = new AtomicBoolean(false);
  private final AtomicInteger _queuedTaskCount = new AtomicInteger(0);
  private final AtomicInteger _processedTaskCount = new AtomicInteger(0);
  private final AtomicInteger _timedOutTaskCount = new AtomicInteger(0);
  private final AtomicInteger _shutdownCanceledTaskCount = new AtomicInteger(0);
  private volatile long _lastQueueStatusLogTime = 0;

  public ThrottleOnCriticalHeapUsageExecutor(ExecutorService executorService,
      ThreadResourceUsageAccountant threadResourceUsageAccountant) {
    this(executorService, threadResourceUsageAccountant, DEFAULT_QUEUE_SIZE,
        DEFAULT_QUEUE_TIMEOUT_MS, DEFAULT_MONITOR_INTERVAL_MS);
  }

  public ThrottleOnCriticalHeapUsageExecutor(ExecutorService executorService,
      ThreadResourceUsageAccountant threadResourceUsageAccountant,
      int maxQueueSize, long defaultQueueTimeoutMs, long monitorIntervalMs) {
    super(executorService);
    _threadResourceUsageAccountant = threadResourceUsageAccountant;
    _maxQueueSize = maxQueueSize;
    _defaultQueueTimeoutMs = defaultQueueTimeoutMs;
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

    // Register metrics for tracking queue statistics
    registerMetrics();

    LOGGER.info(
        "ThrottleOnCriticalHeapUsageExecutor initialized with queue size: {}, default timeout: {}ms, "
            + "monitor interval: {}ms", maxQueueSize, defaultQueueTimeoutMs, monitorIntervalMs);
  }

  /**
   * Register metrics to track queue statistics if ServerMetrics is available
   */
  private void registerMetrics() {
    try {
      // Try to get ServerMetrics instance if it's available
      Class<?> serverMetricsClass = Class.forName("org.apache.pinot.common.metrics.ServerMetrics");
      Object serverMetrics = serverMetricsClass.getMethod("get").invoke(null);

      // Check if it's not the NOOP instance by checking if it's registered
      if (serverMetrics != null) {
        // Get the ServerGauge enum class
        Class<?> serverGaugeClass = Class.forName("org.apache.pinot.common.metrics.ServerGauge");

        // Register gauges for each metric
        registerGauge(serverMetrics, serverGaugeClass, "THROTTLE_EXECUTOR_QUEUE_SIZE",
            () -> (long) _taskQueue.size());
        registerGauge(serverMetrics, serverGaugeClass, "THROTTLE_EXECUTOR_QUEUED_TASKS_TOTAL",
            () -> (long) _queuedTaskCount.get());
        registerGauge(serverMetrics, serverGaugeClass, "THROTTLE_EXECUTOR_PROCESSED_TASKS_TOTAL",
            () -> (long) _processedTaskCount.get());
        registerGauge(serverMetrics, serverGaugeClass, "THROTTLE_EXECUTOR_TIMED_OUT_TASKS_TOTAL",
            () -> (long) _timedOutTaskCount.get());
        registerGauge(serverMetrics, serverGaugeClass, "THROTTLE_EXECUTOR_SHUTDOWN_CANCELED_TASKS_TOTAL",
            () -> (long) _shutdownCanceledTaskCount.get());

        LOGGER.info("Successfully registered ThrottleOnCriticalHeapUsageExecutor metrics");
      }
    } catch (Exception e) {
      // ServerMetrics may not be available or registered yet, which is fine
      LOGGER.debug("ServerMetrics not available for ThrottleOnCriticalHeapUsageExecutor metrics registration: {}",
          e.getMessage());
    }
  }

  /**
   * Helper method to register a gauge metric using reflection
   */
  private void registerGauge(Object serverMetrics, Class<?> serverGaugeClass, String gaugeName,
      java.util.function.Supplier<Long> valueSupplier) {
    try {
      // Get the gauge enum constant
      Object gauge = Enum.valueOf((Class<Enum>) serverGaugeClass, gaugeName);

      // Call setOrUpdateGlobalGauge(gauge, valueSupplier)
      serverMetrics.getClass()
          .getMethod("setOrUpdateGlobalGauge", serverGaugeClass, java.util.function.Supplier.class)
          .invoke(serverMetrics, gauge, valueSupplier);

      LOGGER.debug("Registered gauge: {}", gaugeName);
    } catch (Exception e) {
      LOGGER.warn("Failed to register gauge {}: {}", gaugeName, e.getMessage());
    }
  }

  protected void checkTaskAllowed() {
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
      int initialQueueSize = _taskQueue.size();
      long currentTime = System.currentTimeMillis();

      // Log queue size for monitoring if there are queued tasks (throttled to prevent log flooding)
      if (initialQueueSize > 0 && (currentTime - _lastQueueStatusLogTime) > LOG_THROTTLE_INTERVAL_MS) {
        LOGGER.info("Processing queued tasks. Current queue size: {}, Queued: {}, Processed: {}, "
                + "Timed out: {}, Shutdown canceled: {}",
            initialQueueSize, _queuedTaskCount.get(), _processedTaskCount.get(),
            _timedOutTaskCount.get(), _shutdownCanceledTaskCount.get());
        _lastQueueStatusLogTime = currentTime;
      }

      // Process tasks while heap usage is not critical and queue is not empty
      while (!shouldQueueTask() && !_taskQueue.isEmpty()) {
        QueuedTask queuedTask = _taskQueue.poll();
        if (queuedTask != null) {
          long queueTime = System.currentTimeMillis() - queuedTask.getQueueTime();

          if (queueTime > queuedTask.getTimeoutMs()) {
            // Task has timed out in queue
            queuedTask.timeout();
            _timedOutTaskCount.incrementAndGet();
            LOGGER.warn("Task timed out after {}ms in queue (timeout: {}ms)", queueTime, queuedTask.getTimeoutMs());
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
   * Execute a callable task with custom timeout if queuing is needed.
   * This allows per-task timeout specification instead of using the global default.
   *
   * @param task the callable task to execute
   * @param timeoutMs the timeout in milliseconds for this specific task if it gets queued
   * @return the result of the callable task
   * @throws Exception if execution fails
   */
  public <T> T executeWithTimeout(Callable<T> task, long timeoutMs)
      throws Exception {
    if (shouldQueueTask()) {
      // Queue the task with custom timeout if heap usage is critical
      return queueCallableTask(task, timeoutMs);
    } else {
      // Execute immediately if heap usage is normal
      return task.call();
    }
  }

  /**
   * Execute a runnable task with custom timeout if queuing is needed.
   * This allows per-task timeout specification instead of using the global default.
   *
   * @param task the runnable task to execute
   * @param timeoutMs the timeout in milliseconds for this specific task if it gets queued
   */
  public void executeWithTimeout(Runnable task, long timeoutMs) {
    if (shouldQueueTask()) {
      // Queue the task with custom timeout if heap usage is critical
      queueRunnableTask(task, timeoutMs);
    } else {
      // Execute immediately if heap usage is normal
      task.run();
    }
  }

  /**
   * Queue a callable task and wait for its execution
   */
  private <T> T queueCallableTask(Callable<T> task)
      throws Exception {
    return queueCallableTask(task, _defaultQueueTimeoutMs);
  }

  /**
   * Queue a callable task with custom timeout and wait for its execution
   */
  private <T> T queueCallableTask(Callable<T> task, long timeoutMs)
      throws Exception {
    QueuedCallableTask<T> queuedTask = new QueuedCallableTask<>(task, timeoutMs);

    if (!_taskQueue.offer(queuedTask)) {
      // Queue is full
      throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
          "Task queue is full (size: " + _maxQueueSize + ") due to high heap usage.");
    }

    _queuedTaskCount.incrementAndGet();
    LOGGER.debug("Queued callable task, queue size: {}", _taskQueue.size());

    // Wait for the task to complete or timeout
    return queuedTask.get(timeoutMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Queue a runnable task and wait for its execution
   */
  private void queueRunnableTask(Runnable task) {
    queueRunnableTask(task, _defaultQueueTimeoutMs);
  }

  /**
   * Queue a runnable task with custom timeout and wait for its execution
   */
  private void queueRunnableTask(Runnable task, long timeoutMs) {
    QueuedRunnableTask queuedTask = new QueuedRunnableTask(task, timeoutMs);

    if (!_taskQueue.offer(queuedTask)) {
      // Queue is full
      throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
          "Task queue is full (size: " + _maxQueueSize + ") due to high heap usage.");
    }

    _queuedTaskCount.incrementAndGet();
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
    int remainingTasks = 0;
    while (!_taskQueue.isEmpty()) {
      QueuedTask task = _taskQueue.poll();
      if (task != null) {
        remainingTasks++;
        task.fail(new IllegalStateException("Executor is shutting down"));
        _shutdownCanceledTaskCount.incrementAndGet();
      }
    }

    // Clean up metrics
    cleanupMetrics();

    super.shutdown();

    LOGGER.info("ThrottleOnCriticalHeapUsageExecutor shutdown. Stats - Queued: {}, Processed: {}, "
            + "Timed out: {}, Shutdown canceled: {}",
        _queuedTaskCount.get(), _processedTaskCount.get(), _timedOutTaskCount.get(), _shutdownCanceledTaskCount.get());
  }

  /**
   * Clean up registered metrics during shutdown
   */
  private void cleanupMetrics() {
    try {
      // Try to get ServerMetrics instance if it's available
      Class<?> serverMetricsClass = Class.forName("org.apache.pinot.common.metrics.ServerMetrics");
      Object serverMetrics = serverMetricsClass.getMethod("get").invoke(null);

      if (serverMetrics != null) {
        // Get the ServerGauge enum class
        Class<?> serverGaugeClass = Class.forName("org.apache.pinot.common.metrics.ServerGauge");

        // Remove gauges for each metric
        removeGauge(serverMetrics, serverGaugeClass, "THROTTLE_EXECUTOR_QUEUE_SIZE");
        removeGauge(serverMetrics, serverGaugeClass, "THROTTLE_EXECUTOR_QUEUED_TASKS_TOTAL");
        removeGauge(serverMetrics, serverGaugeClass, "THROTTLE_EXECUTOR_PROCESSED_TASKS_TOTAL");
        removeGauge(serverMetrics, serverGaugeClass, "THROTTLE_EXECUTOR_TIMED_OUT_TASKS_TOTAL");
        removeGauge(serverMetrics, serverGaugeClass, "THROTTLE_EXECUTOR_SHUTDOWN_CANCELED_TASKS_TOTAL");

        LOGGER.info("Successfully cleaned up ThrottleOnCriticalHeapUsageExecutor metrics");
      }
    } catch (Exception e) {
      LOGGER.debug("Error cleaning up ThrottleOnCriticalHeapUsageExecutor metrics: {}", e.getMessage());
    }
  }

  /**
   * Helper method to remove a gauge metric using reflection
   */
  private void removeGauge(Object serverMetrics, Class<?> serverGaugeClass, String gaugeName) {
    try {
      // Get the gauge enum constant
      Object gauge = Enum.valueOf((Class<Enum>) serverGaugeClass, gaugeName);

      // Call removeGlobalGauge(gaugeName, gauge)
      serverMetrics.getClass()
          .getMethod("removeGlobalGauge", String.class, serverGaugeClass)
          .invoke(serverMetrics, gaugeName, gauge);

      LOGGER.debug("Removed gauge: {}", gaugeName);
    } catch (Exception e) {
      LOGGER.debug("Failed to remove gauge {}: {}", gaugeName, e.getMessage());
    }
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
   * Get total number of tasks canceled during shutdown
   */
  public int getShutdownCanceledTaskCount() {
    return _shutdownCanceledTaskCount.get();
  }

  /**
   * Manually register metrics if ServerMetrics becomes available after executor creation.
   * This can be called if metrics registration failed during construction.
   */
  public void registerMetricsIfAvailable() {
    registerMetrics();
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
  private abstract static class QueuedTask {
    private final long _queueTime;
    private final long _timeoutMs;

    protected QueuedTask(long timeoutMs) {
      _queueTime = System.currentTimeMillis();
      _timeoutMs = timeoutMs;
    }

    public long getQueueTime() {
      return _queueTime;
    }

    public long getTimeoutMs() {
      return _timeoutMs;
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
    private volatile T _result;
    private volatile Exception _exception;
    private volatile boolean _completed = false;
    private final Object _lock = new Object();

    public QueuedCallableTask(Callable<T> task, long timeoutMs) {
      super(timeoutMs);
      _task = task;
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
            "Task timed out in queue after " + getTimeoutMs() + "ms due to high heap usage.");
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
   * Wrapper for runnable tasks that can be queued
   */
  private class QueuedRunnableTask extends QueuedTask {
    private final Runnable _task;
    private volatile Exception _exception;
    private volatile boolean _completed = false;
    private final Object _lock = new Object();

    public QueuedRunnableTask(Runnable task, long timeoutMs) {
      super(timeoutMs);
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
            "Task timed out in queue after " + getTimeoutMs() + "ms due to high heap usage.");
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
      }
    }
  }
}
