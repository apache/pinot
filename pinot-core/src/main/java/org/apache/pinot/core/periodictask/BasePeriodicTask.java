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
package org.apache.pinot.core.periodictask;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A base class to implement periodic task interface.
 */
@ThreadSafe
public abstract class BasePeriodicTask implements PeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasePeriodicTask.class);
  private static final String DEFAULT_REQUEST_ID = "auto";

  // Wait for at most 30 seconds while calling stop() for task to terminate
  private static final long MAX_PERIODIC_TASK_STOP_TIME_MILLIS = 30_000L;

  protected final String _taskName;
  protected final long _intervalInSeconds;
  protected final long _initialDelayInSeconds;
  protected final ReentrantLock _runLock;

  private volatile boolean _started;
  private volatile boolean _running;

  // Default properties that tasks may use during execution. This variable is private and does not have any get or set
  // methods to prevent subclasses from gaining direct access to this variable. See run(Properties) method to see how
  // properties are passed and used during task execution.
  private static final Properties DEFAULT_PERIODIC_TASK_PROPERTIES;

  static {
    // Default properties for PeriodicTask execution.
    DEFAULT_PERIODIC_TASK_PROPERTIES = new Properties();
    DEFAULT_PERIODIC_TASK_PROPERTIES.put(PeriodicTask.PROPERTY_KEY_REQUEST_ID, DEFAULT_REQUEST_ID);
  }

  public BasePeriodicTask(String taskName, long runFrequencyInSeconds, long initialDelayInSeconds) {
    _taskName = taskName;
    _intervalInSeconds = runFrequencyInSeconds;
    _initialDelayInSeconds = initialDelayInSeconds;
    _runLock = new ReentrantLock();
  }

  @Override
  public String getTaskName() {
    return _taskName;
  }

  @Override
  public long getIntervalInSeconds() {
    return _intervalInSeconds;
  }

  @Override
  public long getInitialDelayInSeconds() {
    return _initialDelayInSeconds;
  }

  /**
   * Returns the status of the {@code started} flag. This flag will be set after calling {@link #start()}, and reset
   * after calling {@link #stop()}.
   */
  public final boolean isStarted() {
    return _started;
  }

  /**
   * Returns the status of the {@code running} flag. This flag will be set during the task execution.
   */
  public final boolean isRunning() {
    return _running;
  }

  /**
   * {@inheritDoc}
   * <p>
   * This method sets {@code started} flag to true.
   */
  @Override
  public final synchronized void start() {
    if (_started) {
      LOGGER.warn("Task: {} is already started", _taskName);
      return;
    }

    try {
      setUpTask();
    } catch (Exception e) {
      LOGGER.error("Caught exception while setting up task: {}", _taskName, e);
    }

    // mark _started as true only after state has completely initialized, so that run method doesn't end up seeing
    // partially initialized state.
    _started = true;
  }

  /**
   * Can be overridden for extra task setups. This method will be called when the periodic task starts.
   * <p>
   * Possible setups include adding or resetting the metric values.
   */
  protected void setUpTask() {
  }

  /**
   * {@inheritDoc}
   * <p>
   * During the task execution, the {@code running} flag will be set.
   */
  @Override
  public final void run() {
    // Pass default properties object to the actual run method.
    run(DEFAULT_PERIODIC_TASK_PROPERTIES);
  }

  @Override
  public final void run(Properties periodicTaskProperties) {
    try {
      // Don't allow a task to run more than once at a time.
      _runLock.lock();
      _running = true;

      String periodicTaskRequestId = periodicTaskProperties.getProperty(PeriodicTask.PROPERTY_KEY_REQUEST_ID);
      if (_started) {
        long startTime = System.currentTimeMillis();
        LOGGER.info("[TaskRequestId: {}] Start running task: {}", periodicTaskRequestId, _taskName);
        try {
          runTask(periodicTaskProperties);
        } catch (Exception e) {
          LOGGER.error("[TaskRequestId: {}] Caught exception while running task: {}", periodicTaskRequestId, _taskName,
              e);
        }
        LOGGER.info("[TaskRequestId: {}] Finish running task: {} in {}ms", periodicTaskRequestId, _taskName,
            System.currentTimeMillis() - startTime);
      } else {
        LOGGER.warn("[TaskRequestId: {}] Task: {} is skipped because it is not started or already stopped",
            periodicTaskRequestId, _taskName);
      }
    } finally {
      _runLock.unlock();
      _running = false;
    }
  }

  /**
   * Executes the task. This method should early terminate if {@code started} flag is set to false by {@link #stop()}
   * during execution.
   */
  protected abstract void runTask(Properties periodicTaskProperties);

  /**
   * {@inheritDoc}
   * <p>
   * This method sets {@code started} flag to false. If the task is running, this method will block for at most 30
   * seconds until the task finishes.
   */
  @Override
  public final synchronized void stop() {
    if (!_started) {
      LOGGER.warn("Task: {} is not started", _taskName);
      return;
    }
    long startTimeMs = System.currentTimeMillis();
    _started = false;

    try {
      // check if task is done running, or wait for the task to get done, by trying to acquire runLock.
      if (!_runLock.tryLock(MAX_PERIODIC_TASK_STOP_TIME_MILLIS, TimeUnit.MILLISECONDS)) {
        LOGGER
            .warn("Task {} could not be stopped within timeout of {}ms", _taskName, MAX_PERIODIC_TASK_STOP_TIME_MILLIS);
      } else {
        LOGGER.info("Task {} successfully stopped in {}ms", _taskName, System.currentTimeMillis() - startTimeMs);
      }
    } catch (InterruptedException ie) {
      LOGGER.error("Caught InterruptedException while waiting for task: {} to finish", _taskName);
      Thread.currentThread().interrupt();
    } finally {
      if (_runLock.isHeldByCurrentThread()) {
        _runLock.unlock();
      }
    }

    try {
      cleanUpTask();
    } catch (Exception e) {
      LOGGER.error("Caught exception while cleaning up task: {}", _taskName, e);
    }
  }

  /**
   * Can be overridden for extra task cleanups. This method will be called when the periodic task stops.
   * <p>
   * Possible cleanups include removing or resetting the metric values.
   */
  protected void cleanUpTask() {
  }

  @Override
  public String toString() {
    return String
        .format("Task: %s, Interval: %ds, Initial Delay: %ds", _taskName, _intervalInSeconds, _initialDelayInSeconds);
  }
}
