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
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A base class to implement periodic task interface.
 */
@ThreadSafe
public abstract class BasePeriodicTask implements PeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasePeriodicTask.class);

  // Wait for at most 30 seconds while calling stop() for task to terminate
  private static final long MAX_PERIODIC_TASK_STOP_TIME_MILLIS = 30_000L;

  protected final String _taskName;
  protected final long _intervalInSeconds;
  protected final long _initialDelayInSeconds;
  protected final ReentrantLock _runLock;

  private volatile boolean _started;
  private volatile boolean _running;

  // Properties that task may use during execution. null by default.
  protected Properties _activePeriodicTaskProperties;

  public BasePeriodicTask(String taskName, long runFrequencyInSeconds, long initialDelayInSeconds) {
    _taskName = taskName;
    _intervalInSeconds = runFrequencyInSeconds;
    _initialDelayInSeconds = initialDelayInSeconds;
    _runLock = new ReentrantLock();

    // Periodic task parameters are null by default since they are currently used only when tasks are executed manually.
    _activePeriodicTaskProperties = null;
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
    try {
      // Don't allow a task to run more than once at a time.
      _runLock.lock();
      _running = true;

      String periodicTaskRequestId = _activePeriodicTaskProperties.getProperty("requestid");
      if (_started) {
        long startTime = System.currentTimeMillis();
        LOGGER.info("[TaskRequestId: {}] Start running task: {}", periodicTaskRequestId, _taskName);
        try {
          runTask();
        } catch (Exception e) {
          LOGGER.error("[TaskRequestId: {}] Caught exception while running task: {}", periodicTaskRequestId, _taskName, e);
        }
        LOGGER.info("[TaskRequestId: {}] Finish running task: {} in {}ms", periodicTaskRequestId, _taskName, System.currentTimeMillis() - startTime);
      } else {
        LOGGER.warn("[TaskRequestId: {}] Task: {} is skipped because it is not started or already stopped", periodicTaskRequestId, _taskName);
      }
    } finally {
       _runLock.unlock();
       _running = false;
    }
  }

  @Override
  public void run(@Nullable java.util.Properties periodicTaskProperties) {
    Properties savedPeriodicTaskProperties = _activePeriodicTaskProperties;
    try {
      _runLock.lock();

      // save and replace active periodic task parameters
      savedPeriodicTaskProperties = _activePeriodicTaskProperties;
      _activePeriodicTaskProperties = periodicTaskProperties;

      // run task
      run();
    } finally {
      // restore saved periodic task parameters so that normal periodic execution does not get effected by this
      // execution.
      _activePeriodicTaskProperties = savedPeriodicTaskProperties;
      _runLock.unlock();
    }
  }

  /**
   * Executes the task. This method should early terminate if {@code started} flag is set to false by {@link #stop()}
   * during execution.
   */
  protected abstract void runTask();

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
    _started = false;

    try {
      // check if task is done running, or wait for the task to get done, by trying to acquire runLock.
      if (!_runLock.tryLock(MAX_PERIODIC_TASK_STOP_TIME_MILLIS, TimeUnit.MILLISECONDS)) {
        LOGGER.warn("Task: {} did not finish within timeout of {}ms", MAX_PERIODIC_TASK_STOP_TIME_MILLIS);
      } else {
        LOGGER.info("Task: {} finished within timeout of {}ms", MAX_PERIODIC_TASK_STOP_TIME_MILLIS);
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } finally {
      _runLock.unlock();
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
