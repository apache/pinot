/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;


public abstract class PeriodicTask {
  private final String _taskName;
  private long _executionTime;
  private long _intervalSeconds;
  private long _initialDelaySeconds;

  public static final long DEFAULT_RUN_FREQUENCY_IN_SECOND = 3600L;
  public static final long DEFAULT_INITIAL_DELAY_IN_SECOND = 120L;

  public PeriodicTask(String taskName) {
    this(taskName, DEFAULT_RUN_FREQUENCY_IN_SECOND);
  }

  public PeriodicTask(String taskName, long runFrequencyInSeconds) {
    this(taskName, runFrequencyInSeconds, DEFAULT_INITIAL_DELAY_IN_SECOND);
  }

  public PeriodicTask(String taskName, long runFrequencyInSeconds, long initialDelaySeconds) {
    _taskName = taskName;
    _intervalSeconds = runFrequencyInSeconds;
    _initialDelaySeconds = initialDelaySeconds;
    _executionTime = System.currentTimeMillis() + initialDelaySeconds;
  }

  public abstract void runTask();

  public void initTask() {
  }

  protected void setIntervalSeconds(long intervalSeconds) {
    _intervalSeconds = intervalSeconds;
  }

  protected long getIntervalSeconds() {
    return _intervalSeconds;
  }

  protected long getInitialDelaySeconds() {
    return _initialDelaySeconds;
  }

  public String getTaskName() {
    return _taskName;
  }

  public long getExecutionTime() {
    return _executionTime;
  }

  public void updateExecutionTime() {
    _executionTime += _intervalSeconds;
  }
}
