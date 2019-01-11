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
package com.linkedin.pinot.core.periodictask;

/**
 * A base class to implement periodic task interface.
 */
public abstract class BasePeriodicTask implements PeriodicTask {
  protected final String _taskName;
  protected final long _intervalInSeconds;
  protected final long _initialDelayInSeconds;

  public BasePeriodicTask(String taskName, long runFrequencyInSeconds, long initialDelayInSeconds) {
    _taskName = taskName;
    _intervalInSeconds = runFrequencyInSeconds;
    _initialDelayInSeconds = initialDelayInSeconds;
  }

  @Override
  public long getIntervalInSeconds() {
    return _intervalInSeconds;
  }

  @Override
  public long getInitialDelayInSeconds() {
    return _initialDelayInSeconds;
  }

  @Override
  public String getTaskName() {
    return _taskName;
  }

  @Override
  public String toString() {
    return String.format("Task: %s, Interval: %ds, Initial Delay: %ds", _taskName, _intervalInSeconds,
        _initialDelayInSeconds);
  }
}
