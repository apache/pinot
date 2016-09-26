/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.common.query.context;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


public class TimerContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimerContext.class);

  private long arrivalTimeNanos;
  private long schedulerAddTimeNanos;
  private long execStartTimeNanos;
  private long execEndTimeNanos;
  private long responseSubmitTimeNanos;
  // NOTE: make sure to update toString() if you add a field here

  /**
   * Time in Nanoseconds at which the query request was received by the server
   */
  public void setArrivalTimeNanos(long arrivalTimeNanos) {
    this.arrivalTimeNanos = arrivalTimeNanos;
  }

  /**
   * Time at which query request arrived at the server
   * @return query arrival time in nanoseconds
   */
  public long getArrivalTimeNanos() {
    return arrivalTimeNanos;
  }

  /**
   * time at which query request was submitted to the scheduler
   * @param schedulerAddTimeNanos time in nanoseconds
   */
  public void setSchedulerAddTimeNanos(long schedulerAddTimeNanos) {
    this.schedulerAddTimeNanos = schedulerAddTimeNanos;
  }

  /**
   * Time at which query began executing
   * @param execStartTime time in nanoseconds
   */
  public void setExecStartTimeNanos(long execStartTime) {
    this.execStartTimeNanos = execStartTime;
  }

  /**
   * Time elapsed since the arrival of query at the server
   * @return Elapsed time in nanoseconds
   */
  public long getElapsedTimeNanos() {
    return System.nanoTime() - this.arrivalTimeNanos;
  }

  /**
   * Set the time at which query finished execution
   * @param execEndTimeNanos time in nanoseconds
   */
  public void setExecEndTimeNanos(long execEndTimeNanos) {
    this.execEndTimeNanos = execEndTimeNanos;
  }

  /**
   * Time at which serialized response was submitted for responding to broker
   * @param responseSubmitTimeNanos time in nanoseconds
   */
  public void setResponseSubmitTimeNanos(long responseSubmitTimeNanos) {
    this.responseSubmitTimeNanos = responseSubmitTimeNanos;
  }

  // for logging
  @Override
  public String toString() {
    return String.format("%d,%d,%d,%d,%d", arrivalTimeNanos, schedulerAddTimeNanos,
        execStartTimeNanos, execEndTimeNanos, responseSubmitTimeNanos);
  }
}
