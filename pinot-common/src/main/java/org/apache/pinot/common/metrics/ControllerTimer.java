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
package org.apache.pinot.common.metrics;

import org.apache.pinot.common.Utils;


/**
 * Enumeration containing all the timers exposed by the Pinot controller.
 *
 */
public enum ControllerTimer implements AbstractMetrics.Timer {
  TABLE_REBALANCE_EXECUTION_TIME_MS("tableRebalanceExecutionTimeMs", false),
  CRON_SCHEDULER_JOB_EXECUTION_TIME_MS("cronSchedulerJobExecutionTimeMs", false),
  IDEAL_STATE_UPDATE_TIME_MS("IdealStateUpdateTimeMs", false),
  // How long it took the server to start.
  STARTUP_SUCCESS_DURATION_MS("startupSuccessDurationMs", true),
  // Time taken to read the segment from deep store
  DEEP_STORE_SEGMENT_READ_TIME_MS("deepStoreSegmentReadTimeMs", true),
  // Time taken to write the segment to deep store
  DEEP_STORE_SEGMENT_WRITE_TIME_MS("deepStoreSegmentWriteTimeMs", true);


  private final String _timerName;
  private final boolean _global;

  ControllerTimer(String unit, boolean global) {
    _global = global;
    _timerName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getTimerName() {
    return _timerName;
  }

  /**
   * Returns true if the timer is global (not attached to a particular resource)
   *
   * @return true if the timer is global
   */
  @Override
  public boolean isGlobal() {
    return _global;
  }
}
