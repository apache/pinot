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
  // TotalTime = segmentRemoteDownloadTimeMs + segmentProcessingTime (controller local processing)
  SEGMENT_TOTAL_DOWNLOAD_TIME_MS("segmentDownloadTotalTimeMs", false),
  // Time taken to download the segment from remote store
  SEGMENT_REMOTE_DOWNLOAD_TIME_MS("segmentRemoteDownloadTimeMs", false),
  // TotalTime = segmentUploadTimeMs + segmentProcessingTime (controller local processing)
  SEGMENT_TOTAL_UPLOAD_TIME_MS("segmentUploadTotalTimeMs", false),
  // Time taken to upload the segment to remote store
  SEGMENT_REMOTE_UPLOAD_TIME_MS("segmentRemoteUploadTimeMs", false);


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
