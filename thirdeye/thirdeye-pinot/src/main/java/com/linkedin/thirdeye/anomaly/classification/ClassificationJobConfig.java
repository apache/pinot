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

package com.linkedin.thirdeye.anomaly.classification;


public class ClassificationJobConfig {
  private long maxMonitoringWindowInMS;
  private boolean forceSyncDetectionJobs;

  public ClassificationJobConfig() {
    maxMonitoringWindowInMS = 259200000L; // 3 days
    forceSyncDetectionJobs = false;
  }

  public long getMaxMonitoringWindowSizeInMS() {
    return maxMonitoringWindowInMS;
  }

  public void setMaxMonitoringWindowInMS(long maxMonitoringWindowInMS) {
    this.maxMonitoringWindowInMS = maxMonitoringWindowInMS;
  }

  public boolean getForceSyncDetectionJobs() {
    return forceSyncDetectionJobs;
  }

  public void setForceSyncDetectionJobs(boolean forceSyncDetectionJobs) {
    this.forceSyncDetectionJobs = forceSyncDetectionJobs;
  }
}
