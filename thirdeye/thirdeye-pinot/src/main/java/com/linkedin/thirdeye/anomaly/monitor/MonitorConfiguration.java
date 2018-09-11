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

package com.linkedin.thirdeye.anomaly.monitor;

import com.linkedin.thirdeye.api.TimeGranularity;

public class MonitorConfiguration {

  private int defaultRetentionDays = MonitorConstants.DEFAULT_RETENTION_DAYS;
  private int completedJobRetentionDays = MonitorConstants.DEFAULT_COMPLETED_JOB_RETENTION_DAYS;
  private int detectionStatusRetentionDays = MonitorConstants.DEFAULT_DETECTION_STATUS_RETENTION_DAYS;
  private int rawAnomalyRetentionDays = MonitorConstants.DEFAULT_RAW_ANOMALY_RETENTION_DAYS;
  private TimeGranularity monitorFrequency = MonitorConstants.DEFAULT_MONITOR_FREQUENCY;

  public int getCompletedJobRetentionDays() {
    return completedJobRetentionDays;
  }

  public void setCompletedJobRetentionDays(int completedJobRetentionDays) {
    this.completedJobRetentionDays = completedJobRetentionDays;
  }

  public int getDefaultRetentionDays() {
    return defaultRetentionDays;
  }

  public void setDefaultRetentionDays(int defaultRetentionDays) {
    this.defaultRetentionDays = defaultRetentionDays;
  }

  public int getDetectionStatusRetentionDays() {
    return detectionStatusRetentionDays;
  }

  public void setDetectionStatusRetentionDays(int detectionStatusRetentionDays) {
    this.detectionStatusRetentionDays = detectionStatusRetentionDays;
  }

  public int getRawAnomalyRetentionDays() {
    return rawAnomalyRetentionDays;
  }

  public void setRawAnomalyRetentionDays(int rawAnomalyRetentionDays) {
    this.rawAnomalyRetentionDays = rawAnomalyRetentionDays;
  }

  public TimeGranularity getMonitorFrequency() {
    return monitorFrequency;
  }

  public void setMonitorFrequency(TimeGranularity monitorFrequency) {
    this.monitorFrequency = monitorFrequency;
  }
}
