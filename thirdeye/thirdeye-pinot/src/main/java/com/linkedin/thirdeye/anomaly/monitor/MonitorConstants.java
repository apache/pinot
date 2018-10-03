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

import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.api.TimeGranularity;

public class MonitorConstants {
  public enum MonitorType {
    UPDATE,
    EXPIRE
  }

  public static int DEFAULT_RETENTION_DAYS = 30;
  public static int DEFAULT_COMPLETED_JOB_RETENTION_DAYS = 14;
  public static int DEFAULT_DETECTION_STATUS_RETENTION_DAYS = 7;
  public static int DEFAULT_RAW_ANOMALY_RETENTION_DAYS = 30;
  public static TimeGranularity DEFAULT_MONITOR_FREQUENCY = new TimeGranularity(15, TimeUnit.MINUTES);

}
