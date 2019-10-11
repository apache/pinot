/*
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

package org.apache.pinot.thirdeye.anomaly.monitor;

import java.util.concurrent.TimeUnit;

import org.apache.pinot.thirdeye.common.time.TimeGranularity;

public class MonitorConstants {
  public enum MonitorType {
    UPDATE,
    EXPIRE
  }

  public static int DEFAULT_RETENTION_DAYS = 90;
  public static int DEFAULT_COMPLETED_JOB_RETENTION_DAYS = 90;
  public static int DEFAULT_DETECTION_STATUS_RETENTION_DAYS = 90;
  public static int DEFAULT_RAW_ANOMALY_RETENTION_DAYS = 30;
  public static TimeGranularity DEFAULT_MONITOR_FREQUENCY = new TimeGranularity(15, TimeUnit.MINUTES);

}
