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

package org.apache.pinot.thirdeye.anomaly.task;

public class TaskConstants {

  public enum TaskType {
    DATA_QUALITY,              // tasks to detect data quality anomalies
    DETECTION,                 // tasks to detect anomalies
    DETECTION_ALERT,           // tasks to send alerts to customers regarding anomalies
    YAML_DETECTION_ONBOARD,    // tasks to onboard new YAML configured detection
    MONITOR,                   // tasks to clean up expired/invalid execution history
    DETECTION_ONLINE           // tasks to online detection anomalies
  }

  public enum TaskStatus {
    WAITING,
    RUNNING,
    COMPLETED,
    FAILED,
    TIMEOUT
  }
}
