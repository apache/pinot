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


public enum MinionMeter implements AbstractMetrics.Meter {
  HEALTH_CHECK_GOOD_CALLS("healthChecks", true),
  HEALTH_CHECK_BAD_CALLS("healthChecks", true),
  NUMBER_TASKS("tasks", false),
  NUMBER_TASKS_EXECUTED("tasks", false),
  NUMBER_TASKS_COMPLETED("tasks", false),
  NUMBER_TASKS_CANCELLED("tasks", false),
  NUMBER_TASKS_FAILED("tasks", false),
  NUMBER_TASKS_FATAL_FAILED("tasks", false),
  SEGMENT_UPLOAD_FAIL_COUNT("segments", false),
  SEGMENT_DOWNLOAD_FAIL_COUNT("segments", false),
  SEGMENTS_DOWNLOADED("segments", false),
  SEGMENTS_UPLOADED("segments", false),
  SEGMENT_SIZE_DOWNLOADED("bytes", false),
  SEGMENT_SIZE_UPLOADED("bytes", false),
  RECORDS_PER_SEGMENT("rows", false),
  RECORDS_PURGED_PER_SEGMENT("rows", false);

  private final String _meterName;
  private final String _unit;
  private final boolean _global;

  MinionMeter(String unit, boolean global) {
    _meterName = Utils.toCamelCase(name().toLowerCase());
    _unit = unit;
    _global = global;
  }

  @Override
  public String getMeterName() {
    return _meterName;
  }

  @Override
  public String getUnit() {
    return _unit;
  }

  @Override
  public boolean isGlobal() {
    return _global;
  }
}
