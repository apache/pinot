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
package com.linkedin.pinot.common.metrics;

import com.linkedin.pinot.common.Utils;


/**
* Enumeration containing all the meters exposed by the Pinot server.
*
*/
public enum ServerMeter implements AbstractMetrics.Meter {
  QUERIES("queries", true),
  UNCAUGHT_EXCEPTIONS("exceptions", true),
  REQUEST_DESERIALIZATION_EXCEPTIONS("exceptions", true),
  RESPONSE_SERIALIZATION_EXCEPTIONS("exceptions", true),
  QUERY_EXECUTION_EXCEPTIONS("exceptions", false),
  HELIX_ZOOKEEPER_RECONNECTS("reconnects", true),
  DELETED_SEGMENT_COUNT("segments", false),
  REALTIME_ROWS_CONSUMED("rows", true),
  INVALID_REALTIME_ROWS_DROPPED("rows", false),
  REALTIME_CONSUMPTION_EXCEPTIONS("exceptions", true),
  REALTIME_OFFSET_COMMITS("commits", true),
  REALTIME_OFFSET_COMMIT_EXCEPTIONS("exceptions", false),
  ROWS_WITH_ERRORS("rows", false),
  ROWS_NEEDING_CONVERSIONS("rows", false),
  ROWS_WITH_NULL_VALUES("rows", false),
  COLUMNS_WITH_NULL_VALUES("columns", false),
  LLC_CONTROLLER_RESPONSE_COMMIT("messages", false),
  LLC_CONTROLLER_RESPONSE_HOLD("messages", false),
  LLC_CONTROLLER_RESPONSE_CATCH_UP("messages", false),
  LLC_CONTROLLER_RESPONSE_DISCARD("messages", false),
  LLC_CONTROLLER_RESPONSE_KEEP("messages", false),
  LLC_CONTROLLER_RESPONSE_NOT_LEADER("messages", false),
  LLC_CONTROLLER_RESPONSE_FAILED("messages", false),
  LLC_CONTROLLER_RESPONSE_COMMIT_SUCCESS("messages", false);

  private final String meterName;
  private final String unit;
  private final boolean global;

  ServerMeter(String unit, boolean global) {
    this.unit = unit;
    this.global = global;
    this.meterName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getMeterName() {
    return meterName;
  }

  @Override
  public String getUnit() {
    return unit;
  }

  /**
   * Returns true if the metric is global (not attached to a particular resource)
   *
   * @return true if the metric is global
   */
  @Override
  public boolean isGlobal() {
    return global;
  }
}
