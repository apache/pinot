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
* Enumeration containing all the gauges exposed by the Pinot server.
*
*/
public enum ServerGauge implements AbstractMetrics.Gauge {
  DOCUMENT_COUNT("documents", false),
  SEGMENT_COUNT("segments", false),
  LLC_PARTITION_CONSUMING("state", false),
  HIGHEST_KAFKA_OFFSET_CONSUMED("messages", false),
  LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS("seconds", false),
  LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS("seconds", false),
  LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS("seconds", false),
  LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS("seconds", false),
  KAFKA_PARTITION_OFFSET_LAG("messages", false),
  REALTIME_OFFHEAP_MEMORY_USED("bytes", false),
  RUNNING_QUERIES("runningQueries", false),
  NUM_SEGMENTS_SEARCHED("numSegmentsSearched", false),
  REALTIME_SEGMENT_PARTITION_WIDTH("realtimeSegmentPartitionWidth", false);

  private final String gaugeName;
  private final String unit;
  private final boolean global;

  ServerGauge(String unit, boolean global) {
    this.unit = unit;
    this.global = global;
    this.gaugeName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getGaugeName() {
    return gaugeName;
  }

  @Override
  public String getUnit() {
    return unit;
  }

  /**
   * Returns true if the gauge is global (not attached to a particular resource)
   *
   * @return true if the gauge is global
   */
  @Override
  public boolean isGlobal() {
    return global;
  }
}
