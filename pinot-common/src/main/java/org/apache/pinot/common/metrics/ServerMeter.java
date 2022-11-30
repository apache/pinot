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
 * Enumeration containing all the meters exposed by the Pinot server.
 */
public enum ServerMeter implements AbstractMetrics.Meter {
  QUERIES("queries", true),
  UNCAUGHT_EXCEPTIONS("exceptions", true),
  REQUEST_DESERIALIZATION_EXCEPTIONS("exceptions", true),
  RESPONSE_SERIALIZATION_EXCEPTIONS("exceptions", true),
  SCHEDULING_TIMEOUT_EXCEPTIONS("exceptions", true),
  QUERY_EXECUTION_EXCEPTIONS("exceptions", false),
  HELIX_ZOOKEEPER_RECONNECTS("reconnects", true),
  DELETED_SEGMENT_COUNT("segments", false),
  DELETE_TABLE_FAILURES("tables", false),
  REALTIME_ROWS_CONSUMED("rows", true),
  INVALID_REALTIME_ROWS_DROPPED("rows", false),
  INCOMPLETE_REALTIME_ROWS_CONSUMED("rows", false),
  REALTIME_CONSUMPTION_EXCEPTIONS("exceptions", true),
  REALTIME_OFFSET_COMMITS("commits", true),
  REALTIME_OFFSET_COMMIT_EXCEPTIONS("exceptions", false),
  REALTIME_PARTITION_MISMATCH("mismatch", false),
  REALTIME_DEDUP_DROPPED("rows", false),
  UPSERT_KEYS_IN_WRONG_SEGMENT("rows", false),
  PARTIAL_UPSERT_OUT_OF_ORDER("rows", false),
  PARTIAL_UPSERT_KEYS_NOT_REPLACED("rows", false),
  ROWS_WITH_ERRORS("rows", false),
  LLC_CONTROLLER_RESPONSE_NOT_SENT("messages", true),
  LLC_CONTROLLER_RESPONSE_COMMIT("messages", true),
  LLC_CONTROLLER_RESPONSE_HOLD("messages", true),
  LLC_CONTROLLER_RESPONSE_CATCH_UP("messages", true),
  LLC_CONTROLLER_RESPONSE_DISCARD("messages", true),
  LLC_CONTROLLER_RESPONSE_KEEP("messages", true),
  LLC_CONTROLLER_RESPONSE_NOT_LEADER("messages", true),
  LLC_CONTROLLER_RESPONSE_FAILED("messages", true),
  LLC_CONTROLLER_RESPONSE_COMMIT_SUCCESS("messages", true),
  LLC_CONTROLLER_RESPONSE_COMMIT_CONTINUE("messages", true),
  LLC_CONTROLLER_RESPONSE_PROCESSED("messages", true),
  LLC_CONTROLLER_RESPONSE_UPLOAD_SUCCESS("messages", true),
  NUM_DOCS_SCANNED("rows", false),
  NUM_ENTRIES_SCANNED_IN_FILTER("entries", false),
  NUM_ENTRIES_SCANNED_POST_FILTER("entries", false),
  NUM_SEGMENTS_QUERIED("numSegmentsQueried", false),
  NUM_SEGMENTS_PROCESSED("numSegmentsProcessed", false),
  NUM_SEGMENTS_MATCHED("numSegmentsMatched", false),
  NUM_MISSING_SEGMENTS("segments", false),
  RELOAD_FAILURES("segments", false),
  REFRESH_FAILURES("segments", false),
  UNTAR_FAILURES("segments", false),
  SEGMENT_STREAMED_DOWNLOAD_UNTAR_FAILURES("segments", false),
  SEGMENT_DIR_MOVEMENT_FAILURES("segments", false),
  SEGMENT_DOWNLOAD_FAILURES("segments", false),
  SEGMENT_DOWNLOAD_FROM_REMOTE_FAILURES("segments", false),
  SEGMENT_DOWNLOAD_FROM_PEERS_FAILURES("segments", false),
  NUM_RESIZES("numResizes", false),
  NO_TABLE_ACCESS("tables", true),
  INDEXING_FAILURES("attributeValues", true),

  READINESS_CHECK_OK_CALLS("readinessCheck", true),
  READINESS_CHECK_BAD_CALLS("readinessCheck", true),
  QUERIES_PREEMPTED("query", true),

  // Netty connection metrics
  NETTY_CONNECTION_BYTES_RECEIVED("nettyConnection", true),
  NETTY_CONNECTION_RESPONSES_SENT("nettyConnection", true),
  NETTY_CONNECTION_BYTES_SENT("nettyConnection", true),

  NUM_SEGMENTS_PRUNED_INVALID("numSegmentsPrunedInvalid", false),
  NUM_SEGMENTS_PRUNED_BY_LIMIT("numSegmentsPrunedByLimit", false),
  NUM_SEGMENTS_PRUNED_BY_VALUE("numSegmentsPrunedByValue", false),;

  private final String _meterName;
  private final String _unit;
  private final boolean _global;

  ServerMeter(String unit, boolean global) {
    _unit = unit;
    _global = global;
    _meterName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getMeterName() {
    return _meterName;
  }

  @Override
  public String getUnit() {
    return _unit;
  }

  /**
   * Returns true if the metric is global (not attached to a particular resource)
   *
   * @return true if the metric is global
   */
  @Override
  public boolean isGlobal() {
    return _global;
  }
}
