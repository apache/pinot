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

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public abstract class ServerPrometheusMetricsTest extends PinotPrometheusMetricsTest {
  //all exported server metrics have this prefix
  protected static final String EXPORTED_METRIC_PREFIX = "pinot_server_";

  protected static final List<ServerMeter> METERS_ACCEPTING_CLIENT_ID =
      List.of(ServerMeter.REALTIME_ROWS_CONSUMED, ServerMeter.REALTIME_ROWS_SANITIZED,
          ServerMeter.REALTIME_ROWS_FETCHED, ServerMeter.REALTIME_ROWS_FILTERED,
          ServerMeter.INVALID_REALTIME_ROWS_DROPPED, ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED,
          ServerMeter.STREAM_CONSUMER_CREATE_EXCEPTIONS, ServerMeter.ROWS_WITH_ERRORS);

  protected static final List<ServerMeter> METERS_ACCEPTING_RAW_TABLE_NAMES =
      List.of(ServerMeter.SEGMENT_UPLOAD_FAILURE, ServerMeter.SEGMENT_UPLOAD_SUCCESS,
          ServerMeter.SEGMENT_UPLOAD_TIMEOUT);

  //gauges that accept clientId
  protected static final List<ServerGauge> GAUGES_ACCEPTING_CLIENT_ID =
      List.of(ServerGauge.LLC_PARTITION_CONSUMING, ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED,
          ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
          ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS,
          ServerGauge.LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS,
          ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS,
          ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS);

  protected static final List<ServerGauge> GAUGES_ACCEPTING_PARTITION =
      List.of(ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT, ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT,
          ServerGauge.REALTIME_INGESTION_OFFSET_LAG, ServerGauge.REALTIME_INGESTION_DELAY_MS,
          ServerGauge.UPSERT_PRIMARY_KEYS_COUNT, ServerGauge.END_TO_END_REALTIME_INGESTION_DELAY_MS,
          ServerGauge.DEDUP_PRIMARY_KEYS_COUNT);

  protected static final List<ServerGauge> GAUGES_ACCEPTING_RAW_TABLE_NAME =
      List.of(ServerGauge.REALTIME_OFFHEAP_MEMORY_USED, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS,
          ServerGauge.LUCENE_INDEXING_DELAY_MS, ServerGauge.LUCENE_INDEXING_DELAY_DOCS);

  @DataProvider(name = "serverTimers")
  public Object[] serverTimers() {
    return ServerTimer.values();  // Provide all values of ServerTimer enum
  }

  @DataProvider(name = "serverMeters")
  public Object[] serverMeter() {
    return ServerMeter.values();  // Provide all values of ServerTimer enum
  }

  @DataProvider(name = "serverGauges")
  public Object[] serverGauge() {
    return ServerGauge.values();  // Provide all values of ServerTimer enum
  }

  @Override
  protected PinotComponent getPinotComponent() {
    return PinotComponent.SERVER;
  }
}
