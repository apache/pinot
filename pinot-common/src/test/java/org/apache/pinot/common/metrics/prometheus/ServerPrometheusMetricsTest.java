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
package org.apache.pinot.common.metrics.prometheus;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public abstract class ServerPrometheusMetricsTest extends PinotPrometheusMetricsTest {
  //all exported server metrics have this prefix
  private static final String EXPORTED_METRIC_PREFIX = "pinot_server_";

  private static final List<ServerMeter> METERS_ACCEPTING_CLIENT_ID =
      List.of(ServerMeter.REALTIME_ROWS_CONSUMED, ServerMeter.REALTIME_ROWS_SANITIZED,
          ServerMeter.REALTIME_ROWS_FETCHED, ServerMeter.REALTIME_ROWS_FILTERED,
          ServerMeter.INVALID_REALTIME_ROWS_DROPPED, ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED,
          ServerMeter.STREAM_CONSUMER_CREATE_EXCEPTIONS, ServerMeter.ROWS_WITH_ERRORS);

  private static final List<ServerMeter> METERS_ACCEPTING_RAW_TABLE_NAMES =
      List.of(ServerMeter.SEGMENT_UPLOAD_FAILURE, ServerMeter.SEGMENT_UPLOAD_SUCCESS,
          ServerMeter.SEGMENT_UPLOAD_TIMEOUT);

  //gauges that accept clientId
  private static final List<ServerGauge> GAUGES_ACCEPTING_CLIENT_ID =
      List.of(ServerGauge.LLC_PARTITION_CONSUMING, ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED,
          ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
          ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS,
          ServerGauge.LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS,
          ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS,
          ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS);

  private static final List<ServerGauge> GAUGES_ACCEPTING_PARTITION =
      List.of(ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT, ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT,
          ServerGauge.REALTIME_INGESTION_OFFSET_LAG, ServerGauge.REALTIME_INGESTION_DELAY_MS,
          ServerGauge.UPSERT_PRIMARY_KEYS_COUNT, ServerGauge.END_TO_END_REALTIME_INGESTION_DELAY_MS,
          ServerGauge.DEDUP_PRIMARY_KEYS_COUNT);

  private static final List<ServerGauge> GAUGES_ACCEPTING_RAW_TABLE_NAME =
      List.of(ServerGauge.REALTIME_OFFHEAP_MEMORY_USED, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS,
          ServerGauge.LUCENE_INDEXING_DELAY_MS, ServerGauge.LUCENE_INDEXING_DELAY_DOCS);

  private ServerMetrics _serverMetrics;

  @BeforeClass
  public void setup()
      throws Exception {
    _serverMetrics = new ServerMetrics(_pinotMetricsFactory.getPinotMetricsRegistry());
  }

  @Test(dataProvider = "serverTimers")
  public void timerTest(ServerTimer serverTimer) {
    if (serverTimer.isGlobal()) {
      _serverMetrics.addTimedValue(serverTimer, 30_000, TimeUnit.MILLISECONDS);
      assertTimerExportedCorrectly(serverTimer.getTimerName(), EXPORTED_METRIC_PREFIX);
    } else {
      _serverMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, serverTimer, 30_000L, TimeUnit.MILLISECONDS);
      _serverMetrics.addTimedTableValue(ExportedLabelValues.TABLENAME, serverTimer, 30_000L, TimeUnit.MILLISECONDS);

      assertTimerExportedCorrectly(serverTimer.getTimerName(), ExportedLabels.TABLENAME_TABLETYPE,
          EXPORTED_METRIC_PREFIX);
      assertTimerExportedCorrectly(serverTimer.getTimerName(), ExportedLabels.TABLENAME, EXPORTED_METRIC_PREFIX);
    }
  }

  @Test(dataProvider = "serverMeters")
  public void meterTest(ServerMeter serverMeter) {
    if (serverMeter.isGlobal()) {
      _serverMetrics.addMeteredGlobalValue(serverMeter, 4L);
      //we cannot use raw meter names for all meters as exported metrics don't follow any convention currently.
      // For example, meters that track realtime exceptions start with prefix "realtime_exceptions"
      if (meterTrackingRealtimeExceptions(serverMeter)) {
        assertMeterExportedCorrectly(getRealtimeExceptionMeterName(serverMeter));
      } else {
        assertMeterExportedCorrectly(serverMeter.getMeterName());
      }
    } else {
      if (METERS_ACCEPTING_CLIENT_ID.contains(serverMeter)) {
        addMeterWithLabels(serverMeter, CLIENT_ID);
        assertMeterExportedCorrectly(serverMeter.getMeterName(),
            ExportedLabels.PARTITION_TABLENAME_TABLETYPE_KAFKATOPIC);
      } else if (METERS_ACCEPTING_RAW_TABLE_NAMES.contains(serverMeter)) {
        addMeterWithLabels(serverMeter, ExportedLabelValues.TABLENAME);
        assertMeterExportedCorrectly(serverMeter.getMeterName(), ExportedLabels.TABLENAME);
      } else {
        //we pass tableNameWithType to all remaining meters
        addMeterWithLabels(serverMeter, TABLE_NAME_WITH_TYPE);
        assertMeterExportedCorrectly(serverMeter.getMeterName(), ExportedLabels.TABLENAME_TABLETYPE);
      }
    }
  }

  @Test(dataProvider = "serverGauges")
  public void gaugeTest(ServerGauge serverGauge) {
    if (serverGauge.isGlobal()) {
      _serverMetrics.setValueOfGlobalGauge(serverGauge, 10L);
      assertGaugeExportedCorrectly(serverGauge.getGaugeName(), EXPORTED_METRIC_PREFIX);
    } else {
      if (GAUGES_ACCEPTING_CLIENT_ID.contains(serverGauge)) {
        addGaugeWithLabels(serverGauge, CLIENT_ID);
        assertGaugeExportedCorrectly(serverGauge.getGaugeName(),
            ExportedLabels.PARTITION_TABLENAME_TABLETYPE_KAFKATOPIC, EXPORTED_METRIC_PREFIX);
      } else if (GAUGES_ACCEPTING_PARTITION.contains(serverGauge)) {
        addPartitionGaugeWithLabels(serverGauge, TABLE_NAME_WITH_TYPE);
        assertGaugeExportedCorrectly(serverGauge.getGaugeName(), ExportedLabels.PARTITION_TABLENAME_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
      } else if (GAUGES_ACCEPTING_RAW_TABLE_NAME.contains(serverGauge)) {
        addGaugeWithLabels(serverGauge, ExportedLabelValues.TABLENAME);
        assertGaugeExportedCorrectly(serverGauge.getGaugeName(), ExportedLabels.TABLENAME, EXPORTED_METRIC_PREFIX);
      } else {
        addGaugeWithLabels(serverGauge, TABLE_NAME_WITH_TYPE);
        assertGaugeExportedCorrectly(serverGauge.getGaugeName(), ExportedLabels.TABLENAME_TABLETYPE,
            EXPORTED_METRIC_PREFIX);
      }
    }
  }

  private void addGaugeWithLabels(ServerGauge serverGauge, String labels) {
    _serverMetrics.setValueOfTableGauge(labels, serverGauge, 100L);
  }

  private void addPartitionGaugeWithLabels(ServerGauge serverGauge, String labels) {
    _serverMetrics.setValueOfPartitionGauge(labels, 3, serverGauge, 100L);
  }

  public void addMeterWithLabels(ServerMeter serverMeter, String labels) {
    _serverMetrics.addMeteredTableValue(labels, serverMeter, 4L);
  }

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

  private boolean meterTrackingRealtimeExceptions(ServerMeter serverMeter) {
    return serverMeter == ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS
        || serverMeter == ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS
        || serverMeter == ServerMeter.SCHEDULING_TIMEOUT_EXCEPTIONS || serverMeter == ServerMeter.UNCAUGHT_EXCEPTIONS;
  }

  private String getRealtimeExceptionMeterName(ServerMeter serverMeter) {
    String meterName = serverMeter.getMeterName();
    return "realtime_exceptions_" + meterName.substring(0, meterName.lastIndexOf("Exceptions"));
  }

  private void assertMeterExportedCorrectly(String exportedMeterName) {
    assertMeterExportedCorrectly(exportedMeterName, EXPORTED_METRIC_PREFIX);
  }

  private void assertMeterExportedCorrectly(String exportedMeterName, List<String> labels) {
    assertMeterExportedCorrectly(exportedMeterName, labels, EXPORTED_METRIC_PREFIX);
  }
}
