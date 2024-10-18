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

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import io.prometheus.jmx.shaded.io.prometheus.client.exporter.HTTPServer;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class ServerJMXToPromMetricsTest extends PinotJMXToPromMetricsTest {

  private static final String EXPORTED_METRIC_PREFIX = "pinot_server_";
  private ServerMetrics _serverMetrics;
  private HTTPServer _httpServer;

  @BeforeClass
  public void setup()
      throws Exception {

    _httpServer = startExporter(PinotComponent.SERVER);

    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);

    // Initialize ServerMetrics with the registry
    YammerMetricsRegistry yammerMetricsRegistry = new YammerMetricsRegistry();
    _serverMetrics = new ServerMetrics(yammerMetricsRegistry);

    // Enable JMX reporting
    MetricsRegistry metricsRegistry = (MetricsRegistry) yammerMetricsRegistry.getMetricsRegistry();
    JmxReporter jmxReporter = new JmxReporter(metricsRegistry);
    jmxReporter.start();

    _httpClient = new HttpClient();
  }

  /**
   * This test validates each timer defined in {@link ServerTimer}
   */
  @Test(dataProvider = "serverTimers")
  public void timerTest(ServerTimer serverTimer) {
    if (serverTimer.isGlobal()) {
      _serverMetrics.addTimedValue(serverTimer, 30_000, TimeUnit.MILLISECONDS);
    } else {
      _serverMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, serverTimer, 30_000L, TimeUnit.MILLISECONDS);
      _serverMetrics.addTimedTableValue(LABEL_VAL_RAW_TABLENAME, serverTimer, 30_000L, TimeUnit.MILLISECONDS);
    }
    if (serverTimer.isGlobal()) {
      assertTimerExportedCorrectly(serverTimer.getTimerName(), EXPORTED_METRIC_PREFIX);
    } else {
      assertTimerExportedCorrectly(serverTimer.getTimerName(), EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
          EXPORTED_METRIC_PREFIX);
      assertTimerExportedCorrectly(serverTimer.getTimerName(), EXPORTED_LABELS_FOR_RAW_TABLE_NAME,
          EXPORTED_METRIC_PREFIX);
    }
  }

  /**
   * This test validates each meter defined in {@link ServerMeter}
   */
  @Test(dataProvider = "serverMeters")
  public void meterTest(ServerMeter serverMeter) {

    //first, assert on all global meters
    if (serverMeter.isGlobal()) {
      addGlobalMeter(serverMeter);
      //we cannot use raw meter names for all meters as exported metrics don't follow any convention currently.
      // For example, meters that track realtime exceptions start with prefix "realtime_exceptions"
      if (meterTrackingRealtimeExceptions(serverMeter)) {
        assertMeterExportedCorrectly(getRealtimeExceptionMeterName(serverMeter));
      } else {
        assertMeterExportedCorrectly(serverMeter.getMeterName());
      }
    } else {

      //these meters accept the clientId
      List<ServerMeter> metersAcceptingClientId =
          List.of(ServerMeter.REALTIME_ROWS_CONSUMED, ServerMeter.REALTIME_ROWS_SANITIZED,
              ServerMeter.REALTIME_ROWS_FETCHED, ServerMeter.REALTIME_ROWS_FILTERED,
              ServerMeter.INVALID_REALTIME_ROWS_DROPPED, ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED,
              ServerMeter.STREAM_CONSUMER_CREATE_EXCEPTIONS, ServerMeter.ROWS_WITH_ERRORS);

      //these meters accept raw table name
      List<ServerMeter> metersAcceptingRawTableNames =
          List.of(ServerMeter.SEGMENT_UPLOAD_FAILURE, ServerMeter.SEGMENT_UPLOAD_SUCCESS,
              ServerMeter.SEGMENT_UPLOAD_TIMEOUT);

      if (metersAcceptingClientId.contains(serverMeter)) {
        addMeterWithLables(serverMeter, CLIENT_ID);
        assertMeterExportedCorrectly(serverMeter.getMeterName(), EXPORTED_LABELS_FOR_CLIENT_ID);
      } else if (metersAcceptingRawTableNames.contains(serverMeter)) {
        addMeterWithLables(serverMeter, LABEL_VAL_RAW_TABLENAME);
        assertMeterExportedCorrectly(serverMeter.getMeterName(), EXPORTED_LABELS_FOR_RAW_TABLE_NAME);
      } else {
        addMeterWithLables(serverMeter, TABLE_NAME_WITH_TYPE);
        assertMeterExportedCorrectly(serverMeter.getMeterName(), EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
      }
    }
  }

  /**
   * This test validates each gauge defined in {@link ServerGauge}
   */
  @Test(dataProvider = "serverGauges")
  public void gaugeTest(ServerGauge serverGauge) {

    int partition = 3;
    long someVal = 100L;

    if (serverGauge.isGlobal()) {
      _serverMetrics.setValueOfGlobalGauge(serverGauge, 10L);
      assertGaugeExportedCorrectly(serverGauge.getGaugeName(), EXPORTED_METRIC_PREFIX);
    } else {
      //gauges that accept clientId
      List<ServerGauge> gaugesAcceptingClientId =
          List.of(ServerGauge.LLC_PARTITION_CONSUMING, ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED,
              ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
              ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS,
              ServerGauge.LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS,
              ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS,
              ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS);

      List<ServerGauge> gaugesAcceptingPartition =
          List.of(ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT, ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT,
              ServerGauge.REALTIME_INGESTION_OFFSET_LAG, ServerGauge.REALTIME_INGESTION_DELAY_MS,
              ServerGauge.UPSERT_PRIMARY_KEYS_COUNT, ServerGauge.END_TO_END_REALTIME_INGESTION_DELAY_MS,
              ServerGauge.DEDUP_PRIMARY_KEYS_COUNT);

      List<ServerGauge> gaugesAcceptingRawTableName =
          List.of(ServerGauge.REALTIME_OFFHEAP_MEMORY_USED, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS,
              ServerGauge.LUCENE_INDEXING_DELAY_MS, ServerGauge.LUCENE_INDEXING_DELAY_DOCS);

      if (gaugesAcceptingClientId.contains(serverGauge)) {
        _serverMetrics.setValueOfTableGauge(CLIENT_ID, serverGauge, TimeUnit.MILLISECONDS.toSeconds(someVal));
        assertGaugeExportedCorrectly(serverGauge.getGaugeName(), EXPORTED_LABELS_FOR_CLIENT_ID,
            EXPORTED_METRIC_PREFIX);
      } else if (gaugesAcceptingPartition.contains(serverGauge)) {
        _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, partition, serverGauge, someVal);
        assertGaugeExportedCorrectly(serverGauge.getGaugeName(),
            EXPORTED_LABELS_FOR_PARTITION_TABLE_NAME_AND_TYPE, EXPORTED_METRIC_PREFIX);
      } else if (gaugesAcceptingRawTableName.contains(serverGauge)) {
        _serverMetrics.setValueOfTableGauge(LABEL_VAL_RAW_TABLENAME, serverGauge, 5L);
        assertGaugeExportedCorrectly(serverGauge.getGaugeName(), EXPORTED_LABELS_FOR_RAW_TABLE_NAME,
            EXPORTED_METRIC_PREFIX);
      } else {
        _serverMetrics.setValueOfTableGauge(TABLE_NAME_WITH_TYPE, serverGauge, someVal);
        assertGaugeExportedCorrectly(serverGauge.getGaugeName(), EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
            EXPORTED_METRIC_PREFIX);
      }
    }

    //this gauge is currently exported as: `pinot_server_3_Value{database="dedupPrimaryKeysCount",
    // table="dedupPrimaryKeysCount.myTable",tableType="REALTIME",}`. We add an explicit test for it to maintain
    // backward compatibility. todo: ServerGauge.DEDUP_PRIMARY_KEYS_COUNT should be moved to
    //  gaugesThatAcceptPartition. It should be exported as: `pinot_server_dedupPrimaryKeysCount_Value{partition="3",
    //  table="myTable",tableType="REALTIME",}`
//    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, partition, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT, 5L);
//    assertGaugeExportedCorrectly(String.valueOf(partition),
//        List.of(LABEL_KEY_DATABASE, "dedupPrimaryKeysCount", LABEL_KEY_TABLE, "dedupPrimaryKeysCount.myTable",
//            LABEL_KEY_TABLETYPE, LABEL_VAL_TABLETYPE_REALTIME), EXPORTED_METRIC_PREFIX);
  }

  public void addGlobalMeter(ServerMeter serverMeter) {
    _serverMetrics.addMeteredGlobalValue(serverMeter, 4L);
  }

  public void addMeterWithLables(ServerMeter serverMeter, String label) {
    _serverMetrics.addMeteredTableValue(label, serverMeter, 4L);
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

  @Override
  protected SimpleHttpResponse getExportedPromMetrics() {
    try {
      return _httpClient.sendGetRequest(new URI("http://localhost:" + _httpServer.getPort() + "/metrics"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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

  @AfterClass
  public void cleanup() {
    _httpServer.close();
  }
}
