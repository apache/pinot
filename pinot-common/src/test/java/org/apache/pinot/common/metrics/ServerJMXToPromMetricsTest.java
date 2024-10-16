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
import io.prometheus.jmx.BuildInfoCollector;
import io.prometheus.jmx.JavaAgent;
import io.prometheus.jmx.JmxCollector;
import io.prometheus.jmx.common.http.ConfigurationException;
import io.prometheus.jmx.common.http.HTTPServerFactory;
import io.prometheus.jmx.shaded.io.prometheus.client.CollectorRegistry;
import io.prometheus.jmx.shaded.io.prometheus.client.exporter.HTTPServer;
import io.prometheus.jmx.shaded.io.prometheus.client.hotspot.DefaultExports;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class ServerJMXToPromMetricsTest extends PinotJMXToPromMetricsTest {

  private static final String EXPORTED_METRIC_PREFIX = "pinot_server_";
  private ServerMetrics _serverMetrics;

  private int _exporterPort;

  private HTTPServer _httpServer;

  @BeforeClass
  public void setup()
      throws Exception {

    _exporterPort = 9000 + new Random().nextInt(1000);
    String agentArgs = String.format("%s:%s", _exporterPort,
        "../docker/images/pinot/etc/jmx_prometheus_javaagent/configs/server.yml");

    String host = "0.0.0.0";

    try {
      JMXExporterConfig config = parseConfig(agentArgs, host);
      CollectorRegistry registry = new CollectorRegistry();
      (new JmxCollector(new File(config.file), JmxCollector.Mode.AGENT)).register(registry);
      DefaultExports.register(registry);
      _httpServer = (new HTTPServerFactory()).createHTTPServer(config.socket, registry, true, new File(config.file));
    } catch (ConfigurationException var4) {
      System.err.println("Configuration Exception : " + var4.getMessage());
      System.exit(1);
    } catch (IllegalArgumentException var5) {
      System.err.println("Usage: -javaagent:/path/to/JavaAgent.jar=[host:]<port>:<yaml configuration file> " + var5.getMessage());
      System.exit(1);
    }


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
  @Test
  public void serverTimerTest() {

    for (ServerTimer serverTimer : ServerTimer.values()) {
      if (serverTimer.isGlobal()) {
        _serverMetrics.addTimedValue(serverTimer, 30_000, TimeUnit.MILLISECONDS);
      } else {
        _serverMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, serverTimer, 30_000L, TimeUnit.MILLISECONDS);
        _serverMetrics.addTimedTableValue(RAW_TABLE_NAME, serverTimer, 30_000L, TimeUnit.MILLISECONDS);
      }
    }

    for (ServerTimer serverTimer : ServerTimer.values()) {
      if (serverTimer.isGlobal()) {
        assertTimerExportedCorrectly(serverTimer.getTimerName(), EXPORTED_METRIC_PREFIX);
      } else {
        assertTimerExportedCorrectly(serverTimer.getTimerName(), EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
            EXPORTED_METRIC_PREFIX);
        assertTimerExportedCorrectly(serverTimer.getTimerName(), EXPORTED_LABELS_FOR_RAW_TABLE_NAME,
            EXPORTED_METRIC_PREFIX);
      }
    }
  }

  /**
   * This test validates each meter defined in {@link ServerMeter}
   */
  @Test
  public void serverMeterTest() {
    //first, assert on all global meters
    Arrays.stream(ServerMeter.values()).filter(ServerMeter::isGlobal).peek(this::addGlobalMeter)
        .forEach(serverMeter -> {
          //we cannot use raw meter names for all meters as exported metrics don't follow any convention currently.
          // For example, meters that track realtime exceptions start with prefix "realtime_exceptions"
          if (meterTrackingRealtimeExceptions(serverMeter)) {
            assertMeterExportedCorrectly(getRealtimeExceptionMeterName(serverMeter));
          } else {
            assertMeterExportedCorrectly(serverMeter.getMeterName());
          }
        });

    //these meters accept the clientId
    List<ServerMeter> metersAcceptingClientId =
        List.of(ServerMeter.REALTIME_ROWS_CONSUMED, ServerMeter.REALTIME_ROWS_SANITIZED,
            ServerMeter.REALTIME_ROWS_FETCHED, ServerMeter.REALTIME_ROWS_FILTERED,
            ServerMeter.INVALID_REALTIME_ROWS_DROPPED, ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED,
            ServerMeter.STREAM_CONSUMER_CREATE_EXCEPTIONS, ServerMeter.ROWS_WITH_ERRORS);

    metersAcceptingClientId.stream().peek(meter -> addMeterWithLables(meter, CLIENT_ID))
        .forEach(meter -> assertMeterExportedCorrectly(meter.getMeterName(), EXPORTED_LABELS_FOR_CLIENT_ID));

    //these meters accept raw table name
    List<ServerMeter> metersAcceptingRawTableNames =
        List.of(ServerMeter.SEGMENT_UPLOAD_FAILURE, ServerMeter.SEGMENT_UPLOAD_SUCCESS,
            ServerMeter.SEGMENT_UPLOAD_TIMEOUT);

    metersAcceptingRawTableNames.stream().peek(meter -> addMeterWithLables(meter, RAW_TABLE_NAME)).forEach(meter -> {
      assertMeterExportedCorrectly(meter.getMeterName(), EXPORTED_LABELS_FOR_RAW_TABLE_NAME);
    });

    //remaining all meters accept tableNameWithType
    Arrays.stream(ServerMeter.values()).filter(
        serverMeter -> !serverMeter.isGlobal() && !metersAcceptingRawTableNames.contains(serverMeter)
            && !metersAcceptingClientId.contains(serverMeter)).forEach(serverMeter -> {
      addMeterWithLables(serverMeter, TABLE_NAME_WITH_TYPE);
      assertMeterExportedCorrectly(serverMeter.getMeterName(), EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    });
  }

  /**
   * This test validates each gauge defined in {@link ServerGauge}
   */
  @Test
  public void serverGaugeTest() {

    int partition = 3;
    long someVal = 100L;

    //global gauges
    Stream.of(ServerGauge.values()).filter(ServerGauge::isGlobal)
        .peek(gauge -> _serverMetrics.setValueOfGlobalGauge(gauge, 10L))
        .forEach(gauge -> assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_METRIC_PREFIX));

    //local gauges
    //gauges that accept clientId
    List<ServerGauge> gaugesAcceptingClientId =
        List.of(ServerGauge.LLC_PARTITION_CONSUMING, ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED,
            ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
            ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS,
            ServerGauge.LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS,
            ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS,
            ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS);

    gaugesAcceptingClientId.stream()
        .peek(gauge -> _serverMetrics.setValueOfTableGauge(CLIENT_ID, gauge, TimeUnit.MILLISECONDS.toSeconds(someVal)))
        .forEach(gauge -> assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_LABELS_FOR_CLIENT_ID,
            EXPORTED_METRIC_PREFIX));

    //gauges accepting partition
    List<ServerGauge> gaugesAcceptingPartition =
        List.of(ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT, ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT,
            ServerGauge.REALTIME_INGESTION_OFFSET_LAG, ServerGauge.REALTIME_INGESTION_DELAY_MS,
            ServerGauge.UPSERT_PRIMARY_KEYS_COUNT, ServerGauge.END_TO_END_REALTIME_INGESTION_DELAY_MS);

    gaugesAcceptingPartition.stream()
        .peek(gauge -> _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, partition, gauge, someVal))
        .forEach(gauge -> assertGaugeExportedCorrectly(gauge.getGaugeName(),
            EXPORTED_LABELS_FOR_PARTITION_TABLE_NAME_AND_TYPE, EXPORTED_METRIC_PREFIX));

    //gauges accepting raw table name
    List<ServerGauge> gaugesAcceptingRawTableName =
        List.of(ServerGauge.REALTIME_OFFHEAP_MEMORY_USED, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS,
            ServerGauge.LUCENE_INDEXING_DELAY_MS, ServerGauge.LUCENE_INDEXING_DELAY_DOCS);

    gaugesAcceptingRawTableName.stream().peek(gauge -> _serverMetrics.setValueOfTableGauge(RAW_TABLE_NAME, gauge, 5L))
        .forEach(gauge -> assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_LABELS_FOR_RAW_TABLE_NAME,
            EXPORTED_METRIC_PREFIX));

    //all remaining gauges
    Stream.of(ServerGauge.values()).filter(gauge -> !gauge.isGlobal()).filter(
            gauge -> (!gaugesAcceptingClientId.contains(gauge) && !gaugesAcceptingPartition.contains(gauge)
                && !gaugesAcceptingRawTableName.contains(gauge)))
        .peek(gauge -> _serverMetrics.setValueOfTableGauge(TABLE_NAME_WITH_TYPE, gauge, someVal)).forEach(
            gauge -> assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE,
                EXPORTED_METRIC_PREFIX));

    //this gauge is currently exported as: `pinot_server_3_Value{database="dedupPrimaryKeysCount",
    // table="dedupPrimaryKeysCount.myTable",tableType="REALTIME",}`. We add an explicit test for it to maintain
    // backward compatibility. todo: ServerGauge.DEDUP_PRIMARY_KEYS_COUNT should be moved to
    //  gaugesThatAcceptPartition. It should be exported as: `pinot_server_dedupPrimaryKeysCount_Value{partition="3",
    //  table="myTable",tableType="REALTIME",}`
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, partition, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT, 5L);
    assertGaugeExportedCorrectly(String.valueOf(partition),
        List.of("database", "dedupPrimaryKeysCount", "table", "dedupPrimaryKeysCount.myTable", "tableType", "REALTIME"),
        EXPORTED_METRIC_PREFIX);
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
      return _httpClient.sendGetRequest(new URI("http://localhost:" + _exporterPort + "/metrics"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
