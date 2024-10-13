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
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class ServerJMXToPromMetricsTest extends PinotJMXToPromMetricsTest {

  private static final String EXPORTED_METRIC_PREFIX = "pinot_server_";
  private ServerMetrics _serverMetrics;

  @BeforeClass
  public void setup() {
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
          try {
            //we cannot use raw meter names for all meters as exported metrics don't follow any convention currently.
            // For example, meters that track realtime exceptions start with prefix "realtime_exceptions"
            if (meterTracksRealtimeExceptions(serverMeter)) {
              assertMeterExportedCorrectly(getRealtimeExceptionMeterName(serverMeter));
            } else {
              assertMeterExportedCorrectly(serverMeter.getMeterName());
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
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
  public void serverGaugeTest()
      throws Exception {

    //get all exposed metrics before we expose any timers
    List<PromMetric> promMetricsBefore = parseExportedPromMetrics(getExportedPromMetrics().getResponse());

    int partition = 3;
    long someVal = 100L;
    Supplier<Long> someValSupplier = () -> someVal;

    //global gauges

    _serverMetrics.setValueOfGlobalGauge(ServerGauge.VERSION, PinotVersion.VERSION_METRIC_NAME, someVal);
    _serverMetrics.addValueToGlobalGauge(ServerGauge.LLC_SIMULTANEOUS_SEGMENT_BUILDS, someVal);
    _serverMetrics.setValueOfGlobalGauge(ServerGauge.JVM_HEAP_USED_BYTES, someVal);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_USED_DIRECT_MEMORY, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_USED_HEAP_MEMORY, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_ARENAS_DIRECT, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_ARENAS_HEAP, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_CACHE_SIZE_SMALL, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_CACHE_SIZE_NORMAL, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_CHUNK_SIZE, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_THREADLOCALCACHE, someValSupplier);

    Stream.of(ServerGauge.values()).peek(gauge -> _serverMetrics.setOrUpdateGlobalGauge(gauge, someValSupplier))
        .forEach(gauge -> {
          try {
            assertGaugeExportedCorrectly(gauge.getGaugeName(), EXPORTED_METRIC_PREFIX);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    //local gauges
    _serverMetrics.addValueToTableGauge(TABLE_NAME_WITH_TYPE, ServerGauge.DOCUMENT_COUNT, someVal);
    _serverMetrics.addValueToTableGauge(TABLE_NAME_WITH_TYPE, ServerGauge.SEGMENT_COUNT, someVal);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 2, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT, someVal);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 2, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT, someVal);

    //raw table name
    _serverMetrics.addValueToTableGauge(RAW_TABLE_NAME, ServerGauge.REALTIME_OFFHEAP_MEMORY_USED, someVal);
    _serverMetrics.setValueOfTableGauge(RAW_TABLE_NAME, ServerGauge.REALTIME_MERGED_TEXT_IDX_DOCUMENT_AVG_LEN, someVal);
    _serverMetrics.addValueToTableGauge(RAW_TABLE_NAME, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS, someVal);

    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LLC_PARTITION_CONSUMING, someVal);
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED, someVal);
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID,
        ServerGauge.LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(RAW_TABLE_NAME, ServerGauge.CONSUMPTION_QUOTA_UTILIZATION, someVal);

    _serverMetrics.setValueOfTableGauge(TABLE_STREAM_NAME, ServerGauge.STREAM_DATA_LOSS, 1L);

    _serverMetrics.setOrUpdatePartitionGauge(TABLE_NAME_WITH_TYPE, partition, ServerGauge.REALTIME_INGESTION_DELAY_MS,
        someValSupplier);
    _serverMetrics.setOrUpdatePartitionGauge(TABLE_NAME_WITH_TYPE, partition,
        ServerGauge.END_TO_END_REALTIME_INGESTION_DELAY_MS, someValSupplier);
    _serverMetrics.setOrUpdatePartitionGauge(RAW_TABLE_NAME, partition, ServerGauge.LUCENE_INDEXING_DELAY_MS,
        someValSupplier);
    _serverMetrics.setOrUpdatePartitionGauge(RAW_TABLE_NAME, partition, ServerGauge.LUCENE_INDEXING_DELAY_DOCS,
        someValSupplier);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, partition,
        ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT, someVal);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, partition,
        ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT, someVal);
    _serverMetrics.setOrUpdatePartitionGauge(TABLE_NAME_WITH_TYPE, partition, ServerGauge.REALTIME_INGESTION_OFFSET_LAG,
        someValSupplier);

    List<String> labels = List.of("partition", String.valueOf(partition), "table", RAW_TABLE_NAME, "tableType",
        TableType.REALTIME.toString());

    assertGaugeExportedCorrectly("realtimeIngestionOffsetLag", labels, EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("upsertPrimaryKeysInSnapshotCount", labels, EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("upsertValidDocIdSnapshotCount", labels, EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("endToEndRealtimeIngestionDelayMs", labels, EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("realtimeIngestionDelayMs", labels, EXPORTED_METRIC_PREFIX);

//    we only match the metric name for version as the label value is dynamic (based on project version).
    Optional<PromMetric> pinotServerVersionMetricMaybe =
        parseExportedPromMetrics(getExportedPromMetrics().getResponse()).stream()
            .filter(exportedMetric -> exportedMetric.getMetricName().contains("version")).findAny();

    Assert.assertTrue(pinotServerVersionMetricMaybe.isPresent());

    assertGaugeExportedCorrectly("llcSimultaneousSegmentBuilds", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledUsedDirectMemory", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledUsedHeapMemory", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledUsedHeapMemory", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledArenasDirect", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledArenasHeap", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledCacheSizeSmall", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledCacheSizeNormal", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("nettyPooledChunkSize", EXPORTED_METRIC_PREFIX);
    assertGaugeExportedCorrectly("jvmHeapUsedBytes", EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("llcPartitionConsuming", EXPORTED_LABELS_FOR_CLIENT_ID, EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("highestStreamOffsetConsumed", EXPORTED_LABELS_FOR_CLIENT_ID, EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("lastRealtimeSegmentCreationWaitTimeSeconds", EXPORTED_LABELS_FOR_CLIENT_ID,
        EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("lastRealtimeSegmentInitialConsumptionDurationSeconds", EXPORTED_LABELS_FOR_CLIENT_ID,
        EXPORTED_METRIC_PREFIX);

    assertGaugeExportedCorrectly("lastRealtimeSegmentCatchupDurationSeconds", EXPORTED_LABELS_FOR_CLIENT_ID,
        EXPORTED_METRIC_PREFIX);

    List<PromMetric> promMetricsAfter = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    Assert.assertEquals(promMetricsAfter.size() - promMetricsBefore.size(), ServerGauge.values().length);
  }

  public void addGlobalMeter(ServerMeter serverMeter) {
    _serverMetrics.addMeteredGlobalValue(serverMeter, 4L);
  }

  public void addMeterWithLables(ServerMeter serverMeter, String label) {
    _serverMetrics.addMeteredTableValue(label, serverMeter, 4L);
  }

  private boolean meterTracksRealtimeExceptions(ServerMeter serverMeter) {
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
