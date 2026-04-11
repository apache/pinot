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
package org.apache.pinot.controller.api;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.controller.utils.FakeHttpServer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


/**
 * Tests compression stats aggregation in TableSizeReader.
 */
public class TableSizeReaderCompressionStatsTest {
  private static final String URI_PATH = "/table/";
  private static final int TIMEOUT_MSEC = 10000;
  private static final int NUM_REPLICAS = 2;

  private final Executor _executor = Executors.newFixedThreadPool(1);
  private final HttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private final ControllerMetrics _controllerMetrics =
      new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
  private final Map<String, FakeCompressionServer> _serverMap = new HashMap<>();
  private PinotHelixResourceManager _helix;
  private LeadControllerManager _leadControllerManager;

  @BeforeClass
  public void setUp()
      throws IOException {
    _helix = mock(PinotHelixResourceManager.class);
    _leadControllerManager = mock(LeadControllerManager.class);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("compressionTable").setNumReplicas(NUM_REPLICAS).build();
    tableConfig.getIndexingConfig().setCompressionStatsEnabled(true);

    TableConfig flagOffTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("flagOffTable").setNumReplicas(NUM_REPLICAS).build();
    // compressionStatsEnabled defaults to false — do NOT enable it

    ZkHelixPropertyStore mockPropertyStore = mock(ZkHelixPropertyStore.class);

    when(mockPropertyStore.get(ArgumentMatchers.anyString(), ArgumentMatchers.eq(null),
        ArgumentMatchers.eq(AccessOption.PERSISTENT))).thenAnswer((Answer) invocationOnMock -> {
          String path = (String) invocationOnMock.getArguments()[0];
          if (path.contains("offline_OFFLINE")) {
            return TableConfigSerDeUtils.toZNRecord(tableConfig);
          }
          if (path.contains("flagOffTable_OFFLINE")) {
            return TableConfigSerDeUtils.toZNRecord(flagOffTableConfig);
          }
          return null;
        });

    when(_helix.getPropertyStore()).thenReturn(mockPropertyStore);
    when(_helix.getNumReplicas(any(TableConfig.class))).thenReturn(NUM_REPLICAS);
    when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);

    // server0: segment s1 and s2 with compression stats
    Map<String, ColumnCompressionStatsInfo> s1ColStats = new HashMap<>();
    s1ColStats.put("col_a", new ColumnCompressionStatsInfo(10000, 2000, "LZ4"));
    s1ColStats.put("col_b", new ColumnCompressionStatsInfo(20000, 5000, "ZSTANDARD"));

    Map<String, ColumnCompressionStatsInfo> s2ColStats = new HashMap<>();
    s2ColStats.put("col_a", new ColumnCompressionStatsInfo(15000, 3000, "LZ4"));

    List<SegmentSizeInfo> server0Sizes = Arrays.asList(
        new SegmentSizeInfo("s1", 50000, 30000, 7000, "default", s1ColStats),
        new SegmentSizeInfo("s2", 40000, 15000, 3000, "default", s2ColStats));
    FakeCompressionServer s0 = new FakeCompressionServer(Arrays.asList("s1", "s2"), server0Sizes);
    s0.start(URI_PATH, createHandler(200, server0Sizes));
    _serverMap.put("server0", s0);

    // server1: segment s1 and s2 (replica) with same stats
    List<SegmentSizeInfo> server1Sizes = Arrays.asList(
        new SegmentSizeInfo("s1", 50000, 30000, 7000, "default", s1ColStats),
        new SegmentSizeInfo("s2", 40000, 15000, 3000, "default", s2ColStats));
    FakeCompressionServer s1 = new FakeCompressionServer(Arrays.asList("s1", "s2"), server1Sizes);
    s1.start(URI_PATH, createHandler(200, server1Sizes));
    _serverMap.put("server1", s1);

    // server2: segment s3 without compression stats (old server)
    List<SegmentSizeInfo> server2Sizes = Arrays.asList(new SegmentSizeInfo("s3", 60000));
    FakeCompressionServer s2 = new FakeCompressionServer(Arrays.asList("s3"), server2Sizes);
    s2.start(URI_PATH, createHandler(200, server2Sizes));
    _serverMap.put("server2", s2);
  }

  @AfterClass
  public void tearDown() {
    for (FakeCompressionServer server : _serverMap.values()) {
      server.stop();
    }
  }

  private HttpHandler createHandler(int status, List<SegmentSizeInfo> segmentSizes) {
    return httpExchange -> {
      long tableSizeInBytes = 0;
      for (SegmentSizeInfo segmentSize : segmentSizes) {
        tableSizeInBytes += segmentSize.getDiskSizeInBytes();
      }
      TableSizeInfo tableInfo = new TableSizeInfo("compressionTable", tableSizeInBytes, segmentSizes);
      String json = JsonUtils.objectToString(tableInfo);
      httpExchange.sendResponseHeaders(status, json.length());
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(json.getBytes());
      responseBody.close();
    };
  }

  private static class FakeCompressionServer extends FakeHttpServer {
    final List<String> _segments;
    final List<SegmentSizeInfo> _sizes;

    FakeCompressionServer(List<String> segments, List<SegmentSizeInfo> sizes) {
      _segments = segments;
      _sizes = sizes;
    }
  }

  private TableSizeReader.TableSizeDetails testRunner(String[] servers, String table)
      throws InvalidConfigException {
    when(_helix.getServerToSegmentsMap(anyString(), any(), anyBoolean())).thenAnswer(
        (Answer<Map<String, List<String>>>) invocation -> {
          Map<String, List<String>> map = new HashMap<>();
          for (String server : servers) {
            map.put(server, _serverMap.get(server)._segments);
          }
          return map;
        });

    when(_helix.getDataInstanceAdminEndpoints(ArgumentMatchers.anySet())).thenAnswer(
        (Answer<BiMap<String, String>>) invocation -> {
          BiMap<String, String> endpoints = HashBiMap.create(servers.length);
          for (String server : servers) {
            endpoints.put(server, _serverMap.get(server)._endpoint);
          }
          return endpoints;
        });

    TableSizeReader reader =
        new TableSizeReader(_executor, _connectionManager, _controllerMetrics, _helix, _leadControllerManager);
    return reader.getTableSizeDetails(table, TIMEOUT_MSEC, true);
  }

  @Test
  public void testCompressionStatsAggregation()
      throws InvalidConfigException {
    String[] servers = {"server0", "server1"};
    TableSizeReader.TableSizeDetails details = testRunner(servers, "offline");

    TableSizeReader.TableSubTypeSizeDetails offlineDetails = details._offlineSegments;
    assertNotNull(offlineDetails);

    // s1: rawFwdIdx=30000, compressedFwdIdx=7000 (max across replicas)
    // s2: rawFwdIdx=15000, compressedFwdIdx=3000 (max across replicas)
    // Total per replica: raw=45000, compressed=10000
    TableSizeReader.CompressionStats cs = offlineDetails._compressionStats;
    assertNotNull(cs);
    assertEquals(cs._rawForwardIndexSizePerReplicaInBytes, 45000);
    assertEquals(cs._compressedForwardIndexSizePerReplicaInBytes, 10000);

    // Compression ratio = 45000 / 10000 = 4.5
    assertEquals(cs._compressionRatio, 4.5, 0.01);

    // Both segments have stats
    assertEquals(cs._segmentsWithStats, 2);
    assertEquals(cs._totalSegments, 2);
    assertFalse(cs._isPartialCoverage);

    // Verify compression metrics emitted
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType("offline");
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_RAW_FORWARD_INDEX_SIZE_PER_REPLICA), 45000);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_COMPRESSED_FORWARD_INDEX_SIZE_PER_REPLICA), 10000);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_RATIO_PERCENT), 450);

    // Verify per-column compression stats aggregation
    // s1: col_a(raw=10000, compressed=2000), col_b(raw=20000, compressed=5000)
    // s2: col_a(raw=15000, compressed=3000)
    // Aggregated: col_a: raw=10000+15000=25000, compressed=2000+3000=5000
    //             col_b: raw=20000, compressed=5000
    assertFalse(cs._columnCompressionStats.isEmpty(), "Per-column compression stats should be present");
    TableSizeReader.ColumnCompressionDetail colA = cs._columnCompressionStats.get("col_a");
    assertNotNull(colA, "col_a should have compression stats");
    assertEquals(colA._rawForwardIndexSizeBytes, 25000);
    assertEquals(colA._compressedForwardIndexSizeBytes, 5000);
    assertEquals(colA._compressionRatio, 5.0, 0.01);
    assertEquals(colA._compressionCodec, "LZ4");

    TableSizeReader.ColumnCompressionDetail colB = cs._columnCompressionStats.get("col_b");
    assertNotNull(colB, "col_b should have compression stats");
    assertEquals(colB._rawForwardIndexSizeBytes, 20000);
    assertEquals(colB._compressedForwardIndexSizeBytes, 5000);
    assertEquals(colB._compressionRatio, 4.0, 0.01);
    assertEquals(colB._compressionCodec, "ZSTANDARD");

    // Verify storageBreakdown is present
    assertNotNull(offlineDetails._storageBreakdown);
    assertFalse(offlineDetails._storageBreakdown._tiers.isEmpty());
    TableSizeReader.TierSizeInfo defaultTier = offlineDetails._storageBreakdown._tiers.get("default");
    assertNotNull(defaultTier, "default tier should be present");
    assertEquals(defaultTier._count, 2, "Should have 2 segments in default tier");
    assertTrue(defaultTier._sizePerReplicaInBytes > 0, "Tier size should be > 0");
  }

  @Test
  public void testPartialCompressionCoverage()
      throws InvalidConfigException {
    // Mix of servers with and without compression stats
    String[] servers = {"server0", "server1", "server2"};
    TableSizeReader.TableSizeDetails details = testRunner(servers, "offline");

    TableSizeReader.TableSubTypeSizeDetails offlineDetails = details._offlineSegments;
    assertNotNull(offlineDetails);

    // s1 and s2 have compression stats, s3 does not
    TableSizeReader.CompressionStats cs = offlineDetails._compressionStats;
    assertNotNull(cs);
    assertEquals(cs._segmentsWithStats, 2);
    assertEquals(cs._totalSegments, 3);
    assertTrue(cs._isPartialCoverage);

    // Compression ratio still computed from segments that have stats
    assertEquals(cs._rawForwardIndexSizePerReplicaInBytes, 45000);
    assertEquals(cs._compressedForwardIndexSizePerReplicaInBytes, 10000);
    assertEquals(cs._compressionRatio, 4.5, 0.01);
  }

  @Test
  public void testNoCompressionStats()
      throws InvalidConfigException {
    // Only old server without compression stats
    String[] servers = {"server2"};
    TableSizeReader.TableSizeDetails details = testRunner(servers, "offline");

    TableSizeReader.TableSubTypeSizeDetails offlineDetails = details._offlineSegments;
    assertNotNull(offlineDetails);

    TableSizeReader.CompressionStats cs = offlineDetails._compressionStats;
    assertNotNull(cs);
    assertEquals(cs._segmentsWithStats, 0);
    assertEquals(cs._totalSegments, 1);
    // isPartialCoverage should be true: 0 segments have stats but 1 non-missing segment exists
    assertTrue(cs._isPartialCoverage);
    assertEquals(cs._rawForwardIndexSizePerReplicaInBytes, 0);
    assertEquals(cs._compressedForwardIndexSizePerReplicaInBytes, 0);
    assertEquals(cs._compressionRatio, 0.0, 0.01);
  }

  @Test
  public void testCompressionStatsNullWhenFlagOff()
      throws InvalidConfigException {
    // Use servers with compression stats but with compressionStatsEnabled=false on the table config
    String[] servers = {"server0", "server1"};
    TableSizeReader.TableSizeDetails details = testRunner(servers, "flagOffTable");

    TableSizeReader.TableSubTypeSizeDetails offlineDetails = details._offlineSegments;
    assertNotNull(offlineDetails);

    // compressionStats should be null when the flag is OFF (suppressed from JSON via @JsonInclude NON_NULL)
    assertNull(offlineDetails._compressionStats,
        "compressionStats should be null when compressionStatsEnabled is false");

    // storageBreakdown should still be present (REQ-4.2: always reported)
    assertNotNull(offlineDetails._storageBreakdown,
        "storageBreakdown should still be present when compressionStatsEnabled is false");

    // Verify no compression metrics were emitted for this table
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType("flagOffTable");
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_RATIO_PERCENT), 0);
  }
}
