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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsContribution;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.CompressionStatsSummary;
import org.apache.pinot.common.restlet.resources.SegmentCompressionStatsContribution;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.ServerCompressionStatsResponse;
import org.apache.pinot.common.restlet.resources.TableSegments;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.controller.utils.FakeHttpServer;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


/// Tests compression-statistics aggregation in [TableSizeReader].
public class TableSizeReaderCompressionStatsTest {
  private static final String URI_PATH = "/table/";
  private static final int TIMEOUT_MSEC = 10000;
  private static final int NUM_REPLICAS = 2;

  private final ExecutorService _executor = Executors.newFixedThreadPool(4);
  private final PoolingHttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
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
    // compressionStatsEnabled defaults to false; do not enable it.

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
    s1ColStats.put("col_a", rawStats("col_a", 10000, 2000, ChunkCompressionType.LZ4));
    s1ColStats.put("col_b", rawStats("col_b", 20000, 5000, ChunkCompressionType.ZSTANDARD));

    Map<String, ColumnCompressionStatsInfo> s2ColStats = new HashMap<>();
    s2ColStats.put("col_a", rawStats("col_a", 15000, 3000, ChunkCompressionType.LZ4));

    List<SegmentSizeInfo> server0Sizes = List.of(
        new SegmentSizeInfo("s1", 50000, 30000, 7000, s1ColStats),
        new SegmentSizeInfo("s2", 40000, 15000, 3000, s2ColStats));
    FakeCompressionServer s0 = new FakeCompressionServer(List.of("s1", "s2"), server0Sizes);
    startServer(s0, server0Sizes, TableSizeInfo.CURRENT_METADATA_VERSION);
    _serverMap.put("server0", s0);

    // server1: segment s1 and s2 (replica) with same stats
    List<SegmentSizeInfo> server1Sizes = List.of(
        new SegmentSizeInfo("s1", 50000, 30000, 7000, s1ColStats),
        new SegmentSizeInfo("s2", 40000, 15000, 3000, s2ColStats));
    FakeCompressionServer s1 = new FakeCompressionServer(List.of("s1", "s2"), server1Sizes);
    startServer(s1, server1Sizes, TableSizeInfo.CURRENT_METADATA_VERSION);
    _serverMap.put("server1", s1);

    // server2: segment s3 without compression stats (old server)
    List<SegmentSizeInfo> server2Sizes = List.of(new SegmentSizeInfo("s3", 60000));
    FakeCompressionServer s2 = new FakeCompressionServer(List.of("s3"), server2Sizes);
    startServer(s2, server2Sizes, 0);
    _serverMap.put("server2", s2);

    addServer("server3", List.of("s4"), List.of(new SegmentSizeInfo("s4", 1_000, 120, 20,
        Map.of("col_a", rawStats("col_a", 120, 20, ChunkCompressionType.LZ4)))),
        TableSizeInfo.CURRENT_METADATA_VERSION);
    addServer("server4", List.of("s4"), List.of(new SegmentSizeInfo("s4", 1_000, 100, 25,
        Map.of("col_a", rawStats("col_a", 100, 25, ChunkCompressionType.LZ4)))),
        TableSizeInfo.CURRENT_METADATA_VERSION);
    addServer("server5", List.of("s3"), List.of(new SegmentSizeInfo("s3", 1_000, 80, 20,
        Map.of("col_a", rawStats("col_a", 80, 20, ChunkCompressionType.LZ4)))),
        TableSizeInfo.CURRENT_METADATA_VERSION);
    addServer("server6", List.of("s5"), List.of(new SegmentSizeInfo("s5", 1_000, 0, 0)),
        TableSizeInfo.CURRENT_METADATA_VERSION);
  }

  @AfterClass
  public void tearDown() {
    for (FakeCompressionServer server : _serverMap.values()) {
      server.stop();
    }
    _executor.shutdownNow();
    _connectionManager.close();
  }

  private void addServer(String name, List<String> segments, List<SegmentSizeInfo> sizes, int metadataVersion)
      throws IOException {
    FakeCompressionServer server = new FakeCompressionServer(segments, sizes);
    startServer(server, sizes, metadataVersion);
    _serverMap.put(name, server);
  }

  private void startServer(FakeCompressionServer server, List<SegmentSizeInfo> sizes, int metadataVersion)
      throws IOException {
    server.start(URI_PATH, createHandler(200, sizes, metadataVersion));
    server._httpServer.createContext("/tables/", createCompressionHandler(server, sizes, metadataVersion));
  }

  private HttpHandler createHandler(int status, List<SegmentSizeInfo> segmentSizes, int metadataVersion) {
    return httpExchange -> {
      long tableSizeInBytes = 0;
      boolean includeCompressionStats = httpExchange.getRequestURI().getQuery() != null
          && httpExchange.getRequestURI().getQuery().contains("includeCompressionStats=true");
      boolean includeColumnCompressionStats = httpExchange.getRequestURI().getQuery() != null
          && httpExchange.getRequestURI().getQuery().contains("includeColumnCompressionStats=true");
      List<SegmentSizeInfo> responseSegments = new ArrayList<>(segmentSizes.size());
      for (SegmentSizeInfo segmentSize : segmentSizes) {
        tableSizeInBytes += segmentSize.getDiskSizeInBytes();
        responseSegments.add(includeCompressionStats
            ? new SegmentSizeInfo(segmentSize.getSegmentName(), segmentSize.getDiskSizeInBytes(),
                segmentSize.getCompressionStatsUncompressedValueSizeInBytes(),
                segmentSize.getCompressionStatsForwardIndexAndDictionaryStorageSizeInBytes(),
                includeColumnCompressionStats ? segmentSize.getColumnCompressionStats() : null)
            : new SegmentSizeInfo(segmentSize.getSegmentName(), segmentSize.getDiskSizeInBytes()));
      }
      TableSizeInfo tableInfo =
          new TableSizeInfo("compressionTable", tableSizeInBytes, responseSegments, metadataVersion);
      byte[] json = JsonUtils.objectToString(tableInfo).getBytes(StandardCharsets.UTF_8);
      httpExchange.sendResponseHeaders(status, json.length);
      try (OutputStream responseBody = httpExchange.getResponseBody()) {
        responseBody.write(json);
      }
    };
  }

  private HttpHandler createCompressionHandler(FakeCompressionServer server, List<SegmentSizeInfo> segmentSizes,
      int metadataVersion) {
    return httpExchange -> {
      server._compressionRequests.incrementAndGet();
      TableSegments request = JsonUtils.stringToObject(
          new String(httpExchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8), TableSegments.class);
      Map<String, SegmentSizeInfo> sizesBySegment = new HashMap<>();
      for (SegmentSizeInfo segmentSize : segmentSizes) {
        sizesBySegment.put(segmentSize.getSegmentName(), segmentSize);
      }
      boolean includeColumnStats = httpExchange.getRequestURI().getQuery() != null
          && httpExchange.getRequestURI().getQuery().contains("includeColumnCompressionStats=true");
      List<SegmentCompressionStatsContribution> contributions = new ArrayList<>();
      for (String segmentName : request.getSegments()) {
        SegmentSizeInfo size = sizesBySegment.get(segmentName);
        if (size == null) {
          contributions.add(new SegmentCompressionStatsContribution(segmentName, false, -1, -1, null));
          continue;
        }
        long uncompressedValueSize = size.getCompressionStatsUncompressedValueSizeInBytes();
        long storageSize = size.getCompressionStatsForwardIndexAndDictionaryStorageSizeInBytes();
        contributions.add(new SegmentCompressionStatsContribution(segmentName,
            uncompressedValueSize >= 0 && storageSize >= 0, uncompressedValueSize, storageSize,
            includeColumnStats && size.getColumnCompressionStats() != null
                ? toContributions(size.getColumnCompressionStats()) : null));
      }
      ServerCompressionStatsResponse response = new ServerCompressionStatsResponse(metadataVersion, contributions);
      byte[] json = JsonUtils.objectToString(response).getBytes(StandardCharsets.UTF_8);
      httpExchange.sendResponseHeaders(200, json.length);
      try (OutputStream responseBody = httpExchange.getResponseBody()) {
        responseBody.write(json);
      }
    };
  }

  private static class FakeCompressionServer extends FakeHttpServer {
    final List<String> _segments;
    final List<SegmentSizeInfo> _sizes;
    final AtomicInteger _compressionRequests = new AtomicInteger();

    FakeCompressionServer(List<String> segments, List<SegmentSizeInfo> sizes) {
      _segments = segments;
      _sizes = sizes;
    }
  }

  private TableSizeReader createReader(String[] servers)
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

    return new TableSizeReader(_executor, _connectionManager, _controllerMetrics, _helix, _leadControllerManager);
  }

  private TableSizeReader.TableSizeDetails testRunner(String[] servers, String table)
      throws InvalidConfigException {
    TableSizeReader reader = createReader(servers);
    TableSizeReader.TableSizeDetails details = reader.getTableSizeDetails(table, TIMEOUT_MSEC, true, true);
    reader.updateCompressionMetrics(TableNameBuilder.OFFLINE.tableNameWithType(table), details._offlineSegments);
    return details;
  }

  private TableSizeReader.TableSizeDetails testRunner(String[] servers, String table,
      boolean includeColumnCompressionStats)
      throws InvalidConfigException {
    TableSizeReader reader = createReader(servers);
    TableSizeReader.TableSizeDetails details =
        reader.getTableSizeDetails(table, TIMEOUT_MSEC, true, includeColumnCompressionStats);
    reader.updateCompressionMetrics(TableNameBuilder.OFFLINE.tableNameWithType(table), details._offlineSegments);
    return details;
  }

  @Test
  public void testCompressionStatsAggregation()
      throws InvalidConfigException {
    String[] servers = {"server0", "server1"};
    TableSizeReader.TableSizeDetails details = testRunner(servers, "offline");

    TableSizeReader.TableSubTypeSizeDetails offlineDetails = details._offlineSegments;
    assertNotNull(offlineDetails);

    // s1: rawFwdIdx=30000, compressedFwdIdx=7000 (first complete replica)
    // s2: rawFwdIdx=15000, compressedFwdIdx=3000 (first complete replica)
    // Total per replica: raw=45000, compressed=10000
    CompressionStatsSummary cs = offlineDetails._compressionStats;
    assertNotNull(cs);
    assertEquals(cs.getUncompressedValueSizePerReplicaInBytes(), 45000);
    assertEquals(cs.getForwardIndexAndDictionaryStorageSizePerReplicaInBytes(), 10000);

    // Compression ratio = 45000 / 10000 = 4.5
    assertEquals(cs.getCompressionRatio(), 4.5, 0.01);

    // Both segments have stats
    assertEquals(cs.getSegmentsWithCompleteStats(), 2);
    assertEquals(cs.getTotalSegments(), 2);
    assertFalse(cs.isPartialCoverage());

    // Verify compression metrics emitted
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType("offline");
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_UNCOMPRESSED_VALUE_SIZE_PER_REPLICA), 45000);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_FORWARD_INDEX_AND_DICTIONARY_STORAGE_SIZE_PER_REPLICA), 10000);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_RATIO_PERCENT), 450);

    // Verify per-column compression stats aggregation (now top-level on TableSubTypeSizeDetails)
    // s1: col_a(raw=10000, compressed=2000), col_b(raw=20000, compressed=5000)
    // s2: col_a(raw=15000, compressed=3000)
    // Aggregated: col_a: raw=10000+15000=25000, compressed=2000+3000=5000
    //             col_b: raw=20000, compressed=5000
    List<ColumnCompressionStatsInfo> colStats = offlineDetails._columnCompressionStats;
    assertNotNull(colStats, "Per-column compression stats should be present");
    assertFalse(colStats.isEmpty(), "Per-column compression stats should not be empty");
    // List is sorted by column name: col_a, col_b
    ColumnCompressionStatsInfo colA = colStats.get(0);
    assertEquals(colA.getColumn(), "col_a");
    assertEquals(colA.getUncompressedValueSizeInBytes(), 25000);
    assertEquals(colA.getForwardIndexAndDictionaryStorageSizeInBytes(), 5000);
    assertEquals(colA.getCompressionRatio(), 5.0, 0.01);
    assertEquals(colA.getEncodingBreakdown().get(0).getChunkCompressionType(), ChunkCompressionType.LZ4);

    ColumnCompressionStatsInfo colB = colStats.get(1);
    assertEquals(colB.getColumn(), "col_b");
    assertEquals(colB.getUncompressedValueSizeInBytes(), 20000);
    assertEquals(colB.getForwardIndexAndDictionaryStorageSizeInBytes(), 5000);
    assertEquals(colB.getCompressionRatio(), 4.0, 0.01);
    assertEquals(colB.getEncodingBreakdown().get(0).getChunkCompressionType(), ChunkCompressionType.ZSTANDARD);

    for (String segment : List.of("s1", "s2")) {
      List<SegmentSizeInfo> selectedReplicaStats = offlineDetails._segments.get(segment)._serverInfo.values().stream()
          .filter(info -> info.getCompressionStatsUncompressedValueSizeInBytes() >= 0)
          .toList();
      assertEquals(selectedReplicaStats.size(), 1,
          "Bounded replica selection must attach detailed stats only to the selected server");
      Map<String, ColumnCompressionStatsInfo> selectedColumns = selectedReplicaStats.get(0).getColumnCompressionStats();
      assertNotNull(selectedColumns);
      assertTrue(selectedColumns.values().stream().allMatch(info -> info.getCompressionRatio() > 0));
    }
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
    CompressionStatsSummary cs = offlineDetails._compressionStats;
    assertNotNull(cs);
    assertEquals(cs.getSegmentsWithCompleteStats(), 2);
    assertEquals(cs.getTotalSegments(), 3);
    assertTrue(cs.isPartialCoverage());

    // Compression ratio still computed from segments that have stats
    assertEquals(cs.getUncompressedValueSizePerReplicaInBytes(), 45000);
    assertEquals(cs.getForwardIndexAndDictionaryStorageSizePerReplicaInBytes(), 10000);
    assertEquals(cs.getCompressionRatio(), 4.5, 0.01);
  }

  @Test
  public void testNoCompressionStats()
      throws InvalidConfigException {
    // The legacy response contributes to the coverage denominator but not to tracked sizes.
    String[] servers = {"server2"};
    TableSizeReader.TableSizeDetails details = testRunner(servers, "offline");

    TableSizeReader.TableSubTypeSizeDetails offlineDetails = details._offlineSegments;
    assertNotNull(offlineDetails);

    assertNotNull(offlineDetails._compressionStats);
    assertEquals(offlineDetails._compressionStats.getSegmentsWithCompleteStats(), 0);
    assertEquals(offlineDetails._compressionStats.getTotalSegments(), 1);
    assertTrue(offlineDetails._compressionStats.isPartialCoverage());
  }

  @Test
  public void testZeroEligibleColumnsCountAsCompleteCoverage()
      throws InvalidConfigException {
    TableSizeReader.TableSubTypeSizeDetails offlineDetails =
        testRunner(new String[]{"server6"}, "offline")._offlineSegments;

    assertNotNull(offlineDetails._compressionStats);
    assertEquals(offlineDetails._compressionStats.getSegmentsWithCompleteStats(), 1);
    assertEquals(offlineDetails._compressionStats.getTotalSegments(), 1);
    assertFalse(offlineDetails._compressionStats.isPartialCoverage());
    assertEquals(offlineDetails._compressionStats.getUncompressedValueSizePerReplicaInBytes(), 0L);
    assertEquals(offlineDetails._compressionStats.getForwardIndexAndDictionaryStorageSizePerReplicaInBytes(), 0L);
    assertEquals(offlineDetails._compressionStats.getCompressionRatio(), 0.0);
  }

  @Test
  public void testStaleMetricsClearedWhenNoStats()
      throws InvalidConfigException {
    // First run with servers that have stats to emit metrics
    String[] serversWithStats = {"server0", "server1"};
    testRunner(serversWithStats, "offline");

    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType("offline");
    // Verify metrics were emitted
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_RATIO_PERCENT), 450);

    // Now run with only an old server; stale metrics should be cleared.
    String[] serversNoStats = {"server2"};
    testRunner(serversNoStats, "offline");

    // Metrics should be cleared (0 means removed)
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_RATIO_PERCENT), 0);
  }

  @Test
  public void testCompressionStatsSummaryPresentWhenColumnStatsExcluded()
      throws InvalidConfigException {
    // Regression test: compressionStats summary must be present even when includeColumnCompressionStats=false.
    // Previously, the summary was gated on perColumnMax which is only populated when servers are
    // called with includeColumnCompressionStats=true. This caused the summary to be null for the default case.
    String[] servers = {"server0", "server1"};
    TableSizeReader.TableSizeDetails details = testRunner(servers, "offline", false);

    TableSizeReader.TableSubTypeSizeDetails offlineDetails = details._offlineSegments;
    assertNotNull(offlineDetails);

    // Summary must remain available when column details are not requested.
    assertNotNull(offlineDetails._compressionStats,
        "compressionStats summary should be present even when includeColumnCompressionStats=false");
    assertTrue(offlineDetails._compressionStats.getUncompressedValueSizePerReplicaInBytes() > 0,
        "uncompressedValueSizePerReplicaInBytes should be > 0");
    assertTrue(offlineDetails._compressionStats.getForwardIndexAndDictionaryStorageSizePerReplicaInBytes() > 0,
        "forwardIndexAndDictionaryStorageSizePerReplicaInBytes should be > 0");
    assertTrue(offlineDetails._compressionStats.getSegmentsWithCompleteStats() > 0,
        "segmentsWithCompleteStats should be > 0");

    // Per-column list must still be null (not requested)
    assertNull(offlineDetails._columnCompressionStats,
        "columnCompressionStats should be null when includeColumnCompressionStats=false");
    long selectedReplicas = offlineDetails._segments.values().stream()
        .flatMap(segment -> segment._serverInfo.values().stream())
        .filter(info -> info.getCompressionStatsUncompressedValueSizeInBytes() >= 0)
        .peek(info -> assertNull(info.getColumnCompressionStats()))
        .count();
    assertEquals(selectedReplicas, 2L);
  }

  @Test
  public void testColumnCompressionStatsPresentWhenSummaryExcluded()
      throws InvalidConfigException {
    TableSizeReader.TableSizeDetails details = createReader(new String[]{"server0", "server1"})
        .getTableSizeDetails("offline", TIMEOUT_MSEC, true,
            TableSizeReader.CompressionStatsMode.COLUMNS_WITH_SEGMENT_DETAILS);

    TableSizeReader.TableSubTypeSizeDetails offlineDetails = details._offlineSegments;
    assertNull(offlineDetails._compressionStats);
    assertNotNull(offlineDetails._columnCompressionStats);
    assertEquals(offlineDetails._columnCompressionStats.size(), 2);
    assertEquals(offlineDetails._columnCompressionStats.get(0).getColumn(), "col_a");
    assertEquals(offlineDetails._columnCompressionStats.get(1).getColumn(), "col_b");
  }

  @Test
  public void testAggregateOnlyModeDoesNotRetainSelectedSegments()
      throws InvalidConfigException {
    TableSizeReader.TableSizeDetails details = createReader(new String[]{"server0", "server1"})
        .getTableSizeDetails("offline", TIMEOUT_MSEC, true, TableSizeReader.CompressionStatsMode.AGGREGATE_SUMMARY);

    TableSizeReader.TableSubTypeSizeDetails offlineDetails = details._offlineSegments;
    assertNotNull(offlineDetails._compressionStats);
    assertEquals(offlineDetails._compressionStats.getSegmentsWithCompleteStats(), 2);
    assertNull(offlineDetails._columnCompressionStats);
    assertTrue(offlineDetails._segments.values().stream()
        .flatMap(segment -> segment._serverInfo.values().stream())
        .allMatch(info -> info.getCompressionStatsUncompressedValueSizeInBytes() < 0));
  }

  @Test
  public void testCompressionMetricUpdateAndCleanupAreAtomic()
      throws Exception {
    String[] servers = {"server0", "server1"};
    createReader(servers);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    CountDownLatch firstUpdateStarted = new CountDownLatch(1);
    CountDownLatch continueUpdate = new CountDownLatch(1);
    doAnswer(invocation -> {
      firstUpdateStarted.countDown();
      assertTrue(continueUpdate.await(10, TimeUnit.SECONDS));
      return null;
    }).when(metrics).setValueOfTableGauge(anyString(),
        ArgumentMatchers.eq(ControllerGauge.TABLE_COMPRESSION_STATS_UNCOMPRESSED_VALUE_SIZE_PER_REPLICA),
        ArgumentMatchers.anyLong());
    TableSizeReader reader = new TableSizeReader(_executor, _connectionManager, metrics, _helix,
        _leadControllerManager);
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType("offline");

    TableSizeReader.TableSizeDetails details = reader.getTableSizeDetails("offline", TIMEOUT_MSEC, true, true);
    Future<?> update = _executor.submit(
        () -> reader.updateCompressionMetrics(tableNameWithType, details._offlineSegments));
    assertTrue(firstUpdateStarted.await(10, TimeUnit.SECONDS));
    Future<?> cleanup = _executor.submit(() -> reader.clearCompressionMetrics(tableNameWithType));
    continueUpdate.countDown();
    update.get(10, TimeUnit.SECONDS);
    cleanup.get(10, TimeUnit.SECONDS);

    InOrder order = inOrder(metrics);
    order.verify(metrics).setValueOfTableGauge(tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_UNCOMPRESSED_VALUE_SIZE_PER_REPLICA, 45_000);
    order.verify(metrics).setValueOfTableGauge(tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_FORWARD_INDEX_AND_DICTIONARY_STORAGE_SIZE_PER_REPLICA, 10_000);
    order.verify(metrics).setValueOfTableGauge(tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_RATIO_PERCENT, 450);
    order.verify(metrics).removeTableGauge(tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_UNCOMPRESSED_VALUE_SIZE_PER_REPLICA);
    order.verify(metrics).removeTableGauge(tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_FORWARD_INDEX_AND_DICTIONARY_STORAGE_SIZE_PER_REPLICA);
    order.verify(metrics).removeTableGauge(tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_RATIO_PERCENT);
  }

  @Test
  public void testTableSizeReadDoesNotPublishCompressionMetrics()
      throws Exception {
    String[] servers = {"server0", "server1"};
    createReader(servers);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    TableSizeReader reader = new TableSizeReader(_executor, _connectionManager, metrics, _helix,
        _leadControllerManager);

    reader.getTableSizeDetails("offline", TIMEOUT_MSEC, true, true);

    verify(metrics, never()).setValueOfTableGauge(anyString(),
        ArgumentMatchers.eq(ControllerGauge.TABLE_COMPRESSION_STATS_UNCOMPRESSED_VALUE_SIZE_PER_REPLICA),
        ArgumentMatchers.anyLong());
    verify(metrics, never()).setValueOfTableGauge(anyString(),
        ArgumentMatchers.eq(
            ControllerGauge.TABLE_COMPRESSION_STATS_FORWARD_INDEX_AND_DICTIONARY_STORAGE_SIZE_PER_REPLICA),
        ArgumentMatchers.anyLong());
    verify(metrics, never()).setValueOfTableGauge(anyString(),
        ArgumentMatchers.eq(ControllerGauge.TABLE_COMPRESSION_STATS_RATIO_PERCENT),
        ArgumentMatchers.anyLong());
  }

  @Test
  public void testNonLeaderDoesNotPublishCompressionMetrics() {
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(false);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    TableSizeReader reader = new TableSizeReader(_executor, _connectionManager, metrics, _helix,
        leadControllerManager);
    TableSizeReader.TableSubTypeSizeDetails details = new TableSizeReader.TableSubTypeSizeDetails();
    details._compressionStats = new CompressionStatsSummary(100, 25, 4.0, 1, 1, false);

    reader.updateCompressionMetrics("offline_OFFLINE", details);

    verifyNoInteractions(metrics);
  }

  @Test
  public void testCompressionStatsNullWhenFlagOff()
      throws InvalidConfigException {
    // Use servers with compression stats but with compressionStatsEnabled=false on the table config
    String[] servers = {"server0", "server1"};
    for (String server : servers) {
      _serverMap.get(server)._compressionRequests.set(0);
    }
    TableSizeReader.TableSizeDetails details = testRunner(servers, "flagOffTable");

    TableSizeReader.TableSubTypeSizeDetails offlineDetails = details._offlineSegments;
    assertNotNull(offlineDetails);
    assertTrue(offlineDetails._reportedSizeInBytes > 0,
        "A disabled table must still complete ordinary size collection");

    // compressionStats should be null when the flag is OFF (suppressed from JSON via @JsonInclude NON_NULL)
    assertNull(offlineDetails._compressionStats,
        "compressionStats should be null when compressionStatsEnabled is false");

    // columnCompressionStats should also be null when flag is OFF
    assertNull(offlineDetails._columnCompressionStats,
        "columnCompressionStats should be null when compressionStatsEnabled is false");

    // Verify no compression metrics were emitted for this table
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType("flagOffTable");
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_RATIO_PERCENT), 0);
    for (String server : servers) {
      assertEquals(_serverMap.get(server)._compressionRequests.get(), 0,
          "A disabled table must not fan out to the compression endpoint");
    }
  }

  @Test
  public void testReplicaSelectionKeepsSummaryAndColumnPairCoherent()
      throws InvalidConfigException {
    TableSizeReader.TableSizeDetails details = testRunner(new String[]{"server3", "server4"}, "offline");
    CompressionStatsSummary summary = details._offlineSegments._compressionStats;
    assertEquals(summary.getUncompressedValueSizePerReplicaInBytes(), 120L);
    assertEquals(summary.getForwardIndexAndDictionaryStorageSizePerReplicaInBytes(), 20L);
    ColumnCompressionStatsInfo column = details._offlineSegments._columnCompressionStats.get(0);
    assertEquals(column.getUncompressedValueSizeInBytes(), 120L);
    assertEquals(column.getForwardIndexAndDictionaryStorageSizeInBytes(), 20L);
  }

  @Test
  public void testReplicaSelectionFallsBackFromLegacyServer()
      throws InvalidConfigException {
    TableSizeReader.TableSizeDetails details = testRunner(new String[]{"server2", "server5"}, "offline");
    CompressionStatsSummary summary = details._offlineSegments._compressionStats;

    assertEquals(summary.getSegmentsWithCompleteStats(), 1);
    assertEquals(summary.getUncompressedValueSizePerReplicaInBytes(), 80L);
    assertEquals(summary.getForwardIndexAndDictionaryStorageSizePerReplicaInBytes(), 20L);
    assertFalse(summary.isPartialCoverage());
  }

  private static ColumnCompressionStatsInfo rawStats(String column, long rawSize,
      long forwardIndexAndDictionaryStorageSize, ChunkCompressionType chunkCompressionType) {
    return ColumnCompressionStatsInfo.builder(column)
        .withUncompressedValueSizeInBytes(rawSize)
        .withForwardIndexAndDictionaryStorageSizeInBytes(forwardIndexAndDictionaryStorageSize)
        .withCompressionRatio((double) rawSize / forwardIndexAndDictionaryStorageSize)
        .withEncodingBreakdown(List.of(new ColumnCompressionStatsInfo.EncodingBreakdownEntry(
            EncodingType.RAW, chunkCompressionType, 1, rawSize, forwardIndexAndDictionaryStorageSize)))
        .withNumSegments(1)
        .build();
  }

  private static Map<String, ColumnCompressionStatsContribution> toContributions(
      Map<String, ColumnCompressionStatsInfo> columnStats) {
    Map<String, ColumnCompressionStatsContribution> contributions = new HashMap<>();
    columnStats.forEach((column, info) -> {
      List<ColumnCompressionStatsContribution.EncodingContribution> breakdown = new ArrayList<>();
      for (ColumnCompressionStatsInfo.EncodingBreakdownEntry entry : info.getEncodingBreakdown()) {
        breakdown.add(new ColumnCompressionStatsContribution.EncodingContribution(entry.getEncoding(),
            entry.getChunkCompressionType(), entry.getNumSegments(), entry.getUncompressedValueSizeInBytes(),
            entry.getForwardIndexAndDictionaryStorageSizeInBytes()));
      }
      contributions.put(column, new ColumnCompressionStatsContribution(column,
          info.getUncompressedValueSizeInBytes(), info.getForwardIndexAndDictionaryStorageSizeInBytes(),
          info.getObservedIndexes(), breakdown, info.getNumSegments()));
    });
    return contributions;
  }
}
