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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsContribution;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.SegmentCompressionStatsContribution;
import org.apache.pinot.common.restlet.resources.ServerCompressionStatsResponse;
import org.apache.pinot.common.restlet.resources.ServerTableMetadataInfo;
import org.apache.pinot.common.restlet.resources.TableMetadataInfo;
import org.apache.pinot.common.restlet.resources.TableSegments;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Verifies bounded controller fan-out, replica fallback, and aggregation for table metadata compression statistics.
public class TableMetadataReaderCompressionTest {
  private static final int TIMEOUT_MSEC = 10_000;

  private final ExecutorService _executor = Executors.newFixedThreadPool(8);
  private final PoolingHttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private HttpServer _httpServer0;
  private HttpServer _httpServer1;
  private int _portServer0;
  private int _portServer1;

  @BeforeClass
  public void setUp()
      throws IOException {
    _connectionManager.setDefaultMaxPerRoute(8);
    List<SegmentCompressionStatsContribution> contributions = List.of(
        segment("segment0", true, 10_000, 2_000,
            rawStats("col_a", 4_000, 1_000, ChunkCompressionType.LZ4),
            rawStats("col_b", 6_000, 1_000, ChunkCompressionType.ZSTANDARD)),
        segment("segment1", true, 10_000, 2_000,
            rawStats("col_a", 4_000, 1_000, ChunkCompressionType.LZ4),
            rawStats("col_b", 6_000, 1_000, ChunkCompressionType.ZSTANDARD)),
        segment("segment2", true, 10_000, 3_000,
            rawStats("col_a", 4_000, 1_000, ChunkCompressionType.LZ4),
            rawStats("col_b", 6_000, 2_000, ChunkCompressionType.ZSTANDARD)));
    ServerCompressionStatsResponse compressionResponse = compressionResponse(
        List.of("segment0", "segment1", "segment2"), contributions);
    _httpServer0 = startServer(createHandler(currentInfo(), compressionResponse));
    _httpServer1 = startServer(createHandler(currentInfo(), compressionResponse));
    _portServer0 = _httpServer0.getAddress().getPort();
    _portServer1 = _httpServer1.getAddress().getPort();
  }

  @AfterClass
  public void tearDown() {
    if (_httpServer0 != null) {
      _httpServer0.stop(0);
    }
    if (_httpServer1 != null) {
      _httpServer1.stop(0);
    }
    _executor.shutdownNow();
    _connectionManager.close();
  }

  @Test
  public void testCurrentProtocolAggregatesPerReplica() {
    TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
        "testTable_OFFLINE", endpoints(_portServer0, _portServer1), null, 2, TIMEOUT_MSEC, true, true,
        assignment(2, "segment0", "segment1", "segment2"));

    assertEquals(result.getDiskSizeInBytes(), 50_000L);
    assertEquals(result.getNumSegments(), 3L);
    assertEquals(result.getColumnIndexSizeMap().get("col_a").get("forward"), 2_000d);
    assertNotNull(result.getCompressionStats());
    assertEquals(result.getCompressionStats().getUncompressedValueSizePerReplicaInBytes(), 30_000L);
    assertEquals(result.getCompressionStats().getForwardIndexAndDictionaryStorageSizePerReplicaInBytes(), 7_000L);
    assertFalse(result.getCompressionStats().isPartialCoverage());

    List<ColumnCompressionStatsInfo> columns = result.getColumnCompressionStats();
    assertNotNull(columns);
    assertEquals(columns.size(), 2);
    assertEquals(columns.get(0).getColumn(), "col_a");
    assertEquals(columns.get(0).getEncodingBreakdown().get(0).getChunkCompressionType(), ChunkCompressionType.LZ4);
    assertEquals(columns.get(1).getEncodingBreakdown().get(0).getChunkCompressionType(),
        ChunkCompressionType.ZSTANDARD);
  }

  @Test
  public void testIndexSizeAggregationWithSingleServer() {
    TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
        "testTable_OFFLINE", endpoints(_portServer0), null, 1, TIMEOUT_MSEC, false, false,
        assignment(1, "segment0", "segment1", "segment2"));

    assertEquals(result.getColumnIndexSizeMap().get("col_a").get("forward"), 2_000d);
  }

  @Test
  public void testCurrentReplicaCoversSegmentsWhenAnotherReplicaIsUnsupported()
      throws IOException {
    TableMetadataInfo legacy = new TableMetadataInfo("testTable_OFFLINE", 50_000, 3, 1_000,
        Map.of("col_a", 4.0), Map.of("col_a", 50.0), Map.of(), Map.of(), Map.of());
    HttpServer legacyServer = startServer(createLegacyHandler(legacy));
    try {
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(legacyServer.getAddress().getPort(), _portServer0), null, 2, TIMEOUT_MSEC,
          true, true, assignment(2, "segment0", "segment1", "segment2"));

      assertEquals(result.getDiskSizeInBytes(), 50_000L);
      assertNotNull(result.getCompressionStats());
      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), 3);
      assertEquals(result.getCompressionStats().getTotalSegments(), 3);
      assertFalse(result.getCompressionStats().isPartialCoverage());
      assertNotNull(result.getColumnCompressionStats());
    } finally {
      legacyServer.stop(0);
    }
  }

  @Test
  public void testCurrentReplicaCoversSegmentsWhenPreferredReplicaUsesOlderProtocol()
      throws IOException {
    List<SegmentCompressionStatsContribution> staleContributions = List.of(
        segment("segment0", true, 1_000_000, 1),
        segment("segment1", true, 1_000_000, 1),
        segment("segment2", true, 1_000_000, 1));
    ServerCompressionStatsResponse staleResponse = new ServerCompressionStatsResponse(0, staleContributions);
    AtomicInteger staleRequests = new AtomicInteger();
    HttpServer staleServer = startServer(createCountingHandler(currentInfo(), staleResponse, staleRequests));
    try {
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(staleServer.getAddress().getPort(), _portServer0), null, 2, TIMEOUT_MSEC,
          true, true, assignment(2, "segment0", "segment1", "segment2"));

      assertTrue(staleRequests.get() > 0);
      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), 3);
      assertEquals(result.getCompressionStats().getTotalSegments(), 3);
      assertEquals(result.getCompressionStats().getUncompressedValueSizePerReplicaInBytes(), 30_000L);
      assertFalse(result.getCompressionStats().isPartialCoverage());
    } finally {
      staleServer.stop(0);
    }
  }

  @Test
  public void testOlderProtocolOnlyProducesExplicitPartialCoverage()
      throws IOException {
    ServerCompressionStatsResponse staleResponse = new ServerCompressionStatsResponse(0,
        List.of(segment("segment0", true, 1_000_000, 1)));
    HttpServer staleServer = startServer(createHandler(currentInfo(), staleResponse));
    try {
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(staleServer.getAddress().getPort()), null, 1, TIMEOUT_MSEC,
          true, true, assignment(1, "segment0"));

      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), 0);
      assertEquals(result.getCompressionStats().getTotalSegments(), 1);
      assertTrue(result.getCompressionStats().isPartialCoverage());
      assertNull(result.getColumnCompressionStats());
    } finally {
      staleServer.stop(0);
    }
  }

  @Test
  public void testUnsupportedCompressionEndpointProducesExplicitPartialCoverage()
      throws IOException {
    TableMetadataInfo legacy = new TableMetadataInfo("testTable_OFFLINE", 50_000, 3, 1_000,
        Map.of("col_a", 4.0), Map.of("col_a", 50.0), Map.of(), Map.of(), Map.of());
    HttpServer legacyServer = startServer(createLegacyHandler(legacy));
    try {
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(legacyServer.getAddress().getPort()), null, 1, TIMEOUT_MSEC,
          true, true, assignment(1, "segment0", "segment1", "segment2"));

      assertEquals(result.getDiskSizeInBytes(), 50_000L);
      assertNotNull(result.getCompressionStats());
      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), 0);
      assertEquals(result.getCompressionStats().getTotalSegments(), 3);
      assertTrue(result.getCompressionStats().isPartialCoverage());
      assertNull(result.getColumnCompressionStats());
    } finally {
      legacyServer.stop(0);
    }
  }

  @Test
  public void testIncludeColumnStatsFalseDoesNotRequestOrReturnColumns()
      throws IOException {
    AtomicReference<String> query = new AtomicReference<>();
    ServerCompressionStatsResponse summaryOnly = compressionResponse(List.of("segment0"),
        List.of(segment("segment0", true, 10_000, 2_000)));
    HttpServer server = startServer(createRecordingHandler(currentInfo(), summaryOnly, query));
    try {
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(server.getAddress().getPort()), null, 1, TIMEOUT_MSEC, true, false,
          assignment(1, "segment0"));

      assertNotNull(result.getCompressionStats());
      assertNull(result.getColumnCompressionStats());
      assertFalse(query.get() != null && query.get().contains("includeColumnCompressionStats=true"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testCompressionFlagOffSuppressesCompression()
      throws IOException {
    AtomicInteger compressionRequests = new AtomicInteger();
    HttpServer server = startServer(createCountingHandler(currentInfo(), completeResponse(List.of("segment0")),
        compressionRequests));
    try {
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(server.getAddress().getPort()), null, 1, TIMEOUT_MSEC, false, true,
          assignment(1, "segment0"));

      assertEquals(result.getDiskSizeInBytes(), currentInfo().getDiskSizeInBytes());
      assertNull(result.getCompressionStats());
      assertNull(result.getColumnCompressionStats());
      assertEquals(compressionRequests.get(), 0,
          "A disabled table must complete ordinary metadata collection without compression fan-out");
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testDictionaryContributionKeepsEncodingSeparateFromCodec()
      throws IOException {
    ColumnCompressionStatsContribution dictionary = new ColumnCompressionStatsContribution("dict_col", 8_000,
        2_000, List.of("dictionary", "forward_index"),
        List.of(new ColumnCompressionStatsContribution.EncodingContribution(
            EncodingType.DICTIONARY, null, 1, 8_000, 2_000)), 1);
    ServerCompressionStatsResponse response = compressionResponse(List.of("segment0"),
        List.of(segment("segment0", true, 8_000, 2_000, dictionary)));
    HttpServer server = startServer(createHandler(currentInfo(), response));
    try {
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(server.getAddress().getPort()), null, 1, TIMEOUT_MSEC, true, true,
          assignment(1, "segment0"));
      ColumnCompressionStatsInfo column = result.getColumnCompressionStats().get(0);
      assertEquals(column.getEncodingBreakdown().get(0).getEncoding(), EncodingType.DICTIONARY);
      assertNull(column.getEncodingBreakdown().get(0).getChunkCompressionType());
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testReplicaSelectionMatchesTableSizePolicy()
      throws IOException {
    ServerCompressionStatsResponse first = compressionResponse(List.of("segment0"),
        List.of(segment("segment0", true, 120, 20,
            rawStats("col_a", 120, 20, ChunkCompressionType.LZ4))));
    ServerCompressionStatsResponse second = compressionResponse(List.of("segment0"),
        List.of(segment("segment0", true, 100, 25,
            rawStats("col_a", 100, 25, ChunkCompressionType.LZ4))));
    AtomicInteger firstRequests = new AtomicInteger();
    AtomicInteger secondRequests = new AtomicInteger();
    HttpServer firstServer = startServer(createCountingHandler(currentInfo(), first, firstRequests));
    HttpServer secondServer = startServer(createCountingHandler(currentInfo(), second, secondRequests));
    try {
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(firstServer.getAddress().getPort(), secondServer.getAddress().getPort()),
          null, 2, TIMEOUT_MSEC, true, true, assignment(2, "segment0"));

      assertEquals(result.getCompressionStats().getUncompressedValueSizePerReplicaInBytes(), 120L);
      assertEquals(result.getCompressionStats().getForwardIndexAndDictionaryStorageSizePerReplicaInBytes(), 20L);
      assertEquals(result.getColumnCompressionStats().get(0).getUncompressedValueSizeInBytes(), 120L);
      assertEquals(result.getColumnCompressionStats().get(0).getForwardIndexAndDictionaryStorageSizeInBytes(), 20L);
      assertEquals(firstRequests.get(), 1);
      assertEquals(secondRequests.get(), 0, "A complete first replica must avoid replica-multiplied fanout");
    } finally {
      firstServer.stop(0);
      secondServer.stop(0);
    }
  }

  @Test
  public void testIncompleteReplicaColumnsAreNotDoubleCountedOnCompleteFallback()
      throws IOException {
    ServerCompressionStatsResponse incomplete = compressionResponse(List.of("segment0"),
        List.of(segment("segment0", false, -1, -1,
            rawStats("col_a", 100, 25, ChunkCompressionType.LZ4))));
    ServerCompressionStatsResponse complete = compressionResponse(List.of("segment0"),
        List.of(segment("segment0", true, 500, 100,
            rawStats("col_a", 200, 50, ChunkCompressionType.LZ4),
            rawStats("col_b", 300, 50, ChunkCompressionType.ZSTANDARD))));
    HttpServer preferred = startServer(createHandler(currentInfo(), incomplete));
    HttpServer fallback = startServer(createHandler(currentInfo(), complete));
    try {
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(preferred.getAddress().getPort(), fallback.getAddress().getPort()), null, 2,
          TIMEOUT_MSEC, true, true, assignment(2, "segment0"));

      assertEquals(result.getCompressionStats().getUncompressedValueSizePerReplicaInBytes(), 500L);
      assertEquals(result.getCompressionStats().getForwardIndexAndDictionaryStorageSizePerReplicaInBytes(), 100L);
      assertEquals(result.getColumnCompressionStats().get(0).getColumn(), "col_a");
      assertEquals(result.getColumnCompressionStats().get(0).getUncompressedValueSizeInBytes(), 100L);
      assertEquals(result.getColumnCompressionStats().get(1).getColumn(), "col_b");
      assertEquals(result.getColumnCompressionStats().get(1).getUncompressedValueSizeInBytes(), 300L);
    } finally {
      preferred.stop(0);
      fallback.stop(0);
    }
  }

  @Test
  public void testReplicaPreferenceDistributesSegmentsAcrossServers()
      throws IOException {
    List<String> segments = new ArrayList<>();
    for (int i = 0; i < 1_000; i++) {
      segments.add("balancedSegment" + i);
    }
    AtomicInteger firstRequests = new AtomicInteger();
    AtomicInteger secondRequests = new AtomicInteger();
    HttpServer first = startServer(createEchoingCompressionHandler(currentInfo(), firstRequests));
    HttpServer second = startServer(createEchoingCompressionHandler(currentInfo(), secondRequests));
    try {
      Map<String, List<String>> assignment = new LinkedHashMap<>();
      assignment.put("server0", segments);
      assignment.put("server1", segments);
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(first.getAddress().getPort(), second.getAddress().getPort()), null, 2,
          TIMEOUT_MSEC, true, false, assignment);

      assertTrue(firstRequests.get() > 0);
      assertTrue(secondRequests.get() > 0);
      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), segments.size());
      assertFalse(result.getCompressionStats().isPartialCoverage());
    } finally {
      first.stop(0);
      second.stop(0);
    }
  }

  @Test
  public void testStalledPreferredReplicaFallsBackWithinDeadline()
      throws Exception {
    int timeoutMs = 2_000;
    ServerCompressionStatsResponse complete = compressionResponse(List.of("segment0"),
        List.of(segment("segment0", true, 100, 25,
            rawStats("col_a", 100, 25, ChunkCompressionType.LZ4))));
    AtomicInteger preferredRequests = new AtomicInteger();
    AtomicInteger fallbackRequests = new AtomicInteger();
    CountDownLatch preferredStarted = new CountDownLatch(1);
    CountDownLatch releasePreferred = new CountDownLatch(1);
    CountDownLatch fallbackStarted = new CountDownLatch(1);
    HttpServer preferred = startServer(createBlockingCompressionHandler(currentInfo(), complete,
        preferredRequests, preferredStarted, releasePreferred));
    HttpServer fallback = startServer(createSignalingHandler(currentInfo(), complete,
        fallbackRequests, fallbackStarted));
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<TableMetadataInfo> resultFuture = caller.submit(() -> reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(preferred.getAddress().getPort(), fallback.getAddress().getPort()), null, 2,
          timeoutMs, true, true, assignment(2, "segment0")));

      assertTrue(preferredStarted.await(5, TimeUnit.SECONDS));
      assertTrue(fallbackStarted.await(5, TimeUnit.SECONDS),
          "A stalled preferred replica must leave time for a healthy fallback");
      TableMetadataInfo result = resultFuture.get(5, TimeUnit.SECONDS);
      assertEquals(preferredRequests.get(), 1);
      assertEquals(fallbackRequests.get(), 1);
      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), 1);
      assertFalse(result.getCompressionStats().isPartialCoverage());
    } finally {
      releasePreferred.countDown();
      preferred.stop(0);
      fallback.stop(0);
      caller.shutdownNow();
    }
  }

  @Test
  public void testFinalReplicaKeepsOverallDeadlineWhileAnotherSegmentFallsBack()
      throws Exception {
    int timeoutMs = 2_500;
    ServerCompressionStatsResponse preferredResponse = compressionResponse(List.of("segment0", "segment1"),
        List.of(segment("segment0", true, 100, 25), segment("segment1", true, 200, 50)));
    ServerCompressionStatsResponse fallbackResponse = compressionResponse(List.of("segment0"),
        List.of(segment("segment0", true, 100, 25)));
    AtomicInteger preferredRequests = new AtomicInteger();
    AtomicInteger fallbackRequests = new AtomicInteger();
    CountDownLatch preferredStarted = new CountDownLatch(1);
    CountDownLatch releasePreferred = new CountDownLatch(1);
    CountDownLatch fallbackStarted = new CountDownLatch(1);
    HttpServer preferred = startServer(createBlockingCompressionHandler(currentInfo(), preferredResponse,
        preferredRequests, preferredStarted, releasePreferred));
    HttpServer fallback = startServer(createSignalingHandler(currentInfo(), fallbackResponse, fallbackRequests,
        fallbackStarted));
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Map<String, List<String>> assignment = new LinkedHashMap<>();
      assignment.put("server0", List.of("segment0", "segment1"));
      assignment.put("server1", List.of("segment0"));

      Future<TableMetadataInfo> resultFuture = caller.submit(() -> reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(preferred.getAddress().getPort(), fallback.getAddress().getPort()), null, 2,
          timeoutMs, true, false, assignment));

      assertTrue(preferredStarted.await(5, TimeUnit.SECONDS));
      assertTrue(fallbackStarted.await(5, TimeUnit.SECONDS),
          "The retryable segment must fall back before the final replica is released");
      releasePreferred.countDown();
      TableMetadataInfo result = resultFuture.get(5, TimeUnit.SECONDS);

      assertEquals(preferredRequests.get(), 2,
          "Final and retryable segment groups use independent request deadlines");
      assertEquals(fallbackRequests.get(), 1,
          "The retryable segment must fall back without cancelling the final replica request");
      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), 2);
      assertEquals(result.getCompressionStats().getTotalSegments(), 2);
      assertFalse(result.getCompressionStats().isPartialCoverage());
    } finally {
      releasePreferred.countDown();
      preferred.stop(0);
      fallback.stop(0);
      caller.shutdownNow();
    }
  }

  @Test
  public void testIncompleteSegmentRemainsInCoverageDenominator()
      throws IOException {
    ServerCompressionStatsResponse response = compressionResponse(List.of("segment0"),
        List.of(segment("segment0", false, -1, -1,
            rawStats("col_a", 100, 25, ChunkCompressionType.LZ4))));
    HttpServer server = startServer(createHandler(currentInfo(), response));
    try {
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(server.getAddress().getPort()), null, 1, TIMEOUT_MSEC, true, true,
          assignment(1, "segment0"));

      assertNotNull(result.getCompressionStats());
      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), 0);
      assertEquals(result.getCompressionStats().getTotalSegments(), 1);
      assertTrue(result.getCompressionStats().isPartialCoverage());
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testSegmentMissingFromEveryServerRemainsInCoverageDenominator()
      throws IOException {
    ServerCompressionStatsResponse response = compressionResponse(List.of("segment0"), List.of());
    HttpServer server = startServer(createHandler(currentInfo(), response));
    try {
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(server.getAddress().getPort()), null, 1, TIMEOUT_MSEC, true, false,
          assignment(1, "segment0"));

      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), 0);
      assertEquals(result.getCompressionStats().getTotalSegments(), 1);
      assertTrue(result.getCompressionStats().isPartialCoverage());
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testCompressionFanoutUsesRemainingMetadataTimeout()
      throws Exception {
    int timeoutMs = 500;
    AtomicInteger compressionRequests = new AtomicInteger();
    CountDownLatch metadataStarted = new CountDownLatch(1);
    CountDownLatch releaseMetadata = new CountDownLatch(1);
    ServerCompressionStatsResponse response = compressionResponse(List.of("segment0"),
        List.of(segment("segment0", true, 100, 25)));
    ExecutorService serverExecutor = Executors.newFixedThreadPool(2);
    HttpServer server = startServer(exchange -> {
      if (exchange.getRequestURI().getPath().endsWith("/compression-stats")) {
        compressionRequests.incrementAndGet();
        writeJson(exchange, response);
      } else {
        metadataStarted.countDown();
        try {
          releaseMetadata.await();
          writeJson(exchange, currentInfo());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while holding metadata response", e);
        }
      }
    }, serverExecutor);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<TableMetadataInfo> resultFuture = caller.submit(() -> reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(server.getAddress().getPort()), null, 1, timeoutMs, true, false,
          assignment(1, "segment0")));

      assertTrue(metadataStarted.await(5, TimeUnit.SECONDS));
      TableMetadataInfo result = resultFuture.get(5, TimeUnit.SECONDS);
      assertEquals(compressionRequests.get(), 0,
          "Compression collection must not start a second timeout window after metadata fanout");
      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), 0);
      assertEquals(result.getCompressionStats().getTotalSegments(), 1);
      assertTrue(result.getCompressionStats().isPartialCoverage());
    } finally {
      releaseMetadata.countDown();
      server.stop(0);
      serverExecutor.shutdownNow();
      caller.shutdownNow();
    }
  }

  @Test
  public void testMetadataDeadlineIncludesExecutorQueueTime()
      throws Exception {
    AtomicInteger serverRequests = new AtomicInteger();
    HttpServer server = startServer(exchange -> {
      serverRequests.incrementAndGet();
      writeJson(exchange, currentInfo());
    });
    CountDownLatch blockerStarted = new CountDownLatch(1);
    CountDownLatch releaseBlocker = new CountDownLatch(1);
    ThreadPoolExecutor saturatedExecutor =
        new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    saturatedExecutor.submit(() -> {
      blockerStarted.countDown();
      try {
        releaseBlocker.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
      Future<TableMetadataInfo> resultFuture = caller.submit(() ->
          new ServerSegmentMetadataReader(saturatedExecutor, _connectionManager)
              .getAggregatedTableMetadataFromServer("testTable_OFFLINE", endpoints(server.getAddress().getPort()),
                  null, 1, 200, true, false, assignment(1, "segment0")));

      TableMetadataInfo result = resultFuture.get(2, TimeUnit.SECONDS);
      assertTrue(result.getCompressionStats().isPartialCoverage());

      releaseBlocker.countDown();
      saturatedExecutor.shutdown();
      assertTrue(saturatedExecutor.awaitTermination(5, TimeUnit.SECONDS));
      assertEquals(serverRequests.get(), 0, "Cancelled queued HTTP work must not execute after the deadline");
    } finally {
      releaseBlocker.countDown();
      saturatedExecutor.shutdownNow();
      caller.shutdownNow();
      server.stop(0);
    }
  }

  @Test
  public void testLegacyCollectionDoesNotApplyFanoutDeadlineToQueuedRequests()
      throws Exception {
    AtomicInteger serverRequests = new AtomicInteger();
    HttpServer server = startServer(exchange -> {
      serverRequests.incrementAndGet();
      writeJson(exchange, currentInfo());
    });
    CountDownLatch blockerStarted = new CountDownLatch(1);
    CountDownLatch releaseBlocker = new CountDownLatch(1);
    CountDownLatch requestQueued = new CountDownLatch(1);
    ThreadPoolExecutor requestExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>()) {
      @Override
      public void execute(Runnable command) {
        super.execute(command);
        if (getQueue().contains(command)) {
          requestQueued.countDown();
        }
      }
    };
    requestExecutor.submit(() -> {
      blockerStarted.countDown();
      try {
        releaseBlocker.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
      String endpoint = "http://localhost:" + server.getAddress().getPort();
      BiMap<String, String> serverEndpoints = HashBiMap.create();
      serverEndpoints.put("server0", endpoint);

      Future<CompletionServiceHelper.CompletionServiceResponse> resultFuture = caller.submit(() ->
          new CompletionServiceHelper(requestExecutor, _connectionManager, serverEndpoints.inverse())
              .doMultiGetRequest(List.of(endpoint + "/tables/testTable_OFFLINE/metadata"),
                  "testTable_OFFLINE", false, 500));
      assertTrue(requestQueued.await(5, TimeUnit.SECONDS));
      scheduler.schedule(releaseBlocker::countDown, 750, TimeUnit.MILLISECONDS);
      CompletionServiceHelper.CompletionServiceResponse result = resultFuture.get(5, TimeUnit.SECONDS);

      assertEquals(result._failedResponseCount, 0);
      assertEquals(result._httpResponses.size(), 1);
      assertEquals(serverRequests.get(), 1,
          "Legacy callers must allow queued work to start after the per-request timeout interval");
    } finally {
      releaseBlocker.countDown();
      requestExecutor.shutdownNow();
      scheduler.shutdownNow();
      caller.shutdownNow();
      server.stop(0);
    }
  }

  @Test
  public void testDeadlineIncludesResponseBody()
      throws Exception {
    CountDownLatch bodyStarted = new CountDownLatch(1);
    CountDownLatch releaseBody = new CountDownLatch(1);
    ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
    HttpServer server = startServer(exchange -> {
      exchange.sendResponseHeaders(200, 2);
      try (OutputStream responseBody = exchange.getResponseBody()) {
        responseBody.write('{');
        responseBody.flush();
        bodyStarted.countDown();
        try {
          releaseBody.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        responseBody.write('}');
      }
    }, serverExecutor);
    ExecutorService requestExecutor = Executors.newSingleThreadExecutor();
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      String endpoint = "http://localhost:" + server.getAddress().getPort();
      BiMap<String, String> serverEndpoints = HashBiMap.create();
      serverEndpoints.put("server0", endpoint);
      Future<CompletionServiceHelper.CompletionServiceResponse> resultFuture = caller.submit(() ->
          new CompletionServiceHelper(requestExecutor, _connectionManager, serverEndpoints.inverse())
              .doMultiGetRequestUntil(List.of(endpoint + "/tables/testTable_OFFLINE/metadata"),
                  "testTable_OFFLINE", false, 5_000, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200),
                  "deadline test"));

      assertTrue(bodyStarted.await(5, TimeUnit.SECONDS));
      CompletionServiceHelper.CompletionServiceResponse result = resultFuture.get(2, TimeUnit.SECONDS);
      assertEquals(result._failedResponseCount, 1);
      assertTrue(result._httpResponses.isEmpty());
      requestExecutor.submit(() -> { }).get(2, TimeUnit.SECONDS);
    } finally {
      releaseBody.countDown();
      requestExecutor.shutdownNow();
      caller.shutdownNow();
      server.stop(0);
      serverExecutor.shutdownNow();
    }
  }

  @Test
  public void testInterruptedCollectionReleasesBufferedConnection()
      throws Exception {
    CountDownLatch bodyStarted = new CountDownLatch(1);
    CountDownLatch releaseBody = new CountDownLatch(1);
    ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
    HttpServer server = startServer(exchange -> {
      exchange.sendResponseHeaders(200, 2);
      try (OutputStream responseBody = exchange.getResponseBody()) {
        responseBody.write('{');
        responseBody.flush();
        bodyStarted.countDown();
        try {
          releaseBody.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        responseBody.write('}');
      }
    }, serverExecutor);
    ExecutorService requestExecutor = Executors.newSingleThreadExecutor();
    ExecutorService caller = Executors.newSingleThreadExecutor();
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    try {
      String endpoint = "http://localhost:" + server.getAddress().getPort();
      BiMap<String, String> serverEndpoints = HashBiMap.create();
      serverEndpoints.put("server0", endpoint);
      Future<CompletionServiceHelper.CompletionServiceResponse> resultFuture = caller.submit(() ->
          new CompletionServiceHelper(requestExecutor, connectionManager, serverEndpoints.inverse())
              .doMultiGetRequest(List.of(endpoint + "/tables/testTable_OFFLINE/metadata"),
                  "testTable_OFFLINE", false, 5_000));

      assertTrue(bodyStarted.await(5, TimeUnit.SECONDS));
      assertTrue(resultFuture.cancel(true));
      caller.submit(() -> { }).get(2, TimeUnit.SECONDS);
      requestExecutor.submit(() -> { }).get(2, TimeUnit.SECONDS);
      assertEquals(connectionManager.getTotalStats().getLeased(), 0);
    } finally {
      releaseBody.countDown();
      requestExecutor.shutdownNow();
      caller.shutdownNow();
      connectionManager.close();
      server.stop(0);
      serverExecutor.shutdownNow();
    }
  }

  @Test
  public void testCompressionRequestsUseBoundedBatchesAndRollingConcurrency()
      throws Exception {
    int numSegments = 4_001;
    List<String> segments = new ArrayList<>(numSegments);
    for (int i = 0; i < numSegments; i++) {
      segments.add("segment" + i);
    }
    AtomicInteger requests = new AtomicInteger();
    AtomicInteger activeRequests = new AtomicInteger();
    AtomicInteger maxActiveRequests = new AtomicInteger();
    AtomicBoolean oversizedBatch = new AtomicBoolean();
    AtomicBoolean initialWindowTimedOut = new AtomicBoolean();
    CountDownLatch initialWindowStarted = new CountDownLatch(8);
    CountDownLatch blockedRequestStarted = new CountDownLatch(1);
    CountDownLatch ninthRequestStarted = new CountDownLatch(1);
    CountDownLatch releaseBlockedRequest = new CountDownLatch(1);
    ExecutorService serverExecutor = Executors.newFixedThreadPool(9);
    HttpServer server = startServer(exchange -> {
      if (!exchange.getRequestURI().getPath().endsWith("/compression-stats")) {
        writeJson(exchange, currentInfo());
        return;
      }
      int requestNumber = requests.incrementAndGet();
      int active = activeRequests.incrementAndGet();
      maxActiveRequests.accumulateAndGet(active, Math::max);
      try {
        TableSegments requested = JsonUtils.stringToObject(
            new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8), TableSegments.class);
        List<String> requestedSegments = requested.getSegments();
        oversizedBatch.compareAndSet(false, requestedSegments.size() > 500);
        if (requestNumber <= 8) {
          initialWindowStarted.countDown();
          if (!initialWindowStarted.await(5, TimeUnit.SECONDS)) {
            initialWindowTimedOut.set(true);
          }
        }
        if (requestNumber == 1) {
          blockedRequestStarted.countDown();
          if (!releaseBlockedRequest.await(10, TimeUnit.SECONDS)) {
            throw new IOException("Timed out waiting to release the blocked compression request");
          }
        } else if (requestNumber == 9) {
          ninthRequestStarted.countDown();
        }
        List<SegmentCompressionStatsContribution> contributions = new ArrayList<>(requestedSegments.size());
        for (String segmentName : requestedSegments) {
          contributions.add(segment(segmentName, true, 1, 1));
        }
        writeJson(exchange, new ServerCompressionStatsResponse(
            ServerCompressionStatsResponse.CURRENT_METADATA_VERSION, contributions));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while synchronizing compression request wave", e);
      } finally {
        activeRequests.decrementAndGet();
      }
    }, serverExecutor);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<TableMetadataInfo> resultFuture = caller.submit(() -> reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(server.getAddress().getPort()), null, 1, 60_000, true, false,
          Map.of("server0", segments)));

      assertTrue(blockedRequestStarted.await(5, TimeUnit.SECONDS));
      assertTrue(ninthRequestStarted.await(5, TimeUnit.SECONDS),
          "A completed request must immediately free an in-flight slot while another request is stalled");
      releaseBlockedRequest.countDown();
      TableMetadataInfo result = resultFuture.get(10, TimeUnit.SECONDS);

      assertEquals(requests.get(), 9);
      assertFalse(oversizedBatch.get());
      assertFalse(initialWindowTimedOut.get());
      assertEquals(maxActiveRequests.get(), 8);
      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), numSegments);
      assertEquals(result.getCompressionStats().getTotalSegments(), numSegments);
      assertFalse(result.getCompressionStats().isPartialCoverage());
    } finally {
      releaseBlockedRequest.countDown();
      server.stop(0);
      serverExecutor.shutdownNow();
      caller.shutdownNow();
    }
  }

  @Test
  public void testOversizedDetailResponseIsSplitWithoutChangingReplica()
      throws Exception {
    List<String> segments = List.of("segment0", "segment1", "segment2", "segment3", "segment4", "segment5",
        "segment6", "segment7");
    AtomicInteger requests = new AtomicInteger();
    AtomicInteger largestSuccessfulBatch = new AtomicInteger();
    HttpServer server = startServer(exchange -> {
      if (!exchange.getRequestURI().getPath().endsWith("/compression-stats")) {
        writeJson(exchange, currentInfo());
        return;
      }
      requests.incrementAndGet();
      TableSegments requested = JsonUtils.stringToObject(
          new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8), TableSegments.class);
      if (requested.getSegments().size() > 3) {
        exchange.sendResponseHeaders(413, -1);
        exchange.close();
        return;
      }
      largestSuccessfulBatch.accumulateAndGet(requested.getSegments().size(), Math::max);
      writeJson(exchange, completeResponse(requested.getSegments()));
    });
    try {
      List<String> columns = List.of("column0", "column1", "column2", "column3", "column4", "column5",
          "column6", "column7", "column8", "column9");
      TableMetadataInfo result = reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(server.getAddress().getPort()), columns, 1, TIMEOUT_MSEC, true, true,
          Map.of("server0", segments));

      assertEquals(requests.get(), 7);
      assertTrue(largestSuccessfulBatch.get() <= 3);
      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), segments.size());
      assertFalse(result.getCompressionStats().isPartialCoverage());
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testDetailRequestsBoundSegmentColumnContributionsAndConcurrency()
      throws Exception {
    List<String> segments = new ArrayList<>();
    for (int i = 0; i < 401; i++) {
      segments.add("detailSegment" + i);
    }
    List<String> columns = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      columns.add("column" + i);
    }
    AtomicInteger requests = new AtomicInteger();
    AtomicInteger activeRequests = new AtomicInteger();
    AtomicInteger maxActiveRequests = new AtomicInteger();
    AtomicBoolean oversizedBatch = new AtomicBoolean();
    CountDownLatch initialWindowStarted = new CountDownLatch(4);
    CountDownLatch fifthRequestStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstRequest = new CountDownLatch(1);
    ExecutorService serverExecutor = Executors.newFixedThreadPool(5);
    HttpServer server = startServer(exchange -> {
      if (!exchange.getRequestURI().getPath().endsWith("/compression-stats")) {
        writeJson(exchange, currentInfo());
        return;
      }
      int requestNumber = requests.incrementAndGet();
      int active = activeRequests.incrementAndGet();
      maxActiveRequests.accumulateAndGet(active, Math::max);
      try {
        TableSegments requested = JsonUtils.stringToObject(
            new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8), TableSegments.class);
        oversizedBatch.compareAndSet(false, (long) requested.getSegments().size() * columns.size()
            > ServerCompressionStatsResponse.MAX_COLUMN_CONTRIBUTIONS_PER_RESPONSE);
        if (requestNumber <= 4) {
          initialWindowStarted.countDown();
          if (!initialWindowStarted.await(5, TimeUnit.SECONDS)) {
            throw new IOException("Timed out waiting for the detail request window");
          }
        }
        if (requestNumber == 1) {
          if (!releaseFirstRequest.await(10, TimeUnit.SECONDS)) {
            throw new IOException("Timed out waiting to release the first detail request");
          }
        } else if (requestNumber == 5) {
          fifthRequestStarted.countDown();
        }
        writeJson(exchange, completeResponse(requested.getSegments()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while synchronizing detail requests", e);
      } finally {
        activeRequests.decrementAndGet();
      }
    }, serverExecutor);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<TableMetadataInfo> resultFuture = caller.submit(() -> reader().getAggregatedTableMetadataFromServer(
          "testTable_OFFLINE", endpoints(server.getAddress().getPort()), columns, 1, 60_000, true, true,
          Map.of("server0", segments)));

      assertTrue(fifthRequestStarted.await(5, TimeUnit.SECONDS));
      releaseFirstRequest.countDown();
      TableMetadataInfo result = resultFuture.get(10, TimeUnit.SECONDS);

      assertEquals(requests.get(), 5);
      assertFalse(oversizedBatch.get());
      assertEquals(maxActiveRequests.get(), 4);
      assertEquals(result.getCompressionStats().getSegmentsWithCompleteStats(), segments.size());
    } finally {
      releaseFirstRequest.countDown();
      server.stop(0);
      serverExecutor.shutdownNow();
      caller.shutdownNow();
    }
  }

  private ServerSegmentMetadataReader reader() {
    return new ServerSegmentMetadataReader(_executor, _connectionManager);
  }

  private static ServerTableMetadataInfo currentInfo() {
    return ServerTableMetadataInfo.builder("testTable_OFFLINE")
        .withDiskSizeInBytes(50_000)
        .withNumSegments(3)
        .withNumRows(1_000)
        .withColumnLengthMap(Map.of("col_a", 4.0, "col_b", 100.0))
        .withColumnCardinalityMap(Map.of("col_a", 50.0, "col_b", 200.0))
        .withColumnIndexSizeMap(Map.of("col_a", Map.of("forward", 6_000d)))
        .build();
  }

  private static ServerCompressionStatsResponse completeResponse(List<String> segments) {
    List<SegmentCompressionStatsContribution> contributions = new ArrayList<>(segments.size());
    for (String segment : segments) {
      contributions.add(segment(segment, true, 1, 1));
    }
    return new ServerCompressionStatsResponse(ServerCompressionStatsResponse.CURRENT_METADATA_VERSION,
        contributions);
  }

  private static ServerCompressionStatsResponse compressionResponse(List<String> expectedSegments,
      List<SegmentCompressionStatsContribution> contributions) {
    List<SegmentCompressionStatsContribution> responseContributions = new ArrayList<>(expectedSegments.size()
        + contributions.size());
    for (String segment : expectedSegments) {
      responseContributions.add(new SegmentCompressionStatsContribution(segment, false, -1, -1, null));
    }
    responseContributions.addAll(contributions);
    return new ServerCompressionStatsResponse(ServerCompressionStatsResponse.CURRENT_METADATA_VERSION,
        responseContributions);
  }

  private static ColumnCompressionStatsContribution rawStats(String column, long rawSize,
      long forwardIndexAndDictionaryStorageSize, ChunkCompressionType chunkCompressionType) {
    return new ColumnCompressionStatsContribution(column, rawSize, forwardIndexAndDictionaryStorageSize,
        List.of("forward_index"),
        List.of(new ColumnCompressionStatsContribution.EncodingContribution(
            EncodingType.RAW, chunkCompressionType, 1, rawSize, forwardIndexAndDictionaryStorageSize)), 1);
  }

  private static SegmentCompressionStatsContribution segment(String segmentName, boolean complete,
      long rawSize, long forwardIndexAndDictionaryStorageSize, ColumnCompressionStatsContribution... columns) {
    Map<String, ColumnCompressionStatsContribution> columnMap = new LinkedHashMap<>();
    for (ColumnCompressionStatsContribution column : columns) {
      columnMap.put(column.getColumn(), column);
    }
    return new SegmentCompressionStatsContribution(segmentName, complete, rawSize,
        forwardIndexAndDictionaryStorageSize, columnMap);
  }

  private static BiMap<String, String> endpoints(int... ports) {
    BiMap<String, String> endpoints = HashBiMap.create();
    for (int i = 0; i < ports.length; i++) {
      endpoints.put("server" + i, "http://localhost:" + ports[i]);
    }
    return endpoints;
  }

  private static Map<String, List<String>> assignment(int numServers, String... segments) {
    Map<String, List<String>> assignment = new LinkedHashMap<>();
    for (int i = 0; i < numServers; i++) {
      assignment.put("server" + i, List.of(segments));
    }
    return assignment;
  }

  private static HttpHandler createHandler(Object metadata, Object compressionStats) {
    return exchange -> writeJson(exchange,
        exchange.getRequestURI().getPath().endsWith("/compression-stats") ? compressionStats : metadata);
  }

  private static HttpHandler createLegacyHandler(Object metadata) {
    return exchange -> {
      if (exchange.getRequestURI().getPath().endsWith("/compression-stats")) {
        exchange.sendResponseHeaders(404, -1);
        exchange.close();
      } else {
        writeJson(exchange, metadata);
      }
    };
  }

  private static HttpHandler createRecordingHandler(Object metadata, Object compressionStats,
      AtomicReference<String> query) {
    return exchange -> {
      if (exchange.getRequestURI().getPath().endsWith("/compression-stats")) {
        query.set(exchange.getRequestURI().getQuery());
        writeJson(exchange, compressionStats);
      } else {
        writeJson(exchange, metadata);
      }
    };
  }

  private static HttpHandler createCountingHandler(Object metadata, Object compressionStats, AtomicInteger requests) {
    return exchange -> {
      if (exchange.getRequestURI().getPath().endsWith("/compression-stats")) {
        requests.incrementAndGet();
        writeJson(exchange, compressionStats);
      } else {
        writeJson(exchange, metadata);
      }
    };
  }

  private static HttpHandler createEchoingCompressionHandler(Object metadata, AtomicInteger requests) {
    return exchange -> {
      if (exchange.getRequestURI().getPath().endsWith("/compression-stats")) {
        requests.incrementAndGet();
        TableSegments requested = JsonUtils.stringToObject(
            new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8), TableSegments.class);
        writeJson(exchange, completeResponse(requested.getSegments()));
      } else {
        writeJson(exchange, metadata);
      }
    };
  }

  private static HttpHandler createBlockingCompressionHandler(Object metadata, Object compressionStats,
      AtomicInteger requests, CountDownLatch started, CountDownLatch release) {
    return exchange -> {
      if (exchange.getRequestURI().getPath().endsWith("/compression-stats")) {
        requests.incrementAndGet();
        started.countDown();
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while holding compression response", e);
        }
        writeJson(exchange, compressionStats);
      } else {
        writeJson(exchange, metadata);
      }
    };
  }

  private static HttpHandler createSignalingHandler(Object metadata, Object compressionStats, AtomicInteger requests,
      CountDownLatch started) {
    return exchange -> {
      if (exchange.getRequestURI().getPath().endsWith("/compression-stats")) {
        requests.incrementAndGet();
        started.countDown();
        writeJson(exchange, compressionStats);
      } else {
        writeJson(exchange, metadata);
      }
    };
  }

  private static void writeJson(HttpExchange exchange, Object info)
      throws IOException {
    byte[] json = JsonUtils.objectToString(info).getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, json.length);
    try (OutputStream responseBody = exchange.getResponseBody()) {
      responseBody.write(json);
    }
  }

  private static HttpServer startServer(HttpHandler handler)
      throws IOException {
    return startServer(handler, null);
  }

  private static HttpServer startServer(HttpHandler handler, ExecutorService executor)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/tables/", handler);
    if (executor != null) {
      server.setExecutor(executor);
    }
    server.start();
    return server;
  }
}
