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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.IndexSizeBreakdownInfo;
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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests per-index-type size aggregation in [TableSizeReader], driven by `?includeIndexSizeStats=true`.
public class TableSizeReaderIndexSizeTest {
  private static final String URI_PATH = "/tables/";
  private static final int TIMEOUT_MSEC = 10000;
  private static final int NUM_REPLICAS = 2;

  private final ExecutorService _executor = Executors.newFixedThreadPool(4);
  private final PoolingHttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private final ControllerMetrics _controllerMetrics =
      new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
  private final Map<String, FakeSizeServer> _serverMap = new HashMap<>();
  private PinotHelixResourceManager _helix;
  private LeadControllerManager _leadControllerManager;

  @BeforeClass
  public void setUp()
      throws IOException {
    _helix = mock(PinotHelixResourceManager.class);
    _leadControllerManager = mock(LeadControllerManager.class);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("indexSizeTable").setNumReplicas(NUM_REPLICAS).build();
    ZkHelixPropertyStore mockPropertyStore = mock(ZkHelixPropertyStore.class);
    when(mockPropertyStore.get(ArgumentMatchers.anyString(), ArgumentMatchers.eq(null),
        ArgumentMatchers.eq(AccessOption.PERSISTENT))).thenAnswer((Answer) invocationOnMock -> {
          String path = (String) invocationOnMock.getArguments()[0];
          return path.contains("indexSizeTable_OFFLINE") ? TableConfigSerDeUtils.toZNRecord(tableConfig) : null;
        });
    when(_helix.getPropertyStore()).thenReturn(mockPropertyStore);
    when(_helix.getNumReplicas(any(TableConfig.class))).thenReturn(NUM_REPLICAS);
    when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);

    // server0: representative for s1 (larger disk size), non-representative for s2 (smaller disk size).
    addServer("server0", List.of(
        new SegmentSizeInfo("s1", 5000, Map.of("forward_index", 1000L, "inverted_index", 200L)),
        new SegmentSizeInfo("s2", 6000, Map.of("forward_index", 2000L, "bloom_filter", 300L))));

    // server1: replica of server0. Non-representative for s1, representative for s2 (larger disk size).
    addServer("server1", List.of(
        new SegmentSizeInfo("s1", 4999, Map.of("forward_index", 999L, "inverted_index", 199L)),
        new SegmentSizeInfo("s2", 6001, Map.of("forward_index", 2001L, "bloom_filter", 301L))));

    // server2: single segment reporting a negative (unavailable) size for one index type.
    addServer("server2", List.of(
        new SegmentSizeInfo("s3", 1000, Map.of("forward_index", 500L, "dictionary", -1L))));

    // server3: old server that does not report indexSizeInBytes at all, even though the flag was requested.
    addServer("server3", List.of(new SegmentSizeInfo("s4", 700)));

    // server4: replica holding the larger (representative) disk size for s5, but an old server that does not
    // report indexSizeInBytes at all -- simulates a rolling upgrade where the larger replica predates the flag.
    addServer("server4", List.of(new SegmentSizeInfo("s5", 2000)));

    // server5: replica of s5 with a smaller disk size, but a current server that does report indexSizeInBytes.
    addServer("server5", List.of(new SegmentSizeInfo("s5", 1000, Map.of("forward_index", 400L))));

    // server6/server7/server8: three replicas of s6, all reporting indexSizeInBytes, at three different disk
    // sizes. Exercises the "pick the largest reporting replica" half of the selection, not just the
    // "skip the non-reporting replica" half covered by server4/server5 above.
    addServer("server6", List.of(new SegmentSizeInfo("s6", 1000, Map.of("forward_index", 111L))));
    addServer("server7", List.of(new SegmentSizeInfo("s6", 3000, Map.of("forward_index", 333L))));
    addServer("server8", List.of(new SegmentSizeInfo("s6", 2000, Map.of("forward_index", 222L))));

    // server9: reports indexSizeInBytes as an empty (but non-null) map -- must be treated the same as a server
    // that omits indexSizeInBytes entirely, not as a reporting replica contributing zero index types.
    addServer("server9", List.of(new SegmentSizeInfo("s7", 2000, Map.of())));
    addServer("server10", List.of(new SegmentSizeInfo("s7", 1000, Map.of("forward_index", 700L))));

    // server11/server12: two replicas of s8 tied on disk size, both reporting the same indexSizeInBytes value.
    // The donor is whichever one _serverInfo (a HashMap) iterates last -- unspecified -- but both report 150, so
    // the expected total is unambiguous: 300 would mean both were double-counted.
    addServer("server11", List.of(new SegmentSizeInfo("s8", 1500, Map.of("forward_index", 150L))));
    addServer("server12", List.of(new SegmentSizeInfo("s8", 1500, Map.of("forward_index", 150L))));

    // server13: healthy replica of s9, reports indexSizeInBytes. server14: errored replica of the same segment.
    addServer("server13", List.of(new SegmentSizeInfo("s9", 1000, Map.of("forward_index", 900L))));
    addServer("server14", 404, List.of(new SegmentSizeInfo("s9", 1000, Map.of("forward_index", 900L))));

    // server15/server16: both replicas of s10 error out, so the segment is missing from all servers.
    addServer("server15", 404, List.of(new SegmentSizeInfo("s10", 1000, Map.of("forward_index", 1000L))));
    addServer("server16", 404, List.of(new SegmentSizeInfo("s10", 1000, Map.of("forward_index", 1000L))));
  }

  @AfterClass
  public void tearDown() {
    for (FakeSizeServer server : _serverMap.values()) {
      server.stop();
    }
    _executor.shutdownNow();
    _connectionManager.close();
  }

  private void addServer(String name, List<SegmentSizeInfo> sizes)
      throws IOException {
    addServer(name, 200, sizes);
  }

  private void addServer(String name, int status, List<SegmentSizeInfo> sizes)
      throws IOException {
    FakeSizeServer server = new FakeSizeServer(sizes);
    server.start(URI_PATH, createHandler(status, sizes));
    _serverMap.put(name, server);
  }

  private HttpHandler createHandler(int status, List<SegmentSizeInfo> segmentSizes) {
    return httpExchange -> {
      boolean includeIndexSizeStats = httpExchange.getRequestURI().getQuery() != null
          && httpExchange.getRequestURI().getQuery().contains("includeIndexSizeStats=true");
      long tableSizeInBytes = 0;
      List<SegmentSizeInfo> responseSegments = new ArrayList<>(segmentSizes.size());
      for (SegmentSizeInfo segmentSize : segmentSizes) {
        tableSizeInBytes += segmentSize.getDiskSizeInBytes();
        responseSegments.add(includeIndexSizeStats
            ? new SegmentSizeInfo(segmentSize.getSegmentName(), segmentSize.getDiskSizeInBytes(),
                segmentSize.getIndexSizeInBytes())
            : new SegmentSizeInfo(segmentSize.getSegmentName(), segmentSize.getDiskSizeInBytes()));
      }
      TableSizeInfo tableInfo = new TableSizeInfo("indexSizeTable", tableSizeInBytes, responseSegments,
          TableSizeInfo.CURRENT_METADATA_VERSION);
      byte[] json = JsonUtils.objectToString(tableInfo).getBytes(StandardCharsets.UTF_8);
      httpExchange.sendResponseHeaders(status, json.length);
      try (OutputStream responseBody = httpExchange.getResponseBody()) {
        responseBody.write(json);
      }
    };
  }

  private static class FakeSizeServer extends FakeHttpServer {
    final List<SegmentSizeInfo> _sizes;

    FakeSizeServer(List<SegmentSizeInfo> sizes) {
      _sizes = sizes;
    }
  }

  private TableSizeReader createReader(String... servers)
      throws InvalidConfigException {
    when(_helix.getServerToSegmentsMap(anyString(), any(), anyBoolean())).thenAnswer(
        (Answer<Map<String, List<String>>>) invocation -> {
          Map<String, List<String>> map = new HashMap<>();
          for (String server : servers) {
            List<String> segments = _serverMap.get(server)._sizes.stream().map(SegmentSizeInfo::getSegmentName)
                .collect(Collectors.toList());
            map.put(server, segments);
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

  @Test
  public void testFlagOffReturnsNullBreakdown()
      throws InvalidConfigException {
    TableSizeReader reader = createReader("server0", "server1");
    TableSizeReader.TableSubTypeSizeDetails details =
        reader.getTableSubtypeSize("indexSizeTable_OFFLINE", TIMEOUT_MSEC, true);
    assertNull(details._indexSizeBreakdown);
  }

  @Test
  public void testPicksDonorPerSegmentAndMergesDisjointIndexTypes()
      throws InvalidConfigException {
    TableSizeReader reader = createReader("server0", "server1");
    TableSizeReader.TableSubTypeSizeDetails details =
        reader.getTableSubtypeSize("indexSizeTable_OFFLINE", TIMEOUT_MSEC, true, false, true);

    Map<String, IndexSizeBreakdownInfo> breakdown = details._indexSizeBreakdown;
    assertNotNull(breakdown);

    // s1's donor is server0 (diskSize 5000 > 4999): forward_index=1000, inverted_index=200.
    // s2's donor is server1 (diskSize 6001 > 6000): forward_index=2001, bloom_filter=301.
    // A doubled sum (both replicas counted) would give forward_index=3999 instead of 3001.
    assertEquals(breakdown.get("forward_index").getSizePerReplicaInBytes(), 3001L);
    assertEquals(breakdown.get("forward_index").getSegmentsWithStats(), 2);

    assertEquals(breakdown.get("inverted_index").getSizePerReplicaInBytes(), 200L);
    assertEquals(breakdown.get("inverted_index").getSegmentsWithStats(), 1);

    assertEquals(breakdown.get("bloom_filter").getSizePerReplicaInBytes(), 301L);
    assertEquals(breakdown.get("bloom_filter").getSegmentsWithStats(), 1);
  }

  @Test
  public void testNegativeSizesAreFilteredOut()
      throws InvalidConfigException {
    TableSizeReader reader = createReader("server2");
    TableSizeReader.TableSubTypeSizeDetails details =
        reader.getTableSubtypeSize("indexSizeTable_OFFLINE", TIMEOUT_MSEC, true, false, true);

    Map<String, IndexSizeBreakdownInfo> breakdown = details._indexSizeBreakdown;
    assertNotNull(breakdown);
    assertEquals(breakdown.get("forward_index").getSizePerReplicaInBytes(), 500L);
    assertEquals(breakdown.get("forward_index").getSegmentsWithStats(), 1);
    assertTrue(!breakdown.containsKey("dictionary"), "Negative-size entry must not appear in the breakdown");
  }

  @Test
  public void testFlagOnButNoServerReportsSizesYieldsEmptyNotNull()
      throws InvalidConfigException {
    TableSizeReader reader = createReader("server3");
    TableSizeReader.TableSubTypeSizeDetails details =
        reader.getTableSubtypeSize("indexSizeTable_OFFLINE", TIMEOUT_MSEC, true, false, true);

    Map<String, IndexSizeBreakdownInfo> breakdown = details._indexSizeBreakdown;
    assertNotNull(breakdown, "Requesting the flag with no reporting server must yield an empty map, not null");
    assertTrue(breakdown.isEmpty());
  }

  @Test
  public void testPicksReportingReplicaWhenLargestReplicaIsOld()
      throws InvalidConfigException {
    // The largest-disk-size replica (server4) is an old server reporting no indexSizeInBytes at all. Without
    // picking a different donor, s5 would silently drop out of the breakdown despite server5's replica having
    // the data.
    TableSizeReader reader = createReader("server4", "server5");
    TableSizeReader.TableSubTypeSizeDetails details =
        reader.getTableSubtypeSize("indexSizeTable_OFFLINE", TIMEOUT_MSEC, true, false, true);

    Map<String, IndexSizeBreakdownInfo> breakdown = details._indexSizeBreakdown;
    assertNotNull(breakdown);
    assertEquals(breakdown.get("forward_index").getSizePerReplicaInBytes(), 400L);
    assertEquals(breakdown.get("forward_index").getSegmentsWithStats(), 1);

    // The disk-size representative (server4, the larger, non-reporting replica) must stay decoupled from the
    // index-size donor (server5) selected above: they are allowed to be different replicas for the same segment.
    assertEquals(details._reportedSizePerReplicaInBytes, 2000L);
    assertEquals(details._segments.get("s5")._maxReportedSizePerReplicaInBytes, 2000L);
    assertEquals(details._reportedSizeInBytes, 3000L);
  }

  @Test
  public void testPicksLargestAmongMultipleReportingReplicas()
      throws InvalidConfigException {
    // All three replicas of s6 report indexSizeInBytes, at three different disk sizes. The donor must be the
    // largest (server7, disk 3000, forward_index=333), not the first or last one iterated.
    TableSizeReader reader = createReader("server6", "server7", "server8");
    TableSizeReader.TableSubTypeSizeDetails details =
        reader.getTableSubtypeSize("indexSizeTable_OFFLINE", TIMEOUT_MSEC, true, false, true);

    Map<String, IndexSizeBreakdownInfo> breakdown = details._indexSizeBreakdown;
    assertNotNull(breakdown);
    assertEquals(breakdown.get("forward_index").getSizePerReplicaInBytes(), 333L);
    assertEquals(breakdown.get("forward_index").getSegmentsWithStats(), 1);
  }

  @Test
  public void testSkipsReplicaReportingEmptyIndexSizeMap()
      throws InvalidConfigException {
    // server9 holds the larger disk size for s7 but reports indexSizeInBytes as an empty map, which must be
    // treated the same as not reporting at all -- not as a reporting replica contributing zero index types.
    // Without that normalization, s7 would silently drop out of the breakdown despite server10 having the data.
    TableSizeReader reader = createReader("server9", "server10");
    TableSizeReader.TableSubTypeSizeDetails details =
        reader.getTableSubtypeSize("indexSizeTable_OFFLINE", TIMEOUT_MSEC, true, false, true);

    Map<String, IndexSizeBreakdownInfo> breakdown = details._indexSizeBreakdown;
    assertNotNull(breakdown);
    assertEquals(breakdown.get("forward_index").getSizePerReplicaInBytes(), 700L);
    assertEquals(breakdown.get("forward_index").getSegmentsWithStats(), 1);
  }

  @Test
  public void testTiedDiskSizesPickExactlyOneDonorNotBoth()
      throws InvalidConfigException {
    // server11 and server12 both hold s8 at the same disk size (1500) and both report forward_index=150.
    // Regardless of which one wins the tie, exactly one must be summed -- 300 would mean both were double-counted.
    TableSizeReader reader = createReader("server11", "server12");
    TableSizeReader.TableSubTypeSizeDetails details =
        reader.getTableSubtypeSize("indexSizeTable_OFFLINE", TIMEOUT_MSEC, true, false, true);

    Map<String, IndexSizeBreakdownInfo> breakdown = details._indexSizeBreakdown;
    assertNotNull(breakdown);
    assertEquals(breakdown.get("forward_index").getSizePerReplicaInBytes(), 150L);
    assertEquals(breakdown.get("forward_index").getSegmentsWithStats(), 1);
  }

  @Test
  public void testErroredReplicaDoesNotBlockHealthyReplicaFromDonatingIndexSizes()
      throws InvalidConfigException {
    // server14 errors out (404) for s9; server13 is healthy and reports indexSizeInBytes. The errored replica
    // must not prevent the healthy one from contributing to the breakdown, nor count as a missing segment.
    TableSizeReader reader = createReader("server13", "server14");
    TableSizeReader.TableSubTypeSizeDetails details =
        reader.getTableSubtypeSize("indexSizeTable_OFFLINE", TIMEOUT_MSEC, true, false, true);

    Map<String, IndexSizeBreakdownInfo> breakdown = details._indexSizeBreakdown;
    assertNotNull(breakdown);
    assertEquals(breakdown.get("forward_index").getSizePerReplicaInBytes(), 900L);
    assertEquals(breakdown.get("forward_index").getSegmentsWithStats(), 1);
    assertEquals(details._missingSegments, 0);
  }

  @Test
  public void testSegmentMissingFromAllServersIsExcludedFromBreakdown()
      throws InvalidConfigException {
    // server15 and server16 both error out (404) for s10, so it's missing from every server. It must be counted
    // as missing and must not contribute any (bogus) entry to the breakdown.
    TableSizeReader reader = createReader("server15", "server16");
    TableSizeReader.TableSubTypeSizeDetails details =
        reader.getTableSubtypeSize("indexSizeTable_OFFLINE", TIMEOUT_MSEC, true, false, true);

    assertEquals(details._missingSegments, 1);
    Map<String, IndexSizeBreakdownInfo> breakdown = details._indexSizeBreakdown;
    assertNotNull(breakdown);
    assertTrue(breakdown.isEmpty(), "A segment missing from all servers must not contribute to the breakdown");
  }

  @Test
  public void testGetTableSizeDetailsThreadsFlagThroughToSubtype()
      throws InvalidConfigException {
    TableSizeReader reader = createReader("server0", "server1");
    TableSizeReader.TableSizeDetails details =
        reader.getTableSizeDetails("indexSizeTable", TIMEOUT_MSEC, true, false, true);
    assertNotNull(details);
    assertNotNull(details._offlineSegments);
    assertNotNull(details._offlineSegments._indexSizeBreakdown);
    assertEquals(details._offlineSegments._indexSizeBreakdown.get("forward_index").getSizePerReplicaInBytes(), 3001L);
  }

  @Test
  public void testRollingUpgradeDonorSelectionThroughGetTableSizeDetails()
      throws InvalidConfigException {
    // Same rolling-upgrade scenario as testPicksReportingReplicaWhenLargestReplicaIsOld, but through the
    // production getTableSizeDetails entry point rather than the @VisibleForTesting getTableSubtypeSize overload.
    TableSizeReader reader = createReader("server4", "server5");
    TableSizeReader.TableSizeDetails details =
        reader.getTableSizeDetails("indexSizeTable", TIMEOUT_MSEC, true, false, true);

    assertNotNull(details);
    assertNotNull(details._offlineSegments);
    Map<String, IndexSizeBreakdownInfo> breakdown = details._offlineSegments._indexSizeBreakdown;
    assertNotNull(breakdown);
    assertEquals(breakdown.get("forward_index").getSizePerReplicaInBytes(), 400L);
    assertEquals(breakdown.get("forward_index").getSegmentsWithStats(), 1);
  }
}
