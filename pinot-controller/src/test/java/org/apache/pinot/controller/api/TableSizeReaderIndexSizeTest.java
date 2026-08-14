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
    FakeSizeServer server = new FakeSizeServer(sizes);
    server.start(URI_PATH, createHandler(sizes));
    _serverMap.put(name, server);
  }

  private HttpHandler createHandler(List<SegmentSizeInfo> segmentSizes) {
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
      httpExchange.sendResponseHeaders(200, json.length);
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
  public void testRepresentativeServerSelectionAndDisjointMerge()
      throws InvalidConfigException {
    TableSizeReader reader = createReader("server0", "server1");
    TableSizeReader.TableSubTypeSizeDetails details =
        reader.getTableSubtypeSize("indexSizeTable_OFFLINE", TIMEOUT_MSEC, true, false, true);

    Map<String, IndexSizeBreakdownInfo> breakdown = details._indexSizeBreakdown;
    assertNotNull(breakdown);

    // s1's representative is server0 (diskSize 5000 > 4999): forward_index=1000, inverted_index=200.
    // s2's representative is server1 (diskSize 6001 > 6000): forward_index=2001, bloom_filter=301.
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
}
