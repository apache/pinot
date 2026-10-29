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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.restlet.resources.IndexSizeBreakdownInfo;
import org.apache.pinot.common.restlet.resources.ServerTableMetadataInfo;
import org.apache.pinot.common.restlet.resources.TableMetadataInfo;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Covers the controller-side aggregation of `indexSizeBreakdown` over the server fan-out: summing per index type,
/// dividing by the replica count, filtering sentinel values, and refusing to report a total that is missing a server.
public class TableMetadataReaderIndexSizeTest {
  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final int TIMEOUT_MSEC = 10_000;

  private ExecutorService _executor;
  private PoolingHttpClientConnectionManager _connectionManager;
  private final List<HttpServer> _servers = new ArrayList<>();

  @BeforeClass
  public void setUp() {
    _executor = Executors.newFixedThreadPool(4);
    _connectionManager = new PoolingHttpClientConnectionManager();
  }

  @AfterClass
  public void tearDown() {
    for (HttpServer server : _servers) {
      server.stop(0);
    }
    _servers.clear();
    if (_executor != null) {
      _executor.shutdownNow();
    }
    if (_connectionManager != null) {
      _connectionManager.close();
    }
  }

  /// Two servers each holding one replica: the per-index totals are summed and then divided by the replica count, the
  /// same way diskSizeInBytes is, so the reported figure is per replica rather than per physical copy.
  @Test
  public void testSumsAcrossServersAndDividesByReplicas()
      throws Exception {
    int port0 = startServer(respondWith(breakdown("forward_index", 100L, 4, "bloom_filter", 20L, 4)));
    int port1 = startServer(respondWith(breakdown("forward_index", 100L, 4, "bloom_filter", 20L, 4)));

    Map<String, IndexSizeBreakdownInfo> breakdown = readBreakdown(2, port0, port1);

    assertNotNull(breakdown, "Both servers answered, so a breakdown must be reported");
    assertEquals(breakdown.get("forward_index").getSizePerReplicaInBytes(), 100L,
        "200 bytes across 2 replicas is 100 per replica");
    assertEquals(breakdown.get("bloom_filter").getSizePerReplicaInBytes(), 20L);
    assertEquals(breakdown.get("forward_index").getSegmentsWithStats(), 4,
        "8 contributing segments across 2 replicas is 4 per replica");
  }

  /// Index types present on only some servers still aggregate, rather than being dropped for lack of a counterpart.
  @Test
  public void testMergesDisjointIndexTypes()
      throws Exception {
    int port0 = startServer(respondWith(breakdown("forward_index", 50L, 1)));
    int port1 = startServer(respondWith(breakdown("text_index", 70L, 1)));

    Map<String, IndexSizeBreakdownInfo> breakdown = readBreakdown(1, port0, port1);

    assertNotNull(breakdown);
    assertEquals(breakdown.keySet(), Map.of("forward_index", 1, "text_index", 1).keySet());
    assertEquals(breakdown.get("forward_index").getSizePerReplicaInBytes(), 50L);
    assertEquals(breakdown.get("text_index").getSizePerReplicaInBytes(), 70L);
  }

  /// The important negative case. When a server response cannot be parsed its segments never enter
  /// `totalNumSegments` either, so a caller has no way to notice the shortfall. A per-index-type total quietly missing
  /// a whole server is worse than no total, so the field is omitted entirely.
  @Test
  public void testOmitsBreakdownWhenAServerResponseIsUnparseable()
      throws Exception {
    int good = startServer(respondWith(breakdown("forward_index", 100L, 2)));
    int broken = startServer(exchange -> respond(exchange, "{this is not valid json"));

    Map<String, IndexSizeBreakdownInfo> breakdown = readBreakdown(1, good, broken);

    assertNull(breakdown, "A partial fan-out must report absence, not a total that looks complete");
  }

  /// A server that does not report the breakdown at all (feature off, or an older build) contributes nothing and must
  /// not be mistaken for a failure.
  @Test
  public void testNoBreakdownReportedYieldsAbsence()
      throws Exception {
    int port = startServer(respondWith(null));

    Map<String, IndexSizeBreakdownInfo> breakdown = readBreakdown(1, port);

    assertNull(breakdown, "No server reported sizes, so there is nothing to report");
  }

  /// A negative size is a sentinel, never a measurement, and must not reach a total.
  @Test
  public void testFiltersNegativeSizes()
      throws Exception {
    int port = startServer(respondWith(breakdown("forward_index", -1L, 1, "bloom_filter", 30L, 1)));

    Map<String, IndexSizeBreakdownInfo> breakdown = readBreakdown(1, port);

    assertNotNull(breakdown);
    assertTrue(breakdown.containsKey("bloom_filter"), "The valid entry must survive");
    assertEquals(breakdown.get("bloom_filter").getSizePerReplicaInBytes(), 30L);
    assertEquals(breakdown.get("forward_index"), null, "A sentinel size must not be aggregated");
  }

  private Map<String, IndexSizeBreakdownInfo> readBreakdown(int numReplica, int... ports) {
    TableMetadataInfo info =
        new ServerSegmentMetadataReader(_executor, _connectionManager).getAggregatedTableMetadataFromServer(TABLE_NAME,
            endpoints(ports), null, numReplica, TIMEOUT_MSEC, false, false, Map.of(), true);
    assertNotNull(info);
    return info.getIndexSizeBreakdown();
  }

  private static Map<String, IndexSizeBreakdownInfo> breakdown(Object... keySizeSegments) {
    Map<String, IndexSizeBreakdownInfo> breakdown = new java.util.HashMap<>();
    for (int i = 0; i < keySizeSegments.length; i += 3) {
      breakdown.put((String) keySizeSegments[i],
          new IndexSizeBreakdownInfo((Long) keySizeSegments[i + 1], (Integer) keySizeSegments[i + 2]));
    }
    return breakdown;
  }

  private static HttpHandler respondWith(Map<String, IndexSizeBreakdownInfo> breakdown) {
    ServerTableMetadataInfo info = ServerTableMetadataInfo.builder(TABLE_NAME)
        .withDiskSizeInBytes(1000)
        .withNumSegments(2)
        .withNumRows(10)
        .withIndexSizeBreakdown(breakdown)
        .build();
    String json;
    try {
      json = JsonUtils.objectToString(info);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return exchange -> respond(exchange, json);
  }

  private static void respond(HttpExchange exchange, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private int startServer(HttpHandler handler)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/tables/", handler);
    server.start();
    _servers.add(server);
    return server.getAddress().getPort();
  }

  private static BiMap<String, String> endpoints(int... ports) {
    BiMap<String, String> endpoints = HashBiMap.create();
    for (int i = 0; i < ports.length; i++) {
      endpoints.put("server" + i, "http://localhost:" + ports[i]);
    }
    return endpoints;
  }
}
