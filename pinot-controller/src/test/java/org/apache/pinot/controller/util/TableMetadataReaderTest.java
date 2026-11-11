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
package org.apache.pinot.controller.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class TableMetadataReaderTest {
  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final int TIMEOUT_MS = 10_000;

  private final ExecutorService _executor = Executors.newFixedThreadPool(4);
  private final PoolingHttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private final List<HttpServer> _httpServers = new ArrayList<>();

  @BeforeClass
  public void setUp() {
    _connectionManager.setDefaultMaxPerRoute(4);
  }

  @AfterMethod
  public void stopServers() {
    _httpServers.forEach(server -> server.stop(0));
    _httpServers.clear();
  }

  @AfterClass
  public void tearDown() {
    _executor.shutdownNow();
    _connectionManager.close();
  }

  /// Every server holds every segment, so any single server's response is already complete.
  @Test
  public void testTableLevelMetadataWithFullReplication()
      throws Exception {
    Map<String, List<String>> serverToSegments = new LinkedHashMap<>();
    serverToSegments.put("server0", List.of("seg0", "seg1", "seg2"));
    serverToSegments.put("server1", List.of("seg0", "seg1", "seg2"));
    serverToSegments.put("server2", List.of("seg0", "seg1", "seg2"));

    JsonNode metadata = getSegmentsMetadata(serverToSegments);

    assertEquals(segmentNames(metadata), Set.of("seg0", "seg1", "seg2"));
  }

  /// 4 servers with replication 2: each server holds only half of the segments, so every server's response is needed.
  @Test
  public void testTableLevelMetadataWithPartialReplication()
      throws Exception {
    Map<String, List<String>> serverToSegments = new LinkedHashMap<>();
    serverToSegments.put("server0", List.of("seg0", "seg1"));
    serverToSegments.put("server1", List.of("seg1", "seg2"));
    serverToSegments.put("server2", List.of("seg2", "seg3"));
    serverToSegments.put("server3", List.of("seg3", "seg0"));

    JsonNode metadata = getSegmentsMetadata(serverToSegments);

    assertEquals(segmentNames(metadata), Set.of("seg0", "seg1", "seg2", "seg3"));
  }

  /// Each server holds a distinct segment, so dropping any server's response loses its segment.
  @Test
  public void testTableLevelMetadataWithoutReplication()
      throws Exception {
    Map<String, List<String>> serverToSegments = new LinkedHashMap<>();
    serverToSegments.put("server0", List.of("seg0"));
    serverToSegments.put("server1", List.of("seg1"));
    serverToSegments.put("server2", List.of("seg2"));

    JsonNode metadata = getSegmentsMetadata(serverToSegments);

    assertEquals(segmentNames(metadata), Set.of("seg0", "seg1", "seg2"));
  }

  private JsonNode getSegmentsMetadata(Map<String, List<String>> serverToSegments)
      throws Exception {
    BiMap<String, String> serverToEndpoint = HashBiMap.create();
    for (Map.Entry<String, List<String>> entry : serverToSegments.entrySet()) {
      HttpServer httpServer = startServer(entry.getValue());
      serverToEndpoint.put(entry.getKey(), "http://localhost:" + httpServer.getAddress().getPort());
    }

    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    when(helixResourceManager.getServerToSegmentsMap(TABLE_NAME)).thenReturn(serverToSegments);
    when(helixResourceManager.getDataInstanceAdminEndpoints(anySet())).thenReturn(serverToEndpoint);

    TableMetadataReader reader = new TableMetadataReader(_executor, _connectionManager, helixResourceManager);
    return reader.getSegmentsMetadata(TABLE_NAME, null, null, TIMEOUT_MS);
  }

  /// Starts a fake server whose table-level endpoint returns metadata for the given segments.
  private HttpServer startServer(List<String> segments)
      throws IOException {
    ObjectNode tableLevelResponse = JsonUtils.newObjectNode();
    for (String segment : segments) {
      tableLevelResponse.set(segment, JsonUtils.newObjectNode().put("segmentName", segment));
    }
    byte[] tableLevelBody = JsonUtils.objectToString(tableLevelResponse).getBytes(StandardCharsets.UTF_8);

    HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    httpServer.createContext("/tables/" + TABLE_NAME + "/segments/metadata", exchange -> {
      exchange.sendResponseHeaders(200, tableLevelBody.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(tableLevelBody);
      }
    });
    httpServer.start();
    _httpServers.add(httpServer);
    return httpServer;
  }

  private static Set<String> segmentNames(JsonNode metadata) {
    Set<String> segmentNames = new HashSet<>();
    metadata.fieldNames().forEachRemaining(segmentNames::add);
    return segmentNames;
  }
}
