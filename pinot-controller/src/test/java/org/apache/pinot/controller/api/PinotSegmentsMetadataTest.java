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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.controller.utils.FakeHttpServer;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class PinotSegmentsMetadataTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentsMetadataTest.class);
  private static final String URI_PATH = "/tables/";
  private static final int TIMEOUT_MSEC = 10000;
  private final Executor _executor = Executors.newFixedThreadPool(1);
  private final HttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private final Map<String, SegmentsServerMock> _serverMap = new HashMap<>();
  private PinotHelixResourceManager _helix;

  private BiMap<String, String> serverEndpoints(List<String> servers) {
    BiMap<String, String> endpoints = HashBiMap.create(servers.size());
    for (String server : servers) {
      endpoints.put(server, _serverMap.get(server)._endpoint);
    }
    return endpoints;
  }

  @BeforeClass
  public void setUp()
      throws IOException {
    _helix = mock(PinotHelixResourceManager.class);
    when(_helix.hasOfflineTable(anyString())).thenAnswer((Answer) invocationOnMock -> {
      String table = (String) invocationOnMock.getArguments()[0];
      return table.contains("offline");
    });

    when(_helix.hasRealtimeTable(anyString())).thenAnswer((Answer) invocationOnMock -> {
      String table = (String) invocationOnMock.getArguments()[0];
      return table.contains("realtime");
    });

    int counter = 0;
    // server0
    SegmentsServerMock s = new SegmentsServerMock("s1");
    s.updateMetadataMock();
    s.start(URI_PATH, createSegmentMetadataHandler(200, s._segmentMetadata, 0));
    _serverMap.put(serverName(counter), s);
    counter++;

    // server1
    s = new SegmentsServerMock("s2");
    s.updateMetadataMock();
    s.start(URI_PATH, createSegmentMetadataHandler(200, s._segmentMetadata, 0));
    _serverMap.put(serverName(counter), s);
    counter++;

    // server2
    s = new SegmentsServerMock("s3");
    s.updateMetadataMock();
    s.start(URI_PATH, createSegmentMetadataHandler(404, s._segmentMetadata, 0));
    _serverMap.put(serverName(counter), s);
    counter++;
  }

  private String serverName(int counter) {
    return "server" + counter;
  }

  @AfterClass
  public void tearDown() {
    for (Map.Entry<String, SegmentsServerMock> fakeServerEntry : _serverMap.entrySet()) {
      fakeServerEntry.getValue().stop();
    }
  }

  private HttpHandler createSegmentMetadataHandler(final int status, final String segmentMetadata,
      final int sleepTimeMs) {
    return httpExchange -> {
      if (sleepTimeMs > 0) {
        try {
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          LOGGER.info("Handler interrupted during sleep");
        }
      }

      String json = JsonUtils.objectToString(segmentMetadata);
      httpExchange.sendResponseHeaders(status, json.length());
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(json.getBytes());
      responseBody.close();
    };
  }

  private List<String> testMetadataResponse(String table, Map<String, List<String>> serverToSegmentsMap,
      BiMap<String, String> endpoints) {
    ServerSegmentMetadataReader metadataReader = new ServerSegmentMetadataReader(_executor, _connectionManager);
    return metadataReader.getSegmentMetadataFromServer(table, serverToSegmentsMap, endpoints, null, TIMEOUT_MSEC);
  }

  private Map<String, List<String>> getServerToSegments(List<String> servers) {
    Map<String, List<String>> serverToSegmentsMap = new HashMap<>();
    for (String server : servers) {
      serverToSegmentsMap.put(server, Collections.singletonList(_serverMap.get(server)._segment));
    }
    return serverToSegmentsMap;
  }

  @Test
  public void testServerSegmentMetadataFetchSuccess() {
    final List<String> servers = MetadataConstants.SEGMENT_SERVERS.subList(0, 1);
    Map<String, List<String>> serverToSegmentsMap = getServerToSegments(servers);
    BiMap<String, String> endpoints = serverEndpoints(servers);
    String table = "offline";
    List<String> metadata = testMetadataResponse(table, serverToSegmentsMap, endpoints);
    assertEquals(1, metadata.size());
  }

  @Test
  public void testServerSegmentMetadataFetchError() {
    final List<String> servers = MetadataConstants.SEGMENT_SERVERS.subList(0, 2);
    Map<String, List<String>> serverToSegmentsMap = getServerToSegments(servers);
    int expectedNonResponsiveServers = 0;
    int totalResponses = 0;
    for (String server : serverToSegmentsMap.keySet()) {
      if (server.equalsIgnoreCase("server2")) {
        expectedNonResponsiveServers += serverToSegmentsMap.get(server).size();
      }
      totalResponses += serverToSegmentsMap.get(server).size();
    }
    BiMap<String, String> endpoints = serverEndpoints(servers);
    String table = "offline";
    List<String> metadata = testMetadataResponse(table, serverToSegmentsMap, endpoints);
    assertEquals(1, metadata.size());
    assertEquals(expectedNonResponsiveServers, totalResponses - metadata.size());
  }

  public static class SegmentsServerMock extends FakeHttpServer {
    String _segment;
    String _segmentMetadata;

    public SegmentsServerMock(String segment) {
      _segment = segment;
    }

    private void updateMetadataMock()
        throws IOException {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(MetadataConstants.SEGMENT_METADATA_STR);
      ObjectNode objectNode = jsonNode.deepCopy();
      objectNode.put("segmentName", _segment);
      _segmentMetadata = JsonUtils.objectToString(objectNode);
    }
  }

  public static class MetadataConstants {
    public static final List<String> SEGMENT_SERVERS =
        Arrays.asList("server1", "server2", "server3", "server4", "server5");
    public static final String SEGMENT_METADATA_STR =
        "{\n" + "  \"segmentName\" : \"testTable_OFFLINE_default_s1\",\n" + "  \"schemaName\" : null,\n"
            + "  \"crc\" : 1804064321,\n" + "  \"creationTimeMillis\" : 1595127594768,\n"
            + "  \"creationTimeReadable\" : \"2020-07-19T02:59:54:768 UTC\",\n" + "  \"timeGranularitySec\" : null,\n"
            + "  \"startTimeMillis\" : null,\n" + "  \"startTimeReadable\" : null,\n" + "  \"endTimeMillis\" : null,\n"
            + "  \"endTimeReadable\" : null,\n" + "  \"segmentVersion\" : \"v3\",\n" + "  \"creatorName\" : null,\n"
            + "  \"paddingCharacter\" : \"\\u0000\",\n" + "  \"columns\" : [ ],\n" + "  \"indexes\" : [ { } ]\n" + "}";
  }
}
