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
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PinotSegmentsMetadataTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentsMetadataTest.class);
  private final Executor executor = Executors.newFixedThreadPool(1);
  private final HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
  private final String URI_PATH = "/tables/";
  private final int timeoutMsec = 10000;
  private final Map<String, SegmentsServerMock> serverMap = new HashMap<>();
  private PinotHelixResourceManager helix;

  private BiMap<String, String> serverEndpoints(List<String> servers) {
    BiMap<String, String> endpoints = HashBiMap.create(servers.size());
    for (String server : servers) {
      endpoints.put(server, serverMap.get(server).endpoint);
    }
    return endpoints;
  }

  @BeforeClass
  public void setUp()
          throws IOException {
    helix = mock(PinotHelixResourceManager.class);
    when(helix.hasOfflineTable(anyString())).thenAnswer((Answer) invocationOnMock -> {
      String table = (String) invocationOnMock.getArguments()[0];
      return table.contains("offline");
    });

    when(helix.hasRealtimeTable(anyString())).thenAnswer((Answer) invocationOnMock -> {
      String table = (String) invocationOnMock.getArguments()[0];
      return table.contains("realtime");
    });

    int counter = 0;
    // server0
    SegmentsServerMock s = new SegmentsServerMock("s1");
    s.updateMetadataMock();
    s.start(URI_PATH, createSegmentMetadataHandler(200, s.segmentMetadata, 0));
    serverMap.put(serverName(counter), s);
    ++counter;

    // server1
    s = new SegmentsServerMock("s2");
    s.updateMetadataMock();
    s.start(URI_PATH, createSegmentMetadataHandler(200, s.segmentMetadata, 0));
    serverMap.put(serverName(counter), s);
    ++counter;

    // server2
    s = new SegmentsServerMock("s3");
    s.updateMetadataMock();
    s.start(URI_PATH, createSegmentMetadataHandler(404, s.segmentMetadata, 0));
    serverMap.put(serverName(counter), s);
    ++counter;
  }

  private String serverName(int counter) {
    return "server" + counter;
  }

  @AfterClass
  public void tearDown() {
    for (Map.Entry<String, SegmentsServerMock> fakeServerEntry : serverMap.entrySet()) {
      fakeServerEntry.getValue().httpServer.stop(0);
    }
  }

  private HttpHandler createSegmentMetadataHandler(final int status, final String segmentMetadata, final int sleepTimeMs) {
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
    ServerSegmentMetadataReader metadataReader = new ServerSegmentMetadataReader(executor, connectionManager);
    return metadataReader.getSegmentMetadataFromServer(table, serverToSegmentsMap, endpoints, timeoutMsec);
  }

  private Map<String, List<String>> getServerToSegments(List<String> servers) {
    Map<String, List<String>> serverToSegmentsMap = new HashMap<>();
    for (String server : servers) {
      serverToSegmentsMap.put(server, Collections.singletonList(serverMap.get(server).segment));
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
    Assert.assertEquals(1, metadata.size());
  }

  @Test
  public void testServerSegmentMetadataFetchError() {
    final List<String> servers = MetadataConstants.SEGMENT_SERVERS.subList(0, 2);
    Map<String, List<String>> serverToSegmentsMap = getServerToSegments(servers);
    int expectedNonResponsiveServers = 0;
    int totalResponses = 0;
    for (String server : serverToSegmentsMap.keySet()) {
      if (server.equalsIgnoreCase("server2"))
        expectedNonResponsiveServers += serverToSegmentsMap.get(server).size();
      totalResponses += serverToSegmentsMap.get(server).size();
    }
    BiMap<String, String> endpoints = serverEndpoints(servers);
    String table = "offline";
    List<String> metadata = testMetadataResponse(table, serverToSegmentsMap, endpoints);
    Assert.assertEquals(1, metadata.size());
    Assert.assertEquals(expectedNonResponsiveServers, totalResponses - metadata.size());
  }

  public static class SegmentsServerMock {
    String segment;
    String endpoint;
    InetSocketAddress socket = new InetSocketAddress(0);
    String segmentMetadata;
    HttpServer httpServer;

    public SegmentsServerMock(String segment) {
      this.segment = segment;
    }

    private void updateMetadataMock() throws IOException {
      JsonNode jsonNode = JsonUtils.stringToJsonNode(MetadataConstants.SEGMENT_METADATA_STR);
      ObjectNode objectNode = jsonNode.deepCopy();
      objectNode.put("segmentName", segment);
      segmentMetadata = JsonUtils.objectToString(objectNode);
    }

    private void start(String path, HttpHandler handler)
            throws IOException {
      httpServer = HttpServer.create(socket, 0);
      httpServer.createContext(path, handler);
      new Thread(() -> httpServer.start()).start();
      endpoint = "localhost:" + httpServer.getAddress().getPort();
    }
  }

  public static class MetadataConstants {
    public static final List<String> SEGMENT_SERVERS = Arrays.asList("server1", "server2", "server3", "server4", "server5");
    public static final String SEGMENT_METADATA_STR = "{\n" +
            "  \"segmentName\" : \"testTable_OFFLINE_default_s1\",\n" +
            "  \"schemaName\" : null,\n" +
            "  \"crc\" : 1804064321,\n" +
            "  \"creationTimeMillis\" : 1595127594768,\n" +
            "  \"creationTimeReadable\" : \"2020-07-19T02:59:54:768 UTC\",\n" +
            "  \"timeGranularitySec\" : null,\n" +
            "  \"startTimeMillis\" : null,\n" +
            "  \"startTimeReadable\" : null,\n" +
            "  \"endTimeMillis\" : null,\n" +
            "  \"endTimeReadable\" : null,\n" +
            "  \"pushTimeMillis\" : -9223372036854775808,\n" +
            "  \"pushTimeReadable\" : null,\n" +
            "  \"refreshTimeMillis\" : -9223372036854775808,\n" +
            "  \"refreshTimeReadable\" : null,\n" +
            "  \"segmentVersion\" : \"v3\",\n" +
            "  \"creatorName\" : null,\n" +
            "  \"paddingCharacter\" : \"\\u0000\",\n" +
            "  \"columns\" : [ ],\n" +
            "  \"indexes\" : [ { } ]\n" +
            "}";
  }
}
