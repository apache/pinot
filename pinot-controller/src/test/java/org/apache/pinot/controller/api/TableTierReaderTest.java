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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.TableTierInfo;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableTierReader;
import org.apache.pinot.controller.utils.FakeHttpServer;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.ArgumentMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class TableTierReaderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableTierReaderTest.class);
  private static final String URI_PATH = "/tables/";
  private static final int TIMEOUT_MSEC = 3000;
  private static final int EXTENDED_TIMEOUT_FACTOR = 100;

  private final Executor _executor = Executors.newFixedThreadPool(1);
  private final HttpConnectionManager _connectionManager = new MultiThreadedHttpConnectionManager();
  private final Map<String, FakeSizeServer> _serverMap = new HashMap<>();
  private PinotHelixResourceManager _helix;

  @BeforeClass
  public void setUp()
      throws IOException {
    _helix = mock(PinotHelixResourceManager.class);

    int counter = 0;
    // server0 - all good
    Map<String, String> segTierMap = new HashMap<>();
    segTierMap.put("seg01", null);
    segTierMap.put("seg02", "someTier");
    FakeSizeServer s = new FakeSizeServer(segTierMap);
    s.start(URI_PATH, createHandler(200, s._segTierMap, 0));
    _serverMap.put(serverName(counter), s);
    counter++;

    // server1 - all good
    s = new FakeSizeServer(Collections.singletonMap("seg01", null));
    s.start(URI_PATH, createHandler(200, s._segTierMap, 0));
    _serverMap.put(serverName(counter), s);
    counter++;

    // server2 - always 404
    s = new FakeSizeServer(Collections.singletonMap("seg02", "someTier"));
    s.start(URI_PATH, createHandler(404, s._segTierMap, 0));
    _serverMap.put(serverName(counter), s);
    counter++;

    // server3 - empty server
    s = new FakeSizeServer(Collections.emptyMap());
    s.start(URI_PATH, createHandler(200, s._segTierMap, 0));
    _serverMap.put(serverName(counter), s);
    counter++;

    // server4 - missing seg03
    segTierMap = new HashMap<>();
    segTierMap.put("seg02", "someTier");
    segTierMap.put("seg03", "someTier");
    s = new FakeSizeServer(segTierMap);
    segTierMap = new HashMap<>(segTierMap);
    segTierMap.remove("seg03");
    s.start(URI_PATH, createHandler(200, segTierMap, 0));
    _serverMap.put(serverName(counter), s);
    counter++;

    // server5 ... timing out server
    s = new FakeSizeServer(Collections.singletonMap("seg04", "someTier"));
    s.start(URI_PATH, createHandler(200, s._segTierMap, TIMEOUT_MSEC * EXTENDED_TIMEOUT_FACTOR));
    _serverMap.put(serverName(counter), s);
  }

  @AfterClass
  public void tearDown() {
    for (Map.Entry<String, FakeSizeServer> fakeServerEntry : _serverMap.entrySet()) {
      fakeServerEntry.getValue().stop();
    }
  }

  private HttpHandler createHandler(int status, Map<String, String> segTierMap, int sleepTimeMs) {
    return httpExchange -> {
      if (sleepTimeMs > 0) {
        try {
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          LOGGER.info("Handler interrupted during sleep");
        }
      }
      TableTierInfo tableInfo = new TableTierInfo("myTable", segTierMap);
      String json = JsonUtils.objectToString(tableInfo);
      httpExchange.sendResponseHeaders(status, json.length());
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(json.getBytes());
      responseBody.close();
    };
  }

  private String serverName(int index) {
    return "server" + index;
  }

  private static class FakeSizeServer extends FakeHttpServer {
    Map<String, String> _segTierMap;

    FakeSizeServer(Map<String, String> segTierMap) {
      _segTierMap = segTierMap;
    }
  }

  private Map<String, List<String>> subsetOfServerSegments(String... servers) {
    Map<String, List<String>> subset = new HashMap<>();
    for (String server : servers) {
      subset.put(server, new ArrayList<>(_serverMap.get(server)._segTierMap.keySet()));
    }
    return subset;
  }

  private BiMap<String, String> serverEndpoints(String... servers) {
    BiMap<String, String> endpoints = HashBiMap.create(servers.length);
    for (String server : servers) {
      endpoints.put(server, _serverMap.get(server)._endpoint);
    }
    return endpoints;
  }

  private TableTierReader.TableTierDetails testRunner(final String[] servers, String tableName, String segmentName)
      throws InvalidConfigException {
    when(_helix.getServerToSegmentsMap(ArgumentMatchers.anyString()))
        .thenAnswer(invocationOnMock -> subsetOfServerSegments(servers));
    if (segmentName != null) {
      when(_helix.getServers(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
          .thenAnswer(invocationOnMock -> new HashSet<>(Arrays.asList(servers)));
    }
    when(_helix.getDataInstanceAdminEndpoints(ArgumentMatchers.<String>anySet()))
        .thenAnswer(invocationOnMock -> serverEndpoints(servers));
    TableTierReader reader = new TableTierReader(_executor, _connectionManager, _helix);
    return reader.getTableTierDetails(tableName, segmentName, TIMEOUT_MSEC);
  }

  @Test
  public void testGetTableTierInfoAllSuccess()
      throws InvalidConfigException {
    final String[] servers = {"server0", "server1"};
    TableTierReader.TableTierDetails tableTierDetails = testRunner(servers, "myTable_OFFLINE", null);
    assertEquals(tableTierDetails.getSegmentTiers().size(), 2);
    Map<String, String> tiersByServer = tableTierDetails.getSegmentTiers().get("seg01");
    assertEquals(tiersByServer.size(), 2);
    assertNull(tiersByServer.get("server0"));
    assertNull(tiersByServer.get("server1"));
    tiersByServer = tableTierDetails.getSegmentTiers().get("seg02");
    assertEquals(tiersByServer.size(), 1);
    assertEquals(tiersByServer.get("server0"), "someTier");
  }

  @Test
  public void testGetTableTierInfoAllErrors()
      throws InvalidConfigException {
    String[] servers = {"server2", "server5"};
    TableTierReader.TableTierDetails tableTierDetails = testRunner(servers, "myTable_OFFLINE", null);
    assertEquals(tableTierDetails.getSegmentTiers().size(), 2);
    Map<String, String> tiersByServer = tableTierDetails.getSegmentTiers().get("seg02");
    assertEquals(tiersByServer.size(), 1);
    assertEquals(tiersByServer.get("server2"), "NO_RESPONSE_FROM_SERVER");
    tiersByServer = tableTierDetails.getSegmentTiers().get("seg04");
    assertEquals(tiersByServer.size(), 1);
    assertEquals(tiersByServer.get("server5"), "NO_RESPONSE_FROM_SERVER");
  }

  @Test
  public void testGetTableTierInfoFromAllServers()
      throws InvalidConfigException {
    final String[] servers = {"server0", "server1", "server2", "server3", "server4", "server5"};
    TableTierReader.TableTierDetails tableTierDetails = testRunner(servers, "myTable_OFFLINE", null);

    assertEquals(tableTierDetails.getSegmentTiers().size(), 4);
    Map<String, String> tiersByServer = tableTierDetails.getSegmentTiers().get("seg01");
    assertEquals(tiersByServer.size(), 2);
    assertNull(tiersByServer.get("server0"));
    assertNull(tiersByServer.get("server1"));

    tiersByServer = tableTierDetails.getSegmentTiers().get("seg02");
    assertEquals(tiersByServer.size(), 3);
    assertEquals(tiersByServer.get("server0"), "someTier");
    assertEquals(tiersByServer.get("server2"), "NO_RESPONSE_FROM_SERVER");
    assertEquals(tiersByServer.get("server4"), "someTier");

    tiersByServer = tableTierDetails.getSegmentTiers().get("seg03");
    assertEquals(tiersByServer.size(), 1);
    assertEquals(tiersByServer.get("server4"), "SEGMENT_MISSED_ON_SERVER");

    tiersByServer = tableTierDetails.getSegmentTiers().get("seg04");
    assertEquals(tiersByServer.size(), 1);
    assertEquals(tiersByServer.get("server5"), "NO_RESPONSE_FROM_SERVER");
  }

  @Test
  public void testGetSegmentTierInfoFromAllServers()
      throws InvalidConfigException {
    final String[] servers = {"server0", "server1", "server2", "server3", "server4", "server5"};
    TableTierReader.TableTierDetails tableTierDetails = testRunner(servers, "myTable_OFFLINE", "seg01");

    assertEquals(tableTierDetails.getSegmentTiers().size(), 1);
    Map<String, String> tiersByServer = tableTierDetails.getSegmentTiers().get("seg01");
    assertEquals(tiersByServer.size(), 6);
    assertNull(tiersByServer.get("server0"));
    assertNull(tiersByServer.get("server1"));
    assertEquals(tiersByServer.get("server2"), "NO_RESPONSE_FROM_SERVER");
    assertEquals(tiersByServer.get("server3"), "SEGMENT_MISSED_ON_SERVER");
    assertEquals(tiersByServer.get("server4"), "SEGMENT_MISSED_ON_SERVER");
    assertEquals(tiersByServer.get("server5"), "NO_RESPONSE_FROM_SERVER");
  }
}
