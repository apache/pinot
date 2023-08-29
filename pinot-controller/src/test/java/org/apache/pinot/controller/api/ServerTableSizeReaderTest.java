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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.controller.api.resources.ServerTableSizeReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ServerTableSizeReaderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerTableSizeReader.class);

  private static final int SERVER_PORT_START = 10000;
  private static final String URI_PATH = "/table/";
  private static final String TABLE_NAME = "myTable";
  private static final int TIMEOUT_MSEC = 5000;
  private static final int SERVER_COUNT = 6;

  private final ExecutorService _executor = Executors.newFixedThreadPool(3);
  private final PoolingHttpClientConnectionManager _httpConnectionManager = new PoolingHttpClientConnectionManager();
  private final List<HttpServer> _servers = new ArrayList<>();
  private final List<String> _serverList = new ArrayList<>();
  private final List<String> _endpointList = new ArrayList<>();
  private List<Integer> _server1Segments;
  private List<Integer> _server2Segments;
  private TableSizeInfo _tableInfo1;
  private TableSizeInfo _tableInfo3;
  private TableSizeInfo _tableInfo2;

  @BeforeClass
  public void setUp()
      throws IOException {
    for (int i = 0; i < SERVER_COUNT; i++) {
      _serverList.add("server_" + i);
      _endpointList.add("http://localhost:" + (SERVER_PORT_START + i));
    }

    _server1Segments = Arrays.asList(1, 3, 5);
    _server2Segments = Arrays.asList(2, 3);

    _tableInfo1 = createTableSizeInfo(TABLE_NAME, _server1Segments);
    _tableInfo2 = createTableSizeInfo(TABLE_NAME, _server2Segments);
    _tableInfo3 = createTableSizeInfo(TABLE_NAME, new ArrayList<Integer>());
    _servers.add(startServer(SERVER_PORT_START, createHandler(200, _tableInfo1, 0)));
    _servers.add(startServer(SERVER_PORT_START + 1, createHandler(200, _tableInfo2, 0)));
    _servers.add(startServer(SERVER_PORT_START + 3, createHandler(500, null, 0)));
    _servers.add(startServer(SERVER_PORT_START + 4, createHandler(200, null, TIMEOUT_MSEC * 20)));
    _servers.add(startServer(SERVER_PORT_START + 5, createHandler(200, _tableInfo3, 0)));
  }

  @AfterClass
  public void tearDown() {
    for (HttpServer server : _servers) {
      if (server != null) {
        server.stop(0);
      }
    }
  }

  private TableSizeInfo createTableSizeInfo(String tableName, List<Integer> segmentIndexes) {
    long tableSizeInBytes = 0;
    List<SegmentSizeInfo> segments = new ArrayList<>();
    for (int segmentIndex : segmentIndexes) {
      long size = segmentIndexToSize(segmentIndex);
      tableSizeInBytes += size;
      segments.add(new SegmentSizeInfo("seg" + segmentIndex, size));
    }
    return new TableSizeInfo(tableName, tableSizeInBytes, segments);
  }

  private long segmentIndexToSize(int index) {
    return 100 + index * 100;
  }

  private HttpHandler createHandler(final int status, final TableSizeInfo tableSize, final int sleepTimeMs) {
    return new HttpHandler() {
      @Override
      public void handle(HttpExchange httpExchange)
          throws IOException {
        if (sleepTimeMs > 0) {
          try {
            Thread.sleep(sleepTimeMs);
          } catch (InterruptedException e) {
            LOGGER.info("Handler interrupted during sleep");
          }
        }
        String json = JsonUtils.objectToString(tableSize);
        httpExchange.sendResponseHeaders(status, json.length());
        OutputStream responseBody = httpExchange.getResponseBody();
        responseBody.write(json.getBytes());
        responseBody.close();
      }
    };
  }

  private HttpServer startServer(int port, HttpHandler handler)
      throws IOException {
    final HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(URI_PATH, handler);
    new Thread(new Runnable() {
      @Override
      public void run() {
        server.start();
      }
    }).start();
    return server;
  }

  @Test
  public void testServerSizeReader() {
    ServerTableSizeReader reader = new ServerTableSizeReader(_executor, _httpConnectionManager);
    BiMap<String, String> endpoints = HashBiMap.create();
    for (int i = 0; i < 2; i++) {
      endpoints.put(_serverList.get(i), _endpointList.get(i));
    }
    endpoints.put(_serverList.get(5), _endpointList.get(5));
    Map<String, List<SegmentSizeInfo>> serverSizes =
        reader.getSegmentSizeInfoFromServers(endpoints, "foo", TIMEOUT_MSEC);
    assertEquals(serverSizes.size(), 3);
    assertTrue(serverSizes.containsKey(_serverList.get(0)));
    assertTrue(serverSizes.containsKey(_serverList.get(1)));
    assertTrue(serverSizes.containsKey(_serverList.get(5)));

    assertEquals(serverSizes.get(_serverList.get(0)), _tableInfo1.getSegments());
    assertEquals(serverSizes.get(_serverList.get(1)), _tableInfo2.getSegments());
    assertEquals(serverSizes.get(_serverList.get(5)), _tableInfo3.getSegments());
  }

  @Test
  public void testServerSizesErrors() {
    ServerTableSizeReader reader = new ServerTableSizeReader(_executor, _httpConnectionManager);
    BiMap<String, String> endpoints = HashBiMap.create();
    for (int i = 0; i < SERVER_COUNT; i++) {
      endpoints.put(_serverList.get(i), _endpointList.get(i));
    }
    Map<String, List<SegmentSizeInfo>> serverSizes =
        reader.getSegmentSizeInfoFromServers(endpoints, "foo", TIMEOUT_MSEC);
    assertEquals(serverSizes.size(), 3);
    assertTrue(serverSizes.containsKey(_serverList.get(0)));
    assertTrue(serverSizes.containsKey(_serverList.get(1)));
    assertTrue(serverSizes.containsKey(_serverList.get(5)));
    // error and timing out servers are not part of responses
  }
}
