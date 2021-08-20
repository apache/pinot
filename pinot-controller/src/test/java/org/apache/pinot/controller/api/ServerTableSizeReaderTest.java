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
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.controller.api.resources.ServerTableSizeReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ServerTableSizeReaderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerTableSizeReader.class);

  private final ExecutorService executor = Executors.newFixedThreadPool(3);
  private final HttpConnectionManager httpConnectionManager = new MultiThreadedHttpConnectionManager();
  private final int serverPortStart = 10000;
  private final String URI_PATH = "/table/";
  private final List<HttpServer> servers = new ArrayList<>();
  final int timeoutMsec = 5000;
  final String tableName = "myTable";
  final int serverCount = 6;
  private final List<String> serverList = new ArrayList<>();
  private final List<String> endpointList = new ArrayList<>();
  private List<Integer> server1Segments;
  private List<Integer> server2Segments;
  private TableSizeInfo tableInfo1;
  private TableSizeInfo tableInfo3;
  private TableSizeInfo tableInfo2;

  @BeforeClass
  public void setUp()
      throws IOException {
    for (int i = 0; i < serverCount; i++) {
      serverList.add("server_" + i);
      endpointList.add("http://localhost:" + (serverPortStart + i));
    }

    server1Segments = Arrays.asList(1, 3, 5);
    server2Segments = Arrays.asList(2, 3);

    tableInfo1 = createTableSizeInfo(tableName, server1Segments);
    tableInfo2 = createTableSizeInfo(tableName, server2Segments);
    tableInfo3 = createTableSizeInfo(tableName, new ArrayList<Integer>());
    servers.add(startServer(serverPortStart, createHandler(200, tableInfo1, 0)));
    servers.add(startServer(serverPortStart + 1, createHandler(200, tableInfo2, 0)));
    servers.add(startServer(serverPortStart + 3, createHandler(500, null, 0)));
    servers.add(startServer(serverPortStart + 4, createHandler(200, null, timeoutMsec * 20)));
    servers.add(startServer(serverPortStart + 5, createHandler(200, tableInfo3, 0)));
  }

  @AfterClass
  public void tearDown() {
    for (HttpServer server : servers) {
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
    ServerTableSizeReader reader = new ServerTableSizeReader(executor, httpConnectionManager);
    BiMap<String, String> endpoints = HashBiMap.create();
    for (int i = 0; i < 2; i++) {
      endpoints.put(serverList.get(i), endpointList.get(i));
    }
    endpoints.put(serverList.get(5), endpointList.get(5));
    Map<String, List<SegmentSizeInfo>> serverSizes =
        reader.getSegmentSizeInfoFromServers(endpoints, "foo", timeoutMsec);
    Assert.assertEquals(serverSizes.size(), 3);
    Assert.assertTrue(serverSizes.containsKey(serverList.get(0)));
    Assert.assertTrue(serverSizes.containsKey(serverList.get(1)));
    Assert.assertTrue(serverSizes.containsKey(serverList.get(5)));

    Assert.assertEquals(serverSizes.get(serverList.get(0)), tableInfo1.getSegments());
    Assert.assertEquals(serverSizes.get(serverList.get(1)), tableInfo2.getSegments());
    Assert.assertEquals(serverSizes.get(serverList.get(5)), tableInfo3.getSegments());
  }

  @Test
  public void testServerSizesErrors() {
    ServerTableSizeReader reader = new ServerTableSizeReader(executor, httpConnectionManager);
    BiMap<String, String> endpoints = HashBiMap.create();
    for (int i = 0; i < serverCount; i++) {
      endpoints.put(serverList.get(i), endpointList.get(i));
    }
    Map<String, List<SegmentSizeInfo>> serverSizes =
        reader.getSegmentSizeInfoFromServers(endpoints, "foo", timeoutMsec);
    Assert.assertEquals(serverSizes.size(), 3);
    Assert.assertTrue(serverSizes.containsKey(serverList.get(0)));
    Assert.assertTrue(serverSizes.containsKey(serverList.get(1)));
    Assert.assertTrue(serverSizes.containsKey(serverList.get(5)));
    // error and timing out servers are not part of responses
  }
}
