/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.restlet.resources.ServerPerfMetrics;
import com.linkedin.pinot.common.restlet.resources.ServerSegmentInfo;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.util.ServerPerfMetricsReader;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ServerPerfMetricsReaderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerTableSizeReaderTest.class);
  final int _timeoutMsec = 5000;
  final int _serverCount = 3;
  private final ExecutorService _executor = Executors.newFixedThreadPool(3);
  private final HttpConnectionManager _httpConnectionManager = new MultiThreadedHttpConnectionManager();
  private final int _serverPortStart = 10000;
  private final List<HttpServer> _servers = new ArrayList<>();
  private final List<String> _serverList = new ArrayList<>();
  private final List<String> _endpointList = new ArrayList<>();
  private List<Integer> _server1Segments;
  private List<Integer> _server2Segments;
  private List<Integer> _server3Segments;
  private ServerPerfMetrics _server1SegmentsInfo;
  private ServerPerfMetrics _server2SegmentsInfo;
  private ServerPerfMetrics _server3SegmentsInfo;

  @BeforeClass
  public void setUp() throws IOException {
    for (int i = 0; i < _serverCount; i++) {
      _serverList.add("server_" + i);
      _endpointList.add("localhost:" + (_serverPortStart + i));
    }
    _server1Segments = Arrays.asList(1, 3, 5);
    _server2Segments = Arrays.asList(2, 3);
    _server3Segments = Arrays.asList();
    _server1SegmentsInfo = createSegmentsInfo(_serverList.get(0), _server1Segments);
    _server2SegmentsInfo = createSegmentsInfo(_serverList.get(1), _server2Segments);
    _server3SegmentsInfo = createSegmentsInfo(_serverList.get(2), _server3Segments);
    _servers.add(startServer(_serverPortStart, createHandler(200, _server1SegmentsInfo, 0)));
    _servers.add(startServer(_serverPortStart + 1, createHandler(200, _server2SegmentsInfo, 0)));
    _servers.add(startServer(_serverPortStart + 2, createHandler(200, _server3SegmentsInfo, 0)));
  }

  @AfterClass
  public void tearDown() {
    for (HttpServer server : _servers) {
      if (server != null) {
        server.stop(0);
      }
    }
  }

  private ServerPerfMetrics createSegmentsInfo(String serverNameOrEndpoint, List<Integer> segmentIndexes) {
    ServerPerfMetrics serverSegmentsInfo = new ServerPerfMetrics();
    serverSegmentsInfo.segmentCount = segmentIndexes.size();
    serverSegmentsInfo.segmentDiskSizeInBytes = 0;
    for (int segmentIndex : segmentIndexes) {
      serverSegmentsInfo.segmentDiskSizeInBytes += segmentIndexToSize(segmentIndex);
    }
    return serverSegmentsInfo;
  }

  private long segmentIndexToSize(int index) {
    return 100 + index * 100;
  }

  private HttpHandler createHandler(final int status, final ServerPerfMetrics serverSegmentsInfo,
      final int sleepTimeMs) {
    return new HttpHandler() {
      @Override
      public void handle(HttpExchange httpExchange) throws IOException {
        if (sleepTimeMs > 0) {
          try {
            Thread.sleep(sleepTimeMs);
          } catch (InterruptedException e) {
            LOGGER.info("Handler interrupted during sleep");
          }
        }
        String json = new ObjectMapper().writeValueAsString(serverSegmentsInfo);
        httpExchange.sendResponseHeaders(status, json.length());
        OutputStream responseBody = httpExchange.getResponseBody();
        responseBody.write(json.getBytes());
        responseBody.close();
      }
    };
  }

  private HttpServer startServer(int port, HttpHandler handler) throws IOException {
    final HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(CommonConstants.Helix.ServerPerfMetricUris.SERVER_SEGMENT_INFO_URI, handler);
    new Thread(new Runnable() {
      @Override
      public void run() {
        server.start();
      }
    }).start();
    return server;
  }

  @Test
  public void testServerPerfMetricsReader() {
    ServerPerfMetricsReader serverPerfMetricsReader =
        new ServerPerfMetricsReader(_executor, _httpConnectionManager, null);
    ServerSegmentInfo serverSegmentInfo;
    //Check metric for server1
    serverSegmentInfo = serverPerfMetricsReader.getServerPerfMetrics(_endpointList.get(0), false, _timeoutMsec);
    Assert.assertEquals(serverSegmentInfo.getSegmentCount(), _server1SegmentsInfo.getSegmentCount());
    Assert.assertEquals(serverSegmentInfo.getSegmentSizeInBytes(),
        _server1SegmentsInfo.getSegmentDiskSizeInBytes());
    //Check metrics for server2
    serverSegmentInfo = serverPerfMetricsReader.getServerPerfMetrics(_endpointList.get(1), false, _timeoutMsec);
    Assert.assertEquals(serverSegmentInfo.getSegmentCount(), _server2SegmentsInfo.getSegmentCount());
    Assert.assertEquals(serverSegmentInfo.getSegmentSizeInBytes(),
        _server2SegmentsInfo.getSegmentDiskSizeInBytes());
    //Check metrics for server3
    serverSegmentInfo = serverPerfMetricsReader.getServerPerfMetrics(_endpointList.get(2), false, _timeoutMsec);
    Assert.assertEquals(serverSegmentInfo.getSegmentCount(), _server3SegmentsInfo.getSegmentCount());
    Assert.assertEquals(serverSegmentInfo.getSegmentSizeInBytes(),
        _server3SegmentsInfo.getSegmentDiskSizeInBytes());
  }

  @Test
  public void testServerSizesErrors() {
    ServerPerfMetricsReader serverPerfMetricsReader =
        new ServerPerfMetricsReader(_executor, _httpConnectionManager, null);
    ServerSegmentInfo serverSegmentInfo;
    serverSegmentInfo = serverPerfMetricsReader.getServerPerfMetrics("FakeEndPoint", false, _timeoutMsec);
    Assert.assertEquals(serverSegmentInfo.getSegmentCount(), -1);
    Assert.assertEquals(serverSegmentInfo.getSegmentSizeInBytes(), -1);
  }
}
