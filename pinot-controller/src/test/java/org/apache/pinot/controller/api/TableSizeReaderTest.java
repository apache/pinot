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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TableSizeReaderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSizeReaderTest.class);
  private final Executor executor = Executors.newFixedThreadPool(1);
  private final HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
  private final ControllerMetrics _controllerMetrics =
      new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
  private PinotHelixResourceManager helix;
  private Map<String, FakeSizeServer> serverMap = new HashMap<>();
  private final String URI_PATH = "/table/";
  private final int timeoutMsec = 10000;

  @BeforeClass
  public void setUp() throws IOException {
    helix = mock(PinotHelixResourceManager.class);
    when(helix.hasOfflineTable(anyString())).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        String table = (String) invocationOnMock.getArguments()[0];
        return table.indexOf("offline") >= 0;
      }
    });

    when(helix.hasRealtimeTable(anyString())).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        String table = (String) invocationOnMock.getArguments()[0];
        return table.indexOf("realtime") >= 0;
      }
    });

    int counter = 0;
    // server0
    FakeSizeServer s = new FakeSizeServer(Arrays.asList("s2", "s3", "s6"));
    s.start(URI_PATH, createHandler(200, s.sizes, 0));
    serverMap.put(serverName(counter), s);
    ++counter;

    // server1
    s = new FakeSizeServer(Arrays.asList("s2", "s5"));
    s.start(URI_PATH, createHandler(200, s.sizes, 0));
    serverMap.put(serverName(counter), s);
    ++counter;

    // server2
    s = new FakeSizeServer(Arrays.asList("s3", "s6"));
    s.start(URI_PATH, createHandler(404, s.sizes, 0));
    serverMap.put(serverName(counter), s);
    ++counter;

    // server3
    s = new FakeSizeServer(Arrays.asList("r1", "r2"));
    s.start(URI_PATH, createHandler(200, s.sizes, 0));
    serverMap.put(serverName(counter), s);
    ++counter;

    // server4
    s = new FakeSizeServer(Arrays.asList("r2"));
    s.start(URI_PATH, createHandler(200, s.sizes, 0));
    serverMap.put(serverName(counter), s);
    ++counter;

    // server5 ... timing out server
    s = new FakeSizeServer(Arrays.asList("s1", "s3"));
    s.start(URI_PATH, createHandler(200, s.sizes, timeoutMsec * 100));
    serverMap.put(serverName(counter), s);
    ++counter;
  }

  @AfterClass
  public void tearDown() {
    for (Map.Entry<String, FakeSizeServer> fakeServerEntry : serverMap.entrySet()) {
      fakeServerEntry.getValue().httpServer.stop(0);
    }
  }

  private HttpHandler createHandler(final int status, final List<SegmentSizeInfo> segmentSizes, final int sleepTimeMs) {
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

        TableSizeInfo tableInfo = new TableSizeInfo("myTable", 0);
        tableInfo.segments = segmentSizes;
        for (SegmentSizeInfo segmentSize : segmentSizes) {
          tableInfo.diskSizeInBytes += segmentSize.diskSizeInBytes;
        }

        String json = JsonUtils.objectToString(tableInfo);
        httpExchange.sendResponseHeaders(status, json.length());
        OutputStream responseBody = httpExchange.getResponseBody();
        responseBody.write(json.getBytes());
        responseBody.close();
      }
    };
  }

  private String serverName(int index) {
    return "server" + index;
  }

  private static class FakeSizeServer {
    List<String> segments;
    String endpoint;
    InetSocketAddress socket = new InetSocketAddress(0);
    List<SegmentSizeInfo> sizes = new ArrayList<>();
    HttpServer httpServer;

    FakeSizeServer(List<String> segments) {
      this.segments = segments;
      populateSizes(segments);
    }

    void populateSizes(List<String> segments) {
      for (String segment : segments) {
        SegmentSizeInfo sizeInfo = new SegmentSizeInfo(segment, getSegmentSize(segment));
        sizes.add(sizeInfo);
      }
    }

    static long getSegmentSize(String segment) {
      int index = Integer.parseInt(segment.substring(1));
      return 100 + index * 10;
    }

    private void start(String path, HttpHandler handler) throws IOException {
      httpServer = HttpServer.create(socket, 0);
      httpServer.createContext(path, handler);
      new Thread(new Runnable() {
        @Override
        public void run() {
          httpServer.start();
        }
      }).start();
      endpoint = "http://localhost:" + httpServer.getAddress().getPort();
    }
  }

  private Map<String, List<String>> subsetOfServerSegments(String... servers) {
    Map<String, List<String>> subset = new HashMap<>();
    for (String server : servers) {
      subset.put(server, serverMap.get(server).segments);
    }
    return subset;
  }

  private BiMap<String, String> serverEndpoints(String... servers) {
    BiMap<String, String> endpoints = HashBiMap.create(servers.length);
    for (String server : servers) {
      endpoints.put(server, serverMap.get(server).endpoint);
    }
    return endpoints;
  }

  @Test
  public void testNoSuchTable() throws InvalidConfigException {
    TableSizeReader reader = new TableSizeReader(executor, connectionManager, _controllerMetrics, helix);
    Assert.assertNull(reader.getTableSizeDetails("mytable", 5000));
  }

  private TableSizeReader.TableSizeDetails testRunner(final String[] servers, String table)
      throws InvalidConfigException {
    when(helix.getServerToSegmentsMap(anyString())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        return subsetOfServerSegments(servers);
      }
    });

    when(helix.getDataInstanceAdminEndpoints(ArgumentMatchers.<String> anySet())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        return serverEndpoints(servers);
      }
    });

    TableSizeReader reader = new TableSizeReader(executor, connectionManager, _controllerMetrics, helix);
    return reader.getTableSizeDetails(table, timeoutMsec);
  }

  private Map<String, List<String>> segmentToServers(final String... servers) {
    Map<String, List<String>> segmentServers = new HashMap<>();
    for (String server : servers) {
      List<String> segments = serverMap.get(server).segments;
      for (String segment : segments) {
        List<String> segServers = segmentServers.get(segment);
        if (segServers == null) {
          segServers = new ArrayList<String>();
          segmentServers.put(segment, segServers);
        }
        segServers.add(server);
      }
    }
    return segmentServers;
  }

  private void validateTableSubTypeSize(String[] servers, TableSizeReader.TableSubTypeSizeDetails tableSize) {
    Map<String, List<String>> segmentServers = segmentToServers(servers);
    long reportedSize = 0;
    long estimatedSize = 0;
    long maxSegmentSize = 0;
    boolean hasErrors = false;
    for (Map.Entry<String, List<String>> segmentEntry : segmentServers.entrySet()) {
      final String segmentName = segmentEntry.getKey();
      final TableSizeReader.SegmentSizeDetails segmentDetails = tableSize.segments.get(segmentName);
      if (segmentDetails.reportedSizeInBytes != -1) {
        reportedSize += segmentDetails.reportedSizeInBytes;
      }
      if (segmentDetails.estimatedSizeInBytes != -1) {
        estimatedSize += segmentDetails.estimatedSizeInBytes;
      }

      Assert.assertNotNull(segmentDetails);
      final List<String> expectedServers = segmentEntry.getValue();
      final long expectedSegmentSize = FakeSizeServer.getSegmentSize(segmentName);
      int numResponses = expectedServers.size();
      for (String expectedServer : expectedServers) {
        Assert.assertTrue(segmentDetails.serverInfo.containsKey(expectedServer));
        if (expectedServer.equals("server2") || expectedServer.equals("server5")) {
          hasErrors = true;
          --numResponses;
        }
      }
      if (numResponses != 0) {
        Assert.assertEquals(segmentDetails.reportedSizeInBytes, numResponses * expectedSegmentSize);
        Assert.assertEquals(segmentDetails.estimatedSizeInBytes, expectedServers.size() * expectedSegmentSize);
      } else {
        Assert.assertEquals(segmentDetails.reportedSizeInBytes, -1);
        Assert.assertEquals(segmentDetails.estimatedSizeInBytes, -1);
      }
    }
    Assert.assertEquals(tableSize.reportedSizeInBytes, reportedSize);
    Assert.assertEquals(tableSize.estimatedSizeInBytes, estimatedSize);
    if (hasErrors) {
      Assert.assertTrue(tableSize.reportedSizeInBytes != tableSize.estimatedSizeInBytes);
      Assert.assertTrue(tableSize.missingSegments > 0);
    }
  }

  @Test
  public void testGetTableSubTypeSizeAllSuccess() throws InvalidConfigException {
    final String[] servers = {"server0", "server1"};
    String table = "offline";
    TableSizeReader.TableSizeDetails tableSizeDetails = testRunner(servers, table);
    TableSizeReader.TableSubTypeSizeDetails offlineSizes = tableSizeDetails.offlineSegments;
    Assert.assertNotNull(offlineSizes);
    Assert.assertEquals(offlineSizes.segments.size(), 4);
    Assert.assertEquals(offlineSizes.reportedSizeInBytes, offlineSizes.estimatedSizeInBytes);
    validateTableSubTypeSize(servers, offlineSizes);
    Assert.assertNull(tableSizeDetails.realtimeSegments);
    Assert.assertEquals(tableSizeDetails.reportedSizeInBytes, offlineSizes.reportedSizeInBytes);
    Assert.assertEquals(tableSizeDetails.estimatedSizeInBytes, offlineSizes.estimatedSizeInBytes);
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(table);
    Assert.assertEquals(_controllerMetrics.getValueOfTableGauge(tableNameWithType,
        ControllerGauge.TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT), 0);
  }

  @Test
  public void testGetTableSubTypeSizeAllErrors() throws InvalidConfigException {
    final String[] servers = {"server2", "server5"};
    String table = "offline";
    TableSizeReader.TableSizeDetails tableSizeDetails = testRunner(servers, table);
    TableSizeReader.TableSubTypeSizeDetails offlineSizes = tableSizeDetails.offlineSegments;
    Assert.assertNotNull(offlineSizes);
    Assert.assertEquals(offlineSizes.missingSegments, 3);
    Assert.assertEquals(offlineSizes.segments.size(), 3);
    Assert.assertEquals(offlineSizes.reportedSizeInBytes, -1);
    Assert.assertEquals(tableSizeDetails.estimatedSizeInBytes, -1);
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(table);
    Assert.assertEquals(_controllerMetrics.getValueOfTableGauge(tableNameWithType,
        ControllerGauge.TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT), 100);
  }

  @Test
  public void testGetTableSubTypeSizesWithErrors() throws InvalidConfigException {
    final String[] servers = {"server0", "server1", "server2", "server5"};
    String table = "offline";
    TableSizeReader.TableSizeDetails tableSizeDetails = testRunner(servers, "offline");
    TableSizeReader.TableSubTypeSizeDetails offlineSizes = tableSizeDetails.offlineSegments;
    Assert.assertEquals(offlineSizes.segments.size(), 5);
    Assert.assertEquals(offlineSizes.missingSegments, 1);
    Assert.assertTrue(offlineSizes.reportedSizeInBytes != offlineSizes.estimatedSizeInBytes);
    validateTableSubTypeSize(servers, offlineSizes);
    Assert.assertNull(tableSizeDetails.realtimeSegments);
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(table);
    Assert.assertEquals(_controllerMetrics.getValueOfTableGauge(tableNameWithType,
        ControllerGauge.TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT), 20);
  }

  @Test
  public void getTableSizeDetailsRealtimeOnly() throws InvalidConfigException {
    final String[] servers = {"server3", "server4"};
    TableSizeReader.TableSizeDetails tableSizeDetails = testRunner(servers, "realtime");
    Assert.assertNull(tableSizeDetails.offlineSegments);
    TableSizeReader.TableSubTypeSizeDetails realtimeSegments = tableSizeDetails.realtimeSegments;
    Assert.assertEquals(realtimeSegments.segments.size(), 2);
    Assert.assertTrue(realtimeSegments.reportedSizeInBytes == realtimeSegments.estimatedSizeInBytes);
    validateTableSubTypeSize(servers, realtimeSegments);
  }
}
