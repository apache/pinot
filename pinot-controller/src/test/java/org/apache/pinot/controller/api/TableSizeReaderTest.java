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
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.controller.utils.FakeHttpServer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class TableSizeReaderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSizeReaderTest.class);
  private static final String URI_PATH = "/table/";
  private static final int TIMEOUT_MSEC = 10000;
  private static final int EXTENDED_TIMEOUT_FACTOR = 100;
  private static final int NUM_REPLICAS = 2;

  private final Executor _executor = Executors.newFixedThreadPool(1);
  private final HttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private final ControllerMetrics _controllerMetrics =
      new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
  private final Map<String, FakeSizeServer> _serverMap = new HashMap<>();
  private PinotHelixResourceManager _helix;

  @BeforeClass
  public void setUp() throws IOException {
    _helix = mock(PinotHelixResourceManager.class);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setNumReplicas(NUM_REPLICAS).build();
    ZkHelixPropertyStore mockPropertyStore = mock(ZkHelixPropertyStore.class);

    when(mockPropertyStore
        .get(ArgumentMatchers.anyString(), ArgumentMatchers.eq(null), ArgumentMatchers.eq(AccessOption.PERSISTENT)))
        .thenAnswer((Answer) invocationOnMock -> {
          String path = (String) invocationOnMock.getArguments()[0];
          if (path.contains("realtime_REALTIME")) {
            return TableConfigUtils.toZNRecord(tableConfig);
          }
          if (path.contains("offline_OFFLINE")) {
            return TableConfigUtils.toZNRecord(tableConfig);
          }
          return null;
        });

    when(_helix.getPropertyStore()).thenReturn(mockPropertyStore);
    when(_helix.getNumReplicas(ArgumentMatchers.eq(tableConfig))).thenReturn(NUM_REPLICAS);

    int counter = 0;
    // server0
    FakeSizeServer s = new FakeSizeServer(Arrays.asList("s2", "s3", "s6"));
    s.start(URI_PATH, createHandler(200, s._sizes, 0));
    _serverMap.put(serverName(counter), s);
    counter++;

    // server1
    s = new FakeSizeServer(Arrays.asList("s2", "s5"));
    s.start(URI_PATH, createHandler(200, s._sizes, 0));
    _serverMap.put(serverName(counter), s);
    counter++;

    // server2
    s = new FakeSizeServer(Arrays.asList("s3", "s6"));
    s.start(URI_PATH, createHandler(404, s._sizes, 0));
    _serverMap.put(serverName(counter), s);
    counter++;

    // server3
    s = new FakeSizeServer(Arrays.asList("r1", "r2"));
    s.start(URI_PATH, createHandler(200, s._sizes, 0));
    _serverMap.put(serverName(counter), s);
    counter++;

    // server4
    s = new FakeSizeServer(Arrays.asList("r2"));
    s.start(URI_PATH, createHandler(200, s._sizes, 0));
    _serverMap.put(serverName(counter), s);
    counter++;

    // server5 ... timing out server
    s = new FakeSizeServer(Arrays.asList("s1", "s3"));
    s.start(URI_PATH, createHandler(200, s._sizes, TIMEOUT_MSEC * EXTENDED_TIMEOUT_FACTOR));
    _serverMap.put(serverName(counter), s);
    counter++;
  }

  @AfterClass
  public void tearDown() {
    for (Map.Entry<String, FakeSizeServer> fakeServerEntry : _serverMap.entrySet()) {
      fakeServerEntry.getValue().stop();
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

        long tableSizeInBytes = 0;
        for (SegmentSizeInfo segmentSize : segmentSizes) {
          tableSizeInBytes += segmentSize.getDiskSizeInBytes();
        }
        TableSizeInfo tableInfo = new TableSizeInfo("myTable", tableSizeInBytes, segmentSizes);
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

  private static class FakeSizeServer extends FakeHttpServer {
    List<String> _segments;
    List<SegmentSizeInfo> _sizes = new ArrayList<>();

    FakeSizeServer(List<String> segments) {
      _segments = segments;
      populateSizes(segments);
    }

    void populateSizes(List<String> segments) {
      for (String segment : segments) {
        SegmentSizeInfo sizeInfo = new SegmentSizeInfo(segment, getSegmentSize(segment));
        _sizes.add(sizeInfo);
      }
    }

    static long getSegmentSize(String segment) {
      int index = Integer.parseInt(segment.substring(1));
      return 100 + index * 10;
    }
  }

  private Map<String, List<String>> subsetOfServerSegments(String... servers) {
    Map<String, List<String>> subset = new HashMap<>();
    for (String server : servers) {
      subset.put(server, _serverMap.get(server)._segments);
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

  @Test
  public void testNoSuchTable() throws InvalidConfigException {
    TableSizeReader reader = new TableSizeReader(_executor, _connectionManager, _controllerMetrics, _helix);
    assertNull(reader.getTableSizeDetails("mytable", 5000));
  }

  private TableSizeReader.TableSizeDetails testRunner(final String[] servers, String table)
      throws InvalidConfigException {
    when(_helix.getServerToSegmentsMap(anyString())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        return subsetOfServerSegments(servers);
      }
    });

    when(_helix.getDataInstanceAdminEndpoints(ArgumentMatchers.anySet())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        return serverEndpoints(servers);
      }
    });

    TableSizeReader reader = new TableSizeReader(_executor, _connectionManager, _controllerMetrics, _helix);
    return reader.getTableSizeDetails(table, TIMEOUT_MSEC);
  }

  private Map<String, List<String>> segmentToServers(final String... servers) {
    Map<String, List<String>> segmentServers = new HashMap<>();
    for (String server : servers) {
      List<String> segments = _serverMap.get(server)._segments;
      for (String segment : segments) {
        List<String> segServers = segmentServers.computeIfAbsent(segment, k -> new ArrayList<String>());
        segServers.add(server);
      }
    }
    return segmentServers;
  }

  private void validateTableSubTypeSize(String[] servers, TableSizeReader.TableSubTypeSizeDetails tableSize) {
    Map<String, List<String>> segmentServers = segmentToServers(servers);
    long reportedSize = 0;
    long estimatedSize = 0;
    long reportedSizePerReplica = 0L;
    boolean hasErrors = false;
    for (Map.Entry<String, List<String>> segmentEntry : segmentServers.entrySet()) {
      final String segmentName = segmentEntry.getKey();
      final TableSizeReader.SegmentSizeDetails segmentDetails = tableSize._segments.get(segmentName);
      if (segmentDetails._reportedSizeInBytes != TableSizeReader.DEFAULT_SIZE_WHEN_MISSING_OR_ERROR) {
        reportedSize += segmentDetails._reportedSizeInBytes;
      }
      if (segmentDetails._estimatedSizeInBytes != TableSizeReader.DEFAULT_SIZE_WHEN_MISSING_OR_ERROR) {
        estimatedSize += segmentDetails._estimatedSizeInBytes;
      }
      if (segmentDetails._maxReportedSizePerReplicaInBytes != TableSizeReader.DEFAULT_SIZE_WHEN_MISSING_OR_ERROR) {
        reportedSizePerReplica += segmentDetails._maxReportedSizePerReplicaInBytes;
      }

      assertNotNull(segmentDetails);
      final List<String> expectedServers = segmentEntry.getValue();
      final long expectedSegmentSize = FakeSizeServer.getSegmentSize(segmentName);
      int numResponses = expectedServers.size();
      for (String expectedServer : expectedServers) {
        assertTrue(segmentDetails._serverInfo.containsKey(expectedServer));
        if (expectedServer.equals("server2") || expectedServer.equals("server5")) {
          hasErrors = true;
          numResponses--;
        }
      }
      if (numResponses != 0) {
        assertEquals(segmentDetails._reportedSizeInBytes, numResponses * expectedSegmentSize);
        assertEquals(segmentDetails._estimatedSizeInBytes, expectedServers.size() * expectedSegmentSize);
        assertEquals(segmentDetails._maxReportedSizePerReplicaInBytes, expectedSegmentSize);
      } else {
        assertEquals(segmentDetails._reportedSizeInBytes, TableSizeReader.DEFAULT_SIZE_WHEN_MISSING_OR_ERROR);
        assertEquals(segmentDetails._estimatedSizeInBytes, TableSizeReader.DEFAULT_SIZE_WHEN_MISSING_OR_ERROR);
        assertEquals(segmentDetails._maxReportedSizePerReplicaInBytes,
            TableSizeReader.DEFAULT_SIZE_WHEN_MISSING_OR_ERROR);
      }
    }
    assertEquals(tableSize._reportedSizeInBytes, reportedSize);
    assertEquals(tableSize._estimatedSizeInBytes, estimatedSize);
    assertEquals(tableSize._reportedSizePerReplicaInBytes, reportedSizePerReplica);
    if (hasErrors) {
      assertTrue(tableSize._reportedSizeInBytes != tableSize._estimatedSizeInBytes);
      assertTrue(tableSize._missingSegments > 0);
    }
  }

  @Test
  public void testGetTableSubTypeSizeAllSuccess() throws InvalidConfigException {
    final String[] servers = {"server0", "server1"};
    String table = "offline";
    TableSizeReader.TableSizeDetails tableSizeDetails = testRunner(servers, table);
    TableSizeReader.TableSubTypeSizeDetails offlineSizes = tableSizeDetails._offlineSegments;
    assertNotNull(offlineSizes);
    assertEquals(offlineSizes._segments.size(), 4);
    assertEquals(offlineSizes._reportedSizeInBytes, offlineSizes._estimatedSizeInBytes);
    validateTableSubTypeSize(servers, offlineSizes);
    assertNull(tableSizeDetails._realtimeSegments);
    assertEquals(tableSizeDetails._reportedSizeInBytes, offlineSizes._reportedSizeInBytes);
    assertEquals(tableSizeDetails._estimatedSizeInBytes, offlineSizes._estimatedSizeInBytes);
    assertEquals(tableSizeDetails._reportedSizePerReplicaInBytes, offlineSizes._reportedSizePerReplicaInBytes);
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(table);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT), 0);
    assertEquals(MetricValueUtils
            .getTableGaugeValue(_controllerMetrics, tableNameWithType,
                ControllerGauge.TABLE_SIZE_PER_REPLICA_ON_SERVER),
        offlineSizes._estimatedSizeInBytes / NUM_REPLICAS);
    assertEquals(MetricValueUtils
            .getTableGaugeValue(_controllerMetrics, tableNameWithType, ControllerGauge.TABLE_TOTAL_SIZE_ON_SERVER),
        offlineSizes._estimatedSizeInBytes);
    assertEquals(MetricValueUtils
            .getTableGaugeValue(_controllerMetrics, tableNameWithType, ControllerGauge.LARGEST_SEGMENT_SIZE_ON_SERVER),
        160);
  }

  @Test
  public void testGetTableSubTypeSizeAllErrors() throws InvalidConfigException {
    final String[] servers = {"server2", "server5"};
    String table = "offline";
    TableSizeReader.TableSizeDetails tableSizeDetails = testRunner(servers, table);
    TableSizeReader.TableSubTypeSizeDetails offlineSizes = tableSizeDetails._offlineSegments;
    assertNotNull(offlineSizes);
    assertEquals(offlineSizes._missingSegments, 3);
    assertEquals(offlineSizes._segments.size(), 3);
    assertEquals(offlineSizes._reportedSizeInBytes, TableSizeReader.DEFAULT_SIZE_WHEN_MISSING_OR_ERROR);
    assertEquals(tableSizeDetails._estimatedSizeInBytes, TableSizeReader.DEFAULT_SIZE_WHEN_MISSING_OR_ERROR);
    assertEquals(tableSizeDetails._reportedSizePerReplicaInBytes, TableSizeReader.DEFAULT_SIZE_WHEN_MISSING_OR_ERROR);
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(table);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT), 100);
    assertEquals(MetricValueUtils
            .getTableGaugeValue(_controllerMetrics, tableNameWithType,
                ControllerGauge.TABLE_SIZE_PER_REPLICA_ON_SERVER),
        offlineSizes._estimatedSizeInBytes / NUM_REPLICAS);
    assertEquals(MetricValueUtils
            .getTableGaugeValue(_controllerMetrics, tableNameWithType, ControllerGauge.TABLE_TOTAL_SIZE_ON_SERVER),
        offlineSizes._estimatedSizeInBytes);
    assertFalse(MetricValueUtils
        .tableGaugeExists(_controllerMetrics, tableNameWithType, ControllerGauge.LARGEST_SEGMENT_SIZE_ON_SERVER));
  }

  @Test
  public void testGetTableSubTypeSizesWithErrors() throws InvalidConfigException {
    final String[] servers = {"server0", "server1", "server2", "server5"};
    String table = "offline";
    TableSizeReader.TableSizeDetails tableSizeDetails = testRunner(servers, "offline");
    TableSizeReader.TableSubTypeSizeDetails offlineSizes = tableSizeDetails._offlineSegments;
    assertEquals(offlineSizes._segments.size(), 5);
    assertEquals(offlineSizes._missingSegments, 1);
    assertTrue(offlineSizes._reportedSizeInBytes != offlineSizes._estimatedSizeInBytes);
    validateTableSubTypeSize(servers, offlineSizes);
    assertNull(tableSizeDetails._realtimeSegments);
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(table);
    assertEquals(tableSizeDetails._reportedSizeInBytes, offlineSizes._reportedSizeInBytes);
    assertEquals(tableSizeDetails._estimatedSizeInBytes, offlineSizes._estimatedSizeInBytes);
    assertEquals(tableSizeDetails._reportedSizePerReplicaInBytes, offlineSizes._reportedSizePerReplicaInBytes);
    assertEquals(MetricValueUtils.getTableGaugeValue(_controllerMetrics, tableNameWithType,
        ControllerGauge.TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT), 20);
    assertEquals(MetricValueUtils
            .getTableGaugeValue(_controllerMetrics, tableNameWithType,
                ControllerGauge.TABLE_SIZE_PER_REPLICA_ON_SERVER),
        offlineSizes._estimatedSizeInBytes / NUM_REPLICAS);
    assertEquals(MetricValueUtils
            .getTableGaugeValue(_controllerMetrics, tableNameWithType, ControllerGauge.TABLE_TOTAL_SIZE_ON_SERVER),
        offlineSizes._estimatedSizeInBytes);
    assertEquals(MetricValueUtils
            .getTableGaugeValue(_controllerMetrics, tableNameWithType, ControllerGauge.LARGEST_SEGMENT_SIZE_ON_SERVER),
        160);
  }

  @Test
  public void getTableSizeDetailsRealtimeOnly() throws InvalidConfigException {
    final String[] servers = {"server3", "server4"};
    String table = "realtime";
    TableSizeReader.TableSizeDetails tableSizeDetails = testRunner(servers, table);
    assertNull(tableSizeDetails._offlineSegments);
    TableSizeReader.TableSubTypeSizeDetails realtimeSegments = tableSizeDetails._realtimeSegments;
    assertEquals(realtimeSegments._segments.size(), 2);
    assertEquals(realtimeSegments._estimatedSizeInBytes, realtimeSegments._reportedSizeInBytes);
    validateTableSubTypeSize(servers, realtimeSegments);
    assertEquals(tableSizeDetails._reportedSizeInBytes, realtimeSegments._reportedSizeInBytes);
    assertEquals(tableSizeDetails._estimatedSizeInBytes, realtimeSegments._estimatedSizeInBytes);
    assertEquals(tableSizeDetails._reportedSizePerReplicaInBytes, realtimeSegments._reportedSizePerReplicaInBytes);
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(table);
    assertEquals(MetricValueUtils
            .getTableGaugeValue(_controllerMetrics, tableNameWithType,
                ControllerGauge.TABLE_SIZE_PER_REPLICA_ON_SERVER),
        realtimeSegments._estimatedSizeInBytes / NUM_REPLICAS);
    assertEquals(MetricValueUtils
            .getTableGaugeValue(_controllerMetrics, tableNameWithType, ControllerGauge.TABLE_TOTAL_SIZE_ON_SERVER),
        realtimeSegments._estimatedSizeInBytes);
    assertEquals(MetricValueUtils
            .getTableGaugeValue(_controllerMetrics, tableNameWithType, ControllerGauge.LARGEST_SEGMENT_SIZE_ON_SERVER),
        120);
  }
}
