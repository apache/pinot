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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.SegmentConsumerInfo;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.ConsumingSegmentInfoReader;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager.ConsumerState;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.ArgumentMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests the {@link ConsumingSegmentInfoReader}
 */
public class ConsumingSegmentInfoReaderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumingSegmentInfoReaderTest.class);
  private final Executor executor = Executors.newFixedThreadPool(1);
  private final HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
  private PinotHelixResourceManager helix;
  private final Map<String, FakeConsumingInfoServer> serverMap = new HashMap<>();
  private final int timeoutMsec = 10000;

  private static final String TABLE_NAME = "myTable_REALTIME";
  private static final String SEGMENT_NAME_PARTITION_0 = "table__0__29__12345";
  private static final String SEGMENT_NAME_PARTITION_1 = "table__1__32__12345";

  @BeforeClass
  public void setUp() throws IOException {
    helix = mock(PinotHelixResourceManager.class);
    String uriPath = "/tables/";

    // server0 - 1 consumer each for p0 and p1. CONSUMING.
    Map<String, String> partitionToOffset0 = new HashMap<>();
    partitionToOffset0.put("0", "150");
    Map<String, String> partitionToOffset1 = new HashMap<>();
    partitionToOffset1.put("1", "150");
    FakeConsumingInfoServer s0 = new FakeConsumingInfoServer(
        Lists.newArrayList(new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_0, "CONSUMING", 0, partitionToOffset0),
            new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_1, "CONSUMING", 0, partitionToOffset1)));
    s0.start(uriPath, createHandler(200, s0.consumerInfos, 0));
    serverMap.put("server0", s0);

    // server1 - 1 consumer each for p0 and p1. CONSUMING.
    FakeConsumingInfoServer s1 = new FakeConsumingInfoServer(
        Lists.newArrayList(new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_0, "CONSUMING", 0, partitionToOffset0),
            new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_1, "CONSUMING", 0, partitionToOffset1)));
    s1.start(uriPath, createHandler(200, s1.consumerInfos, 0));
    serverMap.put("server1", s1);

    // server2 - p1 consumer CONSUMING. p0 consumer NOT_CONSUMING
    FakeConsumingInfoServer s2 = new FakeConsumingInfoServer(
        Lists.newArrayList(new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_0, "NOT_CONSUMING", 0, partitionToOffset0),
            new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_1, "CONSUMING", 0, partitionToOffset1)));
    s2.start(uriPath, createHandler(200, s2.consumerInfos, 0));
    serverMap.put("server2", s2);

    // server3 - 1 consumer for p1. No consumer for p0
    FakeConsumingInfoServer s3 = new FakeConsumingInfoServer(
        Lists.newArrayList(new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_1, "CONSUMING", 0, partitionToOffset1)));
    s3.start(uriPath, createHandler(200, s3.consumerInfos, 0));
    serverMap.put("server3", s3);

    // server4 - unreachable/error/timeout
    FakeConsumingInfoServer s4 = new FakeConsumingInfoServer(
        Lists.newArrayList(new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_0, "CONSUMING", 0, partitionToOffset0),
            new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_1, "CONSUMING", 0, partitionToOffset1)));
    s4.start(uriPath, createHandler(200, s4.consumerInfos, timeoutMsec * 1000));
    serverMap.put("server4", s4);
  }

  @AfterClass
  public void tearDown() {
    for (Map.Entry<String, FakeConsumingInfoServer> fakeServerEntry : serverMap.entrySet()) {
      fakeServerEntry.getValue().httpServer.stop(0);
    }
  }

  private HttpHandler createHandler(final int status, final List<SegmentConsumerInfo> consumerInfos,
      final int sleepTimeMs) {
    return httpExchange -> {
      if (sleepTimeMs > 0) {
        try {
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          LOGGER.info("Handler interrupted during sleep");
        }
      }
      String json = JsonUtils.objectToString(consumerInfos);
      httpExchange.sendResponseHeaders(status, json.length());
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(json.getBytes());
      responseBody.close();
    };
  }

  /**
   * Server to return fake consuming segment info
   */
  private static class FakeConsumingInfoServer {
    String endpoint;
    InetSocketAddress socket = new InetSocketAddress(0);
    List<SegmentConsumerInfo> consumerInfos;
    HttpServer httpServer;

    FakeConsumingInfoServer(List<SegmentConsumerInfo> consumerInfos) {
      this.consumerInfos = consumerInfos;
    }

    private void start(String path, HttpHandler handler) throws IOException {
      httpServer = HttpServer.create(socket, 0);
      httpServer.createContext(path, handler);
      new Thread(() -> httpServer.start()).start();
      endpoint = "http://localhost:" + httpServer.getAddress().getPort();
    }
  }

  private Map<String, List<String>> subsetOfServerSegments(String... servers) {
    Map<String, List<String>> subset = new HashMap<>();
    for (String server : servers) {
      subset.put(server, serverMap.get(server).consumerInfos.stream().map(SegmentConsumerInfo::getSegmentName)
          .collect(Collectors.toList()));
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

  private ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap testRunner(final String[] servers,
      final Set<String> consumingSegments, String table) throws InvalidConfigException {
    when(helix.getServerToSegmentsMap(anyString())).thenAnswer(invocationOnMock -> subsetOfServerSegments(servers));
    when(helix.getDataInstanceAdminEndpoints(ArgumentMatchers.anySet()))
        .thenAnswer(invocationOnMock -> serverEndpoints(servers));
    when(helix.getConsumingSegments(anyString())).thenAnswer(invocationOnMock -> consumingSegments);
    ConsumingSegmentInfoReader reader = new ConsumingSegmentInfoReader(executor, connectionManager, helix);
    return reader.getConsumingSegmentsInfo(table, timeoutMsec);
  }

  @Test
  public void testEmptyTable() throws InvalidConfigException {
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentsInfoMap =
        testRunner(new String[]{}, Collections.emptySet(), TABLE_NAME);
    Assert.assertTrue(consumingSegmentsInfoMap._segmentToConsumingInfoMap.isEmpty());
  }

  /**
   * 2 servers, 2 partitions, 2 replicas, all CONSUMING
   */
  @Test
  public void testHappyPath() throws InvalidConfigException {
    final String[] servers = {"server0", "server1"};
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentsInfoMap =
        testRunner(servers, Sets.newHashSet(SEGMENT_NAME_PARTITION_0, SEGMENT_NAME_PARTITION_1), TABLE_NAME);

    List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> consumingSegmentInfos =
        consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_0);
    Assert.assertEquals(consumingSegmentInfos.size(), 2);
    for (ConsumingSegmentInfoReader.ConsumingSegmentInfo info : consumingSegmentInfos) {
      checkConsumingSegmentInfo(info, Sets.newHashSet("server0", "server1"), ConsumerState.CONSUMING.toString(), "0",
          "150");
    }
    consumingSegmentInfos = consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_1);
    Assert.assertEquals(consumingSegmentInfos.size(), 2);
    for (ConsumingSegmentInfoReader.ConsumingSegmentInfo info : consumingSegmentInfos) {
      checkConsumingSegmentInfo(info, Sets.newHashSet("server0", "server1"), ConsumerState.CONSUMING.toString(), "1",
          "150");
    }
  }

  /**
   * 2 servers, 2 partitions, 2 replicas. p0 consumer in NOT_CONSUMING
   */
  @Test
  public void testNotConsumingState() throws InvalidConfigException {
    final String[] servers = {"server0", "server2"};
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentsInfoMap =
        testRunner(servers, Sets.newHashSet(SEGMENT_NAME_PARTITION_0, SEGMENT_NAME_PARTITION_1), TABLE_NAME);
    List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> consumingSegmentInfos =
        consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_0);
    Assert.assertEquals(consumingSegmentInfos.size(), 2);
    for (ConsumingSegmentInfoReader.ConsumingSegmentInfo info : consumingSegmentInfos) {
      if (info._serverName.equals("server0")) {
        checkConsumingSegmentInfo(info, Sets.newHashSet("server0"), ConsumerState.CONSUMING.toString(), "0", "150");
      } else {
        checkConsumingSegmentInfo(info, Sets.newHashSet("server2"), ConsumerState.NOT_CONSUMING.toString(), "0", "150");
      }
    }
    consumingSegmentInfos = consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_1);
    Assert.assertEquals(consumingSegmentInfos.size(), 2);
    for (ConsumingSegmentInfoReader.ConsumingSegmentInfo info : consumingSegmentInfos) {
      checkConsumingSegmentInfo(info, Sets.newHashSet("server0", "server2"), ConsumerState.CONSUMING.toString(), "1",
          "150");
    }
  }

  /**
   * 1 servers, 2 partitions, 1 replicas. No consumer for p0. CONSUMING state in idealstate.
   */
  @Test
  public void testNoConsumerButConsumingInIdealState() throws InvalidConfigException {
    final String[] servers = {"server3"};
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentsInfoMap =
        testRunner(servers, Sets.newHashSet(SEGMENT_NAME_PARTITION_0, SEGMENT_NAME_PARTITION_1), TABLE_NAME);
    List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> consumingSegmentInfos =
        consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_0);
    Assert.assertTrue(consumingSegmentInfos.isEmpty());

    consumingSegmentInfos = consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_1);
    Assert.assertEquals(consumingSegmentInfos.size(), 1);
    checkConsumingSegmentInfo(consumingSegmentInfos.get(0), Sets.newHashSet("server3"),
        ConsumerState.CONSUMING.toString(), "1", "150");
  }

  /**
   * 1 servers, 2 partitions, 1 replicas. No consumer for p0. OFFLINE state in idealstate.
   */
  @Test
  public void testNoConsumerOfflineInIdealState() throws InvalidConfigException {
    final String[] servers = {"server3"};
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentsInfoMap =
        testRunner(servers, Sets.newHashSet(SEGMENT_NAME_PARTITION_1), TABLE_NAME);
    List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> consumingSegmentInfos =
        consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_0);
    Assert.assertNull(consumingSegmentInfos);

    consumingSegmentInfos = consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_1);
    Assert.assertEquals(consumingSegmentInfos.size(), 1);
    checkConsumingSegmentInfo(consumingSegmentInfos.get(0), Sets.newHashSet("server3"),
        ConsumerState.CONSUMING.toString(), "1", "150");
  }

  /**
   * 2 servers, 2 partitions, 2 replicas. server4 times out.
   */
  @Test
  public void testErrorFromServer() throws InvalidConfigException {
    final String[] servers = {"server0", "server4"};
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentsInfoMap =
        testRunner(servers, Sets.newHashSet(SEGMENT_NAME_PARTITION_0, SEGMENT_NAME_PARTITION_1), TABLE_NAME);
    List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> consumingSegmentInfos =
        consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_0);
    Assert.assertEquals(consumingSegmentInfos.size(), 1);
    checkConsumingSegmentInfo(consumingSegmentInfos.get(0), Sets.newHashSet("server0"),
        ConsumerState.CONSUMING.toString(), "0", "150");

    consumingSegmentInfos = consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_1);
    Assert.assertEquals(consumingSegmentInfos.size(), 1);
    checkConsumingSegmentInfo(consumingSegmentInfos.get(0), Sets.newHashSet("server0"),
        ConsumerState.CONSUMING.toString(), "1", "150");
  }

  private void checkConsumingSegmentInfo(ConsumingSegmentInfoReader.ConsumingSegmentInfo info, Set<String> serverNames,
      String consumerState, String partition, String offset) {
    Assert.assertTrue(serverNames.contains(info._serverName));
    Assert.assertEquals(info._consumerState, consumerState);
    Assert.assertEquals(info._partitionToOffsetMap.get(partition), offset);
  }
}
