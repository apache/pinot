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
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.SegmentConsumerInfo;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.ConsumingSegmentInfoReader;
import org.apache.pinot.controller.utils.FakeHttpServer;
import org.apache.pinot.spi.config.table.TableStatus;
import org.apache.pinot.spi.utils.CommonConstants.ConsumerState;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.ArgumentMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests the {@link ConsumingSegmentInfoReader}
 */
@Test(groups = "stateless")
public class ConsumingSegmentInfoReaderStatelessTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumingSegmentInfoReaderStatelessTest.class);

  private static final String TABLE_NAME = "myTable_REALTIME";
  private static final String SEGMENT_NAME_PARTITION_0 = "table__0__29__12345";
  private static final String SEGMENT_NAME_PARTITION_1 = "table__1__32__12345";
  private static final int TIMEOUT_MSEC = 10000;
  private static final int EXTENDED_TIMEOUT_FACTOR = 100;

  private final Executor _executor = Executors.newFixedThreadPool(1);
  private final HttpClientConnectionManager _connectionManager = new PoolingHttpClientConnectionManager();
  private PinotHelixResourceManager _helix;
  private final Map<String, FakeConsumingInfoServer> _serverMap = new HashMap<>();

  @BeforeClass
  public void setUp()
      throws IOException {
    _helix = mock(PinotHelixResourceManager.class);
    String uriPath = "/tables/";

    // server0 - 1 consumer each for p0 and p1. CONSUMING.
    Map<String, String> partitionToOffset0 = new HashMap<>();
    partitionToOffset0.put("0", "150");
    Map<String, String> partitionToOffset1 = new HashMap<>();
    partitionToOffset1.put("1", "150");
    FakeConsumingInfoServer s0 = new FakeConsumingInfoServer(Lists.newArrayList(
        new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_0, "CONSUMING", 0, partitionToOffset0,
            new SegmentConsumerInfo.PartitionOffsetInfo(partitionToOffset0, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap())),
        new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_1, "CONSUMING", 0, partitionToOffset1,
            new SegmentConsumerInfo.PartitionOffsetInfo(partitionToOffset1, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap()))));
    s0.start(uriPath, createHandler(200, s0._consumerInfos, 0));
    _serverMap.put("server0", s0);

    // server1 - 1 consumer each for p0 and p1. CONSUMING.
    FakeConsumingInfoServer s1 = new FakeConsumingInfoServer(Lists.newArrayList(
        new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_0, "CONSUMING", 0, partitionToOffset0,
            new SegmentConsumerInfo.PartitionOffsetInfo(partitionToOffset0, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap())),
        new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_1, "CONSUMING", 0, partitionToOffset1,
            new SegmentConsumerInfo.PartitionOffsetInfo(partitionToOffset1, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap()))));
    s1.start(uriPath, createHandler(200, s1._consumerInfos, 0));
    _serverMap.put("server1", s1);

    // server2 - p1 consumer CONSUMING. p0 consumer NOT_CONSUMING
    FakeConsumingInfoServer s2 = new FakeConsumingInfoServer(Lists.newArrayList(
        new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_0, "NOT_CONSUMING", 0, partitionToOffset0,
            new SegmentConsumerInfo.PartitionOffsetInfo(partitionToOffset0, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap())),
        new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_1, "CONSUMING", 0, partitionToOffset1,
            new SegmentConsumerInfo.PartitionOffsetInfo(partitionToOffset1, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap()))));
    s2.start(uriPath, createHandler(200, s2._consumerInfos, 0));
    _serverMap.put("server2", s2);

    // server3 - 1 consumer for p1. No consumer for p0
    FakeConsumingInfoServer s3 = new FakeConsumingInfoServer(Lists.newArrayList(
        new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_1, "CONSUMING", 0, partitionToOffset1,
            new SegmentConsumerInfo.PartitionOffsetInfo(partitionToOffset1, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap()))));
    s3.start(uriPath, createHandler(200, s3._consumerInfos, 0));
    _serverMap.put("server3", s3);

    // server4 - unreachable/error/timeout
    FakeConsumingInfoServer s4 = new FakeConsumingInfoServer(Lists.newArrayList(
        new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_0, "CONSUMING", 0, partitionToOffset0,
            new SegmentConsumerInfo.PartitionOffsetInfo(partitionToOffset0, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap())),
        new SegmentConsumerInfo(SEGMENT_NAME_PARTITION_1, "CONSUMING", 0, partitionToOffset1,
            new SegmentConsumerInfo.PartitionOffsetInfo(partitionToOffset1, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap()))));
    s4.start(uriPath, createHandler(200, s4._consumerInfos, TIMEOUT_MSEC * EXTENDED_TIMEOUT_FACTOR));
    _serverMap.put("server4", s4);
  }

  @AfterClass
  public void tearDown() {
    for (Map.Entry<String, FakeConsumingInfoServer> fakeServerEntry : _serverMap.entrySet()) {
      fakeServerEntry.getValue().stop();
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
  private static class FakeConsumingInfoServer extends FakeHttpServer {
    List<SegmentConsumerInfo> _consumerInfos;

    FakeConsumingInfoServer(List<SegmentConsumerInfo> consumerInfos) {
      _consumerInfos = consumerInfos;
    }
  }

  private Map<String, List<String>> subsetOfServerSegments(String... servers) {
    Map<String, List<String>> subset = new HashMap<>();
    for (String server : servers) {
      subset.put(server, _serverMap.get(server)._consumerInfos.stream().map(SegmentConsumerInfo::getSegmentName)
          .collect(Collectors.toList()));
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

  private void mockSetup(final String[] servers, final Set<String> consumingSegments)
      throws InvalidConfigException {
    when(_helix.getServerToSegmentsMap(anyString())).thenAnswer(invocationOnMock -> subsetOfServerSegments(servers));
    when(_helix.getServers(anyString(), anyString())).thenAnswer(
        invocationOnMock -> new TreeSet<>(Arrays.asList(servers)));
    when(_helix.getDataInstanceAdminEndpoints(ArgumentMatchers.anySet())).thenAnswer(
        invocationOnMock -> serverEndpoints(servers));
    when(_helix.getConsumingSegments(anyString())).thenAnswer(invocationOnMock -> consumingSegments);
  }

  private ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap testRunner(final String[] servers,
      final Set<String> consumingSegments, String table)
      throws InvalidConfigException {
    mockSetup(servers, consumingSegments);
    ConsumingSegmentInfoReader reader = new ConsumingSegmentInfoReader(_executor, _connectionManager, _helix);
    return reader.getConsumingSegmentsInfo(table, TIMEOUT_MSEC);
  }

  private TableStatus.IngestionStatus testRunnerIngestionStatus(final String[] servers,
      final Set<String> consumingSegments, String table)
      throws InvalidConfigException {
    mockSetup(servers, consumingSegments);
    ConsumingSegmentInfoReader reader = new ConsumingSegmentInfoReader(_executor, _connectionManager, _helix);
    return reader.getIngestionStatus(table, TIMEOUT_MSEC);
  }

  private void checkIngestionStatus(final String[] servers, final Set<String> consumingSegments,
      TableStatus.IngestionState expectedState)
      throws InvalidConfigException {
    TableStatus.IngestionStatus ingestionStatus = testRunnerIngestionStatus(servers, consumingSegments, TABLE_NAME);
    assertEquals(ingestionStatus.getIngestionState(), expectedState);
  }

  @Test
  public void testEmptyTable()
      throws InvalidConfigException {
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentsInfoMap =
        testRunner(new String[]{}, Collections.emptySet(), TABLE_NAME);
    checkIngestionStatus(new String[]{}, Collections.emptySet(), TableStatus.IngestionState.HEALTHY);
    assertTrue(consumingSegmentsInfoMap._segmentToConsumingInfoMap.isEmpty());
  }

  /**
   * 2 servers, 2 partitions, 2 replicas, all CONSUMING
   */
  @Test
  public void testHappyPath()
      throws InvalidConfigException {
    final String[] servers = {"server0", "server1"};
    final Set<String> consumingSegments = Sets.newHashSet(SEGMENT_NAME_PARTITION_0, SEGMENT_NAME_PARTITION_1);
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentsInfoMap =
        testRunner(servers, consumingSegments, TABLE_NAME);
    checkIngestionStatus(servers, consumingSegments, TableStatus.IngestionState.HEALTHY);

    List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> consumingSegmentInfos =
        consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_0);
    assertEquals(consumingSegmentInfos.size(), 2);
    for (ConsumingSegmentInfoReader.ConsumingSegmentInfo info : consumingSegmentInfos) {
      checkConsumingSegmentInfo(info, Sets.newHashSet("server0", "server1"), ConsumerState.CONSUMING.toString(), "0",
          "150");
    }
    consumingSegmentInfos = consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_1);
    assertEquals(consumingSegmentInfos.size(), 2);
    for (ConsumingSegmentInfoReader.ConsumingSegmentInfo info : consumingSegmentInfos) {
      checkConsumingSegmentInfo(info, Sets.newHashSet("server0", "server1"), ConsumerState.CONSUMING.toString(), "1",
          "150");
    }
  }

  /**
   * 2 servers, 2 partitions, 2 replicas. p0 consumer in NOT_CONSUMING
   */
  @Test
  public void testNotConsumingState()
      throws InvalidConfigException {
    final String[] servers = {"server0", "server2"};
    final Set<String> consumingSegments = Sets.newHashSet(SEGMENT_NAME_PARTITION_0, SEGMENT_NAME_PARTITION_1);
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentsInfoMap =
        testRunner(servers, consumingSegments, TABLE_NAME);
    checkIngestionStatus(servers, consumingSegments, TableStatus.IngestionState.UNHEALTHY);

    List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> consumingSegmentInfos =
        consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_0);
    assertEquals(consumingSegmentInfos.size(), 2);
    for (ConsumingSegmentInfoReader.ConsumingSegmentInfo info : consumingSegmentInfos) {
      if (info._serverName.equals("server0")) {
        checkConsumingSegmentInfo(info, Sets.newHashSet("server0"), ConsumerState.CONSUMING.toString(), "0", "150");
      } else {
        checkConsumingSegmentInfo(info, Sets.newHashSet("server2"), ConsumerState.NOT_CONSUMING.toString(), "0", "150");
      }
    }
    consumingSegmentInfos = consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_1);
    assertEquals(consumingSegmentInfos.size(), 2);
    for (ConsumingSegmentInfoReader.ConsumingSegmentInfo info : consumingSegmentInfos) {
      checkConsumingSegmentInfo(info, Sets.newHashSet("server0", "server2"), ConsumerState.CONSUMING.toString(), "1",
          "150");
    }
  }

  /**
   * 1 servers, 2 partitions, 1 replicas. No consumer for p0. CONSUMING state in idealstate.
   */
  @Test
  public void testNoConsumerButConsumingInIdealState()
      throws InvalidConfigException {
    final String[] servers = {"server3"};
    final Set<String> consumingSegments = Sets.newHashSet(SEGMENT_NAME_PARTITION_0, SEGMENT_NAME_PARTITION_1);
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentsInfoMap =
        testRunner(servers, consumingSegments, TABLE_NAME);
    checkIngestionStatus(servers, consumingSegments, TableStatus.IngestionState.UNHEALTHY);

    List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> consumingSegmentInfos =
        consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_0);
    assertTrue(consumingSegmentInfos.isEmpty());

    consumingSegmentInfos = consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_1);
    assertEquals(consumingSegmentInfos.size(), 1);
    checkConsumingSegmentInfo(consumingSegmentInfos.get(0), Sets.newHashSet("server3"),
        ConsumerState.CONSUMING.toString(), "1", "150");
  }

  /**
   * 1 servers, 2 partitions, 1 replicas. No consumer for p0. OFFLINE state in idealstate.
   */
  @Test
  public void testNoConsumerOfflineInIdealState()
      throws InvalidConfigException {
    final String[] servers = {"server3"};
    Set<String> consumingSegments = Sets.newHashSet(SEGMENT_NAME_PARTITION_1);
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentsInfoMap =
        testRunner(servers, consumingSegments, TABLE_NAME);
    consumingSegments = Sets.newHashSet(SEGMENT_NAME_PARTITION_0, SEGMENT_NAME_PARTITION_1);
    checkIngestionStatus(servers, consumingSegments, TableStatus.IngestionState.UNHEALTHY);

    List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> consumingSegmentInfos =
        consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_0);
    assertNull(consumingSegmentInfos);

    consumingSegmentInfos = consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_1);
    assertEquals(consumingSegmentInfos.size(), 1);
    checkConsumingSegmentInfo(consumingSegmentInfos.get(0), Sets.newHashSet("server3"),
        ConsumerState.CONSUMING.toString(), "1", "150");
  }

  /**
   * 2 servers, 2 partitions, 2 replicas. server4 times out.
   */
  @Test
  public void testErrorFromServer()
      throws InvalidConfigException {
    final String[] servers = {"server0", "server4"};
    final Set<String> consumingSegments = Sets.newHashSet(SEGMENT_NAME_PARTITION_0, SEGMENT_NAME_PARTITION_1);
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentsInfoMap =
        testRunner(servers, consumingSegments, TABLE_NAME);
    checkIngestionStatus(servers, consumingSegments, TableStatus.IngestionState.UNHEALTHY);

    List<ConsumingSegmentInfoReader.ConsumingSegmentInfo> consumingSegmentInfos =
        consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_0);
    assertEquals(consumingSegmentInfos.size(), 1);
    checkConsumingSegmentInfo(consumingSegmentInfos.get(0), Sets.newHashSet("server0"),
        ConsumerState.CONSUMING.toString(), "0", "150");

    consumingSegmentInfos = consumingSegmentsInfoMap._segmentToConsumingInfoMap.get(SEGMENT_NAME_PARTITION_1);
    assertEquals(consumingSegmentInfos.size(), 1);
    checkConsumingSegmentInfo(consumingSegmentInfos.get(0), Sets.newHashSet("server0"),
        ConsumerState.CONSUMING.toString(), "1", "150");
  }

  private void checkConsumingSegmentInfo(ConsumingSegmentInfoReader.ConsumingSegmentInfo info, Set<String> serverNames,
      String consumerState, String partition, String offset) {
    assertTrue(serverNames.contains(info._serverName));
    assertEquals(info._consumerState, consumerState);
    assertEquals(info._partitionOffsetInfo._currentOffsetsMap.get(partition), offset);
  }
}
