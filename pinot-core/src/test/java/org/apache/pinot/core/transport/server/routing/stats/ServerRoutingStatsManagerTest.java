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
package org.apache.pinot.core.transport.server.routing.stats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ServerRoutingStatsManagerTest {
  @Test
  public void testInitAndShutDown() {
    Map<String, Object> properties = new HashMap<>();

    // Test 1: Test disabled.
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, false);
    ServerRoutingStatsManager manager = new ServerRoutingStatsManager(new PinotConfiguration(properties));
    assertFalse(manager.isEnabled());
    manager.init();
    assertFalse(manager.isEnabled());

    // Test 2: Test enabled.
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    manager = new ServerRoutingStatsManager(new PinotConfiguration(properties));
    assertFalse(manager.isEnabled());
    manager.init();
    assertTrue(manager.isEnabled());

    // Test 3: Shutdown and then init.
    manager.shutDown();
    assertFalse(manager.isEnabled());

    manager.init();
    assertTrue(manager.isEnabled());
  }

  @Test
  public void testEmptyStats() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    ServerRoutingStatsManager manager = new ServerRoutingStatsManager(new PinotConfiguration(properties));
    manager.init();

    List<Pair<String, Integer>> numInFlightReqList = manager.fetchNumInFlightRequestsForAllServers();
    assertTrue(numInFlightReqList.isEmpty());
    Integer numInFlightReq = manager.fetchNumInFlightRequestsForServer("testServer");
    assertNull(numInFlightReq);

    List<Pair<String, Double>> latencyList = manager.fetchEMALatencyForAllServers();
    assertTrue(latencyList.isEmpty());

    Double latency = manager.fetchEMALatencyForServer("testServer");
    assertNull(latency);

    List<Pair<String, Double>> scoreList = manager.fetchHybridScoreForAllServers();
    assertTrue(scoreList.isEmpty());

    Double score = manager.fetchHybridScoreForServer("testServer");
    assertNull(score);
  }

  @Test
  public void testQuerySubmitAndCompletionStats() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_EWMA_ALPHA, 1.0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AUTODECAY_WINDOW_MS, -1);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_WARMUP_DURATION_MS, 0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AVG_INITIALIZATION_VAL, 0.0);
    properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_HYBRID_SCORE_EXPONENT, 3);
    ServerRoutingStatsManager manager = new ServerRoutingStatsManager(new PinotConfiguration(properties));
    manager.init();

    int requestId = 0;

    // Submit stats for server1.
    manager.recordStatsAfterQuerySubmission(requestId++, "server1");
    waitForStatsUpdate(manager, requestId);

    List<Pair<String, Integer>> numInFlightReqList = manager.fetchNumInFlightRequestsForAllServers();
    assertEquals(numInFlightReqList.get(0).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(0).getRight().intValue(), 1);

    Integer numInFlightReq = manager.fetchNumInFlightRequestsForServer("server1");
    assertEquals(numInFlightReq.intValue(), 1);

    List<Pair<String, Double>> latencyList = manager.fetchEMALatencyForAllServers();
    assertEquals(latencyList.get(0).getLeft(), "server1");
    assertEquals(latencyList.get(0).getRight().doubleValue(), 0.0);

    Double latency = manager.fetchEMALatencyForServer("server1");
    assertEquals(latency, 0.0);

    List<Pair<String, Double>> scoreList = manager.fetchHybridScoreForAllServers();
    assertEquals(scoreList.get(0).getLeft(), "server1");
    assertEquals(scoreList.get(0).getRight().doubleValue(), 0.0);

    Double score = manager.fetchHybridScoreForServer("server1");
    assertEquals(score, 0.0);

    // Submit more stats for server 1.
    manager.recordStatsAfterQuerySubmission(requestId++, "server1");
    waitForStatsUpdate(manager, requestId);

    numInFlightReqList = manager.fetchNumInFlightRequestsForAllServers();
    assertEquals(numInFlightReqList.get(0).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(0).getRight().intValue(), 2);

    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server1");
    assertEquals(numInFlightReq.intValue(), 2);

    latencyList = manager.fetchEMALatencyForAllServers();
    assertEquals(latencyList.get(0).getLeft(), "server1");
    assertEquals(latencyList.get(0).getRight().doubleValue(), 0.0);

    latency = manager.fetchEMALatencyForServer("server1");
    assertEquals(latency, 0.0);

    scoreList = manager.fetchHybridScoreForAllServers();
    assertEquals(scoreList.get(0).getLeft(), "server1");
    assertEquals(scoreList.get(0).getRight().doubleValue(), 0.0);

    score = manager.fetchHybridScoreForServer("server1");
    assertEquals(score, 0.0);

    // Add a new server server2.
    manager.recordStatsAfterQuerySubmission(requestId++, "server2");
    waitForStatsUpdate(manager, requestId);


    numInFlightReqList = manager.fetchNumInFlightRequestsForAllServers();
    int server2Index = numInFlightReqList.get(0).getLeft().equals("server2") ? 0 : 1;
    int server1Index = 1 - server2Index;
    assertEquals(numInFlightReqList.get(server2Index).getLeft(), "server2");
    assertEquals(numInFlightReqList.get(server2Index).getRight().intValue(), 1);
    assertEquals(numInFlightReqList.get(server1Index).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(server1Index).getRight().intValue(), 2);

    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server2");
    assertEquals(numInFlightReq.intValue(), 1);
    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server1");
    assertEquals(numInFlightReq.intValue(), 2);

    latencyList = manager.fetchEMALatencyForAllServers();
    server2Index = latencyList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(latencyList.get(server2Index).getLeft(), "server2");
    assertEquals(latencyList.get(server2Index).getRight().doubleValue(), 0.0);
    assertEquals(latencyList.get(server1Index).getLeft(), "server1");
    assertEquals(latencyList.get(server1Index).getRight().doubleValue(), 0.0);

    latency = manager.fetchEMALatencyForServer("server2");
    assertEquals(latency, 0.0);
    latency = manager.fetchEMALatencyForServer("server1");
    assertEquals(latency, 0.0);

    scoreList = manager.fetchHybridScoreForAllServers();
    server2Index = scoreList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(scoreList.get(server2Index).getLeft(), "server2");
    assertEquals(scoreList.get(server2Index).getRight().doubleValue(), 0.0);
    assertEquals(scoreList.get(server1Index).getLeft(), "server1");
    assertEquals(scoreList.get(server1Index).getRight().doubleValue(), 0.0);

    score = manager.fetchHybridScoreForServer("server2");
    assertEquals(score, 0.0);
    score = manager.fetchHybridScoreForServer("server1");
    assertEquals(score, 0.0);

    // Record completion stats for server1
    manager.recordStatsUponResponseArrival(requestId++, "server1", 2);
    waitForStatsUpdate(manager, requestId);

    numInFlightReqList = manager.fetchNumInFlightRequestsForAllServers();
    server2Index = numInFlightReqList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(numInFlightReqList.get(server2Index).getLeft(), "server2");
    assertEquals(numInFlightReqList.get(server2Index).getRight().intValue(), 1);
    assertEquals(numInFlightReqList.get(server1Index).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(server1Index).getRight().intValue(), 1);

    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server2");
    assertEquals(numInFlightReq.intValue(), 1);
    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server1");
    assertEquals(numInFlightReq.intValue(), 1);

    latencyList = manager.fetchEMALatencyForAllServers();
    server2Index = latencyList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(latencyList.get(server2Index).getLeft(), "server2");
    assertEquals(latencyList.get(server2Index).getRight().doubleValue(), 0.0);
    assertEquals(latencyList.get(server1Index).getLeft(), "server1");
    assertEquals(latencyList.get(server1Index).getRight().doubleValue(), 2.0);

    latency = manager.fetchEMALatencyForServer("server2");
    assertEquals(latency, 0.0);
    latency = manager.fetchEMALatencyForServer("server1");
    assertEquals(latency, 2.0);

    scoreList = manager.fetchHybridScoreForAllServers();
    server2Index = scoreList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(scoreList.get(server2Index).getLeft(), "server2");
    assertEquals(scoreList.get(server2Index).getRight().doubleValue(), 0.0);
    assertEquals(scoreList.get(server1Index).getLeft(), "server1");
    assertEquals(scoreList.get(server1Index).getRight().doubleValue(), 54.0);

    score = manager.fetchHybridScoreForServer("server2");
    assertEquals(score, 0.0);
    score = manager.fetchHybridScoreForServer("server1");
    assertEquals(score, 54.0);

    // Record completion stats for server2
    manager.recordStatsUponResponseArrival(requestId++, "server2", 10);
    waitForStatsUpdate(manager, requestId);

    numInFlightReqList = manager.fetchNumInFlightRequestsForAllServers();
    server2Index = numInFlightReqList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(numInFlightReqList.get(server2Index).getLeft(), "server2");
    assertEquals(numInFlightReqList.get(server2Index).getRight().intValue(), 0);
    assertEquals(numInFlightReqList.get(server1Index).getLeft(), "server1");
    assertEquals(numInFlightReqList.get(server1Index).getRight().intValue(), 1);

    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server2");
    assertEquals(numInFlightReq.intValue(), 0);
    numInFlightReq = manager.fetchNumInFlightRequestsForServer("server1");
    assertEquals(numInFlightReq.intValue(), 1);

    latencyList = manager.fetchEMALatencyForAllServers();
    server2Index = latencyList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(latencyList.get(server2Index).getLeft(), "server2");
    assertEquals(latencyList.get(server2Index).getRight().doubleValue(), 10.0);
    assertEquals(latencyList.get(server1Index).getLeft(), "server1");
    assertEquals(latencyList.get(server1Index).getRight().doubleValue(), 2.0);

    latency = manager.fetchEMALatencyForServer("server2");
    assertEquals(latency, 10.0);
    latency = manager.fetchEMALatencyForServer("server1");
    assertEquals(latency, 2.0);

    scoreList = manager.fetchHybridScoreForAllServers();
    server2Index = scoreList.get(0).getLeft().equals("server2") ? 0 : 1;
    server1Index = 1 - server2Index;
    assertEquals(scoreList.get(server2Index).getLeft(), "server2");
    assertEquals(scoreList.get(server2Index).getRight().doubleValue(), 10.0, manager.getServerRoutingStatsStr());
    assertEquals(scoreList.get(server1Index).getLeft(), "server1");
    assertEquals(scoreList.get(server1Index).getRight().doubleValue(), 54.0, manager.getServerRoutingStatsStr());

    score = manager.fetchHybridScoreForServer("server2");
    assertEquals(score, 10.0);
    score = manager.fetchHybridScoreForServer("server1");
    assertEquals(score, 54.0);
  }

  private void waitForStatsUpdate(ServerRoutingStatsManager serverRoutingStatsManager, long taskCount) {
    TestUtils.waitForCondition(aVoid -> {
      return (serverRoutingStatsManager.getCompletedTaskCount() == taskCount);
    }, 10L, 5000, "Failed to record stats for AdaptiveServerSelectorTest");
  }
}
