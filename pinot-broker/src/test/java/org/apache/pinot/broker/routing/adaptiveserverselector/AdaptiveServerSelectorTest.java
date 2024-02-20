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
package org.apache.pinot.broker.routing.adaptiveserverselector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.ExponentialMovingAverage;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class AdaptiveServerSelectorTest {
  private BrokerMetrics _brokerMetrics;

  List<String> _servers = Arrays.asList("server1", "server2", "server3", "server4");
  Map<String, Object> _properties = new HashMap<>();

  @BeforeTest
  public void initBrokerMetrics() {
    // Set up metric registry and broker metrics
    PinotConfiguration brokerConfig = new PinotConfiguration();
    PinotMetricsRegistry metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry(
        brokerConfig.subset(CommonConstants.Broker.METRICS_CONFIG_PREFIX));
    _brokerMetrics = new BrokerMetrics(
        brokerConfig.getProperty(
            CommonConstants.Broker.CONFIG_OF_METRICS_NAME_PREFIX,
            CommonConstants.Broker.DEFAULT_METRICS_NAME_PREFIX),
        metricsRegistry,
        brokerConfig.getProperty(
            CommonConstants.Broker.CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS,
            CommonConstants.Broker.DEFAULT_ENABLE_TABLE_LEVEL_METRICS),
        brokerConfig.getProperty(
            CommonConstants.Broker.CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS,
            Collections.emptyList()));
    _brokerMetrics.initializeGlobalMeters();
    BrokerMetrics.register(_brokerMetrics);
  }

  @Test
  public void testAdaptiveServerSelectorFactory() {
    // Test 1: Test disabling Adaptive Server Selection .
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
        CommonConstants.Broker.AdaptiveServerSelector.Type.NO_OP.name());
    PinotConfiguration cfg = new PinotConfiguration(_properties);
    ServerRoutingStatsManager serverRoutingStatsManager = new ServerRoutingStatsManager(cfg, _brokerMetrics);
    assertNull(AdaptiveServerSelectorFactory.getAdaptiveServerSelector(serverRoutingStatsManager, cfg));

    // Enable stats collection. Without this, AdaptiveServerSelectors cannot be used.
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);

    // Test 2: Test NumInFlightSelector.
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
        CommonConstants.Broker.AdaptiveServerSelector.Type.NUM_INFLIGHT_REQ.name());
    cfg = new PinotConfiguration(_properties);
    serverRoutingStatsManager = new ServerRoutingStatsManager(cfg, _brokerMetrics);
    assertTrue(AdaptiveServerSelectorFactory.getAdaptiveServerSelector(serverRoutingStatsManager,
        cfg) instanceof NumInFlightReqSelector);

    // Test 3: Test LatencySelector.
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
        CommonConstants.Broker.AdaptiveServerSelector.Type.LATENCY.name());
    cfg = new PinotConfiguration(_properties);
    serverRoutingStatsManager = new ServerRoutingStatsManager(cfg, _brokerMetrics);
    assertTrue(AdaptiveServerSelectorFactory.getAdaptiveServerSelector(serverRoutingStatsManager,
        cfg) instanceof LatencySelector);

    // Test 4: Test HybridSelector.
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
        CommonConstants.Broker.AdaptiveServerSelector.Type.HYBRID.name());
    cfg = new PinotConfiguration(_properties);
    serverRoutingStatsManager = new ServerRoutingStatsManager(cfg, _brokerMetrics);
    assertTrue(AdaptiveServerSelectorFactory.getAdaptiveServerSelector(serverRoutingStatsManager,
        cfg) instanceof HybridSelector);

    // Test 5: Test Error.
    assertThrows(IllegalArgumentException.class, () -> {
      _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_TYPE, "Dummy");
      PinotConfiguration config = new PinotConfiguration(_properties);
      ServerRoutingStatsManager manager = new ServerRoutingStatsManager(config, _brokerMetrics);
      AdaptiveServerSelectorFactory.getAdaptiveServerSelector(manager, config);
    });
  }

  @Test
  public void testNumInFlightReqSelector() {
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    PinotConfiguration cfg = new PinotConfiguration(_properties);
    ServerRoutingStatsManager serverRoutingStatsManager = new ServerRoutingStatsManager(cfg, _brokerMetrics);
    serverRoutingStatsManager.init();
    assertTrue(serverRoutingStatsManager.isEnabled());
    long taskCount = 0;

    NumInFlightReqSelector selector = new NumInFlightReqSelector(serverRoutingStatsManager);
    // Map maintaining the number of inflight requests for each server.
    Map<String, Integer> numInflightReqMap = new HashMap<>();

    // TEST 1: Try to fetch the best server when stats are not populated yet.
    List<Pair<String, Double>> serverRankingWithVal = selector.fetchAllServerRankingsWithScores();
    assertTrue(serverRankingWithVal.isEmpty());

    // -1.0 will be returned for all servers.
    serverRankingWithVal = selector.fetchServerRankingsWithScores(_servers);
    for (Pair<String, Double> entry : serverRankingWithVal) {
      assertEquals(entry.getRight(), -1.0);
    }

    // A random server will be returned if any of the candidate servers do not have stats.
    String selectedServer = selector.select(_servers);
    assertTrue(_servers.contains(selectedServer), selectedServer);

    // TEST 2: Populate all servers with equal stats.
    // Current numInFlightRequests:
    //   server1 -> 10
    //   server2 -> 10
    //   server3 -> 10
    //   server4 -> 10
    for (String server : _servers) {
      numInflightReqMap.put(server, 10);
    }
    for (int ii = 0; ii < 10; ii++) {
      for (String server : _servers) {
        serverRoutingStatsManager.recordStatsAfterQuerySubmission(-1, server);
        waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
      }
    }

    serverRankingWithVal = selector.fetchAllServerRankingsWithScores();
    assertEquals(serverRankingWithVal.size(), _servers.size());
    for (Pair<String, Double> entry : serverRankingWithVal) {
      assertEquals(entry.getRight(), (double) numInflightReqMap.get(entry.getLeft()));
    }

    selectedServer = selector.select(_servers);
    assertEquals(selectedServer, _servers.get(0));

    List<String> candidateServers = new ArrayList<>(Arrays.asList("server2", "server3"));
    serverRankingWithVal = selector.fetchServerRankingsWithScores(candidateServers);
    assertEquals(serverRankingWithVal.size(), 2);
    for (Pair<String, Double> entry : serverRankingWithVal) {
      assertEquals(entry.getRight(), (double) numInflightReqMap.get(entry.getLeft()));
    }

    // TEST 3 : Populate all servers with unequal stats.
    // Current numInFlightRequests:
    //   server1 -> 10
    //   server2 -> 11
    //   server3 -> 12
    //   server4 -> 13
    numInflightReqMap.put("server1", 10);
    numInflightReqMap.put("server2", 11);
    numInflightReqMap.put("server3", 12);
    numInflightReqMap.put("server4", 13);

    for (int ii = 0; ii < _servers.size(); ii++) {
      for (int jj = 0; jj < ii; jj++) {
        serverRoutingStatsManager.recordStatsAfterQuerySubmission(-1, _servers.get(ii));
        waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
      }
    }

    serverRankingWithVal = selector.fetchAllServerRankingsWithScores();
    assertEquals(serverRankingWithVal.get(0).getLeft(), "server1");
    assertEquals(serverRankingWithVal.get(0).getRight(), (double) numInflightReqMap.get("server1"));
    assertEquals(serverRankingWithVal.get(1).getLeft(), "server2");
    assertEquals(serverRankingWithVal.get(1).getRight(), (double) numInflightReqMap.get("server2"));
    assertEquals(serverRankingWithVal.get(2).getLeft(), "server3");
    assertEquals(serverRankingWithVal.get(2).getRight(), (double) numInflightReqMap.get("server3"));
    assertEquals(serverRankingWithVal.get(3).getLeft(), "server4");
    assertEquals(serverRankingWithVal.get(3).getRight(), (double) numInflightReqMap.get("server4"));

    selectedServer = selector.select(_servers);
    assertEquals(selectedServer, "server1");

    selectedServer = selector.select(Arrays.asList("server2", "server3"));
    assertEquals(selectedServer, "server2");

    selectedServer = selector.select(Arrays.asList("server3", "server1", "server2"));
    assertEquals(selectedServer, "server1");

    candidateServers = new ArrayList<>(Arrays.asList("server4", "server3", "server1"));
    serverRankingWithVal = selector.fetchServerRankingsWithScores(candidateServers);
    assertEquals(serverRankingWithVal.size(), 3);
    assertEquals(serverRankingWithVal.get(0).getLeft(), "server1");
    assertEquals(serverRankingWithVal.get(0).getRight(), (double) numInflightReqMap.get("server1"));
    assertEquals(serverRankingWithVal.get(1).getLeft(), "server3");
    assertEquals(serverRankingWithVal.get(1).getRight(), (double) numInflightReqMap.get("server3"));
    assertEquals(serverRankingWithVal.get(2).getLeft(), "server4");
    assertEquals(serverRankingWithVal.get(2).getRight(), (double) numInflightReqMap.get("server4"));


    // TEST 4: Populate all servers with unequal stats.
    // Current numInFlightRequests:
    //   server1 -> 12
    //   server2 -> 11
    //   server3 -> 15
    //   server4 -> 13
    numInflightReqMap.put("server1", 12);
    numInflightReqMap.put("server2", 11);
    numInflightReqMap.put("server3", 15);
    numInflightReqMap.put("server4", 13);
    serverRoutingStatsManager.recordStatsAfterQuerySubmission(-1, _servers.get(0));
    waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
    serverRoutingStatsManager.recordStatsAfterQuerySubmission(-1, _servers.get(0));
    waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
    serverRoutingStatsManager.recordStatsAfterQuerySubmission(-1, _servers.get(2));
    waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
    serverRoutingStatsManager.recordStatsAfterQuerySubmission(-1, _servers.get(2));
    waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
    serverRoutingStatsManager.recordStatsAfterQuerySubmission(-1, _servers.get(2));
    waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);

    serverRankingWithVal = selector.fetchAllServerRankingsWithScores();
    assertEquals(serverRankingWithVal.get(0).getLeft(), "server2");
    assertEquals(serverRankingWithVal.get(0).getRight(), (double) numInflightReqMap.get("server2"));
    assertEquals(serverRankingWithVal.get(1).getLeft(), "server1");
    assertEquals(serverRankingWithVal.get(1).getRight(), (double) numInflightReqMap.get("server1"));
    assertEquals(serverRankingWithVal.get(2).getLeft(), "server4");
    assertEquals(serverRankingWithVal.get(2).getRight(), (double) numInflightReqMap.get("server4"));
    assertEquals(serverRankingWithVal.get(3).getLeft(), "server3");
    assertEquals(serverRankingWithVal.get(3).getRight(), (double) numInflightReqMap.get("server3"));

    selectedServer = selector.select(_servers);
    assertEquals(selectedServer, "server2");

    selectedServer = selector.select(Arrays.asList("server2", "server3"));
    assertEquals(selectedServer, "server2");

    selectedServer = selector.select(Arrays.asList("server3", "server1", "server2"));
    assertEquals(selectedServer, "server2");

    candidateServers = new ArrayList<>(Arrays.asList("server2", "server1"));
    serverRankingWithVal = selector.fetchServerRankingsWithScores(candidateServers);
    assertEquals(serverRankingWithVal.size(), 2);
    assertEquals(serverRankingWithVal.get(0).getLeft(), "server2");
    assertEquals(serverRankingWithVal.get(0).getRight(), (double) numInflightReqMap.get("server2"));
    assertEquals(serverRankingWithVal.get(1).getLeft(), "server1");
    assertEquals(serverRankingWithVal.get(1).getRight(), (double) numInflightReqMap.get("server1"));

    // Test 5: Simulate server selection code. Pick the best server using NumInFlightReqSelector during every
    // iteration. Every iteration increases the number of inflight requests but decides with the flip of a coin
    // to decrement the number of request on the server. Verify if NumInFlightReqSelector chooses the best server
    // during every iteration.
    Random rand = new Random();
    for (int ii = 0; ii < 1000; ii++) {
      serverRankingWithVal = selector.fetchAllServerRankingsWithScores();

      // Assert if the ranking is accurate.
      double prevVal = 0.0;
      for (int jj = 0; jj < serverRankingWithVal.size(); jj++) {
        String server = serverRankingWithVal.get(jj).getLeft();
        double numReq = serverRankingWithVal.get(jj).getRight();
        assertEquals(numReq, (double) numInflightReqMap.get(server));
        assertTrue(prevVal <= numReq, prevVal + " " + numReq + " " + server);
        prevVal = numReq;
      }

      // Route the request to the best server.
      selectedServer = serverRankingWithVal.get(0).getLeft();
      serverRoutingStatsManager.recordStatsAfterQuerySubmission(-1, selectedServer);
      waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
      int numReq = numInflightReqMap.get(selectedServer) + 1;
      numInflightReqMap.put(selectedServer, numReq);

      if (rand.nextBoolean()) {
        serverRoutingStatsManager.recordStatsUponResponseArrival(-1, selectedServer, 1);
        numReq = numInflightReqMap.get(selectedServer) - 1;
        numInflightReqMap.put(selectedServer, numReq);
        waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
      }
    }
  }

  @Test
  public void testLatencySelector() {
    double alpha = 0.666;
    long autodecayWindowMs = -1;
    int warmupDurationMs = 0;
    double avgInitializationVal = 0.0;

    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_EWMA_ALPHA, alpha);
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AUTODECAY_WINDOW_MS, autodecayWindowMs);
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_WARMUP_DURATION_MS, warmupDurationMs);
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AVG_INITIALIZATION_VAL,
        avgInitializationVal);
    PinotConfiguration cfg = new PinotConfiguration(_properties);
    ServerRoutingStatsManager serverRoutingStatsManager = new ServerRoutingStatsManager(cfg, _brokerMetrics);
    serverRoutingStatsManager.init();
    assertTrue(serverRoutingStatsManager.isEnabled());
    long taskCount = 0;

    LatencySelector selector = new LatencySelector(serverRoutingStatsManager);
    Map<String, ExponentialMovingAverage> latencyMap = new HashMap<>();
    Random rand = new Random();

    // TEST 1: Try to fetch the best server when stats are not populated yet.

    List<Pair<String, Double>> serverRankingWithVal = selector.fetchAllServerRankingsWithScores();
    assertTrue(serverRankingWithVal.isEmpty());

    // A random server will be returned if any of the candidate servers do not have stats.
    String selectedServer = selector.select(_servers);
    assertTrue(_servers.contains(selectedServer), selectedServer);

    List<String> candidateServers = new ArrayList<>(Arrays.asList("server2"));
    serverRankingWithVal = selector.fetchServerRankingsWithScores(candidateServers);
    assertEquals(serverRankingWithVal.size(), 1);
    assertEquals(serverRankingWithVal.get(0).getLeft(), "server2");
    assertEquals(serverRankingWithVal.get(0).getRight(), -1.0);

    // TEST 2: Populate all servers with equal latencies.
    for (int ii = 0; ii < 10; ii++) {
      for (String server : _servers) {
        latencyMap.computeIfAbsent(server,
            k -> new ExponentialMovingAverage(alpha, autodecayWindowMs, warmupDurationMs, avgInitializationVal, null));
        latencyMap.get(server).compute(2);

        serverRoutingStatsManager.recordStatsUponResponseArrival(-1, server, 2);
        waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
      }
    }

    serverRankingWithVal = selector.fetchAllServerRankingsWithScores();
    assertEquals(serverRankingWithVal.size(), _servers.size());
    for (Pair<String, Double> entry : serverRankingWithVal) {
      assertEquals(entry.getRight(), latencyMap.get(entry.getLeft()).getAverage());
    }

    selectedServer = selector.select(_servers);
    assertEquals(selectedServer, _servers.get(0));

    candidateServers = new ArrayList<>(Arrays.asList("server4", "server3"));
    serverRankingWithVal = selector.fetchServerRankingsWithScores(candidateServers);
    assertEquals(serverRankingWithVal.size(), 2);
    for (Pair<String, Double> entry : serverRankingWithVal) {
      assertEquals(entry.getRight(), latencyMap.get(entry.getLeft()).getAverage());
    }

    // TEST 3: Populate servers with unequal latencies.
    // Latencies added to servers are as follows:
    // server1 -> 1
    // server2 -> 3
    // server3 -> 5
    // server4 -> 7
    for (int ii = 0; ii < _servers.size(); ii++) {
      String server = _servers.get(ii);
      int latency = ii * 2 + 1;
      for (int jj = 0; jj < 10; jj++) {
        latencyMap.get(server).compute(latency);
        serverRoutingStatsManager.recordStatsUponResponseArrival(-1, server, latency);
        waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
      }
    }

    serverRankingWithVal = selector.fetchAllServerRankingsWithScores();
    double prevVal = 0.0;
    for (Pair<String, Double> entry : serverRankingWithVal) {
      String server = entry.getLeft();
      double latency = entry.getRight();
      assertEquals(latency, (double) latencyMap.get(server).getAverage());
      assertTrue(prevVal <= latency, prevVal + " " + latency + " " + server);
      prevVal = latency;
    }

    selectedServer = selector.select(_servers);
    assertEquals(selectedServer, "server1");

    selectedServer = selector.select(Arrays.asList("server3", "server2"));
    assertEquals(selectedServer, "server2");

    selectedServer = selector.select(Arrays.asList("server3", "server4", "server2"));
    assertEquals(selectedServer, "server2");

    candidateServers = new ArrayList<>(Arrays.asList("server4", "server1", "server3"));
    serverRankingWithVal = selector.fetchServerRankingsWithScores(candidateServers);
    assertEquals(serverRankingWithVal.size(), 3);
    prevVal = 0.0;
    for (Pair<String, Double> entry : serverRankingWithVal) {
      String server = entry.getLeft();
      double latency = entry.getRight();
      assertEquals(latency, (double) latencyMap.get(server).getAverage());
      assertTrue(prevVal <= latency, prevVal + " " + latency + " " + server);
      prevVal = latency;
    }

    // Test 4: Simulate server selection code. Pick the best server using LatencySelector during every iteration.
    // Every iteration updates latency for a server. Verify if LatencySelector picks the best server in every iteration.
    for (int ii = 0; ii < 1000; ii++) {
      serverRankingWithVal = selector.fetchAllServerRankingsWithScores();

      prevVal = 0.0;
      for (Pair<String, Double> entry : serverRankingWithVal) {
        String server = entry.getLeft();
        double latency = entry.getRight();
        assertEquals(latency, (double) latencyMap.get(server).getAverage());
        assertTrue(prevVal <= latency, prevVal + " " + latency + " " + server);
        prevVal = latency;
      }

      // Route the request to the best server.
      int latencyMs = rand.nextInt(20);
      selectedServer = serverRankingWithVal.get(0).getLeft();
      serverRoutingStatsManager.recordStatsUponResponseArrival(-1, selectedServer, latencyMs);
      latencyMap.get(selectedServer).compute(latencyMs);
      waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
    }
  }

  @Test
  public void testHybridSelector() {
    double alpha = 0.666;
    long autodecayWindowMs = -1;
    int warmupDurationMs = 0;
    double avgInitializationVal = 0.0;
    int hybridSelectorExponent = 3;

    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION, true);
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_EWMA_ALPHA, alpha);
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AUTODECAY_WINDOW_MS, autodecayWindowMs);
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_WARMUP_DURATION_MS, warmupDurationMs);
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_AVG_INITIALIZATION_VAL,
        avgInitializationVal);
    _properties.put(CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_HYBRID_SCORE_EXPONENT,
        hybridSelectorExponent);

    PinotConfiguration cfg = new PinotConfiguration(_properties);
    ServerRoutingStatsManager serverRoutingStatsManager = new ServerRoutingStatsManager(cfg, _brokerMetrics);
    serverRoutingStatsManager.init();
    assertTrue(serverRoutingStatsManager.isEnabled());

    HybridSelector selector = new HybridSelector(serverRoutingStatsManager);
    long taskCount = 0;

    // TEST 1: Try to fetch the best server when stats are not populated yet.
    List<Pair<String, Double>> serverRankingWithVal = selector.fetchAllServerRankingsWithScores();
    assertTrue(serverRankingWithVal.isEmpty());

    // A random server will be returned if any of the candidate servers do not have stats.
    String selectedServer = selector.select(_servers);
    assertTrue(_servers.contains(selectedServer), selectedServer);

    List<String> candidateServers = new ArrayList<>(Arrays.asList("server2", "server3", "server1", "server4"));
    serverRankingWithVal = selector.fetchServerRankingsWithScores(candidateServers);
    assertEquals(serverRankingWithVal.size(), 4);
    for (Pair<String, Double> entry : serverRankingWithVal) {
      assertEquals(entry.getRight(), -1.0);
    }


    // TEST 2: Populate all servers with equal numInFlightRequests and latencies.
    for (int ii = 0; ii < 10; ii++) {
      for (String server : _servers) {
        serverRoutingStatsManager.recordStatsAfterQuerySubmission(-1, server);
        waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
      }
    }

    for (int ii = 0; ii < 2; ii++) {
      for (String server : _servers) {
        serverRoutingStatsManager.recordStatsUponResponseArrival(-1, server, 2);
        waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
      }
    }

    serverRankingWithVal = selector.fetchAllServerRankingsWithScores();
    assertEquals(serverRankingWithVal.size(), _servers.size());
    StringBuilder debugStr = new StringBuilder();
    for (Pair<String, Double> entry : serverRankingWithVal) {
      debugStr.append(entry.getLeft()).append("=").append(entry.getRight().toString()).append("; ");
      assertEquals(entry.getRight(), serverRankingWithVal.get(0).getRight(), debugStr.toString());
    }

    selectedServer = selector.select(_servers);
    assertEquals(selectedServer, _servers.get(0));

    candidateServers = new ArrayList<>(Arrays.asList("server1", "server2"));
    serverRankingWithVal = selector.fetchServerRankingsWithScores(candidateServers);
    assertEquals(serverRankingWithVal.size(), 2);
    for (Pair<String, Double> entry : serverRankingWithVal) {
      assertEquals(entry.getRight(), serverRankingWithVal.get(0).getRight());
    }

    // Test 3: Populate servers with unequal latencies and numInFlightRequests.
    for (int ii = 0; ii < _servers.size(); ii++) {
      String server = _servers.get(ii);
      int latency = ii * 2 + 1;
      for (int jj = 0; jj < 5; jj++) {
        serverRoutingStatsManager.recordStatsUponResponseArrival(-1, server, latency);
        waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
      }
    }

    serverRankingWithVal = selector.fetchAllServerRankingsWithScores();
    double prevVal = 0.0;
    for (Pair<String, Double> entry : serverRankingWithVal) {
      String server = entry.getLeft();
      double latency = entry.getRight();
      assertTrue(prevVal <= latency, prevVal + " " + latency + " " + server);
      prevVal = latency;
    }

    selectedServer = selector.select(_servers);
    assertEquals(selectedServer, "server1");

    selectedServer = selector.select(Arrays.asList("server3", "server2"));
    assertEquals(selectedServer, "server2");

    selectedServer = selector.select(Arrays.asList("server3", "server4", "server2"));
    assertEquals(selectedServer, "server2");

    candidateServers = new ArrayList<>(Arrays.asList("server1", "server2"));
    serverRankingWithVal = selector.fetchServerRankingsWithScores(candidateServers);
    assertEquals(serverRankingWithVal.size(), 2);
    prevVal = 0.0;
    for (Pair<String, Double> entry : serverRankingWithVal) {
      String server = entry.getLeft();
      double latency = entry.getRight();
      assertTrue(prevVal <= latency, prevVal + " " + latency + " " + server);
      prevVal = latency;
    }

    // Test 4: Simulate server selection code. Pick the best server using HybridSelector during every iteration.
    // Every iteration updates latency and numInFlightRequests for a server. Verify if HybridSelector picks the best
    // server in every iteration.
    Random rand = new Random();
    for (int ii = 0; ii < 1000; ii++) {
      serverRankingWithVal = selector.fetchAllServerRankingsWithScores();

      // Assert if the ranking is accurate.
      prevVal = 0.0;
      for (int jj = 0; jj < serverRankingWithVal.size(); jj++) {
        String server = serverRankingWithVal.get(jj).getLeft();
        double numReq = serverRankingWithVal.get(jj).getRight();
        assertTrue(prevVal <= numReq, prevVal + " " + numReq + " " + server);
        prevVal = numReq;
      }

      // Route the request to the best server.
      selectedServer = serverRankingWithVal.get(0).getLeft();
      serverRoutingStatsManager.recordStatsAfterQuerySubmission(-1, selectedServer);
      waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);

      if (rand.nextBoolean()) {
        serverRoutingStatsManager.recordStatsUponResponseArrival(-1, selectedServer, 1);
        waitForStatsUpdate(serverRoutingStatsManager, ++taskCount);
      }
    }
  }

  private void waitForStatsUpdate(ServerRoutingStatsManager serverRoutingStatsManager, long taskCount) {
    TestUtils.waitForCondition(aVoid -> {
      return (serverRoutingStatsManager.getCompletedTaskCount() == taskCount);
    }, 5L, 5000, "Failed to record stats for AdaptiveServerSelectorTest");
  }
}
