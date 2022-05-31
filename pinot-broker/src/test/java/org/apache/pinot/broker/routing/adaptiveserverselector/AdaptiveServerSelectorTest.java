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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.pinot.common.utils.ExponentialMovingAverage;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class AdaptiveServerSelectorTest {

  ServerRoutingStatsManager _serverRoutingStatsManager = new ServerRoutingStatsManager();
  List<String> _servers =
      Arrays.asList("server1", "server2", "server3", "server4", "server5", "server6", "server7", "server8", "server9",
          "server10");

  @Test
  public void testAdaptiveServerSelectorFactory() {
    PinotConfiguration pinotConfig = mock(PinotConfiguration.class);

    when(pinotConfig.getProperty(Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
        Broker.AdaptiveServerSelector.DEFAULT_TYPE)).thenReturn(Broker.AdaptiveServerSelector.Type.NO_OP.name());
    assertNull(AdaptiveServerSelectorFactory.getAdaptiveServerSelector(_serverRoutingStatsManager, pinotConfig));

    when(pinotConfig.getProperty(Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
        Broker.AdaptiveServerSelector.DEFAULT_TYPE)).thenReturn(
        Broker.AdaptiveServerSelector.Type.NUM_INFLIGHT_REQ.name());
    assertTrue(AdaptiveServerSelectorFactory.getAdaptiveServerSelector(_serverRoutingStatsManager,
        pinotConfig) instanceof NumInFlightReqSelector);

    when(pinotConfig.getProperty(Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
        Broker.AdaptiveServerSelector.DEFAULT_TYPE)).thenReturn(Broker.AdaptiveServerSelector.Type.LATENCY.name());
    assertTrue(AdaptiveServerSelectorFactory.getAdaptiveServerSelector(_serverRoutingStatsManager,
        pinotConfig) instanceof MinLatencySelector);

    when(pinotConfig.getProperty(Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
        Broker.AdaptiveServerSelector.DEFAULT_TYPE)).thenReturn("DummyName");
    assertThrows(IllegalArgumentException.class,
        () -> AdaptiveServerSelectorFactory.getAdaptiveServerSelector(_serverRoutingStatsManager, pinotConfig));
  }

  private List<String> getCandidateServers(List<String> servers, int numCandidates) {
    List<String> candidateServers = new ArrayList<>();
    Set<String> usedCandidates = new HashSet<>();
    Random rand = new Random();

    while (usedCandidates.size() < numCandidates) {
      int randIdx = rand.nextInt(servers.size());
      String server = servers.get(randIdx);
      if (usedCandidates.contains(server)) {
        continue;
      }

      usedCandidates.add(server);
      candidateServers.add(server);
    }

    return candidateServers;
  }

  @Test
  public void testNumInFlightReqSelector()
      throws InterruptedException {
    // Test 1
    if (_serverRoutingStatsManager.isEnabled()) {
      _serverRoutingStatsManager.shutDown();
    }
    _serverRoutingStatsManager.init();
    NumInFlightReqSelector selector = new NumInFlightReqSelector(_serverRoutingStatsManager);

    Random rand = new Random();
    for (int ii = 0; ii < 1000; ii++) {
      List<String> candidateServers = getCandidateServers(_servers, rand.nextInt(_servers.size()));
      if (candidateServers.size() == 0) {
        continue;
      }
      String selectedServer = selector.select(candidateServers);
      assertTrue(candidateServers.contains(selectedServer), String.join(",", candidateServers) + " " + selectedServer);

      _serverRoutingStatsManager.recordStatsForQuerySubmit(selectedServer);

      if (rand.nextBoolean()) {
        int latency = rand.nextInt(1000);
        _serverRoutingStatsManager.recordStatsForQueryCompletion(selectedServer, latency);
      }
    }

    // Test 2
    if (_serverRoutingStatsManager.isEnabled()) {
      _serverRoutingStatsManager.shutDown();
    }
    _serverRoutingStatsManager.init();
    selector = new NumInFlightReqSelector(_serverRoutingStatsManager);
    Map<String, Integer> numInflightReqMap = new HashMap<>();

    for (int ii = 0; ii < 1000; ii++) {
      List<String> serverRanking = selector.fetchServerRanking();
      String selectedServer;
      if (serverRanking.size() == 0) {
        selectedServer = _servers.get(rand.nextInt(_servers.size()));
      } else {
        selectedServer = serverRanking.get(0);

        int prevVal = 0;
        for (String server : serverRanking) {
          int newVal = numInflightReqMap.get(server);
          assertTrue(newVal >= prevVal, newVal + " " + prevVal + " " + server);
          prevVal = newVal;
        }
      }

      _serverRoutingStatsManager.recordStatsForQuerySubmit(selectedServer);
      Thread.sleep(2);

      int numReq = numInflightReqMap.containsKey(selectedServer) ? numInflightReqMap.get(selectedServer) + 1 : 1;
      numInflightReqMap.put(selectedServer, numReq);

      if (rand.nextBoolean()) {
        int latency = rand.nextInt(1000);
        _serverRoutingStatsManager.recordStatsForQueryCompletion(selectedServer, latency);
        Thread.sleep(2);

        numReq = numInflightReqMap.get(selectedServer) - 1;
        numInflightReqMap.put(selectedServer, numReq);
      }
    }
  }

  // TODO(Vivek): Add multi-threaded test.
  @Test
  public void testMinLatencySelector()
      throws InterruptedException {
    // Test 1
    if (_serverRoutingStatsManager.isEnabled()) {
      _serverRoutingStatsManager.shutDown();
    }
    _serverRoutingStatsManager.init();

    MinLatencySelector selector = new MinLatencySelector(_serverRoutingStatsManager);

    Random rand = new Random();
    for (int ii = 0; ii < 1000; ii++) {
      List<String> candidateServers = getCandidateServers(_servers, rand.nextInt(_servers.size()));
      if (candidateServers.size() == 0) {
        continue;
      }
      String selectedServer = selector.select(candidateServers);
      assertTrue(candidateServers.contains(selectedServer), String.join(",", candidateServers) + " " + selectedServer);

      _serverRoutingStatsManager.recordStatsForQuerySubmit(selectedServer);

      int latency = rand.nextInt(1000);
      _serverRoutingStatsManager.recordStatsForQueryCompletion(selectedServer, latency);
    }

    // Test 2
    if (_serverRoutingStatsManager.isEnabled()) {
      _serverRoutingStatsManager.shutDown();
    }
    _serverRoutingStatsManager.init();
    selector = new MinLatencySelector(_serverRoutingStatsManager);
    Map<String, ExponentialMovingAverage> latencyMap = new HashMap<>();

    for (int ii = 0; ii < 1000; ii++) {
      List<String> serverRanking = selector.fetchServerRanking();
      String selectedServer;
      if (serverRanking.size() == 0) {
        selectedServer = _servers.get(rand.nextInt(_servers.size()));
      } else {
        selectedServer = serverRanking.get(0);

        double prevVal = 0.0;
        for (String server : serverRanking) {
          double newVal = latencyMap.get(server).getLatency();
          assertTrue(newVal >= prevVal, newVal + " " + prevVal + " " + server);
          prevVal = newVal;
        }
      }

      _serverRoutingStatsManager.recordStatsForQuerySubmit(selectedServer);
      int latency = rand.nextInt(1000);
      _serverRoutingStatsManager.recordStatsForQueryCompletion(selectedServer, latency);
      Thread.sleep(2);

      latencyMap.computeIfAbsent(selectedServer, k -> new ExponentialMovingAverage(0.01));
      latencyMap.get(selectedServer).compute(latency);
    }
  }
}
