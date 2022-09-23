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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;


/**
 * The {@code NumInFlightReqSelector} is an AdaptiveServerSelector implementation that picks the best server based on
 * the number of in-flight queries being processed by the server. The server with the lowest number of in-flight
 * queries is picked.
 */
public class NumInFlightReqSelector implements AdaptiveServerSelector {
  private final ServerRoutingStatsManager _serverRoutingStatsManager;
  private final Random _random;

  public NumInFlightReqSelector(ServerRoutingStatsManager serverRoutingStatsManager) {
    _serverRoutingStatsManager = serverRoutingStatsManager;
    _random = new Random();
  }

  @Override
  public String select(List<String> serverCandidates) {
    String selectedServer = null;
    int minNumInFlightRequests = Integer.MAX_VALUE;

    // TODO: If two or more servers have same number of in flight requests, break the tie intelligently.
    for (String server : serverCandidates) {
      Integer numInFlightRequests = _serverRoutingStatsManager.fetchNumInFlightRequestsForServer(server);

      // No stats for this server. That means this server hasn't received any queries yet.
      if (numInFlightRequests == null) {
        int randIdx = _random.nextInt(serverCandidates.size());
        selectedServer = serverCandidates.get(randIdx);
        break;
      }

      if (numInFlightRequests < minNumInFlightRequests) {
        minNumInFlightRequests = numInFlightRequests;
        selectedServer = server;
      }
    }

    return selectedServer;
  }

  @Override
  public List<Pair<String, Double>> fetchAllServerRankingsWithScores() {
    List<Pair<String, Integer>> tempPairList = _serverRoutingStatsManager.fetchNumInFlightRequestsForAllServers();

    List<Pair<String, Double>> pairList = new ArrayList<>();
    for (Pair<String, Integer> p : tempPairList) {
      Pair<String, Double> pair = new ImmutablePair<>(p.getLeft(), new Double(p.getRight()));
      pairList.add(pair);
    }

    // Let's shuffle the list before sorting. This helps with randomly choosing different servers if there is a tie.
    Collections.shuffle(pairList);
    Collections.sort(pairList, (o1, o2) -> {
      int val = Double.compare(o1.getRight(), o2.getRight());
      return val;
    });

    return pairList;
  }
}
