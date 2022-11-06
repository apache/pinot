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

import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;


/**
 * The {@code HybridSelector} is an AdaptiveServerSelector implementation that picks the best server based on the
 * following parameters:
 * 1. Num of in-flight requests (A)
 * 2. EMA of in-flight requests (B)
 * 3. EMA of latencies (C)
 * This selector implementation is based on the ideas suggested in the paper -
 * https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf
 *
 * The Hybrid score for each server is calculated as follows. The server with the lowest Hybrid score is picked.
 *       HybridScore = Math.pow(A+B, N) * C
 * N -> Configurable exponent with default value of 3.
 */
public class HybridSelector implements AdaptiveServerSelector {
  private final ServerRoutingStatsManager _serverRoutingStatsManager;
  private final Random _random;

  public HybridSelector(ServerRoutingStatsManager serverRoutingStatsManager) {
    _serverRoutingStatsManager = serverRoutingStatsManager;
    _random = new Random();
  }

  @Override
  public String select(List<String> serverCandidates) {
    String selectedServer = null;
    Double minScore = Double.MAX_VALUE;

    // TODO: If two or more servers have the same score, break the tie intelligently.
    for (String server : serverCandidates) {
      Double score = _serverRoutingStatsManager.fetchHybridScoreForServer(server);

      // No stats for this server. That means this server hasn't received any queries yet.
      if (score == null) {
        int randIdx = _random.nextInt(serverCandidates.size());
        selectedServer = serverCandidates.get(randIdx);
        break;
      }

      if (score < minScore) {
        minScore = score;
        selectedServer = server;
      }
    }

    return selectedServer;
  }

  @Override
  public List<Pair<String, Double>> fetchAllServerRankingsWithScores() {
    List<Pair<String, Double>> pairList = _serverRoutingStatsManager.fetchHybridScoreForAllServers();

    // Let's shuffle the list before sorting. This helps with randomly choosing different servers if there is a tie.
    Collections.shuffle(pairList);
    Collections.sort(pairList, (o1, o2) -> {
      int val = Double.compare(o1.getRight(), o2.getRight());
      return val;
    });

    return pairList;
  }
}
