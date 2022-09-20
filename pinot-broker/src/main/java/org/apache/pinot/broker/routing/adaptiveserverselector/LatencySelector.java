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
 * The {@code LatencySelector} is an AdaptiveServerSelector implementation that picks the best server based on
 * the exponential weighted moving average (EMA) of query latencies seen by the server. The EMA latencies are
 * updated when the broker receives the query responses from the respective servers. It is a reactive algorithm and
 * will be useful only for a selective usecases. Please perform usecase testing before enabling this method of
 * server selection.
 */
public class LatencySelector implements AdaptiveServerSelector {
  private final ServerRoutingStatsManager _serverRoutingStatsManager;
  private final Random _random;

  public LatencySelector(ServerRoutingStatsManager serverRoutingStatsManager) {
    _serverRoutingStatsManager = serverRoutingStatsManager;
    _random = new Random();
  }

  @Override
  public String select(List<String> serverCandidates) {
    String selectedServer = null;
    Double minLatency = Double.MAX_VALUE;

    // TODO: If two or more servers have same latency, break the tie intelligently.
    for (String server : serverCandidates) {
      Double latency = _serverRoutingStatsManager.fetchEMALatencyForServer(server);

      // No stats for this server. That means this server hasn't received any queries yet.
      if (latency == null) {
        int randIdx = _random.nextInt(serverCandidates.size());
        selectedServer = serverCandidates.get(randIdx);
        break;
      }

      if (latency < minLatency) {
        minLatency = latency;
        selectedServer = server;
      }
    }

    return selectedServer;
  }

  @Override
  public List<Pair<String, Double>> fetchAllServerRankingsWithScores() {
    List<Pair<String, Double>> pairList = _serverRoutingStatsManager.fetchEMALatencyForAllServers();

    // Let's shuffle the list before sorting. This helps with randomly choosing different servers if there is a tie.
    Collections.shuffle(pairList);
    Collections.sort(pairList, (o1, o2) -> {
      int val = Double.compare(o1.getRight(), o2.getRight());
      return val;
    });

    return pairList;
  }
}
