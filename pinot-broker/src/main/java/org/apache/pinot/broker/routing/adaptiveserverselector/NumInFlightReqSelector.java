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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsEntry;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;


public class NumInFlightReqSelector implements AdaptiveServerSelector {
  private final ServerRoutingStatsManager _serverRoutingStatsManager;

  public NumInFlightReqSelector(ServerRoutingStatsManager serverRoutingStatsManager) {
    _serverRoutingStatsManager = serverRoutingStatsManager;
  }

  @Override
  public String select(List<String> serverCandidates) {
    String selectedServer = null;
    int minValue = Integer.MAX_VALUE;

    for (String server : serverCandidates) {
      ServerRoutingStatsEntry serverStats = _serverRoutingStatsManager.getServerStats(server);

      // No stats for this server. That means this server hasn't received any queries yet.
      if (serverStats == null) {
        selectedServer = server;
        break;
      }

      int numInFlightRequests = serverStats.getNumInFlightRequests();
      if (numInFlightRequests < minValue) {
        minValue = numInFlightRequests;
        selectedServer = server;
      }
    }

    return selectedServer;
  }

  @Override
  public List<String> fetchServerRanking() {
    List<Pair<String, Integer>> pairList = new ArrayList<>();
    Iterator<Map.Entry<String, ServerRoutingStatsEntry>> serverItr =
        _serverRoutingStatsManager.getServerRoutingStatsItr();

    while (serverItr.hasNext()) {
      Map.Entry<String, ServerRoutingStatsEntry> entry = serverItr.next();
      String server = entry.getKey();
      ServerRoutingStatsEntry serverStats = entry.getValue();
      Preconditions.checkState(serverStats != null, "ServerStats is null for server=" + server);

      int numInFlightRequests = serverStats.getNumInFlightRequests();
      pairList.add(Pair.of(server, numInFlightRequests));
    }

    Collections.sort(pairList, (o1, o2) -> o1.getRight() - o2.getRight());

    List<String> serverRankList = new ArrayList<>();
    for (Pair<String, Integer> entry : pairList) {
      serverRankList.add(entry.getLeft());
    }
    return serverRankList;
  }
}
