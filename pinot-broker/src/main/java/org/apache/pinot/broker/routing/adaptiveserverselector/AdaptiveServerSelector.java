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

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;


/**
 * The {@code AdaptiveServerSelector} intelligently selects the best available server for a segment during query
 * processing. The decision is made based on stats recorded for each server during query processing.
 */
public interface AdaptiveServerSelector {

  /**
   * Picks the best server to route a query from the list of candidate servers.
   *
   * @param serverCandidates Candidate servers from which the best server should be chosen.
   * @return server identifier
   */
  String select(List<String> serverCandidates);

  /**
   * Returns the ranking of servers ordered from best to worst along with the absolute scores based on which the
   * servers are ranked. Based on the implementation of the interface, the score could refer to different things. For
   * NumInFlightReqSelector, score is the number of inflight requests. For LatencySelector, score is the EMA latency.
   * For HybridSelector, score is the hybridScore which is computed by combining latency and # inflight requests.
   *
   * @return List of servers along with their values ranked from best to worst.
   */
  List<Pair<String, Double>> fetchAllServerRankingsWithScores();
}
