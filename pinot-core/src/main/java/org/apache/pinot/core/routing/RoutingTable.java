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
package org.apache.pinot.core.routing;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.transport.ServerInstance;


public class RoutingTable {
  // Optional segments are those not online as in ExternalView but might have been online on servers already, e.g.
  // the newly created consuming segments. Such segments were simply skipped by brokers at query routing time, but that
  // had caused wrong query results, particularly for upsert tables. Instead, we should pass such segments to servers
  // and let them decide how to handle them, e.g. skip them upon issues or include them for better query results.
  private final Map<ServerInstance, Pair<List<String>/*non-optional segments*/, List<String>/*optional segments*/>>
      _serverInstanceToSegmentsMap;
  private final List<String> _unavailableSegments;
  private final int _numPrunedSegments;

  public RoutingTable(Map<ServerInstance, List<String>> serverInstanceToSegmentsMap, List<String> unavailableSegments,
      int numPrunedSegments) {
    this(serverInstanceToSegmentsMap, Collections.emptyMap(), unavailableSegments, numPrunedSegments);
  }

  public RoutingTable(Map<ServerInstance, List<String>> serverInstanceToSegmentsMap,
      Map<ServerInstance, List<String>> serverInstanceToOptionalSegmentsMap, List<String> unavailableSegments,
      int numPrunedSegments) {
    _serverInstanceToSegmentsMap = new HashMap<>();
    // Loop over the serverInstanceToSegmentsMap, so that servers that only have optional segments are skipped.
    // This makes the support of optional segments backward compatible easily, because just as before servers always
    // get some non-optional segments to process the query.
    // TODO: support when servers only have some optional segments to process.
    serverInstanceToSegmentsMap.forEach((k, v) -> {
      List<String> optionalSegments = serverInstanceToOptionalSegmentsMap.get(k);
      _serverInstanceToSegmentsMap.put(k, Pair.of(v, optionalSegments));
    });
    _unavailableSegments = unavailableSegments;
    _numPrunedSegments = numPrunedSegments;
  }

  public Map<ServerInstance, Pair<List<String>, List<String>>> getServerInstanceToSegmentsMap() {
    return _serverInstanceToSegmentsMap;
  }

  public List<String> getUnavailableSegments() {
    return _unavailableSegments;
  }

  public int getNumPrunedSegments() {
    return _numPrunedSegments;
  }
}
