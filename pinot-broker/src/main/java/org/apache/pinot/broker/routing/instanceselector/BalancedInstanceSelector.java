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
package org.apache.pinot.broker.routing.instanceselector;

import java.util.Collection;
import java.util.Map;
import org.apache.pinot.broker.routing.adaptiveserverselector.PriorityPoolInstanceSelector;
import org.apache.pinot.broker.routing.adaptiveserverselector.ServerSelectionContext;


/// Instance selector to balance the number of segments served by each selected server instance.
///
/// If AdaptiveServerSelection is enabled, the request is routed to the best available server for a segment
/// when it is processed below. This is a best effort approach in distributing the query to all available servers.
/// If some servers are performing poorly, they might not end up being picked for any of the segments. For example,
/// there's a query for Segments 1 (Seg1), 2 (Seg2) and 3 (Seg3). The servers are S1, S2, S3. The algorithm works as
/// follows:
/// - Step1: Process seg1. Fetch server rankings. Pick the best server.
/// - Step2: Process seg2. Fetch server rankings (could have changed or not since Step 1). Pick the best server.
/// - Step3: Process seg3. Fetch server rankings (could have changed or not since Step 2). Pick the best server.
///
/// If AdaptiveServerSelection is disabled, the selection algorithm will always evenly distribute the traffic to all
/// replicas of each segment, and will try to select different replica id for each segment. The algorithm is very
/// light-weight and will do best effort to balance the number of segments served by each selected server instance.
public class BalancedInstanceSelector extends BaseInstanceSelector {

  @Override
  public SelectionResult select(Collection<String> segments, int requestId, SegmentStates segmentStates,
      Map<String, String> queryOptions) {
    ServerSelectionContext ctx = new ServerSelectionContext(queryOptions, _config);
    SelectionCollector collector;
    if (_priorityPoolInstanceSelector != null) {
      collector = selectAdaptive(segments, segmentStates, ctx);
    } else if (ctx.isUseFixedReplica()) {
      collector = selectSameCandidateIndex(segments, segmentStates, _tableNameHashForFixedReplicaRouting);
    } else {
      collector = selectRoundRobin(segments, requestId, segmentStates);
    }
    collector.emitPoolSegQueries(_brokerMetrics, queryOptions);
    return collector.build();
  }

  /// Selects the best server for each segment, from the server stats at the time the segment is processed.
  private SelectionCollector selectAdaptive(Collection<String> segments, SegmentStates segmentStates,
      ServerSelectionContext ctx) {
    PriorityPoolInstanceSelector.QuerySelector querySelector = _priorityPoolInstanceSelector.forQuery(ctx);
    // Not spread: most segments of a group are selected on the same candidate, the best one when they are processed
    SelectionCollector collector = new SelectionCollector(segmentStates, segments.size(), false);
    for (String segment : segments) {
      GroupRouting routing = collector.resolveRouting(segment);
      if (routing != null) {
        collector.add(routing, querySelector.select(routing.getCandidates()), segment);
      }
    }
    return collector;
  }

  /// Selects, for the i-th routable segment, the candidate at index `(requestId + i) % numCandidates`, so that
  /// consecutive segments are selected on different candidates.
  private SelectionCollector selectRoundRobin(Collection<String> segments, int requestId, SegmentStates segmentStates) {
    SelectionCollector collector = new SelectionCollector(segmentStates, segments.size(), true);
    int numCandidates = 0;
    int candidateIndex = 0;
    for (String segment : segments) {
      GroupRouting routing = collector.resolveRouting(segment);
      if (routing != null) {
        if (routing.getNumCandidates() != numCandidates) {
          numCandidates = routing.getNumCandidates();
          candidateIndex = requestId % numCandidates;
        }
        collector.add(routing, candidateIndex, segment);
        requestId++;
        if (++candidateIndex == numCandidates) {
          candidateIndex = 0;
        }
      }
    }
    return collector;
  }
}
