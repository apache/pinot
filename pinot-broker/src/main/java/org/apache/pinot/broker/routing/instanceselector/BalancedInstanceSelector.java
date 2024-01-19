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

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.HashUtil;

/**
 * Instance selector to balance the number of segments served by each selected server instance.
 * <p>If AdaptiveServerSelection is enabled, the request is routed to the best available server for a segment
 * when it is processed below. This is a best effort approach in distributing the query to all available servers.
 * If some servers are performing poorly, they might not end up being picked for any of the segments. For example,
 * there's a query for Segments 1 (Seg1), 2 (Seg2) and Seg3). The servers are S1, S2, S3. The algorithm works as
 * follows:
 *    Step1: Process seg1. Fetch server rankings. Pick the best server.
 *    Step2: Process seg2. Fetch server rankings (could have changed or not since Step 1). Pick the best server.
 *    Step3: Process seg3. Fetch server rankings (could have changed or not since Step 2). Pick the best server.
 *
 * <p>If AdaptiveServerSelection is disabled, the selection algorithm will always evenly distribute the traffic to all
 * replicas of each segment, and will try to select different replica id for each segment. The algorithm is very
 * light-weight and will do best effort to balance the number of segments served by each selected server instance.
 */
public class BalancedInstanceSelector extends BaseInstanceSelector {

  public BalancedInstanceSelector(String tableNameWithType, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics, @Nullable AdaptiveServerSelector adaptiveServerSelector, Clock clock,
      boolean useStickyRouting) {
    super(tableNameWithType, propertyStore, brokerMetrics, adaptiveServerSelector, clock, useStickyRouting);
  }

  @Override
  Pair<Map<String, String>, Map<String, String>> select(List<String> segments, int requestId,
      SegmentStates segmentStates, Map<String, String> queryOptions) {
    Map<String, String> segmentToSelectedInstanceMap = new HashMap<>(HashUtil.getHashMapCapacity(segments.size()));
    // No need to adjust this map per total segment numbers, as optional segments should be empty most of the time.
    Map<String, String> optionalSegmentToInstanceMap = new HashMap<>();
    if (_adaptiveServerSelector != null) {
      for (String segment : segments) {
        List<SegmentInstanceCandidate> candidates = segmentStates.getCandidates(segment);
        // NOTE: candidates can be null when there is no enabled instances for the segment, or the instance selector has
        // not been updated (we update all components for routing in sequence)
        if (candidates == null) {
          continue;
        }
        List<String> candidateInstances = new ArrayList<>(candidates.size());
        for (SegmentInstanceCandidate candidate : candidates) {
          candidateInstances.add(candidate.getInstance());
        }
        String selectedInstance = _adaptiveServerSelector.select(candidateInstances);
        // This can only be offline when it is a new segment. And such segment is marked as optional segment so that
        // broker or server can skip it upon any issue to process it.
        if (candidates.get(candidateInstances.indexOf(selectedInstance)).isOnline()) {
          segmentToSelectedInstanceMap.put(segment, selectedInstance);
        } else {
          optionalSegmentToInstanceMap.put(segment, selectedInstance);
        }
      }
    } else {
      for (String segment : segments) {
        List<SegmentInstanceCandidate> candidates = segmentStates.getCandidates(segment);
        // NOTE: candidates can be null when there is no enabled instances for the segment, or the instance selector has
        // not been updated (we update all components for routing in sequence)
        if (candidates == null) {
          continue;
        }
        int selectedIdx;
        if (isUseConsistentRouting(queryOptions)) {
          // candidates array is always sorted
          selectedIdx = Math.abs(_tableNameHashForConsistentRouting % candidates.size());
        } else {
          selectedIdx = requestId++ % candidates.size();
        }
        SegmentInstanceCandidate selectedCandidate = candidates.get(selectedIdx);
        // This can only be offline when it is a new segment. And such segment is marked as optional segment so that
        // broker or server can skip it upon any issue to process it.
        if (selectedCandidate.isOnline()) {
          segmentToSelectedInstanceMap.put(segment, selectedCandidate.getInstance());
        } else {
          optionalSegmentToInstanceMap.put(segment, selectedCandidate.getInstance());
        }
      }
    }
    return Pair.of(segmentToSelectedInstanceMap, optionalSegmentToInstanceMap);
  }
}
