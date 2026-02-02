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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.routing.adaptiveserverselector.ServerSelectionContext;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Instance selector for replica-group routing strategy.
 * <p>The selection algorithm will always evenly distribute the traffic to all replicas of each segment, and will select
 * the same index of the enabled instances for all segments with the same number of replicas. The algorithm is very
 * light-weight and will do best effort to select the least servers for the request.
 * <p>The algorithm relies on the mirror segment assignment from replica-group segment assignment strategy. With mirror
 * segment assignment, any server in one replica-group will always have a corresponding server in other replica-groups
 * that have the same segments assigned. For an example, if S1 is a server in replica-group 1, and it has mirror server
 * S2 in replica-group 2 and S3 in replica-group 3. All segments assigned to S1 will also be assigned to S2 and S3. In
 * stable scenario (external view matches ideal state), all segments assigned to S1 will have the same enabled instances
 * of [S1, S2, S3] sorted (in alphabetical order). If we pick the same index of enabled instances for all segments for a
 * request, only one of S1, S2, S3 will be picked, so it is guaranteed that we pick the least server instances for the
 * request (there is no guarantee on choosing servers from the same replica-group though). In transitioning/error
 * scenario (external view does not match ideal state), there is no guarantee on picking the least server instances, but
 * the traffic is guaranteed to be evenly distributed to all available instances to avoid overwhelming hotspot servers.
 *<p> If the query option NUM_REPLICA_GROUPS_TO_QUERY is provided, the servers to be picked will be from different
 * replica groups such that segments are evenly distributed amongst the provided value of NUM_REPLICA_GROUPS_TO_QUERY.
 * Thus in case of [S1, S2, S3] if NUM_REPLICA_GROUPS_TO_QUERY = 2, the ReplicaGroup S1 and ReplicaGroup S2 will be
 * selected such that half the segments will come from S1 and other half from S2. If NUM_REPLICA_GROUPS_TO_QUERY value
 * is much greater than available servers, then ReplicaGroupInstanceSelector will behave similar to
 * BalancedInstanceSelector.
 * <p>If AdaptiveServerSelection is enabled, a single snapshot of the server ranking is fetched. This ranking is
 * referenced to pick the best available server for each segment. The algorithm ends up picking the minimum number of
 * servers required to process a query because it references a single snapshot of the server rankings. Currently,
 * NUM_REPLICA_GROUPS_TO_QUERY is not supported if AdaptiveServerSelection is enabled.
 */
public class ReplicaGroupInstanceSelector extends BaseInstanceSelector {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaGroupInstanceSelector.class);

  @Override
  public SelectionResult select(List<String> segments, int requestId,
      SegmentStates segmentStates, Map<String, String> queryOptions) {
    ServerSelectionContext ctx = new ServerSelectionContext(queryOptions, _config);
    if (_adaptiveServerSelector != null) {
      // Adaptive Server Selection is enabled.
      List<SegmentInstanceCandidate> candidateServers = fetchCandidateServersForQuery(segments, segmentStates);

      // Fetch serverRankList before looping through all the segments. This is important to make sure that we pick
      // the least amount of instances for a query by referring to a single snapshot of the rankings.
      List<String> serverRankList = _priorityPoolInstanceSelector.rank(ctx, candidateServers);
      Map<String, Integer> serverRankMap = new HashMap<>();
      for (int idx = 0; idx < serverRankList.size(); idx++) {
        serverRankMap.put(serverRankList.get(idx), idx);
      }
      Pair<Map<String, String>, Map<String, String>> selectedServers =
          selectServers(segments, requestId, segmentStates, serverRankMap, ctx);
      return new SelectionResult(selectedServers, List.of(), 0);
    } else {
      // Adaptive Server Selection is NOT enabled.
      Pair<Map<String, String>, Map<String, String>> selectedServers =
          selectServers(segments, requestId, segmentStates, null, ctx);
      return new SelectionResult(selectedServers, List.of(), 0);
    }
  }

  private Pair<Map<String, String>, Map<String, String>> selectServers(List<String> segments, int requestId,
      SegmentStates segmentStates, @Nullable Map<String, Integer> serverRankMap, ServerSelectionContext ctx) {

    Map<String, String> segmentToSelectedInstanceMap = new HashMap<>(HashUtil.getHashMapCapacity(segments.size()));
    // No need to adjust this map per total segment numbers, as optional segments should be empty most of the time.
    Map<String, String> optionalSegmentToInstanceMap = new HashMap<>();
    Map<Integer, Integer> poolToSegmentCount = new HashMap<>();
    boolean useFixedReplica = ctx.isUseFixedReplica();
    Integer numReplicaGroupsToQuery = QueryOptionsUtils.getNumReplicaGroupsToQuery(ctx.getQueryOptions());
    int numReplicaGroups = numReplicaGroupsToQuery != null ? numReplicaGroupsToQuery : 1;
    int replicaOffset = 0;
    for (String segment : segments) {
      List<SegmentInstanceCandidate> candidates = segmentStates.getCandidates(segment);
      // NOTE: candidates can be null when there are no enabled instances for the segment, or the instance selector has
      // not been updated (we update all components for routing in sequence)
      if (candidates == null) {
        continue;
      }

      // Round-robin selection (default behavior)
      int numCandidates = candidates.size();
      int instanceIdx = (requestId + replicaOffset) % numCandidates;
      SegmentInstanceCandidate selectedInstance = candidates.get(instanceIdx);
      if (useFixedReplica) {
        // Adaptive Server Selection cannot be used with fixed replica routing.
        // The candidates array is always sorted
        selectedInstance = candidates.get((_tableNameHashForFixedReplicaRouting + replicaOffset) % numCandidates);
      } else if (MapUtils.isNotEmpty(serverRankMap)) {
        // Adaptive Server Selection is enabled.
        // Use the instance with the best rank if all servers have stats populated, else use the round-robin selected
        // instance
        selectedInstance = candidates.stream()
            .anyMatch(candidate -> !serverRankMap.containsKey(candidate.getInstance()))
            ? selectedInstance
            : candidates.stream()
                .min(Comparator.comparingInt(candidate -> serverRankMap.get(candidate.getInstance())))
                .orElse(selectedInstance);
      }

      poolToSegmentCount.merge(selectedInstance.getPool(), 1, Integer::sum);
      // This can only be offline when it is a new segment. And such segment is marked as optional segment so that
      // broker or server can skip it upon any issue to process it.
      if (selectedInstance.isOnline()) {
        segmentToSelectedInstanceMap.put(segment, selectedInstance.getInstance());
      } else {
        optionalSegmentToInstanceMap.put(segment, selectedInstance.getInstance());
      }
      if (numReplicaGroups > numCandidates) {
        numReplicaGroups = numCandidates;
      }
      replicaOffset = (replicaOffset + 1) % numReplicaGroups;
    }
    for (Map.Entry<Integer, Integer> entry : poolToSegmentCount.entrySet()) {
      _brokerMetrics.addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, entry.getValue(),
          BrokerMetrics.getTagForPreferredPool(ctx.getQueryOptions()), String.valueOf(entry.getKey()));
    }
    return Pair.of(segmentToSelectedInstanceMap, optionalSegmentToInstanceMap);
  }

  private List<SegmentInstanceCandidate> fetchCandidateServersForQuery(List<String> segments,
      SegmentStates segmentStates) {
    Map<String, SegmentInstanceCandidate> candidateServers = new HashMap<>();
    for (String segment : segments) {
      List<SegmentInstanceCandidate> candidates = segmentStates.getCandidates(segment);
      if (candidates == null) {
        continue;
      }
      for (SegmentInstanceCandidate candidate : candidates) {
        candidateServers.put(candidate.getInstance(), candidate);
      }
    }
    return new ArrayList<>(candidateServers.values());
  }

  @Override
  void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, Long> newSegmentCreationTimeMap) {
    if (_tableConfig.isUpsertEnabled() || _tableConfig.isDedupEnabled()) {
      updateSegmentMapsForUpsertTable(idealState, externalView, onlineSegments, newSegmentCreationTimeMap);
    } else {
      super.updateSegmentMaps(idealState, externalView, onlineSegments, newSegmentCreationTimeMap);
    }
  }

  /**
   *
   * <pre>
   * Instances unavailable for any old segment should not exist in _oldSegmentCandidatesMap or _newSegmentStateMap for
   * segments with the same instances in ideal state.
   *
   * The maps are calculated in the following steps to meet the strict replica-group guarantee:
   *   1. Compute the online instances for both old and new segments
   *   2. Compare online instances for old segments with instances in ideal state and gather the unavailable instances
   *   for each set of instances
   *   3. Exclude the unavailable instances from the online instances map for both old and new segment map
   * </pre>
   */
  void updateSegmentMapsForUpsertTable(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, Long> newSegmentCreationTimeMap) {
    _oldSegmentCandidatesMap.clear();
    int newSegmentMapCapacity = HashUtil.getHashMapCapacity(newSegmentCreationTimeMap.size());
    _newSegmentStateMap = new HashMap<>(newSegmentMapCapacity);

    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();

    // Get the online instances for the segments
    Map<String, Set<String>> oldSegmentToOnlineInstancesMap =
        new HashMap<>(HashUtil.getHashMapCapacity(onlineSegments.size()));
    Map<String, Set<String>> newSegmentToOnlineInstancesMap = new HashMap<>(newSegmentMapCapacity);
    for (String segment : onlineSegments) {
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);
      assert idealStateInstanceStateMap != null;
      Map<String, String> externalViewInstanceStateMap = externalViewAssignment.get(segment);
      Set<String> onlineInstances;
      if (externalViewInstanceStateMap == null) {
        onlineInstances = Collections.emptySet();
      } else {
        onlineInstances = getOnlineInstances(idealStateInstanceStateMap, externalViewInstanceStateMap);
      }
      if (newSegmentCreationTimeMap.containsKey(segment)) {
        newSegmentToOnlineInstancesMap.put(segment, onlineInstances);
      } else {
        oldSegmentToOnlineInstancesMap.put(segment, onlineInstances);
      }
    }

    // Calculate the unavailable instances based on the old segments' online instances for each combination of instances
    // in the ideal state
    Map<Set<String>, Set<String>> unavailableInstancesMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : oldSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      Set<String> onlineInstances = entry.getValue();
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);
      Set<String> instancesInIdealState = idealStateInstanceStateMap.keySet();
      Set<String> unavailableInstances =
          unavailableInstancesMap.computeIfAbsent(instancesInIdealState, k -> new HashSet<>());
      for (String instance : instancesInIdealState) {
        if (!onlineInstances.contains(instance)) {
          if (unavailableInstances.add(instance)) {
            LOGGER.warn(
                "Found unavailable instance: {} in instance group: {} for segment: {}, table: {} (IS: {}, EV: {})",
                instance, instancesInIdealState, segment, _tableNameWithType, idealStateInstanceStateMap,
                externalViewAssignment.get(segment));
          }
        }
      }
    }

    // Iterate over the maps and exclude the unavailable instances
    for (Map.Entry<String, Set<String>> entry : oldSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      // NOTE: onlineInstances is either a TreeSet or an EmptySet (sorted)
      Set<String> onlineInstances = entry.getValue();
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);

      // Build mapping from instance to position in ideal state (ideal state replica ID)
      Map<String, Integer> instanceToIdealStateReplicaId = new HashMap<>();
      int idealStateReplicaId = 0;
      for (String instance : convertToSortedMap(idealStateInstanceStateMap).keySet()) {
        instanceToIdealStateReplicaId.put(instance, idealStateReplicaId);
        idealStateReplicaId++;
      }

      Set<String> unavailableInstances = unavailableInstancesMap.get(idealStateInstanceStateMap.keySet());
      List<SegmentInstanceCandidate> candidates = new ArrayList<>(onlineInstances.size());
      for (String instance : onlineInstances) {
        if (!unavailableInstances.contains(instance)) {
          candidates.add(new SegmentInstanceCandidate(instance, true, getPool(instance),
              instanceToIdealStateReplicaId.get(instance)));
        }
      }
      _oldSegmentCandidatesMap.put(segment, candidates);
    }

    for (Map.Entry<String, Set<String>> entry : newSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      Set<String> onlineInstances = entry.getValue();
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);
      Map<String, String> sortedIdealStateInstanceStateMap = convertToSortedMap(idealStateInstanceStateMap);
      // Build mapping from instance to position in ideal state (ideal state replica ID)
      Map<String, Integer> instanceToIdealStateReplicaId = new HashMap<>();
      int idealStateReplicaId = 0;
      for (String instance : sortedIdealStateInstanceStateMap.keySet()) {
        instanceToIdealStateReplicaId.put(instance, idealStateReplicaId);
        idealStateReplicaId++;
      }

      Set<String> unavailableInstances =
          unavailableInstancesMap.getOrDefault(idealStateInstanceStateMap.keySet(), Collections.emptySet());
      List<SegmentInstanceCandidate> candidates = new ArrayList<>(idealStateInstanceStateMap.size());
      for (String instance : sortedIdealStateInstanceStateMap.keySet()) {
        if (!unavailableInstances.contains(instance)) {
          candidates.add(new SegmentInstanceCandidate(instance, onlineInstances.contains(instance), getPool(instance),
              instanceToIdealStateReplicaId.get(instance)));
        }
      }
      _newSegmentStateMap.put(segment, new NewSegmentState(newSegmentCreationTimeMap.get(segment), candidates));
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Got _newSegmentStateMap: {}, _oldSegmentCandidatesMap: {}", _newSegmentStateMap.keySet(),
          _oldSegmentCandidatesMap.keySet());
    }
  }
}
