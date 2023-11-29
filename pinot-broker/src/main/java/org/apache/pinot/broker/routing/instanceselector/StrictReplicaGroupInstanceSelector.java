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

import com.google.common.base.Preconditions;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Instance selector for strict replica-group routing strategy.
 *
 * <pre>
 * The strict replica-group routing strategy always routes the query to the instances within the same replica-group.
 * (Note that the replica-group information is derived from the ideal state of the table, where the instances are sorted
 * alphabetically in the instance state map, so the replica-groups in the instance selector might not match the
 * replica-groups in the instance partitions.) The instances in a replica-group should have all the online segments
 * (segments with ONLINE/CONSUMING instances in the ideal state and selected by the pre-selector) available
 * (ONLINE/CONSUMING in the external view) in order to serve queries. If any segment is unavailable in the
 * replica-group, we mark the whole replica-group down and not serve queries with this replica-group.
 *
 * The selection algorithm is the same as {@link ReplicaGroupInstanceSelector}, and will always evenly distribute the
 * traffic to all replica-groups that have all online segments available.
 *
 * The algorithm relies on the mirror segment assignment from replica-group segment assignment strategy. With mirror
 * segment assignment, any server in one replica-group will always have a corresponding server in other replica-groups
 * that have the same segments assigned. For example, if S1 is a server in replica-group 1, and it has mirror server
 * S2 in replica-group 2 and S3 in replica-group 3. All segments assigned to S1 will also be assigned to S2 and S3. In
 * stable scenario (external view matches ideal state), all segments assigned to S1 will have the same enabled instances
 * of [S1, S2, S3] sorted (in alphabetical order). If we always pick the same index of enabled instances for all
 * segments, only one of S1, S2, S3 will be picked, and all the segments are processed by the same server. In
 * transitioning/error scenario (external view does not match ideal state), if a segment is down on S1, we mark all
 * segments with the same assignment ([S1, S2, S3]) down on S1 to ensure that we always route the segments to the same
 * replica-group.
 *
 * Note that new segments won't be used to exclude instances from serving when the segment is unavailable.
 *
 * But the default behavior can be too strict for some use cases. In worst case, there can be an unavailable segment in
 * every replica group, so instance selector is left no replica groups to use. Sometimes, it's better to continue to
 * select a replica group to process the available segments and report unavailable ones in the selected replica group.
 * So a new option, bestEffort, is added for such cases.
 * </pre>
 */
public class StrictReplicaGroupInstanceSelector implements InstanceSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(StrictReplicaGroupInstanceSelector.class);

  private final String _tableNameWithType;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final AdaptiveServerSelector _adaptiveServerSelector;
  private final BrokerMetrics _brokerMetrics;
  private final Clock _clock;

  // These 3 variables are the cached states to help accelerate the change processing
  private Set<String> _enabledInstances;
  private final Map<String, List<SegmentInstanceCandidate>> _oldSegmentCandidatesMap = new HashMap<>();
  private Map<String, NewSegmentState> _newSegmentStateMap;

  // Key is the set of instances hosting the same group of segments, like [S1, S2, S3] in the comment above.
  private final Map<Set<String>, InstanceGroup> _instanceGroups = new HashMap<>();

  // Use _segmentGroupStates to hold many segment states as needed for instance selection (multi-threaded) and make
  // it volatile to update those states atomically so that instance selection is done with a consistent snapshot.
  private volatile SegmentGroupStates _segmentGroupStates;

  public StrictReplicaGroupInstanceSelector(String tableNameWithType, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics, @Nullable AdaptiveServerSelector adaptiveServerSelector, Clock clock) {
    _tableNameWithType = tableNameWithType;
    _propertyStore = propertyStore;
    _brokerMetrics = brokerMetrics;
    _adaptiveServerSelector = adaptiveServerSelector;
    _clock = clock;
  }

  @Override
  public void init(Set<String> enabledInstances, IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    _enabledInstances = enabledInstances;
    Map<String, Long> newSegmentCreationTimeMap =
        InstanceSelectorUtils.getNewSegmentCreationTimeMapFromZK(_tableNameWithType, idealState, externalView,
            onlineSegments, _propertyStore, _clock);
    updateSegmentMaps(idealState, externalView, onlineSegments, newSegmentCreationTimeMap);
    refreshSegmentStates();
  }

  @Override
  public void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {
    _enabledInstances = enabledInstances;
    refreshSegmentStates();
  }

  @Override
  public void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    Map<String, Long> newSegmentCreationTimeMap =
        InstanceSelectorUtils.getNewSegmentCreationTimeMapFromExistingStates(_tableNameWithType, idealState,
            externalView, onlineSegments, _newSegmentStateMap, _oldSegmentCandidatesMap, _clock);
    updateSegmentMaps(idealState, externalView, onlineSegments, newSegmentCreationTimeMap);
    refreshSegmentStates();
  }

  @Override
  public SelectionResult select(BrokerRequest brokerRequest, List<String> segments, long requestId) {
    Map<String, String> queryOptions =
        (brokerRequest.getPinotQuery() != null && brokerRequest.getPinotQuery().getQueryOptions() != null)
            ? brokerRequest.getPinotQuery().getQueryOptions() : Collections.emptyMap();
    int requestIdInt = (int) (requestId % MAX_REQUEST_ID);
    List<String> unavailableSegments = new ArrayList<>();
    Pair<Map<String, String>, Map<String, String>> segmentToInstanceMap =
        select(segments, requestIdInt, unavailableSegments, queryOptions);
    return new SelectionResult(segmentToInstanceMap, unavailableSegments, 0);
  }

  @Override
  public Set<String> getServingInstances() {
    return _segmentGroupStates.getServingInstances();
  }

  /**
   * Override the default updateSegmentMaps method to identify new and old segments as before, but meanwhile to group
   * segments up if they are hosted by same set of instances. For every such group of segments, we can also track the
   * available and unavailable segments on each instance from the set of instances hosting them. This info allows us
   * to pick instance at segment group level later on, and report unavailable segments on the selected instance quickly.
   */
  private void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, Long> newSegmentCreationTimeMap) {
    // Continue to track sets of new/old segments so that the logic to change new to old segments continues to work.
    _oldSegmentCandidatesMap.clear();
    _newSegmentStateMap = new HashMap<>(HashUtil.getHashMapCapacity(newSegmentCreationTimeMap.size()));
    _instanceGroups.clear();

    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    int instanceGroupId = 0;
    for (String segment : onlineSegments) {
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);
      assert idealStateInstanceStateMap != null;
      Map<String, String> externalViewInstanceStateMap = externalViewAssignment.get(segment);
      Set<String> instancesInIdealState = idealStateInstanceStateMap.keySet();
      InstanceGroup instanceGroup = _instanceGroups.get(instancesInIdealState);
      if (instanceGroup == null) {
        instanceGroup = new InstanceGroup(instanceGroupId++, instancesInIdealState);
        _instanceGroups.put(instancesInIdealState, instanceGroup);
      }
      List<SegmentInstanceCandidate> candidates;
      boolean isNewSegment = false;
      if (externalViewInstanceStateMap == null) {
        if (newSegmentCreationTimeMap.containsKey(segment)) {
          // New segment
          candidates = InstanceSelectorUtils.getCandidatesForNewSegment(idealStateInstanceStateMap, instance -> false);
          _newSegmentStateMap.put(segment, new NewSegmentState(newSegmentCreationTimeMap.get(segment), candidates));
          isNewSegment = true;
        } else {
          // Old segment
          candidates = Collections.emptyList();
          _oldSegmentCandidatesMap.put(segment, candidates);
        }
      } else {
        TreeSet<String> onlineInstances =
            InstanceSelectorUtils.getOnlineInstances(idealStateInstanceStateMap, externalViewInstanceStateMap);
        if (newSegmentCreationTimeMap.containsKey(segment)) {
          // New segment
          candidates =
              InstanceSelectorUtils.getCandidatesForNewSegment(idealStateInstanceStateMap, onlineInstances::contains);
          _newSegmentStateMap.put(segment, new NewSegmentState(newSegmentCreationTimeMap.get(segment), candidates));
          isNewSegment = true;
        } else {
          // Old segment
          candidates = onlineInstances.stream().map(instance -> new SegmentInstanceCandidate(instance, true))
              .collect(Collectors.toList());
          _oldSegmentCandidatesMap.put(segment, candidates);
        }
      }
      instanceGroup.addSegment(segment, candidates, isNewSegment);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Got _newSegmentStateMap: {}, _oldSegmentCandidatesMap: {}", _newSegmentStateMap.keySet(),
          _oldSegmentCandidatesMap.keySet());
    }
  }

  private void refreshSegmentStates() {
    // Continue to use the sets of new/old segments and their candidates to get the list of serving instances.
    // The list of serving instances could be a bit more than needed for strict replica group policy.
    Set<String> servingInstances = new HashSet<>();
    for (Map.Entry<String, List<SegmentInstanceCandidate>> entry : _oldSegmentCandidatesMap.entrySet()) {
      List<SegmentInstanceCandidate> candidates = entry.getValue();
      InstanceSelectorUtils.getEnabledCandidatesAndAddToServingInstances(candidates, _enabledInstances,
          servingInstances);
    }
    for (Map.Entry<String, NewSegmentState> entry : _newSegmentStateMap.entrySet()) {
      NewSegmentState newSegmentState = entry.getValue();
      List<SegmentInstanceCandidate> candidates = newSegmentState.getCandidates();
      InstanceSelectorUtils.getEnabledCandidatesAndAddToServingInstances(candidates, _enabledInstances,
          servingInstances);
    }
    _instanceGroups.values().forEach(ig -> ig.checkEnabledInstances(_enabledInstances));
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Got instanceGroups: {} after checking enabled instances: {}", _instanceGroups, _enabledInstances);
    }
    _segmentGroupStates = new SegmentGroupStates(_instanceGroups, servingInstances);
  }

  private Pair<Map<String, String>, Map<String, String>> select(List<String> segments, int requestId,
      List<String> unavailableSegments, Map<String, String> queryOptions) {
    SegmentGroupStates segmentGroupStates = _segmentGroupStates;
    if (_adaptiveServerSelector == null) {
      // Adaptive Server Selection is NOT enabled.
      return selectServersUsingRoundRobin(segmentGroupStates, segments, requestId, unavailableSegments, queryOptions);
    }
    // Adaptive Server Selection is enabled.
    List<String> serverRankList = new ArrayList<>();
    List<String> candidateServers = fetchCandidateServersForQuery(segmentGroupStates, segments);
    // Fetch serverRankList before looping through all the segments. This is important to make sure that we pick
    // the least amount of instances for a query by referring to a single snapshot of the rankings.
    List<Pair<String, Double>> serverRankListWithScores =
        _adaptiveServerSelector.fetchServerRankingsWithScores(candidateServers);
    for (Pair<String, Double> entry : serverRankListWithScores) {
      serverRankList.add(entry.getLeft());
    }
    return selectServersUsingAdaptiveServerSelector(segmentGroupStates, segments, requestId, unavailableSegments,
        serverRankList, queryOptions);
  }

  private static Pair<Map<String, String>, Map<String, String>> selectServersUsingRoundRobin(
      SegmentGroupStates segmentGroupStates, List<String> segments, int requestId, List<String> unavailableSegments,
      Map<String, String> queryOptions) {
    Map<String, String> segmentToSelectedInstanceMap = new HashMap<>(HashUtil.getHashMapCapacity(segments.size()));
    // No need to adjust this map per total segment numbers, as optional segments should be empty most of the time.
    Map<String, String> optionalSegmentToInstanceMap = new HashMap<>();
    boolean useCompleteReplicaGroup = QueryOptionsUtils.isUseCompleteReplicaGroup(queryOptions);
    Integer numReplicaGroupsToQuery = QueryOptionsUtils.getNumReplicaGroupsToQuery(queryOptions);
    int numReplicaGroups = numReplicaGroupsToQuery == null ? 1 : numReplicaGroupsToQuery;
    int[] indexCache = initInstanceIndexCache(segmentGroupStates.getInstanceGroupCount());
    Map<String, InstanceGroup> segmentInstanceGroupMap = segmentGroupStates.getSegmentInstanceGroupMap();
    int replicaOffset = 0;
    for (String segment : segments) {
      InstanceGroup instanceGroup = segmentInstanceGroupMap.get(segment);
      // In case the instance selector has not been updated.
      if (instanceGroup == null) {
        continue;
      }
      int selectedInstanceIdx = indexCache[instanceGroup.getId()];
      if (selectedInstanceIdx == -1) {
        int availableInstanceCnt = instanceGroup.getNumAvailableInstances(useCompleteReplicaGroup);
        if (availableInstanceCnt == 0) {
          if (!instanceGroup.isNewSegment(segment)) {
            unavailableSegments.add(segment);
          }
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "No available instance for {} segment: {} from instanceGroup: {} with useCompleteReplicaGroup: {}",
                instanceGroup.isNewSegment(segment) ? "new" : "old", segment, instanceGroup.getId(),
                useCompleteReplicaGroup);
          }
          continue;
        }
        selectedInstanceIdx = (requestId + replicaOffset) % availableInstanceCnt;
        indexCache[instanceGroup.getId()] = selectedInstanceIdx;
      }
      String selectedInstance = instanceGroup.getInstance(selectedInstanceIdx, useCompleteReplicaGroup);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Selected instance: {} for segment: {} from instanceGroup: {} with useCompleteReplicaGroup: {}",
            selectedInstance, segment, instanceGroup.getId(), useCompleteReplicaGroup);
      }
      checkSegmentState(segment, selectedInstance, instanceGroup, segmentToSelectedInstanceMap,
          optionalSegmentToInstanceMap, unavailableSegments);
      // Round robin selection across segment groups.
      replicaOffset = (replicaOffset + 1) % numReplicaGroups;
    }
    return Pair.of(segmentToSelectedInstanceMap, optionalSegmentToInstanceMap);
  }

  private static List<String> fetchCandidateServersForQuery(SegmentGroupStates segmentGroupStates,
      List<String> segments) {
    Set<String> candidateServers = new HashSet<>();
    Map<String, InstanceGroup> segmentInstanceGroupMap = segmentGroupStates.getSegmentInstanceGroupMap();
    for (String segment : segments) {
      InstanceGroup instanceGroup = segmentInstanceGroupMap.get(segment);
      candidateServers.addAll(instanceGroup._availableInstances);
    }
    return new ArrayList<>(candidateServers);
  }

  private static Pair<Map<String, String>, Map<String, String>> selectServersUsingAdaptiveServerSelector(
      SegmentGroupStates segmentGroupStates, List<String> segments, int requestId, List<String> unavailableSegments,
      List<String> serverRankList, Map<String, String> queryOptions) {
    // Copy the volatile reference to get a consistent view of the states to complete the selection.
    Map<String, String> segmentToSelectedInstanceMap = new HashMap<>(HashUtil.getHashMapCapacity(segments.size()));
    // No need to adjust this map per total segment numbers, as optional segments should be empty most of the time.
    Map<String, String> optionalSegmentToInstanceMap = new HashMap<>();
    boolean useCompleteReplicaGroup = QueryOptionsUtils.isUseCompleteReplicaGroup(queryOptions);
    int[] indexCache = initInstanceIndexCache(segmentGroupStates.getInstanceGroupCount());
    Map<String, InstanceGroup> segmentInstanceGroupMap = segmentGroupStates.getSegmentInstanceGroupMap();
    for (String segment : segments) {
      InstanceGroup instanceGroup = segmentInstanceGroupMap.get(segment);
      // In case the instance selector has not been updated.
      if (instanceGroup == null) {
        continue;
      }
      // TODO: Support round robin and numReplicaGroupsToQuery with Adaptive Server Selection.
      int selectedInstanceIdx = indexCache[instanceGroup.getId()];
      if (selectedInstanceIdx == -1) {
        int availableInstanceCnt = instanceGroup.getNumAvailableInstances(useCompleteReplicaGroup);
        if (availableInstanceCnt == 0) {
          if (!instanceGroup.isNewSegment(segment)) {
            unavailableSegments.add(segment);
          }
          continue;
        }
        selectedInstanceIdx = selectInstanceAdaptively(instanceGroup, serverRankList, requestId % availableInstanceCnt,
            useCompleteReplicaGroup);
        indexCache[instanceGroup.getId()] = selectedInstanceIdx;
      }
      String selectedInstance = instanceGroup.getInstance(selectedInstanceIdx, useCompleteReplicaGroup);
      checkSegmentState(segment, selectedInstance, instanceGroup, segmentToSelectedInstanceMap,
          optionalSegmentToInstanceMap, unavailableSegments);
    }
    return Pair.of(segmentToSelectedInstanceMap, optionalSegmentToInstanceMap);
  }

  private static int selectInstanceAdaptively(InstanceGroup instanceGroup, List<String> serverRankList,
      int defaultInstanceIdx, boolean useCompleteReplicaGroup) {
    if (!serverRankList.isEmpty()) {
      return defaultInstanceIdx;
    }
    int selectedInstanceIdx = defaultInstanceIdx;
    int availableInstanceCnt = instanceGroup.getNumAvailableInstances(useCompleteReplicaGroup);
    int minIdx = Integer.MAX_VALUE;
    for (int instanceIdx = 0; instanceIdx < availableInstanceCnt; instanceIdx++) {
      String candidateInstance = instanceGroup.getInstance(instanceIdx, useCompleteReplicaGroup);
      int idx = serverRankList.indexOf(candidateInstance);
      if (idx == -1) {
        // Skip until stats for all servers are populated.
        break;
      }
      if (idx < minIdx) {
        minIdx = idx;
        selectedInstanceIdx = instanceIdx;
      }
    }
    return selectedInstanceIdx;
  }

  private static void checkSegmentState(String segment, String selectedInstance, InstanceGroup instanceGroup,
      Map<String, String> segmentToSelectedInstanceMap, Map<String, String> optionalSegmentToInstanceMap,
      List<String> unavailableSegments) {
    InstanceSegmentStates instanceSegmentStates = instanceGroup.getInstanceSegmentStates(selectedInstance);
    if (instanceSegmentStates.isAvailableSegment(segment)) {
      segmentToSelectedInstanceMap.put(segment, selectedInstance);
    } else if (instanceSegmentStates.isOfflineNewSegments(segment)) {
      optionalSegmentToInstanceMap.put(segment, selectedInstance);
    } else if (instanceSegmentStates.isUnavailableSegments(segment)) {
      unavailableSegments.add(segment);
    } else if (LOGGER.isDebugEnabled()) {
      // NOTE: the segments states tracked by the instance selector may not be updated yet. Like the other instance
      // selectors, we just skip this segment and not report it as unavailable segment either.
      LOGGER.debug("No segment states for segment: {} on instance: {}", segment, selectedInstance);
    }
  }

  private static int[] initInstanceIndexCache(int size) {
    // TODO: make this instanceIndexCache thread-local to reuse.
    int[] idxCache = new int[size];
    Arrays.fill(idxCache, -1);
    return idxCache;
  }

  private static class InstanceGroup {
    private final int _id;
    private final Set<String> _instancesInIdealState;
    private final Set<String> _allSegments;
    private final Set<String> _newSegments;
    private final Map<String, InstanceSegmentStates> _instanceSegmentStatesMap;
    private final List<String> _availableInstances;
    private final List<String> _completeInstances;

    public InstanceGroup(int id, Set<String> instancesInIdealState) {
      _id = id;
      _instancesInIdealState = instancesInIdealState;
      _allSegments = new HashSet<>();
      _newSegments = new HashSet<>();
      _instanceSegmentStatesMap = new HashMap<>();
      _availableInstances = new ArrayList<>();
      _completeInstances = new ArrayList<>();
    }

    private InstanceGroup(int id, Set<String> instancesInIdealState, Set<String> allSegments, Set<String> newSegments,
        Map<String, InstanceSegmentStates> instanceSegmentStatesMap, List<String> availableInstances,
        List<String> fullyAvailableInstances) {
      _id = id;
      // Those collections are effectively immutable after InstanceGroup is created in updateSegmentMaps(), which
      // always creates new InstanceGroups upon getting new external view or ideal states.
      _instancesInIdealState = instancesInIdealState;
      _allSegments = allSegments;
      _newSegments = newSegments;
      _instanceSegmentStatesMap = instanceSegmentStatesMap;
      // Those collections can be updated after InstanceGroups are created when getting new set of enabled instances.
      // And they are read concurrently when selecting servers to route queries so make a deep copy.
      _availableInstances = new ArrayList<>(availableInstances);
      _completeInstances = new ArrayList<>(fullyAvailableInstances);
    }

    public void addSegment(String segment, List<SegmentInstanceCandidate> candidates, boolean isNewSegment) {
      _allSegments.add(segment);
      if (isNewSegment) {
        // If no instances are available for the instance group, all segments are unavailable except the new segments.
        _newSegments.add(segment);
      }
      Set<String> candidateInstances =
          candidates.stream().map(SegmentInstanceCandidate::getInstance).collect(Collectors.toSet());
      Set<String> onlineInstances =
          candidates.stream().filter(SegmentInstanceCandidate::isOnline).map(SegmentInstanceCandidate::getInstance)
              .collect(Collectors.toSet());
      for (String instance : _instancesInIdealState) {
        InstanceSegmentStates instanceSegmentStates =
            _instanceSegmentStatesMap.computeIfAbsent(instance, i -> new InstanceSegmentStates());
        if (onlineInstances.contains(instance)) {
          instanceSegmentStates._availableSegments.add(segment);
        } else if (candidateInstances.contains(instance)) {
          // New segments can have candidate instance that's offline, and those segments are not reported as
          // unavailable segments in query response.
          Preconditions.checkState(isNewSegment, "Only newSegment can be offline on candidate instance: " + instance);
          instanceSegmentStates._offlineNewSegments.add(segment);
        } else {
          // Old segments always have same set of onlineInstances and candidateInstances, otherwise, it's unavailable
          // segment and reported back in query response.
          instanceSegmentStates._unavailableSegments.add(segment);
        }
      }
    }

    public void checkEnabledInstances(Set<String> enabledInstances) {
      _availableInstances.clear();
      _completeInstances.clear();
      if (_instanceSegmentStatesMap.isEmpty()) {
        return;
      }
      for (Map.Entry<String, InstanceSegmentStates> entry : _instanceSegmentStatesMap.entrySet()) {
        String instance = entry.getKey();
        if (!enabledInstances.contains(instance)) {
          continue;
        }
        InstanceSegmentStates segmentStates = entry.getValue();
        if (segmentStates._availableSegments.isEmpty()) {
          continue;
        }
        _availableInstances.add(instance);
      }
      Collections.sort(_availableInstances);
      for (String instance : _availableInstances) {
        if (_instanceSegmentStatesMap.get(instance)._unavailableSegments.isEmpty()) {
          _completeInstances.add(instance);
        }
      }
    }

    public int getId() {
      return _id;
    }

    public boolean isNewSegment(String segment) {
      return _newSegments.contains(segment);
    }

    public int getNumAvailableInstances(boolean useCompleteReplicaGroup) {
      return useCompleteReplicaGroup ? _completeInstances.size() : _availableInstances.size();
    }

    public String getInstance(int instanceIdx, boolean useCompleteReplicaGroup) {
      return useCompleteReplicaGroup ? _completeInstances.get(instanceIdx) : _availableInstances.get(instanceIdx);
    }

    public InstanceSegmentStates getInstanceSegmentStates(String instance) {
      return _instanceSegmentStatesMap.get(instance);
    }

    public InstanceGroup copyForRead() {
      return new InstanceGroup(_id, _instancesInIdealState, _allSegments, _newSegments, _instanceSegmentStatesMap,
          _availableInstances, _completeInstances);
    }

    @Override
    public String toString() {
      return "InstanceGroup{" + "_id=" + _id + ", _instancesInIdealState=" + _instancesInIdealState + ", _allSegments="
          + _allSegments + ", _newSegments=" + _newSegments + ", _instanceSegmentStatesMap=" + _instanceSegmentStatesMap
          + ", _availableInstances=" + _availableInstances + ", _completeInstances=" + _completeInstances + '}';
    }
  }

  private static class InstanceSegmentStates {
    private final Set<String> _availableSegments = new HashSet<>();
    private final Set<String> _unavailableSegments = new HashSet<>();
    // Those offline NewSegments are treated as optional segments, so that broker or server can skip it upon any
    // issue to process it.
    private final Set<String> _offlineNewSegments = new HashSet<>();

    public InstanceSegmentStates copyForRead() {
      InstanceSegmentStates iss = new InstanceSegmentStates();
      iss._availableSegments.addAll(_availableSegments);
      iss._unavailableSegments.addAll(_unavailableSegments);
      iss._offlineNewSegments.addAll(_offlineNewSegments);
      return iss;
    }

    public boolean isAvailableSegment(String segment) {
      return _availableSegments.contains(segment);
    }

    public boolean isOfflineNewSegments(String segment) {
      return _offlineNewSegments.contains(segment);
    }

    public boolean isUnavailableSegments(String segment) {
      return _unavailableSegments.contains(segment);
    }

    @Override
    public String toString() {
      return "InstanceSegmentStates{" + "_availableSegments=" + _availableSegments + ", _unavailableSegments="
          + _unavailableSegments + ", _offlineNewSegments=" + _offlineNewSegments + '}';
    }
  }

  private static class SegmentGroupStates {
    private final Set<String> _servingInstances;
    private final List<InstanceGroup> _instanceGroups = new ArrayList<>();
    private final Map<String, InstanceGroup> _segmentInstanceGroupMap = new HashMap<>();

    public SegmentGroupStates(Map<Set<String>, InstanceGroup> instanceGroups, Set<String> servingInstances) {
      _servingInstances = servingInstances;
      for (InstanceGroup instanceGroup : instanceGroups.values()) {
        InstanceGroup copy = instanceGroup.copyForRead();
        instanceGroup._allSegments.forEach(segment -> _segmentInstanceGroupMap.put(segment, copy));
        _instanceGroups.add(copy);
      }
    }

    public int getInstanceGroupCount() {
      return _instanceGroups.size();
    }

    public Set<String> getServingInstances() {
      return _servingInstances;
    }

    public Map<String, InstanceGroup> getSegmentInstanceGroupMap() {
      return _segmentInstanceGroupMap;
    }
  }
}
