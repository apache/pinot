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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.FailOpenPolicy;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.broker.routing.adaptiveserverselector.PriorityPoolInstanceSelector;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.failuredetector.NoOpFailureDetector;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.FALLBACK_POOL_ID;


/// Base implementation of instance selector. Selector maintains a map from segment to enabled ONLINE/CONSUMING server
/// instances that serves the segment and a set of unavailable segments (no enabled instance or all enabled instances
/// are in OFFLINE/ERROR state).
///
/// Special handling of new segment: It is common for new segment to be partially available or not available at all in
/// all instances.
/// 1) We don't report new segment as unavailable segments.
/// 2) To avoid creating hotspot instances, unavailable instances for new segment won't be excluded for instance
/// selection. When it is selected, we don't serve the new segment.
///
/// Definition of new segment:
/// 1) Segment created more than 5 minutes ago.
/// - If we first see a segment via initialization, we look up segment creation time from zookeeper.
/// - If we first see a segment via onAssignmentChange initialization, we use the calling time of onAssignmentChange
/// as approximation.
/// 2) We retire new segment as old when:
/// - The creation time is more than 5 minutes ago
/// - Any instance for new segment is in ERROR state
/// - External view for segment converges with ideal state
///
/// Note that this implementation means:
/// 1) Inconsistent selection of new segments across queries (some queries will serve new segments and others won't).
/// 2) When there is no state update from helix, new segments won't be retired because of the time passing (those with
/// creation time more than 5 minutes ago).
/// TODO: refresh new/old segment state where there is no update from helix for long time.
///
/// Alongside the selection state, this class maintains a [TableReplicaHealth] describing how well the
/// table's segments are replicated across the instances it can route to, exposed through
/// [#getReplicaHealth()]. Because it is derived from what routing can actually use, it accounts for both
/// external view divergence and disabled instances, and for the replica groups that
/// [ReplicaGroupInstanceSelector] takes out of service wholesale. Measuring it here and reporting it in
/// [org.apache.pinot.broker.routing.manager.BaseBrokerRoutingManager] is deliberate: a selector cannot
/// tell whether it covers the whole table or a sampled subset of it, and only the routing manager knows
/// which one owns the table's gauges.
public abstract class BaseInstanceSelector implements InstanceSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseInstanceSelector.class);
  /// Tags the INFO logs that print the whole new-segment map on every rebuild ("Got N new segments: ..."). To drop
  /// them without code changes, add to the log4j2 config:
  /// `<MarkerFilter marker="ROUTING_NEW_SEGMENTS" onMatch="DENY" onMismatch="NEUTRAL"/>`
  private static final Marker NEW_SEGMENTS_MARKER = MarkerFactory.getMarker("ROUTING_NEW_SEGMENTS");
  // To prevent int overflow, reset the request id once it reaches this value
  protected static final long MAX_REQUEST_ID = 1_000_000_000;
  /// Maximum number of distinct (ideal state, external view) instance state map pairs whose candidate lists one
  /// rebuild shares between segments. Past this bound, the rebuild stops caching new pairs: segments with an already
  /// cached pair keep sharing its lists, and segments with any other pair get their own lists, as if there was no
  /// sharing. In a realtime table, all the segments of a stream partition usually have the same pair, so the number of
  /// distinct pairs is close to the number of partitions, far below this bound.
  static final int MAX_SHARED_CANDIDATE_LISTS = 4096;
  // Shared across all tables to bound log volume during an outage, but FAIL_OPEN_ROUTING_FAILURES meter still
  // tracks the full failure count regardless of throttling, and FAIL_OPEN_ELIGIBLE_SEGMENTS still records which
  // tables had unavailable segments.
  // TODO(STREAMANALYTICS-4865): consider reusing the broker's query-log rate limiter instead.
  private static final RateLimiter FAIL_OPEN_ROUTING_FAILURE_LOG_RATE_LIMITER = RateLimiter.create(1);
  // Caps the number of unavailable segment names included in the fail-open failure warning log so that log line
  // size doesn't scale with the number of unavailable segments.
  private static final int MAX_LOGGED_UNAVAILABLE_SEGMENTS = 5;

  protected TableConfig _tableConfig;
  protected String _tableNameWithType;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected BrokerMetrics _brokerMetrics;
  protected AdaptiveServerSelector _adaptiveServerSelector;
  // Will be null if and only if adaptiveServerSelector is null
  protected PriorityPoolInstanceSelector _priorityPoolInstanceSelector;
  protected Clock _clock;
  protected InstanceSelectorConfig _config;
  protected long _newSegmentExpirationTimeInSeconds;
  protected boolean _emitSinglePoolSegmentsMetric;
  protected int _tableNameHashForFixedReplicaRouting;
  protected FailOpenPolicy _failOpenPolicy = FailOpenPolicy.disabled();
  protected FailureDetector _failureDetector = new NoOpFailureDetector();

  // These 3 variables are the cached states to help accelerate the change processing
  protected Set<String> _enabledInstances;
  // For old segments, all candidates are online
  // Reduce this map to reduce garbage
  protected final Map<String, List<SegmentInstanceCandidate>> _oldSegmentCandidatesMap = new HashMap<>();
  protected Map<String, NewSegmentState> _newSegmentStateMap;
  /// Number of ONLINE/CONSUMING instances in the ideal state, for old segments that have fewer
  /// candidates than that. Only used for metrics
  ///
  /// Kept sparse deliberately: an absent entry means "as many candidates as the ideal state assigns",
  /// so a healthy table stores nothing here. Read it through [#getExpectedReplicas].
  protected final Map<String, Integer> _oldSegmentExpectedReplicasMap = new HashMap<>();

  // _segmentStates is needed for instance selection (multi-threaded), so it is made volatile.
  protected volatile SegmentStates _segmentStates;
  // Published together with _segmentStates and read back by the routing manager to report the table's
  // gauges. Volatile so that a reader on another thread cannot see it lagging the segment states it was
  // computed alongside.
  protected volatile TableReplicaHealth _replicaHealth;
  protected Map<String, ServerInstance> _enabledServerStore;
  // Only read by the rebuild thread. Not final so that tests can disable or bound the sharing.
  @VisibleForTesting
  int _maxSharedCandidateLists = MAX_SHARED_CANDIDATE_LISTS;
  // Instances not administratively disabled, including ones currently excluded from _enabledInstances by failure
  // detection. Used as the candidate pool for fail-open routing so we don't fail open onto a server an operator
  // has intentionally taken down, while still allowing fail-open onto servers only flagged unhealthy.
  protected Set<String> _failOpenEligibleInstances;

  @Override
  public void init(TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics, @Nullable AdaptiveServerSelector adaptiveServerSelector, Clock clock,
      InstanceSelectorConfig config, Set<String> enabledInstances, Map<String, ServerInstance> enabledServerMap,
      IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    _tableConfig = tableConfig;
    _tableNameWithType = tableConfig.getTableName();
    _propertyStore = propertyStore;
    _brokerMetrics = brokerMetrics;
    _adaptiveServerSelector = adaptiveServerSelector;
    _clock = clock;
    _config = config;
    _newSegmentExpirationTimeInSeconds = config.getNewSegmentExpirationTimeInSeconds();
    _emitSinglePoolSegmentsMetric = config.shouldEmitSinglePoolSegmentsMetrics();
    // Using raw table name to ensure queries spanning across REALTIME and OFFLINE tables are routed to the same
    // instance
    // Math.abs(Integer.MIN_VALUE) = Integer.MIN_VALUE, so we use & 0x7FFFFFFF to get a positive value
    _tableNameHashForFixedReplicaRouting =
        TableNameBuilder.extractRawTableName(_tableNameWithType).hashCode() & 0x7FFFFFFF;

    _priorityPoolInstanceSelector =
        _adaptiveServerSelector == null ? null : new PriorityPoolInstanceSelector(_adaptiveServerSelector);
    if (_adaptiveServerSelector != null && config.isUseFixedReplica()) {
      throw new IllegalArgumentException(
          "AdaptiveServerSelector and consistent routing cannot be enabled at the same time");
    }
    _enabledInstances = enabledInstances;
    _enabledServerStore = enabledServerMap;
    _failOpenEligibleInstances = Set.copyOf(enabledServerMap.keySet());
    Map<String, Long> newSegmentCreationTimeMap =
        getNewSegmentCreationTimeMapFromZK(idealState, externalView, onlineSegments);
    updateSegmentMaps(idealState, externalView, onlineSegments, newSegmentCreationTimeMap);
    refreshSegmentStates();
  }

  @Override
  public void setFailOpenRouting(FailOpenPolicy failOpenPolicy, FailureDetector failureDetector) {
    _failOpenPolicy = failOpenPolicy;
    _failureDetector = failureDetector;
  }

  /// Returns how well the table's segments are currently replicated across the instances this selector
  /// can route to. Never null once [#init] has run.
  @Override
  public TableReplicaHealth getReplicaHealth() {
    return _replicaHealth;
  }

  /// Returns the number of instances the ideal state assigns to the given old segment in an
  /// ONLINE/CONSUMING state. See [#_oldSegmentExpectedReplicasMap] for why this is stored sparsely.
  private int getExpectedReplicas(String segment, int numCandidates) {
    return Math.max(1, _oldSegmentExpectedReplicasMap.getOrDefault(segment, numCandidates));
  }

  /// Records an old segment's candidates, along with the ideal-state replica count the replica health needs
  /// that the candidate list does not preserve. The single entry point for both structures, so that they
  /// cannot fall out of step.
  protected void putOldSegment(String segment, List<SegmentInstanceCandidate> candidates,
      Map<String, String> idealStateInstanceStateMap) {
    _oldSegmentCandidatesMap.put(segment, candidates);
    // Stored only when it differs from the candidate count, see _oldSegmentExpectedReplicasMap
    int numIdealStateReplicas = getNumInstancesOnlineForRouting(idealStateInstanceStateMap);
    if (numIdealStateReplicas > candidates.size()) {
      _oldSegmentExpectedReplicasMap.put(segment, numIdealStateReplicas);
    }
  }

  /// Returns the number of instances in the given ideal state assignment that are ONLINE/CONSUMING.
  protected static int getNumInstancesOnlineForRouting(Map<String, String> idealStateInstanceStateMap) {
    int numOnlineForRouting = 0;
    for (String state : idealStateInstanceStateMap.values()) {
      if (isOnlineForRouting(state)) {
        numOnlineForRouting++;
      }
    }
    return numOnlineForRouting;
  }

  /// Returns whether the instance state is online for routing purpose (ONLINE/CONSUMING).
  static boolean isOnlineForRouting(@Nullable String state) {
    return SegmentStateModel.ONLINE.equals(state) || SegmentStateModel.CONSUMING.equals(state);
  }

  /// Returns a map from new segment to their creation time based on the ZK metadata.
  Map<String, Long> getNewSegmentCreationTimeMapFromZK(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    List<String> potentialNewSegments = new ArrayList<>();
    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    for (String segment : onlineSegments) {
      assert idealStateAssignment.containsKey(segment);
      if (isPotentialNewSegment(idealStateAssignment.get(segment), externalViewAssignment.get(segment))) {
        potentialNewSegments.add(segment);
      }
    }

    Map<String, Long> newSegmentCreationTimeMap = new HashMap<>();
    long currentTimeMs = _clock.millis();
    String segmentZKMetadataPathPrefix =
        ZKMetadataProvider.constructPropertyStorePathForResource(_tableNameWithType) + "/";
    List<String> segmentZKMetadataPaths = new ArrayList<>(potentialNewSegments.size());
    for (String segment : potentialNewSegments) {
      segmentZKMetadataPaths.add(segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
    for (ZNRecord record : znRecords) {
      if (record == null) {
        continue;
      }
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(record);
      long creationTimeMs = SegmentUtils.getSegmentCreationTimeMs(segmentZKMetadata);
      if (InstanceSelector.isNewSegment(creationTimeMs, currentTimeMs, _newSegmentExpirationTimeInSeconds * 1000)) {
        newSegmentCreationTimeMap.put(segmentZKMetadata.getSegmentName(), creationTimeMs);
      }
    }
    LOGGER.info(NEW_SEGMENTS_MARKER,
        "Got {} new segments: {} for table: {} by reading ZK metadata, current time: {}",
        newSegmentCreationTimeMap.size(), newSegmentCreationTimeMap, _tableNameWithType, currentTimeMs);
    return newSegmentCreationTimeMap;
  }

  /// Returns whether a segment is qualified as a new segment.
  /// A segment is count as old when:
  /// - Any instance for the segment is in ERROR state
  /// - External view for the segment converges with ideal state
  static boolean isPotentialNewSegment(Map<String, String> idealStateInstanceStateMap,
      @Nullable Map<String, String> externalViewInstanceStateMap) {
    if (externalViewInstanceStateMap == null) {
      return true;
    }
    boolean hasConverged = true;
    // Only track ONLINE/CONSUMING instances within the ideal state
    for (Map.Entry<String, String> entry : idealStateInstanceStateMap.entrySet()) {
      if (isOnlineForRouting(entry.getValue())) {
        String externalViewState = externalViewInstanceStateMap.get(entry.getKey());
        if (externalViewState == null || externalViewState.equals(SegmentStateModel.OFFLINE)) {
          hasConverged = false;
        } else if (externalViewState.equals(SegmentStateModel.ERROR)) {
          return false;
        }
      }
    }
    return !hasConverged;
  }

  /// Returns the online instances for routing purpose.
  static Set<String> getOnlineInstances(Map<String, String> idealStateInstanceStateMap,
      Map<String, String> externalViewInstanceStateMap) {
    Set<String> onlineInstances = new HashSet<>();
    // Only track ONLINE/CONSUMING instances within the ideal state
    for (Map.Entry<String, String> entry : idealStateInstanceStateMap.entrySet()) {
      String instance = entry.getKey();
      // NOTE: DO NOT check if EV matches IS because it is a valid state when EV is CONSUMING while IS is ONLINE
      if (isOnlineForRouting(entry.getValue()) && isOnlineForRouting(externalViewInstanceStateMap.get(instance))) {
        onlineInstances.add(instance);
      }
    }
    return onlineInstances;
  }

  /// Returns a map that iterates the entries of the given map in key order: the map itself when it is a
  /// [SortedMap] or when its iteration order is already sorted, otherwise a sorted copy.
  ///
  /// The instance state maps read from ZooKeeper are insertion-ordered, and are usually written in key order, so
  /// the linear check avoids the copy in the common case.
  static Map<String, String> sortByKeyIfNeeded(Map<String, String> map) {
    if (map instanceof SortedMap) {
      return map;
    }
    String previousKey = null;
    for (String key : map.keySet()) {
      if (previousKey != null && previousKey.compareTo(key) > 0) {
        return new TreeMap<>(map);
      }
      previousKey = key;
    }
    return map;
  }

  /// Returns the candidates of an old segment: the instances that are ONLINE/CONSUMING in both the ideal state and
  /// the external view, in instance order.
  List<SegmentInstanceCandidate> computeOldSegmentCandidates(Map<String, String> idealStateInstanceStateMap,
      Map<String, String> externalViewInstanceStateMap) {
    List<SegmentInstanceCandidate> candidates = new ArrayList<>(idealStateInstanceStateMap.size());
    int idealStateReplicaId = 0;
    for (Map.Entry<String, String> entry : sortByKeyIfNeeded(idealStateInstanceStateMap).entrySet()) {
      String instance = entry.getKey();
      // NOTE: DO NOT check if EV matches IS because it is a valid state when EV is CONSUMING while IS is ONLINE
      if (isOnlineForRouting(entry.getValue()) && isOnlineForRouting(externalViewInstanceStateMap.get(instance))) {
        candidates.add(new SegmentInstanceCandidate(instance, true, getPool(instance), idealStateReplicaId));
      }
      idealStateReplicaId++;
    }
    return candidates;
  }

  /// Returns the candidates of a new segment: the instances that are ONLINE/CONSUMING in the ideal state, in instance
  /// order. A candidate is online when the instance is ONLINE/CONSUMING in the external view.
  List<SegmentInstanceCandidate> computeNewSegmentCandidates(Map<String, String> idealStateInstanceStateMap,
      @Nullable Map<String, String> externalViewInstanceStateMap) {
    List<SegmentInstanceCandidate> candidates = new ArrayList<>(idealStateInstanceStateMap.size());
    int idealStateReplicaId = 0;
    for (Map.Entry<String, String> entry : sortByKeyIfNeeded(idealStateInstanceStateMap).entrySet()) {
      if (isOnlineForRouting(entry.getValue())) {
        String instance = entry.getKey();
        boolean online =
            externalViewInstanceStateMap != null && isOnlineForRouting(externalViewInstanceStateMap.get(instance));
        candidates.add(new SegmentInstanceCandidate(instance, online, getPool(instance), idealStateReplicaId));
      }
      idealStateReplicaId++;
    }
    return candidates;
  }

  /// Updates the segment maps based on the given ideal state, external view, online segments (segments with
  /// ONLINE/CONSUMING instances in the ideal state and pre-selected by the `SegmentPreSelector`) and new
  /// segments. After this update:
  /// - Old segments' online instances should be tracked in _oldSegmentCandidatesMap
  /// - New segments' state (creation time and candidate instances) should be tracked in _newSegmentStateMap
  void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, Long> newSegmentCreationTimeMap) {
    _oldSegmentCandidatesMap.clear();
    _oldSegmentExpectedReplicasMap.clear();
    _newSegmentStateMap = new HashMap<>(HashUtil.getHashMapCapacity(newSegmentCreationTimeMap.size()));

    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    InstanceStateMapsCache instanceStateMapsCache = new InstanceStateMapsCache(_maxSharedCandidateLists);
    int numSinglePoolSegments = 0;
    Set<Integer> pools = new HashSet<>();
    for (String segment : onlineSegments) {
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);
      Long newSegmentCreationTimeMs = newSegmentCreationTimeMap.get(segment);
      Map<String, String> externalViewInstanceStateMap = externalViewAssignment.get(segment);

      if (newSegmentCreationTimeMs != null) {
        // New segment
        _newSegmentStateMap.put(segment, new NewSegmentState(newSegmentCreationTimeMs,
            computeNewSegmentCandidates(idealStateInstanceStateMap, externalViewInstanceStateMap)));
      } else if (externalViewInstanceStateMap == null) {
        // Old segment
        putOldSegment(segment, List.of(), idealStateInstanceStateMap);
      } else {
        // Old segment. Segments with the same instance state maps share the candidate list.
        InstanceStateMaps instanceStateMaps =
            instanceStateMapsCache.canonicalize(idealStateInstanceStateMap, externalViewInstanceStateMap);
        List<SegmentInstanceCandidate> candidates = instanceStateMaps._oldSegmentCandidates;
        if (candidates == null) {
          candidates = computeOldSegmentCandidates(idealStateInstanceStateMap, externalViewInstanceStateMap);
          instanceStateMaps._oldSegmentCandidates = candidates;
        }
        putOldSegment(segment, candidates, idealStateInstanceStateMap);
      }
      if (_emitSinglePoolSegmentsMetric) {
        pools.clear();
        for (String instance : idealStateInstanceStateMap.keySet()) {
          pools.add(getPool(instance));
        }
        if (pools.size() < 2) {
          numSinglePoolSegments++;
        }
      }
    }
    if (_emitSinglePoolSegmentsMetric) {
      _brokerMetrics.addMeteredTableValue(_tableNameWithType, BrokerMeter.SINGLE_POOL_SEGMENTS, numSinglePoolSegments);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Got _newSegmentStateMap: {}, _oldSegmentCandidatesMap: {}", _newSegmentStateMap.keySet(),
          _oldSegmentCandidatesMap.keySet());
    }
  }

  /// Refreshes the \_segmentStates based on the in-memory states.
  /// Note that the whole \_segmentStates has to be updated together to avoid partial state update.
  void refreshSegmentStates() {
    Map<String, List<SegmentInstanceCandidate>> instanceCandidatesMap =
        new HashMap<>(HashUtil.getHashMapCapacity(_oldSegmentCandidatesMap.size() + _newSegmentStateMap.size()));
    Set<String> servingInstances = new HashSet<>();
    Set<String> unavailableSegments = new HashSet<>();
    Set<String> segmentsWithoutCandidates = new HashSet<>();
    int minPercentOfReplicas = TableReplicaHealth.FULLY_REPLICATED_PERCENT;
    // Segments seen at exactly minPercentOfReplicas so far. Reset whenever a lower percentage displaces the
    // minimum, so that the pair published below always describes the same population.
    int numSegmentsAtMinPercentOfReplicas = 0;
    Map<String, List<SegmentInstanceCandidate>> failOpenCandidatesMap = new HashMap<>();

    for (Map.Entry<String, List<SegmentInstanceCandidate>> entry : _oldSegmentCandidatesMap.entrySet()) {
      String segment = entry.getKey();
      List<SegmentInstanceCandidate> candidates = entry.getValue();
      List<SegmentInstanceCandidate> enabledCandidates =
          getEnabledCandidatesAndAddToServingInstances(candidates, servingInstances);
      int expectedReplicas = getExpectedReplicas(segment, candidates.size());
      int servingReplicas = enabledCandidates.size();
      if (TableReplicaHealth.shouldMeasure(expectedReplicas)) {
        int percentOfReplicas = TableReplicaHealth.toPercent(servingReplicas, expectedReplicas);
        if (percentOfReplicas < minPercentOfReplicas) {
          minPercentOfReplicas = percentOfReplicas;
          numSegmentsAtMinPercentOfReplicas = 1;
        } else if (percentOfReplicas == minPercentOfReplicas) {
          numSegmentsAtMinPercentOfReplicas++;
        }
      }
      if (!enabledCandidates.isEmpty()) {
        instanceCandidatesMap.put(segment, enabledCandidates);
        if (shouldTrackFailOpenCandidatesForHealthySegments()) {
          addFailOpenCandidates(segment, candidates, failOpenCandidatesMap);
        }
      } else {
        List<String> candidateInstances = new ArrayList<>(candidates.size());
        List<SegmentInstanceCandidate> failOpenCandidates = new ArrayList<>(candidates.size());
        for (SegmentInstanceCandidate candidate : candidates) {
          String instanceId = candidate.getInstance();
          candidateInstances.add(instanceId);
          if (_failOpenEligibleInstances.contains(instanceId)) {
            failOpenCandidates.add(candidate);
          }
        }
        LOGGER.warn("Failed to find servers hosting old segment: {} for table: {} "
                + "(all candidate instances: {} are disabled, counting segment as unavailable)", segment,
            _tableNameWithType, candidateInstances);
        unavailableSegments.add(segment);
        if (failOpenCandidates.isEmpty()) {
          segmentsWithoutCandidates.add(segment);
        } else {
          failOpenCandidatesMap.put(segment, failOpenCandidates);
        }
        _brokerMetrics.addMeteredTableValue(_tableNameWithType, BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT, 1);
      }
    }

    for (Map.Entry<String, NewSegmentState> entry : _newSegmentStateMap.entrySet()) {
      String segment = entry.getKey();
      NewSegmentState newSegmentState = entry.getValue();
      List<SegmentInstanceCandidate> candidates = newSegmentState.getCandidates();
      List<SegmentInstanceCandidate> enabledCandidates =
          getEnabledCandidatesAndAddToServingInstances(candidates, servingInstances);
      if (!enabledCandidates.isEmpty()) {
        instanceCandidatesMap.put(segment, enabledCandidates);
      } else {
        // Do not count new segment as unavailable
        List<String> candidateInstances = new ArrayList<>(candidates.size());
        for (SegmentInstanceCandidate candidate : candidates) {
          candidateInstances.add(candidate.getInstance());
        }
        LOGGER.info("Failed to find servers hosting new segment: {} for table: {} "
                + "(all candidate instances: {} are disabled, but not counting new segment as unavailable)", segment,
            _tableNameWithType, candidateInstances);
      }
      if (shouldTrackFailOpenCandidatesForHealthySegments()) {
        addFailOpenCandidates(segment, candidates, failOpenCandidatesMap);
      }
    }

    SegmentStates previousSegmentStates = _segmentStates;
    Set<String> previousUnavailableSegments =
        previousSegmentStates != null ? previousSegmentStates.getUnavailableSegments() : Set.of();
    Set<String> previousSegmentsWithoutCandidates =
        previousSegmentStates != null ? previousSegmentStates.getSegmentsWithoutCandidates() : Set.of();
    for (String segment : unavailableSegments) {
      boolean wasAlreadyUnavailableWithCandidates =
          previousUnavailableSegments.contains(segment) && !previousSegmentsWithoutCandidates.contains(segment);
      if (!segmentsWithoutCandidates.contains(segment) && !wasAlreadyUnavailableWithCandidates) {
        _brokerMetrics.addMeteredTableValue(_tableNameWithType, BrokerMeter.FAIL_OPEN_ELIGIBLE_SEGMENTS, 1);
      }
    }

    _segmentStates =
        new SegmentStates(instanceCandidatesMap, servingInstances, unavailableSegments, segmentsWithoutCandidates,
            failOpenCandidatesMap, Set.copyOf(_newSegmentStateMap.keySet()));
    _replicaHealth = new TableReplicaHealth(minPercentOfReplicas, numSegmentsAtMinPercentOfReplicas,
        unavailableSegments.size());
  }

  /// Returns the enabled candidates, and adds their instances to the serving instances.
  ///
  /// When every candidate is enabled, returns the given list itself rather than a copy. That publishes the list to
  /// the query path, which is safe because candidate lists are never modified once built. It also keeps the lists
  /// that [#updateSegmentMaps] shares between segments shared in the published [SegmentStates].
  private List<SegmentInstanceCandidate> getEnabledCandidatesAndAddToServingInstances(
      List<SegmentInstanceCandidate> candidates, Set<String> servingInstances) {
    int numCandidates = candidates.size();
    int numLeadingEnabledCandidates = 0;
    while (numLeadingEnabledCandidates < numCandidates) {
      String instance = candidates.get(numLeadingEnabledCandidates).getInstance();
      if (!_enabledInstances.contains(instance)) {
        break;
      }
      servingInstances.add(instance);
      numLeadingEnabledCandidates++;
    }
    if (numLeadingEnabledCandidates == numCandidates) {
      return candidates;
    }
    List<SegmentInstanceCandidate> enabledCandidates = new ArrayList<>(numCandidates - 1);
    for (int i = 0; i < numLeadingEnabledCandidates; i++) {
      enabledCandidates.add(candidates.get(i));
    }
    // The candidate at numLeadingEnabledCandidates is disabled
    for (int i = numLeadingEnabledCandidates + 1; i < numCandidates; i++) {
      SegmentInstanceCandidate candidate = candidates.get(i);
      String instance = candidate.getInstance();
      if (_enabledInstances.contains(instance)) {
        enabledCandidates.add(candidate);
        servingInstances.add(instance);
      }
    }
    return enabledCandidates;
  }

  private void addFailOpenCandidates(String segment, List<SegmentInstanceCandidate> candidates,
      Map<String, List<SegmentInstanceCandidate>> failOpenCandidatesMap) {
    List<SegmentInstanceCandidate> failOpenCandidates = null;
    for (SegmentInstanceCandidate candidate : candidates) {
      String instanceId = candidate.getInstance();
      if (!_enabledInstances.contains(instanceId) && _failOpenEligibleInstances.contains(instanceId)) {
        if (failOpenCandidates == null) {
          failOpenCandidates = new ArrayList<>();
        }
        failOpenCandidates.add(candidate);
      }
    }
    if (failOpenCandidates != null) {
      failOpenCandidatesMap.put(segment, failOpenCandidates);
    }
  }

  /// {@inheritDoc}
  ///
  /// Updates the cached enabled instances and re-calculates [#_segmentStates].
  @Override
  public void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {
    _enabledInstances = enabledInstances;
    refreshSegmentStates();
  }

  @Override
  public void onInstancesChange(Set<String> routableInstances, Set<String> failOpenEligibleInstances,
      List<String> changedInstances) {
    _failOpenEligibleInstances = failOpenEligibleInstances;
    onInstancesChange(routableInstances, changedInstances);
  }

  /// {@inheritDoc}
  ///
  /// Updates the cached maps ([#_oldSegmentCandidatesMap] and [#_newSegmentStateMap], and re-calculates
  /// [#_segmentStates] based on the cached states.
  @Override
  public void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    Map<String, Long> newSegmentCreationTimeMap =
        getNewSegmentCreationTimeMapFromExistingStates(idealState, externalView, onlineSegments);
    updateSegmentMaps(idealState, externalView, onlineSegments, newSegmentCreationTimeMap);
    refreshSegmentStates();
  }

  /// Returns a map from new segment to their creation time based on the existing in-memory states.
  Map<String, Long> getNewSegmentCreationTimeMapFromExistingStates(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    Map<String, Long> newSegmentCreationTimeMap = new HashMap<>();
    long currentTimeMs = _clock.millis();
    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    for (String segment : onlineSegments) {
      NewSegmentState newSegmentState = _newSegmentStateMap.get(segment);
      long creationTimeMs = 0;
      if (newSegmentState != null) {
        // It was a new segment before, check the creation time and segment state to see if it is still a new segment
        if (InstanceSelector.isNewSegment(newSegmentState.getCreationTimeMs(), currentTimeMs,
            _newSegmentExpirationTimeInSeconds * 1000)) {
          creationTimeMs = newSegmentState.getCreationTimeMs();
        }
      } else if (!_oldSegmentCandidatesMap.containsKey(segment)) {
        // This is the first time we see this segment, use the current time as the creation time
        creationTimeMs = currentTimeMs;
      }
      // For recently created segment, check if it is qualified as new segment
      if (creationTimeMs > 0) {
        assert idealStateAssignment.containsKey(segment);
        if (isPotentialNewSegment(idealStateAssignment.get(segment), externalViewAssignment.get(segment))) {
          newSegmentCreationTimeMap.put(segment, creationTimeMs);
        }
      }
    }
    LOGGER.info(NEW_SEGMENTS_MARKER,
        "Got {} new segments: {} for table: {} by processing existing states, current time: {}",
        newSegmentCreationTimeMap.size(), newSegmentCreationTimeMap, _tableNameWithType, currentTimeMs);
    return newSegmentCreationTimeMap;
  }

  @Override
  public SelectionResult select(BrokerRequest brokerRequest, List<String> segments, long requestId) {
    Map<String, String> queryOptions =
        (brokerRequest.getPinotQuery() != null && brokerRequest.getPinotQuery().getQueryOptions() != null)
            ? brokerRequest.getPinotQuery().getQueryOptions() : Map.of();
    int requestIdInt = (int) (requestId % MAX_REQUEST_ID);
    // Copy the volatile reference so that the segment-to-instance map and unavailable segments have a consistent view
    // of the state.
    SegmentStates segmentStates = _segmentStates;
    Set<String> unavailableSegments = segmentStates.getUnavailableSegments();
    Set<String> segmentsWithoutCandidates = segmentStates.getSegmentsWithoutCandidates();
    boolean failOpenEnabled = _failOpenPolicy.isEnabled();
    InstanceMapping mapping = select(segments, requestIdInt, segmentStates, queryOptions, failOpenEnabled);

    if (unavailableSegments.isEmpty()) {
      recordPoolMetrics(mapping, queryOptions);
      return new SelectionResult(mapping, List.of(), 0);
    } else {
      List<String> unavailableSegmentsForRequest = new ArrayList<>();
      boolean failOpenEligible = false;
      for (String segment : segments) {
        if (unavailableSegments.contains(segment)) {
          unavailableSegmentsForRequest.add(segment);
          failOpenEligible |= !segmentsWithoutCandidates.contains(segment);
        }
      }
      if (failOpenEligible) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ELIGIBLE_REQUESTS, 1);
      }
      if (!failOpenEligible || !failOpenEnabled) {
        recordPoolMetrics(mapping, queryOptions);
        return new SelectionResult(mapping, unavailableSegmentsForRequest, 0);
      }

      SelectionResult failOpenResult = selectFailOpen(segments, requestIdInt, segmentStates, queryOptions,
          unavailableSegmentsForRequest);
      if (failOpenResult != null) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
        return failOpenResult;
      }
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
      if (FAIL_OPEN_ROUTING_FAILURE_LOG_RATE_LIMITER.tryAcquire()) {
        List<String> loggedUnavailableSegments = unavailableSegmentsForRequest.size() > MAX_LOGGED_UNAVAILABLE_SEGMENTS
            ? unavailableSegmentsForRequest.subList(0, MAX_LOGGED_UNAVAILABLE_SEGMENTS)
            : unavailableSegmentsForRequest;
        LOGGER.warn("Fail-open routing failed for table: {} with {} unavailable segments (sample: {})",
            _tableNameWithType, unavailableSegmentsForRequest.size(), loggedUnavailableSegments);
      }
      recordPoolMetrics(mapping, queryOptions);
      return new SelectionResult(mapping, unavailableSegmentsForRequest, 0);
    }
  }

  /// Attempts to replace unavailable segments with eligible fail-open candidates.
  ///
  /// @return a complete fail-open selection, or `null` when an unavailable segment has no eligible candidate
  @Nullable
  private SelectionResult selectFailOpen(List<String> segments, int requestId, SegmentStates segmentStates,
      Map<String, String> queryOptions, List<String> unavailableSegmentsForRequest) {
    Set<String> unhealthyServers = new HashSet<>(_failureDetector.getUnhealthyServers());
    Map<String, List<SegmentInstanceCandidate>> candidatesMap = new HashMap<>();
    // Unhealthy servers a healthy segment may fall back to, exposed via getFailOpenCandidates() on the fail-open
    // states. Kept separate from candidatesMap so a selector only uses them as a last resort, when it can't satisfy its
    // constraints (e.g. one replica group per partition) with healthy servers alone.
    Map<String, List<SegmentInstanceCandidate>> fallbackCandidatesMap = new HashMap<>();
    // Always recompute over the full, original per-query segment list (not just the unavailable subset) so that
    // selectForFailOpen() sees the exact same iteration order/positions the ideal select() call used. Healthy segments
    // keep their normal candidates, so they only land on an unhealthy server if the selector explicitly falls back to
    // fallbackCandidatesMap. The segment that was actually unavailable now has real candidates instead of being
    // skipped, so it naturally takes its place in whatever rotation the selector uses.
    List<String> segmentsToSelect = segments;
    Set<String> unavailableSegments = Set.copyOf(unavailableSegmentsForRequest);
    Set<String> skippableSegments = new HashSet<>();
    for (String segment : segmentsToSelect) {
      List<SegmentInstanceCandidate> failOpenCandidates = segmentStates.getFailOpenCandidates(segment);
      List<SegmentInstanceCandidate> eligibleFailOpenCandidates =
          getEligibleFailOpenCandidates(failOpenCandidates, unhealthyServers);
      if (unavailableSegments.contains(segment)) {
        if (eligibleFailOpenCandidates.isEmpty()) {
          return null;
        }
        candidatesMap.put(segment, eligibleFailOpenCandidates);
        continue;
      }
      List<SegmentInstanceCandidate> candidates = segmentStates.getCandidates(segment);
      if (candidates != null && !candidates.isEmpty()) {
        candidatesMap.put(segment, candidates);
        if (!eligibleFailOpenCandidates.isEmpty()) {
          fallbackCandidatesMap.put(segment, eligibleFailOpenCandidates);
        }
      } else if (!shouldFailOpenSegmentsWithoutHealthyCandidates() && failOpenCandidates != null) {
        // The normal pass skips this segment (e.g. a new segment whose candidates are all unhealthy), so skip it here
        // too rather than letting it influence how the rest of the query is routed. Only new segments can reach this
        // branch: an old segment without enabled candidates is unavailable and handled above.
        skippableSegments.add(segment);
      } else if (!eligibleFailOpenCandidates.isEmpty()) {
        // No healthy route at all (e.g. a new segment whose candidates are all unhealthy), so fail open for it.
        candidatesMap.put(segment, eligibleFailOpenCandidates);
      } else if (canSkipSegmentForFailOpen(segment, segmentStates)) {
        skippableSegments.add(segment);
      } else {
        return null;
      }
    }

    SegmentStates failOpenStates = new SegmentStates(candidatesMap, segmentStates.getServingInstances(), Set.of(),
        Set.of(), fallbackCandidatesMap);
    InstanceMapping failOpenMapping =
        selectForFailOpen(segmentsToSelect, requestId, failOpenStates, queryOptions);
    Set<String> assignedSegments = new HashSet<>(failOpenMapping.segmentToInstanceMap().keySet());
    assignedSegments.addAll(failOpenMapping.optionalSegmentToInstanceMap().keySet());
    Set<String> requiredSegments = new HashSet<>(segmentsToSelect);
    requiredSegments.removeAll(skippableSegments);
    if (!assignedSegments.containsAll(requiredSegments)) {
      return null;
    }
    recordPoolMetrics(failOpenMapping, queryOptions);
    // Keep recovery owned by the failure detector. Async query responses need epoch-aware ordering before they can
    // safely update server health because an older success must not overwrite a newer failure.
    return new SelectionResult(failOpenMapping, List.of(), 0);
  }

  /**
   * Returns the fail-open candidates that the failure detector currently reports as unhealthy, preserving their order.
   */
  private static List<SegmentInstanceCandidate> getEligibleFailOpenCandidates(
      @Nullable List<SegmentInstanceCandidate> failOpenCandidates, Set<String> unhealthyServers) {
    if (failOpenCandidates == null) {
      return List.of();
    }
    List<SegmentInstanceCandidate> eligibleCandidates = new ArrayList<>(failOpenCandidates.size());
    for (SegmentInstanceCandidate candidate : failOpenCandidates) {
      if (unhealthyServers.contains(candidate.getInstance())) {
        eligibleCandidates.add(candidate);
      }
    }
    return eligibleCandidates;
  }

  /**
   * Records {@link BrokerMeter#POOL_SEG_QUERIES} once per query, from whichever {@link InstanceMapping} is actually
   * being returned to the caller (the ideal mapping, or the fail-open mapping when fail-open succeeded). Recording
   * from the final mapping - rather than eagerly during the ideal pass and separately patching only the segments
   * that were unavailable - ensures the metric reflects the real instance (and pool) a segment ends up on even when
   * fail-open reselects an already-healthy sibling segment to keep a partition on one consistent replica group.
   */
  private void recordPoolMetrics(InstanceMapping mapping, Map<String, String> queryOptions) {
    Map<Integer, Integer> poolToSegmentCount = new HashMap<>();
    for (String instanceId : mapping.segmentToInstanceMap().values()) {
      poolToSegmentCount.merge(getPool(instanceId), 1, Integer::sum);
    }
    for (String instanceId : mapping.optionalSegmentToInstanceMap().values()) {
      poolToSegmentCount.merge(getPool(instanceId), 1, Integer::sum);
    }
    for (Map.Entry<Integer, Integer> entry : poolToSegmentCount.entrySet()) {
      _brokerMetrics.addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, entry.getValue(),
          BrokerMetrics.getTagForPreferredPool(queryOptions), String.valueOf(entry.getKey()));
    }
  }

  /**
   * Selects against query-local fail-open candidates, over the full, original per-query segment list (not just the
   * unavailable subset) so implementations see the exact same iteration order/positions the ideal {@link #select}
   * call used. Built-in selectors override this to bypass sticky or adaptive preferences while retaining their
   * normal coverage guarantees. In {@code failOpenSegmentStates}, {@link SegmentStates#getCandidates} holds the
   * healthy segments' normal candidates and the unavailable segments' fail-open candidates, while
   * {@link SegmentStates#getFailOpenCandidates} holds the healthy segments' unhealthy servers; implementations must
   * only use the latter when no assignment satisfying their constraints exists without them.
   */
  protected InstanceMapping selectForFailOpen(List<String> segments, int requestId,
      SegmentStates failOpenSegmentStates, Map<String, String> queryOptions) {
    return select(segments, requestId, failOpenSegmentStates, queryOptions);
  }

  /**
   * Returns whether healthy segments should also track their unhealthy (fail-open eligible) servers. During fail-open
   * reselection, a healthy segment with enabled candidates keeps them as its candidates and only exposes these servers
   * via {@link SegmentStates#getFailOpenCandidates}, as a last-resort fallback for selectors that can't otherwise keep
   * a partition on one consistent replica (e.g. multi-stage). A healthy segment without any enabled candidates (e.g. a
   * new segment) uses them as its candidates directly, unless
   * {@link #shouldFailOpenSegmentsWithoutHealthyCandidates()} says to skip it. Note: fail-open reselection always
   * recomputes over the full per-query segment list regardless of this flag.
   */
  protected boolean shouldTrackFailOpenCandidatesForHealthySegments() {
    return false;
  }

  /**
   * Returns whether a segment that is not unavailable but has no healthy candidates (e.g. a new segment whose
   * candidates are all unhealthy) should be routed to its fail-open candidates during fail-open reselection. When
   * {@code false}, such a segment is skipped, matching how the normal pass treats it.
   */
  protected boolean shouldFailOpenSegmentsWithoutHealthyCandidates() {
    return true;
  }

  /**
   * Returns whether a segment without fail-open candidates may be omitted from a complete reselection. Consults the
   * query's captured {@code segmentStates} snapshot (not any separately-mutable, live field) so this stays
   * consistent with the rest of the query's fail-open decisions even if an assignment refresh races this query.
   */
  protected boolean canSkipSegmentForFailOpen(String segment, SegmentStates segmentStates) {
    return segmentStates.isNewSegment(segment);
  }

  @Override
  public Set<String> getServingInstances() {
    return _segmentStates.getServingInstances();
  }

  @VisibleForTesting
  int getPool(String instanceID) {
    int pool = FALLBACK_POOL_ID;
    ServerInstance server = _enabledServerStore.get(instanceID);
    if (server == null) {
      LOGGER.warn("Failed to find server {} in the enabledServerManager when update segmentsMap for table {}",
          instanceID, _tableNameWithType);
    } else {
      pool = server.getPool();
    }
    return pool;
  }

  /// Selects the server instances for the given segments based on the request id and segment states. Returns an
  /// [InstanceSelector.InstanceMapping] containing two maps from segment to selected server instance. The optional map
  /// covers new segments that are not online yet; instead of simply skipping them at the broker, we can send them to
  /// servers and let servers decide how to handle them.
  protected abstract InstanceMapping select(List<String> segments, int requestId, SegmentStates segmentStates,
      Map<String, String> queryOptions);

  protected InstanceMapping select(List<String> segments, int requestId, SegmentStates segmentStates,
      Map<String, String> queryOptions, boolean failOpenEnabled) {
    return select(segments, requestId, segmentStates, queryOptions);
  }

  /// The ideal state and external view instance state maps of a segment, compared by content. Within one rebuild,
  /// segments with equal maps get the same routing candidates, so the canonical instance (see
  /// [InstanceStateMapsCache]) caches what is derived from the maps, and the segments share it.
  ///
  /// Not thread safe. Only used by the rebuild thread, for the duration of one rebuild.
  static final class InstanceStateMaps {
    final Map<String, String> _idealStateInstanceStateMap;
    @Nullable
    final Map<String, String> _externalViewInstanceStateMap;
    private final int _hashCode;
    // Derived from the maps, computed on first use
    @Nullable
    Set<String> _onlineInstances;
    @Nullable
    List<SegmentInstanceCandidate> _oldSegmentCandidates;
    // Used by the strict replica-group selector to collect the unavailable instances once per instance state maps
    boolean _unavailableInstancesCollected;

    InstanceStateMaps(Map<String, String> idealStateInstanceStateMap,
        @Nullable Map<String, String> externalViewInstanceStateMap) {
      _idealStateInstanceStateMap = idealStateInstanceStateMap;
      _externalViewInstanceStateMap = externalViewInstanceStateMap;
      _hashCode = 31 * idealStateInstanceStateMap.hashCode() + Objects.hashCode(externalViewInstanceStateMap);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof InstanceStateMaps)) {
        return false;
      }
      InstanceStateMaps that = (InstanceStateMaps) o;
      // NOTE: Map.equals() ignores the iteration order, so maps with the same entries in a different order share one
      //       canonical InstanceStateMaps. This is correct because every candidate list is built in the order of
      //       sortByKeyIfNeeded(), which does not depend on the iteration order of the maps.
      return _hashCode == that._hashCode && _idealStateInstanceStateMap.equals(that._idealStateInstanceStateMap)
          && Objects.equals(_externalViewInstanceStateMap, that._externalViewInstanceStateMap);
    }

    @Override
    public int hashCode() {
      return _hashCode;
    }
  }

  /// Returns one canonical [InstanceStateMaps] per distinct pair of instance state maps, so that the segments of one
  /// rebuild with equal maps share what is derived from them.
  ///
  /// The cache keeps at most `maxSize` pairs. Past that bound, it stops caching new pairs: a pair already in the cache
  /// still returns its canonical [InstanceStateMaps], while any other pair returns a new [InstanceStateMaps] for each
  /// call, which makes each segment compute its own state, as if there was no sharing.
  ///
  /// Not thread safe. Only used by the rebuild thread, for the duration of one rebuild.
  static final class InstanceStateMapsCache {
    private final int _maxSize;
    private final Map<InstanceStateMaps, InstanceStateMaps> _cache = new HashMap<>();

    InstanceStateMapsCache(int maxSize) {
      _maxSize = maxSize;
    }

    InstanceStateMaps canonicalize(Map<String, String> idealStateInstanceStateMap,
        @Nullable Map<String, String> externalViewInstanceStateMap) {
      InstanceStateMaps instanceStateMaps =
          new InstanceStateMaps(idealStateInstanceStateMap, externalViewInstanceStateMap);
      if (_maxSize <= 0) {
        return instanceStateMaps;
      }
      InstanceStateMaps canonical = _cache.get(instanceStateMaps);
      if (canonical != null) {
        return canonical;
      }
      if (_cache.size() < _maxSize) {
        _cache.put(instanceStateMaps, instanceStateMaps);
      }
      return instanceStateMaps;
    }
  }
}
