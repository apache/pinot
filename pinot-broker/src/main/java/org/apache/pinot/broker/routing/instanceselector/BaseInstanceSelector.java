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

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.broker.routing.adaptiveserverselector.PriorityPoolInstanceSelector;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
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


/// Base implementation of instance selector. Selector maintains the enabled ONLINE/CONSUMING server instances that
/// serve each segment, and the unavailable segments (no enabled instance or all enabled instances are in
/// OFFLINE/ERROR state).
///
/// Segments with the same candidates (disabled instances included), of the same kind (old or new) and with the same
/// number of expected replicas share one [CandidateGroup], so the per-segment state is reduced to the id of the group
/// of the segment, plus the creation time of the new segments (see [SegmentGroups]). An instance change only
/// recomputes per-group state (see [SegmentStates]), and the selection works on the groups.
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
  static final long MAX_REQUEST_ID = 1_000_000_000;
  // To prevent int overflow when adding a replica offset, reduce the table name hash for fixed replica routing modulo
  // this value: the largest int multiple of the least common multiple of 1 to 22, which keeps the selected candidate
  // for up to 22 candidates, and for most tables with more candidates
  static final int MAX_TABLE_NAME_HASH = 2_095_133_040;

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

  protected Set<String> _enabledInstances;

  // _segmentStates is needed for instance selection (multi-threaded), so it is made volatile.
  protected volatile SegmentStates _segmentStates;
  protected Map<String, ServerInstance> _enabledServerStore;

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
        (TableNameBuilder.extractRawTableName(_tableNameWithType).hashCode() & 0x7FFFFFFF) % MAX_TABLE_NAME_HASH;

    _priorityPoolInstanceSelector =
        _adaptiveServerSelector == null ? null : new PriorityPoolInstanceSelector(_adaptiveServerSelector);
    if (_adaptiveServerSelector != null && config.isUseFixedReplica()) {
      throw new IllegalArgumentException(
          "AdaptiveServerSelector and consistent routing cannot be enabled at the same time");
    }
    _enabledInstances = enabledInstances;
    _enabledServerStore = enabledServerMap;
    Object2LongOpenHashMap<String> newSegmentCreationTimeMap =
        getNewSegmentCreationTimeMapFromZK(idealState, externalView, onlineSegments);
    updateSegmentStates(idealState, externalView, onlineSegments, newSegmentCreationTimeMap);
  }

  /// Returns how well the table's segments are currently replicated across the instances this selector
  /// can route to. Never null once [#init] has run.
  @Override
  public TableReplicaHealth getReplicaHealth() {
    return _segmentStates.getReplicaHealth();
  }

  /// Returns a map from new segment to their creation time based on the ZK metadata.
  Object2LongOpenHashMap<String> getNewSegmentCreationTimeMapFromZK(IdealState idealState, ExternalView externalView,
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

    Object2LongOpenHashMap<String> newSegmentCreationTimeMap = new Object2LongOpenHashMap<>();
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
      if (SegmentGroupsBuilder.isOnlineForRouting(entry.getValue())) {
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

  /// Returns whether to compute the candidates with the strict replica-group rules: an instance unavailable for any old
  /// segment is excluded from the candidates of every segment with the same instances in the ideal state, so that
  /// these segments are always routed to the same instance.
  boolean isStrictReplicaGroupRouting() {
    return false;
  }

  /// Rebuilds the segment groups from the given ideal state, external view, online segments (segments with
  /// ONLINE/CONSUMING instances in the ideal state and pre-selected by the `SegmentPreSelector`) and new segments,
  /// then publishes the segment states.
  private void updateSegmentStates(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Object2LongOpenHashMap<String> newSegmentCreationTimeMap) {
    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    SegmentGroupsBuilder segmentGroupsBuilder =
        new SegmentGroupsBuilder(_tableNameWithType, newSegmentCreationTimeMap, onlineSegments.size(),
            isStrictReplicaGroupRouting(), this::getPool);
    for (String segment : onlineSegments) {
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);
      assert idealStateInstanceStateMap != null;
      segmentGroupsBuilder.addSegment(segment, idealStateInstanceStateMap, externalViewAssignment.get(segment));
    }
    if (_emitSinglePoolSegmentsMetric) {
      _brokerMetrics.addMeteredTableValue(_tableNameWithType, BrokerMeter.SINGLE_POOL_SEGMENTS,
          segmentGroupsBuilder.getNumSinglePoolSegments());
    }
    SegmentGroups segmentGroups = segmentGroupsBuilder.build();
    LOGGER.debug("Got {} candidate groups for {} segments of table: {}", segmentGroups.getNumGroups(),
        onlineSegments.size(), _tableNameWithType);
    refreshSegmentStates(segmentGroups);
  }

  /// Publishes the segment states of the given segment groups under the enabled instances, along with the replica
  /// health. Reuses the routing of every group whose candidates did not change enabled state since the published
  /// segment states, when they are of the same segment groups.
  ///
  /// Apart from the DEBUG log of the unavailable segments, the cost depends on the number of groups and instances, not
  /// on the number of segments.
  private void refreshSegmentStates(SegmentGroups segmentGroups) {
    int numSlots = segmentGroups.getNumSlots();
    boolean[] enabledBySlot = new boolean[numSlots];
    for (int slot = 0; slot < numSlots; slot++) {
      enabledBySlot[slot] = _enabledInstances.contains(segmentGroups._slotInstances[slot]);
    }
    SegmentStates previousSegmentStates = _segmentStates;
    boolean[] previousEnabledBySlot =
        previousSegmentStates != null && previousSegmentStates._segmentGroups == segmentGroups
            ? previousSegmentStates._enabledBySlot
            : null;

    int numGroups = segmentGroups.getNumGroups();
    GroupRouting[] routings = new GroupRouting[numGroups];
    int minPercentOfReplicas = TableReplicaHealth.FULLY_REPLICATED_PERCENT;
    // Segments seen at exactly minPercentOfReplicas so far. Reset whenever a lower percentage displaces the
    // minimum, so that the pair published below always describes the same population.
    int numSegmentsAtMinPercentOfReplicas = 0;
    int numUnavailableSegments = 0;
    for (int groupId = 0; groupId < numGroups; groupId++) {
      CandidateGroup group = segmentGroups._groups[groupId];
      GroupRouting routing =
          previousEnabledBySlot != null && !hasEnabledStateChange(group, previousEnabledBySlot, enabledBySlot)
              ? previousSegmentStates._routings[groupId]
              : group.filterEnabled(enabledBySlot);
      routings[groupId] = routing;
      int groupSize = segmentGroups._groupSizes[groupId];
      int numServingReplicas = routing.getNumCandidates();
      if (group._new) {
        if (numServingReplicas == 0) {
          LOGGER.info("Failed to find servers hosting {} new segments (e.g. {}) for table: {} (all candidate "
                  + "instances: {} are disabled, but not counting new segments as unavailable)", groupSize,
              group._sampleSegment, _tableNameWithType, getCandidateInstances(group));
        }
        continue;
      }
      int expectedReplicas = group._expectedReplicas;
      if (TableReplicaHealth.shouldMeasure(expectedReplicas)) {
        int percentOfReplicas = TableReplicaHealth.toPercent(numServingReplicas, expectedReplicas);
        if (percentOfReplicas < minPercentOfReplicas) {
          minPercentOfReplicas = percentOfReplicas;
          numSegmentsAtMinPercentOfReplicas = groupSize;
        } else if (percentOfReplicas == minPercentOfReplicas) {
          numSegmentsAtMinPercentOfReplicas += groupSize;
        }
      }
      if (numServingReplicas == 0) {
        numUnavailableSegments += groupSize;
        LOGGER.warn("Failed to find servers hosting {} old segments (e.g. {}) for table: {} (all candidate instances: "
                + "{} are disabled, counting segments as unavailable)", groupSize, group._sampleSegment,
            _tableNameWithType, getCandidateInstances(group));
        _brokerMetrics.addMeteredTableValue(_tableNameWithType, BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT, groupSize);
      }
    }
    SegmentStates segmentStates = new SegmentStates(segmentGroups, routings, enabledBySlot,
        new TableReplicaHealth(minPercentOfReplicas, numSegmentsAtMinPercentOfReplicas, numUnavailableSegments));
    if (numUnavailableSegments > 0 && LOGGER.isDebugEnabled()) {
      List<String> unavailableSegments = new ArrayList<>(numUnavailableSegments);
      segmentGroups._segmentToGroupId.object2IntEntrySet().fastForEach(entry -> {
        if (segmentStates.isUnavailable(entry.getIntValue())) {
          unavailableSegments.add(entry.getKey());
        }
      });
      LOGGER.debug("Got {} unavailable segments: {} for table: {}", numUnavailableSegments, unavailableSegments,
          _tableNameWithType);
    }
    _segmentStates = segmentStates;
  }

  private static boolean hasEnabledStateChange(CandidateGroup group, boolean[] previousEnabledBySlot,
      boolean[] enabledBySlot) {
    for (int slot : group._routing._slots) {
      if (previousEnabledBySlot[slot] != enabledBySlot[slot]) {
        return true;
      }
    }
    return false;
  }

  private static List<String> getCandidateInstances(CandidateGroup group) {
    List<String> candidateInstances = new ArrayList<>(group._routing.getNumCandidates());
    for (SegmentInstanceCandidate candidate : group._routing.getCandidates()) {
      candidateInstances.add(candidate.getInstance());
    }
    return candidateInstances;
  }

  /// {@inheritDoc}
  ///
  /// Updates the cached enabled instances and re-calculates [#_segmentStates] for the same segment groups.
  @Override
  public void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {
    _enabledInstances = enabledInstances;
    refreshSegmentStates(_segmentStates._segmentGroups);
  }

  /// {@inheritDoc}
  ///
  /// Rebuilds the segment groups and re-calculates [#_segmentStates].
  @Override
  public void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    Object2LongOpenHashMap<String> newSegmentCreationTimeMap =
        getNewSegmentCreationTimeMapFromExistingStates(idealState, externalView, onlineSegments);
    updateSegmentStates(idealState, externalView, onlineSegments, newSegmentCreationTimeMap);
  }

  /// Returns a map from new segment to their creation time based on the existing in-memory states.
  Object2LongOpenHashMap<String> getNewSegmentCreationTimeMapFromExistingStates(IdealState idealState,
      ExternalView externalView, Set<String> onlineSegments) {
    Object2LongOpenHashMap<String> newSegmentCreationTimeMap = new Object2LongOpenHashMap<>();
    long currentTimeMs = _clock.millis();
    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    SegmentGroups segmentGroups = _segmentStates._segmentGroups;
    for (String segment : onlineSegments) {
      long creationTimeMs = 0;
      long previousCreationTimeMs = segmentGroups._newSegmentCreationTimeMap.getLong(segment);
      if (previousCreationTimeMs > 0) {
        // It was a new segment before, check the creation time and segment state to see if it is still a new segment
        if (InstanceSelector.isNewSegment(previousCreationTimeMs, currentTimeMs,
            _newSegmentExpirationTimeInSeconds * 1000)) {
          creationTimeMs = previousCreationTimeMs;
        }
      } else if (segmentGroups._segmentToGroupId.getInt(segment) == SegmentStates.UNKNOWN_SEGMENT) {
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
  public SelectionResult select(BrokerRequest brokerRequest, Collection<String> segments, long requestId) {
    Map<String, String> queryOptions =
        (brokerRequest.getPinotQuery() != null && brokerRequest.getPinotQuery().getQueryOptions() != null)
            ? brokerRequest.getPinotQuery().getQueryOptions()
            : Map.of();
    int requestIdInt = (int) (requestId % MAX_REQUEST_ID);
    // Read the volatile reference once, so that the whole selection has a consistent view of the state
    return select(segments, requestIdInt, _segmentStates, queryOptions);
  }

  @Override
  public Set<String> getServingInstances() {
    return _segmentStates.getServingInstances();
  }

  private int getPool(String instanceID) {
    int pool = FALLBACK_POOL_ID;
    ServerInstance server = _enabledServerStore.get(instanceID);
    if (server == null) {
      LOGGER.warn("Failed to find server {} in the enabledServerManager when building the candidate groups for "
          + "table {}", instanceID, _tableNameWithType);
    } else {
      pool = server.getPool();
    }
    return pool;
  }

  /// Selects, for every segment, the candidate at index `floorMod(baseIndex, numCandidates)`, so that the segments with
  /// the same number of candidates (e.g. the segments of mirror servers) are selected on the same candidate index.
  SelectionCollector selectSameCandidateIndex(Collection<String> segments, SegmentStates segmentStates, int baseIndex) {
    SelectionCollector collector = new SelectionCollector(segmentStates, segments.size(), false);
    int numCandidates = 0;
    int candidateIndex = 0;
    for (String segment : segments) {
      GroupRouting routing = collector.resolveRouting(segment);
      if (routing != null) {
        if (routing.getNumCandidates() != numCandidates) {
          numCandidates = routing.getNumCandidates();
          candidateIndex = Math.floorMod(baseIndex, numCandidates);
        }
        collector.add(routing, candidateIndex, segment);
      }
    }
    return collector;
  }

  /// Selects the server instances for the given segments based on the request id and segment states. Returns the
  /// segments to query on each selected instance, where the optional segments cover new segments that are not online
  /// yet: instead of simply skipping them at the broker, we can send them to servers and let servers decide how to
  /// handle them. Also returns the requested segments that are unavailable, in the order of the given segments.
  protected abstract SelectionResult select(Collection<String> segments, int requestId, SegmentStates segmentStates,
      Map<String, String> queryOptions);
}
