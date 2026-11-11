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
package org.apache.pinot.controller.helix.core.assignment.segment;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategy;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategyFactory;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Base segment assignment which contains the common assignment strategies.
///
/// - Non-replica-group based assignment (1 replica-group and 1 partition in instance partitions):
///
///   Assign the segment to the instance with the least number of segments. In case of a tie, assign the segment to
///   the instance with the smallest index in the list. Use Helix AutoRebalanceStrategy to rebalance the table.
/// - Replica-group based assignment (multiple replica-groups or partitions in instance partitions):
///
///   Among replica-groups, always mirror the assignment (pick the same index of the instance).
///
///   Within each partition, assign the segment to the instances with the least segments already assigned. In case
///   of a tie, assign to the instance with the smallest index in the list. Do this for one replica-group and mirror
///   the assignment to other replica-groups.
///
///   To rebalance a table, within each partition, first calculate the number of segments on each instance, loop
///   over all the segments and keep the assignment if number of segments for the instance has not been reached and
///   track the not assigned segments, then assign the left-over segments to the instances with the least segments, or
///   the smallest index if there is a tie. Repeat the process for all the partitions in one replica-group, and mirror
///   the assignment to other replica-groups. With this greedy algorithm, the result is deterministic and with minimum
///   segment moves.
/// - Partition-pinned assignment (table with strict replica-group routing):
///
///   All segments of a segment partition are assigned to the same instances, recorded per partition in the partition
///   assignment of the ideal state (see [org.apache.pinot.common.assignment.PartitionAssignmentUtils]). A new segment
///   is assigned to the instances of its partition when the partition has an entry, and the entry is created from the
///   instance partitions otherwise. To rebalance a table, the target instances of each partition are computed from the
///   instance partitions, and the segments follow their partition. The entries of the partitions without segment are
///   dropped.
public abstract class BaseSegmentAssignment implements SegmentAssignment {
  /// Partition id of a segment whose partition id is not known, see [#getPartitionIds].
  public static final int NO_PARTITION_ID = -1;
  /// Partition id returned for a segment not in the partition ids, see [#getPartitionIds].
  public static final int SEGMENT_NOT_FOUND = Integer.MIN_VALUE;

  /// Partition ids without any segment, see [#getPartitionIds].
  protected static final Object2IntMap<String> EMPTY_SEGMENT_PARTITION_IDS =
      Object2IntMaps.unmodifiable(createSegmentPartitionIds(0));

  protected final Logger _logger = LoggerFactory.getLogger(getClass());

  protected HelixManager _helixManager;
  protected String _tableNameWithType;
  protected int _replication;
  protected String _partitionColumn;
  protected TableConfig _tableConfig;
  // Whether the segments of the table are pinned to their partition (see the class Javadoc), decided by
  // isPartitionPinned() on initialization
  protected boolean _partitionPinned;

  // Cache of segment name to the name of the tier it is eligible for (null value means not eligible for any tier), to
  // avoid reading each segment's ZK metadata on every rebalanceTiers() invocation.
  // NOTE:
  // 1. This cache is used for table rebalance only. During rebalance, rebalanceTable() (and thus rebalanceTiers()) can
  //    be invoked multiple times when the ideal state changes during the rebalance process. The eligible tier of a
  //    segment is derived from its ZK metadata, which is not expected to change during a rebalance, so it is resolved
  //    once (with a single bulk ZK read) and reused across invocations. A fresh SegmentAssignment instance is created
  //    per rebalance (see SegmentAssignmentFactory), so the cache is scoped to a single rebalance.
  // 2. Segments that are added after the initial bulk read (e.g. realtime segments that commit during a long
  //    rebalance) will be absent from the cache; they are resolved on demand and merged in, so they are still routed
  //    to the correct tier.
  @Nullable
  private Map<String, String> _segmentToTierName;

  // Cache of segment name to partition id from the segment ZK metadata, for the segments without partition id in their
  // name that have been requested, so that the target assignment can be recalculated without reading them again.
  // Follows the contract of the partition ids (see getPartitionIds()): NO_PARTITION_ID when the partition id cannot be
  // determined, SEGMENT_NOT_FOUND for the segments not read yet. Same scope as _segmentToTierName.
  private final Object2IntOpenHashMap<String> _segmentToPartitionId = new Object2IntOpenHashMap<>();

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _tableNameWithType = tableConfig.getTableName();
    _tableConfig = tableConfig;
    _replication = tableConfig.getReplication();
    _partitionColumn = TableConfigUtils.getPartitionColumn(_tableConfig);
    _partitionPinned = isPartitionPinned();
    _segmentToPartitionId.defaultReturnValue(SEGMENT_NOT_FOUND);
    if (_partitionColumn == null) {
      _logger.info("Initialized with replication: {} without partition column, partitionPinned: {} for table: {}",
          _replication, _partitionPinned, _tableNameWithType);
    } else {
      _logger.info("Initialized with replication: {} and partition column: {}, partitionPinned: {} for table: {}",
          _replication, _partitionColumn, _partitionPinned, _tableNameWithType);
    }
  }

  /// Returns whether the segments of the table are pinned to their partition (see the class Javadoc): the table uses
  /// strict replica-group routing. Subclasses add the conditions they need to pin the segments. A table that is not
  /// pinned although it needs to be is reported by [#warnIfNotPinned].
  protected boolean isPartitionPinned() {
    return SegmentAssignmentUtils.isStrictReplicaGroupRouting(_tableConfig);
  }

  /// Logs a warning when the segments of the table are not pinned to their partition although they must be collocated
  /// per partition (see [TableConfigUtils#requiresStrictReplicaGroupAssignment]), in which case they are assigned with
  /// the segment assignment strategy: an upsert or dedup table without strict replica-group routing (the beta
  /// multi-stage replica-group routing), or a table that cannot be pinned (see [#isPartitionPinned]). Called on table
  /// rebalance rather than on initialization, because an instance is created for every segment assignment.
  protected void warnIfNotPinned() {
    if (!_partitionPinned && TableConfigUtils.requiresStrictReplicaGroupAssignment(_tableConfig)) {
      _logger.warn("Segments of table: {} must be collocated per partition, but they are not pinned to their partition "
              + "because {}, so they can be split across instances when the instance partitions change",
          _tableNameWithType, SegmentAssignmentUtils.isStrictReplicaGroupRouting(_tableConfig)
              ? "the table has no partition column" : "the table does not use strict replica-group routing");
    }
  }

  /// Returns the instances of the given partition of a partition-pinned table, creating its entry in the partition
  /// assignment of the given ideal state when the partition has no entry yet, or an empty one (a rebalance in low disk
  /// mode without minimum available replicas empties the entry while it drops the partition from its current
  /// instances). The entry is created from the existing assignment of the partition (see [#getExistingAssignment])
  /// when there is one, else from the given candidate instances computed from the instance partitions. An existing
  /// entry wins over the candidate instances, so that the segments of the partition stay collocated when the instance
  /// partitions change: the partition moves on the next rebalance.
  protected List<String> getOrCreatePartitionInstances(IdealState idealState, int partitionId,
      List<String> candidateInstances) {
    List<String> instances = PartitionAssignmentUtils.getInstances(idealState, partitionId);
    if (CollectionUtils.isNotEmpty(instances)) {
      return instances;
    }
    Set<String> existingAssignment = getExistingAssignment(partitionId, idealState.getRecord().getMapFields());
    Collection<String> newInstances = existingAssignment != null ? existingAssignment : candidateInstances;
    _logger.info("Adding partition: {} with instances: {} from {} to the partition assignment for table: {}",
        partitionId, newInstances, existingAssignment != null ? "existing segments" : "instance partitions",
        _tableNameWithType);
    return PartitionAssignmentUtils.setInstances(idealState, partitionId, newInstances);
  }

  /// Returns the instances hosting the existing segments of the given partition, used to create the entry of a
  /// partition of a table created before the partition assignment was introduced, or `null` if the partition has no
  /// segment. Returns `null` by default.
  @Nullable
  protected Set<String> getExistingAssignment(int partitionId, Map<String, Map<String, String>> currentAssignment) {
    return null;
  }

  /// Returns the partition id of the given segment, read from the segment name when possible, and from the segment ZK
  /// metadata otherwise, unless read by an earlier call (see `_segmentToPartitionId`), or [#NO_PARTITION_ID] if the
  /// partition id cannot be determined, including when the segment has no ZK metadata. A failed read throws.
  protected int getPartitionId(String segmentName) {
    Integer partitionIdFromName = SegmentUtils.getPartitionIdFromSegmentName(segmentName);
    if (partitionIdFromName != null) {
      return partitionIdFromName;
    }
    int partitionId = _segmentToPartitionId.getInt(segmentName);
    if (partitionId == SEGMENT_NOT_FOUND) {
      partitionId = cachePartitionId(segmentName,
          ZKMetadataProvider.getSegmentZKMetadata(_helixManager.getHelixPropertyStore(), _tableNameWithType,
              segmentName));
    }
    return partitionId;
  }

  /// Returns the partition ids of the given segments, read from the segment name when possible, and from the segment
  /// ZK metadata otherwise. The ZK metadata of the segments without partition id in their name is read with a single
  /// batched request, except for the segments read by an earlier call (see `_segmentToPartitionId`), so that a table
  /// whose segment names carry the partition id (e.g. LLC segments) reads none.
  ///
  /// Every given segment is in the returned map, with [#NO_PARTITION_ID] when its partition id cannot be determined,
  /// including when the segment has no ZK metadata. A failed read throws instead, so that it is not taken (and
  /// cached) as a segment without partition id. The map is looked up with `getInt`, which returns
  /// [#SEGMENT_NOT_FOUND] for a segment not in it. Both values are negative, which partition ids never are.
  protected Object2IntMap<String> getPartitionIds(Collection<String> segmentNames) {
    Object2IntOpenHashMap<String> partitionIds = createSegmentPartitionIds(segmentNames.size());
    List<String> segmentsToRead = new ArrayList<>();
    for (String segmentName : segmentNames) {
      Integer partitionIdFromName = SegmentUtils.getPartitionIdFromSegmentName(segmentName);
      if (partitionIdFromName != null) {
        partitionIds.put(segmentName, partitionIdFromName.intValue());
        continue;
      }
      int partitionId = _segmentToPartitionId.getInt(segmentName);
      if (partitionId == SEGMENT_NOT_FOUND) {
        segmentsToRead.add(segmentName);
      } else {
        partitionIds.put(segmentName, partitionId);
      }
    }
    if (!segmentsToRead.isEmpty()) {
      List<SegmentZKMetadata> segmentsZKMetadata =
          ZKMetadataProvider.getSegmentsZKMetadata(_helixManager.getHelixPropertyStore(), _tableNameWithType,
              segmentsToRead, null, true);
      for (int i = 0; i < segmentsToRead.size(); i++) {
        String segmentName = segmentsToRead.get(i);
        partitionIds.put(segmentName, cachePartitionId(segmentName, segmentsZKMetadata.get(i)));
      }
    }
    return partitionIds;
  }

  /// Returns the partition id of a segment without partition id in its name from its ZK metadata (`null` when the
  /// segment has none), or [#NO_PARTITION_ID] if it cannot be determined, and caches it in `_segmentToPartitionId`.
  private int cachePartitionId(String segmentName, @Nullable SegmentZKMetadata segmentZKMetadata) {
    Integer partitionId =
        segmentZKMetadata != null ? SegmentUtils.getSegmentPartitionId(segmentZKMetadata, _partitionColumn) : null;
    int cachedPartitionId = partitionId != null ? partitionId : NO_PARTITION_ID;
    _segmentToPartitionId.put(segmentName, cachedPartitionId);
    return cachedPartitionId;
  }

  /// Returns an empty map for the partition ids of the given number of segments (see [#getPartitionIds]), which
  /// returns [#SEGMENT_NOT_FOUND] for the segments not in it.
  private static Object2IntOpenHashMap<String> createSegmentPartitionIds(int expectedSize) {
    Object2IntOpenHashMap<String> segmentPartitionIds = new Object2IntOpenHashMap<>(expectedSize);
    segmentPartitionIds.defaultReturnValue(SEGMENT_NOT_FOUND);
    return segmentPartitionIds;
  }

  /// Rebalances tiers and returns a pair of tier assignments and non-tier assignment.
  protected Pair<List<Map<String, Map<String, String>>>, Map<String, Map<String, String>>> rebalanceTiers(
      Map<String, Map<String, String>> currentAssignment, @Nullable List<Tier> sortedTiers,
      @Nullable Map<String, InstancePartitions> tierInstancePartitionsMap, boolean bootstrap) {
    if (sortedTiers == null) {
      return Pair.of(null, currentAssignment);
    }

    Preconditions.checkState(tierInstancePartitionsMap != null, "Tier to instancePartitions map is null");
    _logger.info("Rebalancing tiers: {} for table: {} with bootstrap: {}", tierInstancePartitionsMap.keySet(),
        _tableNameWithType, bootstrap);

    // Resolve the eligible tier of each segment once (single bulk ZK read) and reuse it across the multiple
    // rebalanceTiers() invocations of this rebalance, instead of reading each segment's ZK metadata every time.
    // Any new segments come later would not be in _segmentToTierName map, and categorized in the null (default) tier.
    if (_segmentToTierName == null) {
      _segmentToTierName =
          SegmentAssignmentUtils.getSegmentToTierNameMap(_helixManager, _tableNameWithType, sortedTiers);
    }

    // Get tier to segment assignment map i.e. current assignments split by tiers they are eligible for
    SegmentAssignmentUtils.TierSegmentAssignment tierSegmentAssignment =
        new SegmentAssignmentUtils.TierSegmentAssignment(sortedTiers, currentAssignment, _segmentToTierName);
    Map<String, Map<String, Map<String, String>>> tierNameToSegmentAssignmentMap =
        tierSegmentAssignment.getTierNameToSegmentAssignmentMap();

    // For each tier, calculate new assignment using instancePartitions for that tier
    List<Map<String, Map<String, String>>> newTierAssignments = new ArrayList<>(tierNameToSegmentAssignmentMap.size());
    for (Map.Entry<String, Map<String, Map<String, String>>> entry : tierNameToSegmentAssignmentMap.entrySet()) {
      String tierName = entry.getKey();
      Map<String, Map<String, String>> tierCurrentAssignment = entry.getValue();

      InstancePartitions tierInstancePartitions = tierInstancePartitionsMap.get(tierName);
      Preconditions.checkNotNull(tierInstancePartitions, "Failed to find instance partitions for tier: %s of table: %s",
          tierName, _tableNameWithType);

      // Initialize segment assignment strategy based on the tier instance partitions
      SegmentAssignmentStrategy segmentAssignmentStrategy =
          SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(_helixManager, _tableConfig, tierName,
              tierInstancePartitions);

      _logger.info("Rebalancing tier: {} for table: {} with bootstrap: {}, instance partitions: {}", tierName,
          _tableNameWithType, bootstrap, tierInstancePartitions);
      newTierAssignments.add(reassignSegments(tierName, tierCurrentAssignment, tierInstancePartitions, bootstrap,
          segmentAssignmentStrategy));
    }

    return Pair.of(newTierAssignments, tierSegmentAssignment.getNonTierSegmentAssignment());
  }

  /// Rebalances segments in the current assignment using the instancePartitions and returns new assignment
  protected Map<String, Map<String, String>> reassignSegments(String instancePartitionType,
      Map<String, Map<String, String>> currentAssignment, InstancePartitions instancePartitions, boolean bootstrap,
      SegmentAssignmentStrategy segmentAssignmentStrategy) {
    Map<String, Map<String, String>> newAssignment;
    if (bootstrap) {
      _logger.info("Bootstrapping segment assignment for {} segments of table: {}", instancePartitionType,
          _tableNameWithType);

      // When bootstrap is enabled, start with an empty assignment and reassign all segments
      newAssignment = new TreeMap<>();
      for (String segment : currentAssignment.keySet()) {
        List<String> assignedInstances =
            segmentAssignmentStrategy.assignSegment(segment, newAssignment, instancePartitions);
        newAssignment
            .put(segment, SegmentAssignmentUtils.getInstanceStateMap(assignedInstances, SegmentStateModel.ONLINE));
      }
    } else {
      // Use segment assignment strategy
      newAssignment =
          segmentAssignmentStrategy.reassignSegments(currentAssignment, instancePartitions);
    }
    return newAssignment;
  }
}
