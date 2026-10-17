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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
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
///   instance partitions, and the segments follow their partition.
public abstract class BaseSegmentAssignment implements SegmentAssignment {
  protected final Logger _logger = LoggerFactory.getLogger(getClass());

  protected HelixManager _helixManager;
  protected String _tableNameWithType;
  protected int _replication;
  protected String _partitionColumn;
  protected TableConfig _tableConfig;
  protected ControllerMetrics _controllerMetrics;
  // Whether all segments of a partition must be assigned to the same instances (strict replica-group routing), see
  // the class Javadoc. Subclasses may further require the instance partitions to support it.
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

  // Cache of segment name to partition id (null value means the partition id cannot be determined) for the segments
  // whose partition id is not in the segment name, loaded with a single bulk ZK read on first use. Same scope and
  // limitations as _segmentToTierName; a segment absent from the cache is resolved on demand and merged in.
  @Nullable
  private Map<String, Integer> _segmentToPartitionId;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig, @Nullable ControllerMetrics controllerMetrics) {
    _helixManager = helixManager;
    _tableNameWithType = tableConfig.getTableName();
    _tableConfig = tableConfig;
    _replication = tableConfig.getReplication();
    _partitionColumn = TableConfigUtils.getPartitionColumn(_tableConfig);
    _controllerMetrics = controllerMetrics;
    _partitionPinned = SegmentAssignmentUtils.isStrictReplicaGroupRouting(tableConfig);
    if (_partitionColumn == null) {
      _logger.info("Initialized with replication: {} without partition column, partitionPinned: {} for table: {}",
          _replication, _partitionPinned, _tableNameWithType);
    } else {
      _logger.info("Initialized with replication: {} and partition column: {}, partitionPinned: {} for table: {}",
          _replication, _partitionColumn, _partitionPinned, _tableNameWithType);
    }
  }

  /// Returns the instances of the given partition of a partition-pinned table, creating its entry in the partition
  /// assignment of the given ideal state when the partition has no entry yet. A missing entry is created from the
  /// existing assignment of the partition when the partition already has segments (a table created before the
  /// partition assignment was introduced), else from the given candidate instances computed from the instance
  /// partitions.
  ///
  /// When the entry differs from the candidate instances, the instance partitions have changed since the partition
  /// was placed and the table needs a rebalance. The mismatch is logged and metered, and the entry wins so that the
  /// segments of the partition stay collocated.
  protected List<String> getOrCreatePartitionInstances(IdealState idealState, int partitionId,
      List<String> candidateInstances, Supplier<Collection<String>> existingAssignmentSupplier) {
    List<String> instances = PartitionAssignmentUtils.getInstances(idealState, partitionId);
    if (instances == null) {
      Collection<String> existingAssignment = existingAssignmentSupplier.get();
      if (existingAssignment != null) {
        _logger.info("Adding partition: {} with instances: {} from existing segments to the partition assignment for "
            + "table: {}", partitionId, existingAssignment, _tableNameWithType);
        instances = PartitionAssignmentUtils.toInstanceList(existingAssignment);
      } else {
        _logger.info("Adding partition: {} with instances: {} from instance partitions to the partition assignment "
            + "for table: {}", partitionId, candidateInstances, _tableNameWithType);
        instances = PartitionAssignmentUtils.toInstanceList(candidateInstances);
      }
      PartitionAssignmentUtils.setInstances(idealState, partitionId, instances);
    }
    if (instances.size() != candidateInstances.size() || !instances.containsAll(candidateInstances)) {
      _logger.warn("Instances: {} of partition: {} in the partition assignment are inconsistent with the ones: {} "
              + "computed from the instance partitions for table: {}, using the ones from the partition assignment",
          instances, partitionId, candidateInstances, _tableNameWithType);
      if (_controllerMetrics != null) {
        _controllerMetrics.addMeteredTableValue(_tableNameWithType,
            ControllerMeter.CONTROLLER_REALTIME_TABLE_SEGMENT_ASSIGNMENT_MISMATCH, 1L);
      }
    }
    return instances;
  }

  /// Returns the partition id of the given segment for table rebalance, or `null` if it cannot be determined. The
  /// partition id is read from the segment name when possible, and from the segment ZK metadata otherwise, which is
  /// fetched for all segments of the table with a single bulk ZK read on first use (see `_segmentToPartitionId`).
  @Nullable
  protected Integer getPartitionIdForRebalance(String segmentName) {
    Integer partitionId = SegmentUtils.getPartitionIdFromSegmentName(segmentName);
    if (partitionId != null) {
      return partitionId;
    }
    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixManager.getHelixPropertyStore();
    if (_segmentToPartitionId == null) {
      _segmentToPartitionId = new HashMap<>();
      for (SegmentZKMetadata segmentZKMetadata : ZKMetadataProvider.getSegmentsZKMetadata(propertyStore,
          _tableNameWithType)) {
        _segmentToPartitionId.put(segmentZKMetadata.getSegmentName(),
            SegmentUtils.getSegmentPartitionId(segmentZKMetadata, _partitionColumn));
      }
    }
    if (!_segmentToPartitionId.containsKey(segmentName)) {
      SegmentZKMetadata segmentZKMetadata =
          ZKMetadataProvider.getSegmentZKMetadata(propertyStore, _tableNameWithType, segmentName);
      _segmentToPartitionId.put(segmentName,
          segmentZKMetadata != null ? SegmentUtils.getSegmentPartitionId(segmentZKMetadata, _partitionColumn) : null);
    }
    return _segmentToPartitionId.get(segmentName);
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
