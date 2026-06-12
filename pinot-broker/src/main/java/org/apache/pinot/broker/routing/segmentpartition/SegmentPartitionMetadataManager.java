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
package org.apache.pinot.broker.routing.segmentpartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetchListener;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo.PartitionInfo;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The `PartitionDataManager` manages partitions of a table. It manages
///   1. all the online segments associated with the partition and their allocated servers
///   2. all the replica of a specific segment.
/// It provides API to query
///   1. For each partition ID, what are the servers that contains ALL segments belong to this partition ID.
///   2. For each server, what are all the partition IDs and list of segments of those partition IDs on this server.
///
/// When computing the fully replicated servers for a partition, segments that have not been fully replicated per
/// their latest ideal assignment (newly pushed segments, new consuming segments, or segments uploaded under a new
/// name to replace other segments) are excluded from the partition info until all their replicas become available,
/// so that they don't reduce the fully replicated servers while loading. This protection is bounded by the configured
/// new segment expiration time since the ideal assignment of the segment last changed; after that the segment is
/// treated as a regular segment, so that a replica failing to load doesn't exclude the segment from being served
/// forever. Segments without any available replica are also excluded because they cannot be served regardless of the
/// server picked, and are reported via [TablePartitionReplicatedServersInfo#getUnavailableSegments()].
public class SegmentPartitionMetadataManager implements SegmentZkMetadataFetchListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPartitionMetadataManager.class);
  private static final int INVALID_PARTITION_ID = -1;
  private static final long INVALID_CREATION_TIME_MS = -1L;

  private final String _tableNameWithType;

  // static content, if anything changes for the following. a rebuild of routing table is needed.
  private final String _partitionColumn;
  private final String _partitionFunctionName;
  private final int _numPartitions;
  private final long _newSegmentExpirationMs;

  // cache-able content, only follow changes if onlineSegments list (of ideal-state) is changed.
  private final Map<String, SegmentInfo> _segmentInfoMap = new HashMap<>();

  // computed value based on status change.
  private transient TablePartitionInfo _tablePartitionInfo;
  private transient TablePartitionReplicatedServersInfo _tablePartitionReplicatedServersInfo;

  public SegmentPartitionMetadataManager(String tableNameWithType, String partitionColumn, String partitionFunctionName,
      int numPartitions, long newSegmentExpirationMs) {
    _tableNameWithType = tableNameWithType;
    _partitionColumn = partitionColumn;
    _partitionFunctionName = partitionFunctionName;
    _numPartitions = numPartitions;
    _newSegmentExpirationMs = newSegmentExpirationMs;
  }

  @Override
  public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
      List<ZNRecord> znRecords) {
    long currentTimeMs = System.currentTimeMillis();
    int numSegments = onlineSegments.size();
    for (int i = 0; i < numSegments; i++) {
      String segment = onlineSegments.get(i);
      ZNRecord znRecord = znRecords.get(i);
      SegmentInfo segmentInfo = new SegmentInfo(getPartitionId(segment, znRecord), getCreationTimeMs(znRecord));
      segmentInfo.updateServers(getOnlineServers(externalView, segment), idealState.getInstanceStateMap(segment),
          currentTimeMs);
      _segmentInfoMap.put(segment, segmentInfo);
    }
    computeAllTablePartitionInfo();
  }

  private int getPartitionId(String segment, @Nullable ZNRecord znRecord) {
    SegmentPartitionInfo segmentPartitionInfo =
        SegmentPartitionUtils.extractPartitionInfo(_tableNameWithType, _partitionColumn, segment, znRecord);
    if (segmentPartitionInfo == null || segmentPartitionInfo == SegmentPartitionUtils.INVALID_PARTITION_INFO) {
      return INVALID_PARTITION_ID;
    }
    if (!_partitionColumn.equals(segmentPartitionInfo.getPartitionColumn())) {
      return INVALID_PARTITION_ID;
    }
    PartitionFunction partitionFunction = segmentPartitionInfo.getPartitionFunction();
    if (!_partitionFunctionName.equalsIgnoreCase(partitionFunction.getName())) {
      return INVALID_PARTITION_ID;
    }
    if (_numPartitions != partitionFunction.getNumPartitions()) {
      return INVALID_PARTITION_ID;
    }
    Set<Integer> partitions = segmentPartitionInfo.getPartitions();
    if (partitions.size() != 1) {
      return INVALID_PARTITION_ID;
    }
    return partitions.iterator().next();
  }

  private static long getCreationTimeMs(@Nullable ZNRecord znRecord) {
    if (znRecord == null) {
      return INVALID_CREATION_TIME_MS;
    }
    return SegmentUtils.getSegmentCreationTimeMs(new SegmentZKMetadata(znRecord));
  }

  private static boolean isServingState(String instanceState) {
    return instanceState.equals(SegmentStateModel.ONLINE) || instanceState.equals(SegmentStateModel.CONSUMING);
  }

  private static List<String> getOnlineServers(ExternalView externalView, String segment) {
    Map<String, String> instanceStateMap = externalView.getStateMap(segment);
    if (instanceStateMap == null) {
      return List.of();
    }
    List<String> onlineServers = new ArrayList<>(instanceStateMap.size());
    for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
      if (isServingState(entry.getValue())) {
        onlineServers.add(entry.getKey());
      }
    }
    return onlineServers;
  }

  @Override
  public synchronized void onAssignmentChange(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments, List<String> pulledSegments, List<ZNRecord> znRecords) {
    // Update segment partition id for the pulled segments
    int numSegments = pulledSegments.size();
    for (int i = 0; i < numSegments; i++) {
      String segment = pulledSegments.get(i);
      ZNRecord znRecord = znRecords.get(i);
      _segmentInfoMap.put(segment, new SegmentInfo(getPartitionId(segment, znRecord), getCreationTimeMs(znRecord)));
    }
    // Update online servers and replication state for all online segments
    long currentTimeMs = System.currentTimeMillis();
    for (String segment : onlineSegments) {
      SegmentInfo segmentInfo = _segmentInfoMap.get(segment);
      if (segmentInfo == null) {
        // NOTE: This should not happen, but we still handle it gracefully by adding an invalid SegmentInfo
        LOGGER.error("Failed to find segment info for segment: {} in table: {} while handling assignment change",
            segment, _tableNameWithType);
        segmentInfo = new SegmentInfo(INVALID_PARTITION_ID, INVALID_CREATION_TIME_MS);
        _segmentInfoMap.put(segment, segmentInfo);
      }
      segmentInfo.updateServers(getOnlineServers(externalView, segment), idealState.getInstanceStateMap(segment),
          currentTimeMs);
    }
    _segmentInfoMap.keySet().retainAll(onlineSegments);
    computeAllTablePartitionInfo();
  }

  @Override
  public synchronized void refreshSegment(String segment, @Nullable ZNRecord znRecord) {
    int partitionId = getPartitionId(segment, znRecord);
    long pushTimeMs = getCreationTimeMs(znRecord);
    SegmentInfo segmentInfo = _segmentInfoMap.get(segment);
    if (segmentInfo == null) {
      // NOTE: This should not happen, but we still handle it gracefully by adding an invalid SegmentInfo
      LOGGER.error("Failed to find segment info for segment: {} in table: {} while handling segment refresh", segment,
          _tableNameWithType);
      segmentInfo = new SegmentInfo(partitionId, pushTimeMs);
      _segmentInfoMap.put(segment, segmentInfo);
    } else {
      segmentInfo._partitionId = partitionId;
      segmentInfo._creationTimeMs = pushTimeMs;
    }
    computeAllTablePartitionInfo();
  }

  public TablePartitionInfo getTablePartitionInfo() {
    return _tablePartitionInfo;
  }

  public TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo() {
    return _tablePartitionReplicatedServersInfo;
  }

  private void computeAllTablePartitionInfo() {
    computeTablePartitionReplicatedServersInfo();
    computeTablePartitionInfo();
  }

  private void computeTablePartitionReplicatedServersInfo() {
    PartitionInfo[] partitionInfoMap = new PartitionInfo[_numPartitions];
    List<String> segmentsWithInvalidPartition = new ArrayList<>();
    List<Pair<String, Integer>> unavailableSegments = new ArrayList<>();
    List<Triple<String, Integer, Integer>> segmentsReducingFullyReplicatedServers = new ArrayList<>();
    List<Map.Entry<String, SegmentInfo>> pendingSegmentInfoEntries = new ArrayList<>();
    long currentTimeMs = System.currentTimeMillis();
    for (Map.Entry<String, SegmentInfo> entry : _segmentInfoMap.entrySet()) {
      String segment = entry.getKey();
      SegmentInfo segmentInfo = entry.getValue();
      int partitionId = segmentInfo._partitionId;
      if (partitionId == INVALID_PARTITION_ID || partitionId >= _numPartitions) {
        segmentsWithInvalidPartition.add(segment);
        continue;
      }
      // Process pending (not fully replicated yet) segments in the end
      if (isPendingSegment(segmentInfo, currentTimeMs)) {
        pendingSegmentInfoEntries.add(entry);
        continue;
      }
      List<String> onlineServers = segmentInfo._onlineServers;
      PartitionInfo partitionInfo = partitionInfoMap[partitionId];
      if (onlineServers.isEmpty()) {
        // The segment has no available replica, thus it cannot be served regardless of the server picked. Exclude it
        // from the partition info instead of clearing the fully replicated servers, which would fail all the queries
        // on the partition without making the segment available.
        unavailableSegments.add(Pair.of(segment, partitionId));
      } else {
        if (partitionInfo == null) {
          Set<String> fullyReplicatedServers = new HashSet<>(onlineServers);
          List<String> segments = new ArrayList<>();
          segments.add(segment);
          partitionInfo = new PartitionInfo(fullyReplicatedServers, segments);
          partitionInfoMap[partitionId] = partitionInfo;
        } else {
          if (partitionInfo._fullyReplicatedServers.retainAll(onlineServers)) {
            segmentsReducingFullyReplicatedServers.add(
                Triple.of(segment, partitionId, partitionInfo._fullyReplicatedServers.size()));
          }
          partitionInfo._segments.add(segment);
        }
      }
    }
    if (!segmentsWithInvalidPartition.isEmpty()) {
      int numSegments = segmentsWithInvalidPartition.size();
      LOGGER.warn("(table-partition-rs-info) Found {} segments: {} with invalid partition in table: {}", numSegments,
          numSegments <= 10 ? segmentsWithInvalidPartition : segmentsWithInvalidPartition.subList(0, 10) + "...",
          _tableNameWithType);
    }
    if (!unavailableSegments.isEmpty()) {
      int numSegments = unavailableSegments.size();
      LOGGER.warn("Found {} unavailable segments (name,partition): {} in table: {}", numSegments,
          numSegments <= 10 ? unavailableSegments : unavailableSegments.subList(0, 10) + "...", _tableNameWithType);
    }
    if (!segmentsReducingFullyReplicatedServers.isEmpty()) {
      int numSegments = segmentsReducingFullyReplicatedServers.size();
      LOGGER.warn("Found {} segments (name,partition,reducedTo): {} reducing fully replicated servers in table: {}",
          numSegments, numSegments <= 10 ? segmentsReducingFullyReplicatedServers
              : segmentsReducingFullyReplicatedServers.subList(0, 10) + "...", _tableNameWithType);
    }
    // Process pending segments
    if (!pendingSegmentInfoEntries.isEmpty()) {
      List<String> excludedPendingSegments = new ArrayList<>();
      for (Map.Entry<String, SegmentInfo> entry : pendingSegmentInfoEntries) {
        String segment = entry.getKey();
        SegmentInfo segmentInfo = entry.getValue();
        int partitionId = segmentInfo._partitionId;
        List<String> onlineServers = segmentInfo._onlineServers;
        PartitionInfo partitionInfo = partitionInfoMap[partitionId];
        if (partitionInfo == null) {
          // If the pending segment is the first segment of a partition, treat it as regular segment if it has
          // available replicas
          if (!onlineServers.isEmpty()) {
            Set<String> fullyReplicatedServers = new HashSet<>(onlineServers);
            List<String> segments = new ArrayList<>();
            segments.add(segment);
            partitionInfo = new PartitionInfo(fullyReplicatedServers, segments);
            partitionInfoMap[partitionId] = partitionInfo;
          } else {
            excludedPendingSegments.add(segment);
          }
        } else {
          // If the pending segment is not the first segment of a partition, add it only if it won't reduce the fully
          // replicated servers. It is common that a segment not fully replicated yet (newly pushed, a new consuming
          // segment, or a segment uploaded under a new name to replace other segments) doesn't have all the replicas
          // available, and we want to exclude it from the partition info until all the replicas are available.
          //noinspection SlowListContainsAll
          if (onlineServers.containsAll(partitionInfo._fullyReplicatedServers)) {
            partitionInfo._segments.add(segment);
          } else {
            excludedPendingSegments.add(segment);
          }
        }
      }
      if (!excludedPendingSegments.isEmpty()) {
        int numSegments = excludedPendingSegments.size();
        LOGGER.info("Excluded {} pending segments: {}... without all replicas available in table: {}", numSegments,
            numSegments <= 10 ? excludedPendingSegments : excludedPendingSegments.subList(0, 10) + "...",
            _tableNameWithType);
      }
    }
    List<String> unavailableSegmentNames;
    if (unavailableSegments.isEmpty()) {
      unavailableSegmentNames = List.of();
    } else {
      unavailableSegmentNames = new ArrayList<>(unavailableSegments.size());
      for (Pair<String, Integer> unavailableSegment : unavailableSegments) {
        unavailableSegmentNames.add(unavailableSegment.getLeft());
      }
    }
    _tablePartitionReplicatedServersInfo =
        new TablePartitionReplicatedServersInfo(_tableNameWithType, _partitionColumn, _partitionFunctionName,
            _numPartitions, partitionInfoMap, segmentsWithInvalidPartition, unavailableSegmentNames);
  }

  /// Returns whether the segment is pending, i.e. it has not been fully replicated per its latest ideal assignment.
  /// Pending segments (newly pushed segments, new consuming segments, or segments uploaded under a new name to
  /// replace other segments) are excluded from the partition info if they would reduce the fully replicated servers,
  /// until all their replicas become available. The pending state is bounded by the configured new segment expiration
  /// time since the ideal assignment of the segment last changed, so that a replica failing to load doesn't exclude
  /// the segment from being served forever. When the target servers cannot be determined from the ideal state, fall
  /// back to the time based check on the segment creation time.
  private boolean isPendingSegment(SegmentInfo segmentInfo, long currentTimeMs) {
    if (segmentInfo._fullyReplicatedOnce) {
      return false;
    }
    if (segmentInfo._numTargetServers > 0) {
      return currentTimeMs - segmentInfo._targetServersUpdateTimeMs <= _newSegmentExpirationMs;
    }
    return InstanceSelector.isNewSegment(segmentInfo._creationTimeMs, currentTimeMs, _newSegmentExpirationMs);
  }

  private void computeTablePartitionInfo() {
    List<List<String>> segmentsByPartition = new ArrayList<>();
    for (int i = 0; i < _numPartitions; i++) {
      segmentsByPartition.add(new ArrayList<>());
    }
    List<String> segmentsWithInvalidPartition = new ArrayList<>();
    for (Map.Entry<String, SegmentInfo> entry : _segmentInfoMap.entrySet()) {
      String segment = entry.getKey();
      SegmentInfo segmentInfo = entry.getValue();
      int partitionId = segmentInfo._partitionId;
      if (partitionId == INVALID_PARTITION_ID) {
        segmentsWithInvalidPartition.add(segment);
      } else if (partitionId < segmentsByPartition.size()) {
        segmentsByPartition.get(partitionId).add(segment);
      } else {
        LOGGER.warn("Found segment: {} with partitionId: {} larger than numPartitions: {} in table: {}",
            segment, partitionId, _numPartitions, _tableNameWithType);
        segmentsWithInvalidPartition.add(segment);
      }
    }
    if (!segmentsWithInvalidPartition.isEmpty()) {
      int numSegments = segmentsWithInvalidPartition.size();
      LOGGER.warn("(table-partition-info) Found {} segments: {} with invalid partition in table: {}", numSegments,
          numSegments <= 10 ? segmentsWithInvalidPartition : segmentsWithInvalidPartition.subList(0, 10) + "...",
          _tableNameWithType);
    }
    _tablePartitionInfo =
        new TablePartitionInfo(_tableNameWithType, _partitionColumn, _partitionFunctionName, _numPartitions,
            segmentsByPartition, segmentsWithInvalidPartition);
  }

  private static class SegmentInfo {
    int _partitionId;
    long _creationTimeMs;
    List<String> _onlineServers = List.of();
    /// Number of target servers (servers with ONLINE/CONSUMING state in the ideal state). The target servers are
    /// tracked as count and order-independent hash instead of the full server set to reduce the memory footprint.
    int _numTargetServers;
    /// Order-independent hash of the target servers, used together with the count to detect ideal assignment changes.
    /// A hash collision can only delay the reset of the replication state, which is benign.
    int _targetServersHash;
    /// Time when the target servers were last changed, used to bound how long the segment can stay pending
    long _targetServersUpdateTimeMs;
    /// Whether the segment has been observed fully replicated (online servers covering the target servers from the
    /// ideal state) at least once since its target servers last changed. Once fully replicated, losing a replica
    /// reduces the fully replicated servers of the partition (queries are routed to the remaining replicas); before
    /// that, the segment is excluded from the partition info to not reduce the fully replicated servers while its
    /// replicas are still loading.
    boolean _fullyReplicatedOnce;

    SegmentInfo(int partitionId, long creationTimeMs) {
      _partitionId = partitionId;
      _creationTimeMs = creationTimeMs;
    }

    void updateServers(List<String> onlineServers, @Nullable Map<String, String> targetInstanceStateMap,
        long currentTimeMs) {
      _onlineServers = onlineServers;
      int numTargetServers = 0;
      int targetServersHash = 0;
      boolean coversTargetServers = true;
      if (targetInstanceStateMap != null) {
        for (Map.Entry<String, String> entry : targetInstanceStateMap.entrySet()) {
          if (isServingState(entry.getValue())) {
            String server = entry.getKey();
            numTargetServers++;
            targetServersHash += server.hashCode();
            if (coversTargetServers && !onlineServers.contains(server)) {
              coversTargetServers = false;
            }
          }
        }
      }
      // Recompute the replication state when the ideal assignment changes (e.g. rebalance)
      if (numTargetServers != _numTargetServers || targetServersHash != _targetServersHash) {
        _numTargetServers = numTargetServers;
        _targetServersHash = targetServersHash;
        _targetServersUpdateTimeMs = currentTimeMs;
        _fullyReplicatedOnce = false;
      }
      if (!_fullyReplicatedOnce && numTargetServers > 0 && coversTargetServers) {
        _fullyReplicatedOnce = true;
      }
    }
  }
}
