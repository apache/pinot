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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetchListener;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * The {@code PartitionDataManager} manages partitions of a table. It manages
 *   1. all the online segments associated with the partition and their allocated servers
 *   2. all the replica of a specific segment.
 * It provides API to query
 *   1. For each partition ID, what are the servers that contains ALL segments belong to this partition ID.
 *   2. For each server, what are all the partition IDs and list of segments of those partition IDs on this server.
 */
public class SegmentPartitionMetadataManager implements SegmentZkMetadataFetchListener {
  private final String _tableNameWithType;

  // static content, if anything changes for the following. a rebuild of routing table is needed.
  private final String _partitionColumn;
  private final String _partitionFunctionName;
  private final int _numPartitions;

  // cache-able content, only follow changes if onlineSegments list (of ideal-state) is changed.
  private final Map<String, List<String>> _segmentToOnlineServersMap = new HashMap<>();
  private final Map<String, Integer> _segmentToPartitionMap = new HashMap<>();

  // computed value based on status change.
  private Map<Integer, Set<String>> _partitionToFullyReplicatedServersMap;
  private Map<Integer, List<String>> _partitionToSegmentsMap;

  public SegmentPartitionMetadataManager(String tableNameWithType, String partitionColumn, String partitionFunctionName,
      int numPartitions) {
    _tableNameWithType = tableNameWithType;
    _partitionColumn = partitionColumn;
    _partitionFunctionName = partitionFunctionName;
    _numPartitions = numPartitions;
  }

  @Override
  public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
      List<ZNRecord> znRecords) {
    for (int idx = 0; idx < onlineSegments.size(); idx++) {
      String segment = onlineSegments.get(idx);
      ZNRecord znRecord = znRecords.get(idx);
      // extract single partition info
      SegmentPartitionInfo segmentPartitionInfo = SegmentPartitionUtils.extractPartitionInfoFromSegmentZKMetadata(
          _tableNameWithType, _partitionColumn, segment, znRecord);
      if (validate(segmentPartitionInfo)) {
        // update segment to partition map
        Integer partitionId = segmentPartitionInfo.getPartitions().iterator().next();
        _segmentToPartitionMap.put(segment, partitionId);
      }
      // update segment to server list.
      updateSegmentServerOnlineMap(externalView, segment);
    }
    computePartitionMaps();
  }

  private void updateSegmentServerOnlineMap(ExternalView externalView, String segment) {
    Map<String, String> instanceStateMap = externalView.getStateMap(segment);
    List<String> serverList = instanceStateMap == null ? Collections.emptyList()
        : instanceStateMap.entrySet().stream()
            .filter(entry -> CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE.equals(entry.getValue()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    _segmentToOnlineServersMap.put(segment, serverList);
  }

  private boolean validate(SegmentPartitionInfo segmentPartitionInfo) {
    if (segmentPartitionInfo == null || segmentPartitionInfo.getPartitionFunction() == null
        || segmentPartitionInfo.getPartitions() == null) {
      return false;
    }
    // TODO: check more than just function name here but also function config.
    return _partitionFunctionName.equals(segmentPartitionInfo.getPartitionFunction().getName())
        && _partitionColumn.equals(segmentPartitionInfo.getPartitionColumn())
        && _numPartitions == segmentPartitionInfo.getNumPartitions()
        && segmentPartitionInfo.getPartitions().size() == 1;
  }

  @Override
  public synchronized void onAssignmentChange(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments, List<String> pulledSegments, List<ZNRecord> znRecords) {
    // update segment zk metadata for the pulled segments
    for (int idx = 0; idx < pulledSegments.size(); idx++) {
      String segment = pulledSegments.get(idx);
      ZNRecord znRecord = znRecords.get(idx);
      SegmentPartitionInfo segmentPartitionInfo = SegmentPartitionUtils.extractPartitionInfoFromSegmentZKMetadata(
          _tableNameWithType, _partitionColumn, segment, znRecord);
      if (validate(segmentPartitionInfo)) {
        // update segment to partition map
        Integer partitionId = segmentPartitionInfo.getPartitions().iterator().next();
        _segmentToPartitionMap.put(segment, partitionId);
      } else {
        // remove the segment from the partition map
        _segmentToPartitionMap.remove(segment);
      }
    }
    // update the server online information based on external view.
    for (String onlineSegment : onlineSegments) {
      // update segment to server list.
      updateSegmentServerOnlineMap(externalView, onlineSegment);
    }
    _segmentToPartitionMap.keySet().retainAll(onlineSegments);
    _segmentToOnlineServersMap.keySet().retainAll(onlineSegments);
    computePartitionMaps();
  }

  @Override
  public synchronized void refreshSegment(String segment, @Nullable ZNRecord znRecord) {
    SegmentPartitionInfo segmentPartitionInfo = SegmentPartitionUtils.extractPartitionInfoFromSegmentZKMetadata(
        _tableNameWithType, _partitionColumn, segment, znRecord);
    if (validate(segmentPartitionInfo)) {
      // update segment to partition map
      Integer partitionId = segmentPartitionInfo.getPartitions().iterator().next();
      _segmentToPartitionMap.put(segment, partitionId);
    } else {
      // remove the segment from the partition map
      _segmentToPartitionMap.remove(segment);
    }
    computePartitionMaps();
  }

  public Map<Integer, Set<String>> getPartitionToFullyReplicatedServersMap() {
    return _partitionToFullyReplicatedServersMap;
  }

  public Map<Integer, List<String>> getPartitionToSegmentsMap() {
    return _partitionToSegmentsMap;
  }

  private void computePartitionMaps() {
    Map<Integer, Set<String>> partitionToFullyReplicatedServersMap = new HashMap<>();
    Map<Integer, List<String>> partitionToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, List<String>> segmentServerEntry : _segmentToOnlineServersMap.entrySet()) {
      String segment = segmentServerEntry.getKey();
      int partitionId = _segmentToPartitionMap.getOrDefault(segment, -1);
      if (partitionId >= 0) {
        // update the partition to full replicate server set map.
        Set<String> existingServerSet = partitionToFullyReplicatedServersMap.get(partitionId);
        if (existingServerSet == null) {
          existingServerSet = new HashSet<>(segmentServerEntry.getValue());
        } else {
          existingServerSet.retainAll(segmentServerEntry.getValue());
        }
        partitionToFullyReplicatedServersMap.put(partitionId, existingServerSet);
        // update the partition to list of segments map
        List<String> segmentList = partitionToSegmentsMap.get(partitionId);
        if (segmentList == null) {
          segmentList = new ArrayList<>();
        }
        segmentList.add(segment);
        partitionToSegmentsMap.put(partitionId, segmentList);
      }
    }
    _partitionToFullyReplicatedServersMap = partitionToFullyReplicatedServersMap;
    _partitionToSegmentsMap = partitionToSegmentsMap;
  }
}
