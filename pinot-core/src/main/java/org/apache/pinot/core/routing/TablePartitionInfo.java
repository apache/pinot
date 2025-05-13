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
package org.apache.pinot.core.routing;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class TablePartitionInfo {
  private final String _tableNameWithType;
  private final String _partitionColumn;
  private final String _partitionFunctionName;
  private final int _numPartitions;
  private final PartitionInfo[] _partitionInfoMap;
  private final List<String> _segmentsWithInvalidPartition;
  /**
   * New segments may not have all replicas available soon after creation, and hence they can potentially reduce
   * the fullyReplicatedServers in PartitionInfo. To prevent that, we store such segments separately in this map.
   */
  private final Map<Integer, List<String>> _excludedNewSegments;

  public TablePartitionInfo(String tableNameWithType, String partitionColumn, String partitionFunctionName,
      int numPartitions, PartitionInfo[] partitionInfoMap, List<String> segmentsWithInvalidPartition,
      Map<Integer, List<String>> excludedNewSegments) {
    _tableNameWithType = tableNameWithType;
    _partitionColumn = partitionColumn;
    _partitionFunctionName = partitionFunctionName;
    _numPartitions = numPartitions;
    _partitionInfoMap = partitionInfoMap;
    _segmentsWithInvalidPartition = segmentsWithInvalidPartition;
    _excludedNewSegments = excludedNewSegments;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public String getPartitionColumn() {
    return _partitionColumn;
  }

  public String getPartitionFunctionName() {
    return _partitionFunctionName;
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  public PartitionInfo[] getPartitionInfoMap() {
    return _partitionInfoMap;
  }

  public List<String> getSegmentsWithInvalidPartition() {
    return _segmentsWithInvalidPartition;
  }

  public Map<Integer, List<String>> getExcludedNewSegments() {
    return _excludedNewSegments;
  }

  /**
   * TablePartitionInfo stores segments for a partition in two places: the PartitionInfo map, and the excluded new
   * segments. We get segments from both of those places and return the concatenation of the two lists.
   */
  public List<String> getSegmentsInPartition(int partition) {
    Preconditions.checkState(partition < _partitionInfoMap.length, "Attempted to fetch partition=%s, but only have %s",
        partition, _partitionInfoMap.length);
    TablePartitionInfo.PartitionInfo info = _partitionInfoMap[partition];
    List<String> excludedNewSegments = _excludedNewSegments.get(partition);
    // Collect all segments and return.
    List<String> result = new ArrayList<>();
    if (info != null) {
      result.addAll(info._segments);
    }
    if (excludedNewSegments != null) {
      result.addAll(excludedNewSegments);
    }
    return result;
  }

  public static class PartitionInfo {
    public final Set<String> _fullyReplicatedServers;
    public final List<String> _segments;

    public PartitionInfo(Set<String> fullyReplicatedServers, List<String> segments) {
      _fullyReplicatedServers = fullyReplicatedServers;
      _segments = segments;
    }
  }
}
