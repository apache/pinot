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

import java.util.List;
import java.util.Set;


public class TablePartitionInfo {
  private final String _tableNameWithType;
  private final String _partitionColumn;
  private final String _partitionFunctionName;
  private final int _numPartitions;
  private final PartitionInfo[] _partitionInfoMap;
  private final Set<String> _segmentsWithInvalidPartition;

  public TablePartitionInfo(String tableNameWithType, String partitionColumn, String partitionFunctionName,
      int numPartitions, PartitionInfo[] partitionInfoMap, Set<String> segmentsWithInvalidPartition) {
    _tableNameWithType = tableNameWithType;
    _partitionColumn = partitionColumn;
    _partitionFunctionName = partitionFunctionName;
    _numPartitions = numPartitions;
    _partitionInfoMap = partitionInfoMap;
    _segmentsWithInvalidPartition = segmentsWithInvalidPartition;
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

  public Set<String> getSegmentsWithInvalidPartition() {
    return _segmentsWithInvalidPartition;
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
