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
import javax.annotation.Nullable;


/// An advanced version of [TablePartitionInfo] that also contains information about the fully replicated servers
/// for each partition.
public class TablePartitionReplicatedServersInfo {
  private final String _tableNameWithType;
  private final String _partitionColumn;
  private final String _partitionFunctionName;
  private final int _numPartitions;
  private final PartitionInfo[] _partitionInfoMap;
  private final List<String> _segmentsWithInvalidPartition;
  private final Set<Integer> _partitionsWithOnlyDeferredSegments;

  /// @deprecated Defaults [#getPartitionsWithOnlyDeferredSegments()] to empty, i.e. claims that no partition holds
  ///             deferred data, which is the unsafe direction (see that method). Use the overload and pass the real
  ///             set.
  @Deprecated
  public TablePartitionReplicatedServersInfo(String tableNameWithType, String partitionColumn,
      String partitionFunctionName, int numPartitions, PartitionInfo[] partitionInfoMap,
      List<String> segmentsWithInvalidPartition) {
    this(tableNameWithType, partitionColumn, partitionFunctionName, numPartitions, partitionInfoMap,
        segmentsWithInvalidPartition, Set.of());
  }

  public TablePartitionReplicatedServersInfo(String tableNameWithType, String partitionColumn,
      String partitionFunctionName, int numPartitions, PartitionInfo[] partitionInfoMap,
      List<String> segmentsWithInvalidPartition, @Nullable Set<Integer> partitionsWithOnlyDeferredSegments) {
    _tableNameWithType = tableNameWithType;
    _partitionColumn = partitionColumn;
    _partitionFunctionName = partitionFunctionName;
    _numPartitions = numPartitions;
    _partitionInfoMap = partitionInfoMap;
    _segmentsWithInvalidPartition = segmentsWithInvalidPartition;
    _partitionsWithOnlyDeferredSegments =
        partitionsWithOnlyDeferredSegments != null ? partitionsWithOnlyDeferredSegments : Set.of();
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

  /// Returns the partitions that have no entry in [#getPartitionInfoMap()] *only* because all of their segments were
  /// deferred: every one of them is a new segment (recently created or pushed) that does not have all of its replicas
  /// online yet, so including it would leave the partition without a fully replicated server.
  ///
  /// A `null` slot in [#getPartitionInfoMap()] therefore has several causes: the partition genuinely holds no data, all
  /// of its segments are deferred (this set), or its segments hold invalid partition metadata (see
  /// [#getSegmentsWithInvalidPartition()]). Only the first is safe to read as empty. A consumer that needs one server
  /// to scan a whole partition (e.g. a colocated join in the multi-stage engine) must fail the query on the others
  /// rather than silently dropping their rows; one that scatters over all the servers holding the table (the regular
  /// routing path) can ignore this set, because it picks the deferred segments up through the routing table.
  ///
  /// Empty when there is nothing to report, and never `null`: the consumers above read it without a null check.
  public Set<Integer> getPartitionsWithOnlyDeferredSegments() {
    return _partitionsWithOnlyDeferredSegments;
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
