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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;


public class SegmentPartitionInfo {
  private final String _partitionColumn;
  private final PartitionFunction _partitionFunction;
  private final Set<Integer> _partitions;
  private final boolean _hasPartitionFunctionConfig;
  @Nullable
  private final Map<String, String> _partitionFunctionConfig;

  public SegmentPartitionInfo(String partitionColumn, PartitionFunction partitionFunction,
      Set<Integer> partitions) {
    this(partitionColumn, partitionFunction, partitions, false, null);
  }

  /// Retains the constructor configuration even when a partition-function plugin does not expose it through its getter.
  public SegmentPartitionInfo(String partitionColumn, PartitionFunction partitionFunction, Set<Integer> partitions,
      @Nullable Map<String, String> partitionFunctionConfig) {
    this(partitionColumn, partitionFunction, partitions, true, partitionFunctionConfig);
  }

  private SegmentPartitionInfo(String partitionColumn, PartitionFunction partitionFunction, Set<Integer> partitions,
      boolean hasPartitionFunctionConfig, @Nullable Map<String, String> partitionFunctionConfig) {
    _partitionColumn = partitionColumn;
    _partitionFunction = partitionFunction;
    _partitions = partitions;
    _hasPartitionFunctionConfig = hasPartitionFunctionConfig;
    // Preserve null versus empty configuration, and allow null entries accepted by the metadata representation.
    _partitionFunctionConfig = partitionFunctionConfig == null
        ? null
        : Collections.unmodifiableMap(new HashMap<>(partitionFunctionConfig));
  }

  public String getPartitionColumn() {
    return _partitionColumn;
  }

  public PartitionFunction getPartitionFunction() {
    return _partitionFunction;
  }

  public Set<Integer> getPartitions() {
    return _partitions;
  }

  public boolean hasPartitionFunctionConfig() {
    return _hasPartitionFunctionConfig;
  }

  @Nullable
  public Map<String, String> getPartitionFunctionConfig() {
    return _partitionFunctionConfig;
  }
}
