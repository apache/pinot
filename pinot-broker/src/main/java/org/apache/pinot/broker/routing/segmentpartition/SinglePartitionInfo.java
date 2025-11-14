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

import java.util.Objects;
import org.apache.pinot.segment.spi.partition.PartitionFunction;


/**
 * Representation for a single immutable combination of partitionColumn, partitionFunction,
 * and partition. This is exactly like SegmentPartitionInfo except it only supports one partition.
 */
public class SinglePartitionInfo {
  private final String _partitionColumn;
  private final PartitionFunction _partitionFunction;
  private final int _partition;

  public SinglePartitionInfo(String partitionColumn, PartitionFunction partitionFunction, int partition) {
    _partitionColumn = partitionColumn;
    _partitionFunction = partitionFunction;
    _partition = partition;
  }

  public String getPartitionColumn() {
    return _partitionColumn;
  }

  public PartitionFunction getPartitionFunction() {
    return _partitionFunction;
  }

  public int getPartition() {
    return _partition;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    SinglePartitionInfo that = (SinglePartitionInfo) other;
    return _partition == that._partition
        && Objects.equals(_partitionColumn, that._partitionColumn)
        && Objects.equals(_partitionFunction.getName(), that._partitionFunction.getName())
        && _partitionFunction.getNumPartitions() == that._partitionFunction.getNumPartitions()
        && Objects.equals(_partitionFunction.getFunctionConfig(), that._partitionFunction.getFunctionConfig());
  }

  @Override
  public int hashCode() {
    return Objects.hash(_partitionColumn, _partition, _partitionFunction.getName(),
        _partitionFunction.getNumPartitions(), _partitionFunction.getFunctionConfig());
  }
}
