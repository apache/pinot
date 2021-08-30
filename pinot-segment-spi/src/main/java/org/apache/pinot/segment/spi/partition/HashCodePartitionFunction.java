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
package org.apache.pinot.segment.spi.partition;

import com.google.common.base.Preconditions;
import java.util.Objects;


/**
 * Hash code partition function, where:
 * <ul>
 *   <li> partitionId = value.hashCode() % {@link #_numPartitions}</li>
 * </ul>
 */
public class HashCodePartitionFunction implements PartitionFunction {
  private static final String NAME = "HashCode";
  private final int _numPartitions;

  public HashCodePartitionFunction(int numPartitions) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0, specified", numPartitions);
    _numPartitions = numPartitions;
  }

  @Override
  public int getPartition(Object value) {
    return Math.abs(value.hashCode()) % _numPartitions;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public PartitionFunctionType getFunctionType() {
    return PartitionFunctionType.HashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HashCodePartitionFunction that = (HashCodePartitionFunction) o;
    return _numPartitions == that._numPartitions;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_numPartitions, PartitionFunctionType.HashCode);
  }
}
