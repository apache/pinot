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


/**
 * Modulo operation based partition function, where:
 * <ul>
 *   <li> partitionId = value % {@link #_numPartitions}</li>
 * </ul>
 *
 */
public class ModuloPartitionFunction implements PartitionFunction {
  private static final String NAME = "Modulo";
  private final int _numPartitions;

  /**
   * Constructor for the class.
   * @param numPartitions Number of partitions.
   */
  public ModuloPartitionFunction(int numPartitions) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0, specified", numPartitions);
    _numPartitions = numPartitions;
  }

  /**
   * Returns partition id for a given value. Assumes that the passed in object
   * is either an Integer, or a string representation of an Integer.
   *
   * @param value Value for which to determine the partition id.
   * @return Partition id for the given value.
   */
  @Override
  public int getPartition(Object value) {
    if (value instanceof Integer) {
      return toNonNegative((Integer) value % _numPartitions);
    } else if (value instanceof Long) {
      // Since _numPartitions is int, the modulo should also be int.
      return toNonNegative((int) ((Long) value % _numPartitions));
    } else if (value instanceof String) {
      // Parse String as Long, to support both Integer and Long.
      return toNonNegative((int) (Long.parseLong((String) value) % _numPartitions));
    } else {
      throw new IllegalArgumentException(
          "Illegal argument for partitioning, expected Integer, got: " + value.getClass().getSimpleName());
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  // Keep it for backward-compatibility, use getName() instead
  @Override
  public String toString() {
    return NAME;
  }

  private int toNonNegative(int partition) {
    return partition < 0 ? partition + _numPartitions : partition;
  }
}
