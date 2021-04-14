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
package org.apache.pinot.segment.local.partition;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import org.apache.pinot.segment.spi.partition.PartitionFunction;


/**
 * Implementation of {@link Byte array partitioner}
 *
 */
public class ByteArrayPartitionFunction implements PartitionFunction {
  private static final String NAME = "ByteArray";
  private final int _numPartitions;

  /**
   * Constructor for the class.
   * @param numPartitions Number of partitions
   */
  public ByteArrayPartitionFunction(int numPartitions) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0, specified", numPartitions);
    _numPartitions = numPartitions;
  }

  @Override
  public int getPartition(Object valueIn) {
    return abs(Arrays.hashCode(valueIn.toString().getBytes())) % _numPartitions;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  public String toString() {
    return NAME;
  }

  private int abs(int n) {
    return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n);
  }
}

