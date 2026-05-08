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
package org.apache.pinot.common.partition.function;

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionIntNormalizer;
import org.apache.pinot.spi.annotations.PartitionFunctionType;


/**
 * {@link PartitionFunction} that hashes the input via {@link String#hashCode()} and runs the
 * configured {@link PartitionIntNormalizer} (default {@link PartitionIntNormalizer#KAFKA_ABS}, the
 * Kafka-style {@code abs(hash) % N} that maps {@code Integer.MIN_VALUE -> 0}) to derive the
 * partition id.
 */
@PartitionFunctionType(names = "HashCode")
public class HashCodePartitionFunction implements PartitionFunction {
  private static final String NAME = "HashCode";
  private static final PartitionIntNormalizer DEFAULT_NORMALIZER = PartitionIntNormalizer.KAFKA_ABS;
  private final int _numPartitions;
  private final PartitionIntNormalizer _normalizer;

  public HashCodePartitionFunction(int numPartitions, @Nullable Map<String, String> functionConfig) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0, was: %s", numPartitions);
    _numPartitions = numPartitions;
    _normalizer = PartitionFunctionConfigs.normalizer(functionConfig, DEFAULT_NORMALIZER);
  }

  @Override
  public int getPartition(String value) {
    return _normalizer.getPartitionId(value.hashCode(), _numPartitions);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  public String getPartitionIdNormalizer() {
    return _normalizer.name();
  }

  // Keep it for backward-compatibility, use getName() instead
  @Override
  public String toString() {
    return NAME;
  }
}
