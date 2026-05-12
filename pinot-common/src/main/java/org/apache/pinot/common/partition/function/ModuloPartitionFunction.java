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
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;


/// Modulo operation based partition function. Treats the input string as a base-10 long and runs
/// the configured [PartitionIdNormalizer] (default [PartitionIdNormalizer#POSITIVE_MODULO])
/// over it.
public class ModuloPartitionFunction implements PartitionFunction {
  private static final String NAME = "Modulo";
  private static final PartitionIdNormalizer DEFAULT_NORMALIZER = PartitionIdNormalizer.POSITIVE_MODULO;
  private final int _numPartitions;
  private final PartitionIdNormalizer _normalizer;

  public ModuloPartitionFunction(int numPartitions, @Nullable Map<String, String> functionConfig) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0, was: %s", numPartitions);
    _numPartitions = numPartitions;
    _normalizer = PartitionFunctionConfigs.normalizer(functionConfig, DEFAULT_NORMALIZER);
  }

  @Override
  public int getPartition(String value) {
    return _normalizer.getPartitionId(Long.parseLong(value), _numPartitions);
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
  public PartitionIdNormalizer getPartitionIdNormalizer() {
    return _normalizer;
  }

  // Keep it for backward-compatibility, use getName() instead
  @Override
  public String toString() {
    return NAME;
  }
}
