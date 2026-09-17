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

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;


public class SegmentPartitionInfo {
  // Canonicalize at metadata load/refresh time. Weak references allow unused configurations to be reclaimed.
  private static final Interner<PartitionFunctionKey> PARTITION_FUNCTION_KEYS = Interners.newWeakInterner();

  private final String _partitionColumn;
  private final PartitionFunction _partitionFunction;
  private final Set<Integer> _partitions;
  @Nullable
  private final PartitionFunctionKey _partitionFunctionKey;

  public SegmentPartitionInfo(String partitionColumn, PartitionFunction partitionFunction,
      Set<Integer> partitions) {
    this(partitionColumn, partitionFunction, partitions,
        partitionFunction != null ? partitionFunction.getFunctionConfig() : null);
  }

  /// Retains the metadata configuration independently of the partition function's getter.
  public SegmentPartitionInfo(String partitionColumn, PartitionFunction partitionFunction, Set<Integer> partitions,
      @Nullable Map<String, String> partitionFunctionConfig) {
    _partitionColumn = partitionColumn;
    _partitionFunction = partitionFunction;
    _partitions = partitions;
    // Preserve null versus empty configuration, and allow null entries accepted by the metadata representation.
    Map<String, String> config = partitionFunctionConfig == null
        ? null
        : Collections.unmodifiableMap(new HashMap<>(partitionFunctionConfig));
    // The invalid-metadata sentinel has no partition function.
    _partitionFunctionKey = partitionFunction == null
        ? null
        : PARTITION_FUNCTION_KEYS.intern(new PartitionFunctionKey(partitionFunction.getClass(),
            partitionFunction.getName(), partitionFunction.getNumPartitions(),
            partitionFunction.getPartitionIdNormalizer(), config));
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

  /// Returns a shared immutable identity for equivalent partition functions, or null for invalid metadata.
  /// Query-local caches can compare these keys by reference without inspecting configuration maps.
  @Nullable
  public Object getPartitionFunctionKey() {
    return _partitionFunctionKey;
  }

  /// Immutable, thread-safe constructor-state snapshot. Full equality is used only during metadata canonicalization.
  private record PartitionFunctionKey(Class<? extends PartitionFunction> functionClass, String name, int numPartitions,
                                      PartitionIdNormalizer normalizer, @Nullable Map<String, String> functionConfig) {
  }
}
