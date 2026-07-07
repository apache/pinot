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
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.spi.utils.hash.MurmurHashFunctions;


/// [PartitionFunction] for Pinot's logical UUID type. Accepts either canonical RFC 4122 text or the
/// 32-character hexadecimal rendering of the stored 16-byte value, hashes the decoded bytes via Murmur2,
/// and applies the configured [PartitionIdNormalizer] (default [PartitionIdNormalizer#MASK]).
/// Select it explicitly with `ColumnPartitionConfig.functionName = "Uuid"` when the upstream producer
/// partitions on Murmur2 of raw UUID bytes.
public class UuidPartitionFunction implements PartitionFunction {
  private static final String NAME = "Uuid";
  private static final PartitionIdNormalizer DEFAULT_NORMALIZER = PartitionIdNormalizer.MASK;
  private final int _numPartitions;
  @Nullable
  private final Map<String, String> _functionConfig;
  private final PartitionIdNormalizer _normalizer;

  public UuidPartitionFunction(int numPartitions, @Nullable Map<String, String> functionConfig) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0, was: %s", numPartitions);
    _numPartitions = numPartitions;
    _functionConfig = functionConfig != null ? Collections.unmodifiableMap(functionConfig) : null;
    _normalizer = PartitionFunctionConfigs.normalizer(functionConfig, DEFAULT_NORMALIZER);
  }

  @Override
  public int getPartition(String value) {
    byte[] uuidBytes = UuidUtils.toBytes(value);
    return _normalizer.getPartitionId(MurmurHashFunctions.murmurHash2(uuidBytes), _numPartitions);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Nullable
  @Override
  public Map<String, String> getFunctionConfig() {
    return _functionConfig;
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
