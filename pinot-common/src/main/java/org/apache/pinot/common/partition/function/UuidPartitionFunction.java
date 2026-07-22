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
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.spi.utils.hash.MurmurHashFunctions;


/// Partition function for Pinot's logical UUID type. Parses the canonical RFC 4122 UUID string into its
/// 16-byte binary form, hashes those bytes via Murmur2, and runs the configured [PartitionIdNormalizer]
/// (default [PartitionIdNormalizer#MASK]) to derive the partition id.
///
/// This matches what an external producer that hashes the 16-byte UUID via Murmur2 would compute (the
/// most common convention for UUID-keyed messages in Kafka/Pulsar/Kinesis).
///
/// Hashing the binary form avoids two pitfalls of hashing the canonical UUID string directly: the dashes
/// do not contribute entropy, and producers that emit raw UUID bytes on the wire would otherwise need a
/// Pinot-only canonical-format step in their partitioning code.
public class UuidPartitionFunction implements PartitionFunction {
  private static final String NAME = "Uuid";
  private static final PartitionIdNormalizer DEFAULT_NORMALIZER = PartitionIdNormalizer.MASK;
  private final int _numPartitions;
  private final PartitionIdNormalizer _normalizer;

  public UuidPartitionFunction(int numPartitions, @Nullable Map<String, String> functionConfig) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0, was: %s", numPartitions);
    _numPartitions = numPartitions;
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
