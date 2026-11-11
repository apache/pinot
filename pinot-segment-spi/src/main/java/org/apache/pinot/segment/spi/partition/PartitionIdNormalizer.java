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
import java.util.Locale;


/// Maps a raw signed integer hash output to a non-negative partition id in `[0, numPartitions)`.
///
/// [PartitionFunction] implementations apply the configured normalizer in their
/// `getPartition(...)` body and report it via [PartitionFunction#getPartitionIdNormalizer()].
/// The framework also uses the reported value for identity / staleness matching between
/// config-side and segment-side partition metadata.
public enum PartitionIdNormalizer {
  /// Compute the remainder, then shift negative remainders into the valid range with `+ numPartitions`.
  POSITIVE_MODULO {
    @Override
    int toPartitionId(int value, int numPartitions) {
      int partition = value % numPartitions;
      return partition < 0 ? partition + numPartitions : partition;
    }

    @Override
    int toPartitionId(long value, int numPartitions) {
      long partition = value % numPartitions;
      return (int) (partition < 0 ? partition + numPartitions : partition);
    }
  },
  /// Compute the remainder, then take its absolute value.
  ABS {
    @Override
    int toPartitionId(int value, int numPartitions) {
      int partition = value % numPartitions;
      return partition < 0 ? -partition : partition;
    }

    @Override
    int toPartitionId(long value, int numPartitions) {
      long partition = value % numPartitions;
      return (int) (partition < 0 ? -partition : partition);
    }
  },
  /// Mask the sign bit before applying modulo.
  MASK {
    @Override
    int toPartitionId(int value, int numPartitions) {
      return (value & Integer.MAX_VALUE) % numPartitions;
    }

    @Override
    int toPartitionId(long value, int numPartitions) {
      return (int) ((value & Long.MAX_VALUE) % numPartitions);
    }
  },
  /// Pre-modulo abs (Kafka-style) `abs(value) % numPartitions` that handles `Integer.MIN_VALUE -> 0`
  /// (and `Long.MIN_VALUE -> 0`) to avoid the `Math.abs` overflow corner. Matches the
  /// legacy semantics of `HashCodePartitionFunction` and `ByteArrayPartitionFunction`.
  PRE_MODULO_ABS {
    @Override
    int toPartitionId(int value, int numPartitions) {
      int abs = (value == Integer.MIN_VALUE) ? 0 : Math.abs(value);
      return abs % numPartitions;
    }

    @Override
    int toPartitionId(long value, int numPartitions) {
      long abs = (value == Long.MIN_VALUE) ? 0L : Math.abs(value);
      return (int) (abs % numPartitions);
    }
  },
  /// Identity. Returns the input unchanged (narrowed to `int` for the long overload). Use only
  /// when the upstream `PartitionFunction#getPartition` value is already guaranteed to be in
  /// `[0, numPartitions)` — e.g. lookup-style functions like `BoundedColumnValuePartitionFunction`.
  /// The framework does NOT validate that the input is in range; passing an out-of-range value
  /// yields an out-of-range partition id.
  NO_OP {
    @Override
    int toPartitionId(int value, int numPartitions) {
      return value;
    }

    @Override
    int toPartitionId(long value, int numPartitions) {
      return (int) value;
    }
  };

  public final int getPartitionId(int value, int numPartitions) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
    return toPartitionId(value, numPartitions);
  }

  public final int getPartitionId(long value, int numPartitions) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
    return toPartitionId(value, numPartitions);
  }

  public static PartitionIdNormalizer fromConfigString(String partitionIdNormalizer) {
    Preconditions.checkArgument(partitionIdNormalizer != null && !partitionIdNormalizer.trim().isEmpty(),
        "'partitionIdNormalizer' must not be blank");
    try {
      return valueOf(partitionIdNormalizer.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported partitionIdNormalizer: " + partitionIdNormalizer, e);
    }
  }

  abstract int toPartitionId(int value, int numPartitions);

  abstract int toPartitionId(long value, int numPartitions);
}
