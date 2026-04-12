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
package org.apache.pinot.segment.spi.partition.pipeline;

import com.google.common.base.Preconditions;
import java.util.Locale;


/**
 * Normalizes the final INT output from a compiled partition pipeline into a partition id.
 */
public enum PartitionIntNormalizer {
  /**
   * Computes the remainder first, then shifts negative partition ids into the valid range with {@code + numPartitions}.
   */
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
  /**
   * Computes the remainder first, then takes the absolute value of the remainder.
   */
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
  /**
   * Makes the raw INT/LONG output non-negative first by masking the sign bit, then applies modulo.
   */
  MASK {
    @Override
    int toPartitionId(int value, int numPartitions) {
      return (value & Integer.MAX_VALUE) % numPartitions;
    }

    @Override
    int toPartitionId(long value, int numPartitions) {
      return (int) ((value & Long.MAX_VALUE) % numPartitions);
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

  public static PartitionIntNormalizer fromConfigString(String partitionIdNormalizer) {
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
