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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Interface for partition function.
 *
 * Implementations of this interface are assumed not to be stateful.
 * That is, two invocations of {@code PartitionFunction.getPartition(value)}
 * with the same value are expected to produce the same result. Implementations must also be safe for
 * concurrent invocation by multiple threads.
 */
public interface PartitionFunction extends Serializable {

  /**
   * Method to compute and return partition id for the given value.
   * NOTE: The value is expected to be a string representation of the actual value.
   *
   * @param value Value for which to determine the partition id.
   * @return partition id for the value.
   */
  int getPartition(String value);

  /**
   * Returns the name of the partition function.
   * @return Name of the partition function.
   */
  String getName();

  /**
   * Returns the total number of possible partitions.
   * @return Number of possible partitions.
   */
  int getNumPartitions();

  @Nullable
  default Map<String, String> getFunctionConfig() {
    return null;
  }

  /**
   * Reports the int-normalizer name (a {@link PartitionIntNormalizer} value, e.g.
   * {@code POSITIVE_MODULO}, {@code ABS}, {@code MASK}) that most closely describes this partition
   * function's internal modulo semantics. The framework uses this label only for identity / staleness
   * matching between config-side and segment-side partition metadata; it is NOT used to drive runtime
   * normalization for legacy functions, which perform their own modulo internally.
   *
   * <p><b>Descriptive only for legacy functions:</b> classic implementations
   * ({@code Modulo}, {@code Murmur}, {@code Murmur3}, {@code Fnv}, {@code HashCode},
   * {@code ByteArray}) report the closest-matching normalizer name even though their actual
   * implementation may differ subtly at edge cases (notably Kafka-style abs handling
   * {@code Integer.MIN_VALUE -> 0} vs strict mod-then-abs in {@link PartitionIntNormalizer#ABS}).
   * Do not assume that recomputing a partition id via {@code PartitionIntNormalizer.<X>.getPartitionId(rawHash, N)}
   * will reproduce the legacy function's output bit-for-bit.
   *
   * <p>Returns {@code null} when the function does not map cleanly onto any standard normalizer
   * (e.g. {@code BoundedColumnValue}).
   */
  @JsonIgnore
  @Nullable
  default String getPartitionIdNormalizer() {
    return null;
  }
}
