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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/// Interface for partition function.
///
/// Implementations of this interface are assumed not to be stateful. That is, two invocations of
/// `PartitionFunction.getPartition(value)` with the same value are expected to produce the same
/// result. Implementations must also be safe for concurrent invocation by multiple threads.
public interface PartitionFunction extends Serializable {

  /// Method to compute and return partition id for the given value.
  /// NOTE: The value is expected to be a string representation of the actual value.
  ///
  /// @param value Value for which to determine the partition id.
  /// @return partition id for the value.
  int getPartition(String value);

  /// Returns the canonical name of the partition function.
  ///
  /// @return Name of the partition function.
  String getName();

  /// Returns every name (canonical + aliases) under which this partition function should be
  /// registered with `PartitionFunctionFactory`. Defaults to a single-entry list containing
  /// [#getName()]. Override only when you want additional aliases — e.g.
  /// `MurmurPartitionFunction` registers under both `Murmur` and `Murmur2`.
  @JsonIgnore
  default List<String> getNames() {
    return Collections.singletonList(getName());
  }

  /// Returns the total number of possible partitions.
  ///
  /// @return Number of possible partitions.
  int getNumPartitions();

  @Nullable
  default Map<String, String> getFunctionConfig() {
    return null;
  }

  /// Reports the [PartitionIdNormalizer] that describes this partition function's int-to-id
  /// mapping. The framework uses this for identity / staleness matching between config-side and
  /// segment-side partition metadata, and (for the built-in implementations) as the actual driver
  /// of the int-to-id computation.
  ///
  /// Each implementation must declare its own value — there is intentionally no default. Plug-ins
  /// whose output is already in `[0, numPartitions)` (e.g. lookup-style functions) should return
  /// [PartitionIdNormalizer#POSITIVE_MODULO] (a no-op label).
  PartitionIdNormalizer getPartitionIdNormalizer();
}
