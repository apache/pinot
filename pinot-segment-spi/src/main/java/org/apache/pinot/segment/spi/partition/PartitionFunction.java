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

import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Interface for partition function.
 *
 * Implementations of this interface are assumed not to be stateful.
 * That is, two invocations of {@code PartitionFunction.getPartition(value)}
 * with the same value are expected to produce the same result.
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
}
