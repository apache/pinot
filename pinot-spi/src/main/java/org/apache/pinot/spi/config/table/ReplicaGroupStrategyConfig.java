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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Class representing configurations related to segment assignment strategy.
 */
public class ReplicaGroupStrategyConfig extends BaseJsonConfig {
  private final String _partitionColumn;
  private final int _numInstancesPerPartition;

  @JsonCreator
  public ReplicaGroupStrategyConfig(@JsonProperty("partitionColumn") @Nullable String partitionColumn,
      @JsonProperty(value = "numInstancesPerPartition", required = true) int numInstancesPerPartition) {
    Preconditions.checkArgument(numInstancesPerPartition > 0, "'numInstancesPerPartition' must be positive");
    _partitionColumn = partitionColumn;
    _numInstancesPerPartition = numInstancesPerPartition;
  }

  /**
   * Returns the name of column used for partitioning. If this is set to null, we use the table level replica groups.
   * Otherwise, we use the partition level replica groups.
   * TODO: use partition info from SegmentPartitionConfig
   *
   * @return Name of partitioning column.
   */
  @Nullable
  public String getPartitionColumn() {
    return _partitionColumn;
  }

  /**
   * Returns the number of instances that segments for a partition span.
   *
   * @return Number of instances used for a partition.
   */
  public int getNumInstancesPerPartition() {
    return _numInstancesPerPartition;
  }
}
