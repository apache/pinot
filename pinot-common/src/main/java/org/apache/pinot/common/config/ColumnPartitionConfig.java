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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;


public class ColumnPartitionConfig extends BaseJsonConfig {
  private final String _functionName;
  private int _numPartitions;

  /**
   * Constructor for the class.
   *
   * @param functionName Name of the partition function.
   * @param numPartitions Number of partitions for this column.
   */
  @JsonCreator
  public ColumnPartitionConfig(@JsonProperty(value = "functionName", required = true) String functionName,
      @JsonProperty(value = "numPartitions", required = true) int numPartitions) {
    Preconditions.checkArgument(functionName != null, "'functionName' must be configured");
    Preconditions.checkArgument(numPartitions > 0, "'numPartitions' must be positive");
    _functionName = functionName;
    _numPartitions = numPartitions;
  }

  /**
   * Returns the partition function name for the column.
   *
   * @return Partition function name.
   */
  public String getFunctionName() {
    return _functionName;
  }

  /**
   * Returns the number of partitions for this column.
   *
   * @return Number of partitions.
   */
  public int getNumPartitions() {
    return _numPartitions;
  }

  public void setNumPartitions(int numPartitions) {
    Preconditions.checkArgument(numPartitions > 0, "'numPartitions' must be positive");
    _numPartitions = numPartitions;
  }
}
