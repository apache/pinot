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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import javax.annotation.Nonnull;
import org.apache.pinot.common.utils.EqualityUtils;


@SuppressWarnings("unused") // Suppress incorrect warnings as methods used for ser/de.
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnPartitionConfig {
  public static final String PARTITION_VALUE_DELIMITER = ",";

  @ConfigKey("functionName")
  private String _functionName;

  @ConfigKey("numPartitions")
  private int _numPartitions;

  public ColumnPartitionConfig() {
  }

  /**
   * Constructor for the class.
   *
   * @param functionName Name of the partition function.
   * @param numPartitions Number of partitions for this column.
   */
  public ColumnPartitionConfig(@Nonnull @JsonProperty("functionName") String functionName,
      @JsonProperty("numPartitions") int numPartitions) {
    _functionName = functionName;

    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > zero, specified: " + numPartitions);
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
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > zero, specified: " + numPartitions);
    _numPartitions = numPartitions;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof ColumnPartitionConfig) {
      ColumnPartitionConfig that = (ColumnPartitionConfig) obj;
      return _functionName.equals(that._functionName) && _numPartitions == that._numPartitions;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = EqualityUtils.hashCodeOf(_functionName);
    hashCode = EqualityUtils.hashCodeOf(hashCode, _numPartitions);
    return hashCode;
  }

  @Override
  public String toString() {
    return "ColumnPartitionConfig{" + "_functionName='" + _functionName + '\'' + ", _numPartitions=" + _numPartitions
        + '}';
  }
}
