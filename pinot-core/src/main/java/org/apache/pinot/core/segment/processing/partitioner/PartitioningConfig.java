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
package org.apache.pinot.core.segment.processing.partitioner;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;


/**
 * Config for Partitioner
 */
@JsonDeserialize(builder = PartitioningConfig.Builder.class)
public class PartitioningConfig {

  private static final PartitionerFactory.PartitionerType DEFAULT_PARTITIONER_TYPE =
      PartitionerFactory.PartitionerType.NO_OP;

  private final PartitionerFactory.PartitionerType _partitionerType;
  private final int _numPartitions;
  private final String _columnName;
  private final String _transformFunction;
  private final ColumnPartitionConfig _columnPartitionConfig;

  private PartitioningConfig(PartitionerFactory.PartitionerType partitionerType, int numPartitions, String columnName,
      String transformFunction, ColumnPartitionConfig columnPartitionConfig) {
    _partitionerType = partitionerType;
    _numPartitions = numPartitions;
    _columnName = columnName;
    _transformFunction = transformFunction;
    _columnPartitionConfig = columnPartitionConfig;
  }

  /**
   * The type of Partitioner
   */
  public PartitionerFactory.PartitionerType getPartitionerType() {
    return _partitionerType;
  }

  /**
   * The number of partitions to create
   */
  public int getNumPartitions() {
    return _numPartitions;
  }

  /**
   * The column name to use for partitioning
   */
  public String getColumnName() {
    return _columnName;
  }

  /**
   * The transform function to use for calculating partitions
   */
  public String getTransformFunction() {
    return _transformFunction;
  }

  /**
   * Column partition config from a table config
   */
  public ColumnPartitionConfig getColumnPartitionConfig() {
    return _columnPartitionConfig;
  }

  /**
   * Builder for a PartitioningConfig
   */
  @JsonPOJOBuilder(withPrefix = "set")
  public static class Builder {
    private PartitionerFactory.PartitionerType partitionerType = DEFAULT_PARTITIONER_TYPE;
    private int numPartitions;
    private String columnName;
    private String transformFunction;
    private ColumnPartitionConfig columnPartitionConfig;

    public Builder setPartitionerType(PartitionerFactory.PartitionerType partitionerType) {
      this.partitionerType = partitionerType;
      return this;
    }

    public Builder setNumPartitions(int numPartitions) {
      this.numPartitions = numPartitions;
      return this;
    }

    public Builder setColumnName(String columnName) {
      this.columnName = columnName;
      return this;
    }

    public Builder setTransformFunction(String transformFunction) {
      this.transformFunction = transformFunction;
      return this;
    }

    public Builder setColumnPartitionConfig(ColumnPartitionConfig columnPartitionConfig) {
      this.columnPartitionConfig = columnPartitionConfig;
      return this;
    }

    public PartitioningConfig build() {
      return new PartitioningConfig(partitionerType, numPartitions, columnName, transformFunction,
          columnPartitionConfig);
    }
  }

  @Override
  public String toString() {
    return "PartitioningConfig{" + "_partitionerType=" + _partitionerType + ", _numPartitions=" + _numPartitions
        + ", _columnName='" + _columnName + '\'' + ", _transformFunction='" + _transformFunction + '\''
        + ", _columnPartitionConfig=" + _columnPartitionConfig + '\'' + '}';
  }
}
