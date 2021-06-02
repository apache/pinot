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

import com.google.common.base.Preconditions;
import org.apache.pinot.segment.spi.partition.Partitioner;


/**
 * Factory for Partitioner and PartitionFilter
 */
public final class PartitionerFactory {

  private PartitionerFactory() {

  }

  public enum PartitionerType {
    NO_OP,
    /**
     * Creates not more than the configured fixed number of partitions
     */
    ROUND_ROBIN,
    /**
     * Partitions using a column value
     */
    COLUMN_VALUE,
    /**
     * Evaluates a function expression to determine partition value
     */
    TRANSFORM_FUNCTION,
    /**
     * Partitions using {@link org.apache.pinot.spi.config.table.ColumnPartitionConfig} as defined in the table config
     */
    TABLE_PARTITION_CONFIG
  }

  /**
   * Construct a Partitioner using the PartitioningConfig
   */
  public static Partitioner getPartitioner(PartitionerConfig config) {

    Partitioner partitioner = null;
    switch (config.getPartitionerType()) {
      case NO_OP:
        partitioner = new NoOpPartitioner();
        break;
      case ROUND_ROBIN:
        Preconditions
            .checkState(config.getNumPartitions() > 0, "Must provide numPartitions > 0 for ROUND_ROBIN partitioner");
        partitioner = new RoundRobinPartitioner(config.getNumPartitions());
        break;
      case COLUMN_VALUE:
        Preconditions
            .checkState(config.getColumnName() != null, "Must provide columnName for COLUMN_VALUE partitioner");
        partitioner = new ColumnValuePartitioner(config.getColumnName());
        break;
      case TRANSFORM_FUNCTION:
        Preconditions.checkState(config.getTransformFunction() != null,
            "Must provide transformFunction for TRANSFORM_FUNCTION partitioner");
        partitioner = new TransformFunctionPartitioner(config.getTransformFunction());
        break;
      case TABLE_PARTITION_CONFIG:
        Preconditions.checkState(config.getColumnName() != null,
            "Must provide columnName for TABLE_PARTITION_CONFIG Partitioner");
        Preconditions.checkState(config.getColumnPartitionConfig() != null,
            "Must provide columnPartitionConfig for TABLE_PARTITION_CONFIG Partitioner");
        partitioner = new TableConfigPartitioner(config.getColumnName(), config.getColumnPartitionConfig());
        break;
    }
    return partitioner;
  }
}
