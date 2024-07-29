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

import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Partitioner which computes partition values based on the ColumnPartitionConfig from the table config
 */
public class TableConfigPartitioner implements Partitioner {

  private final String _column;
  private final PartitionFunction _partitionFunction;

  public TableConfigPartitioner(String columnName, ColumnPartitionConfig columnPartitionConfig) {
    _column = columnName;
    _partitionFunction = PartitionFunctionFactory
        .getPartitionFunction(columnPartitionConfig.getFunctionName(), columnPartitionConfig.getNumPartitions(),
                columnPartitionConfig.getFunctionConfig());
  }

  @Override
  public String getPartition(GenericRow genericRow) {
    return String.valueOf(_partitionFunction.getPartition(FieldSpec.getStringValue(genericRow.getValue(_column))));
  }
}
