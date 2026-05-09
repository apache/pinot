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

import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionPipelineFunction;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Partitioner which computes partition values based on the ColumnPartitionConfig from the table config
 */
public class TableConfigPartitioner implements Partitioner {
  /// Bucket name used when the row's partition column is null or when the partition expression evaluates to null
  /// mid-chain. Distinct from any numeric partition id so downstream readers can identify and handle these rows.
  public static final String NULL_PARTITION = "null";

  private final String _column;
  private final PartitionFunction _partitionFunction;
  /// True when the partition function was compiled with BYTES input; raw byte[] values are passed directly.
  private final boolean _isBytesMode;

  public TableConfigPartitioner(String columnName, ColumnPartitionConfig columnPartitionConfig) {
    this(columnName, columnPartitionConfig, null);
  }

  public TableConfigPartitioner(String columnName, ColumnPartitionConfig columnPartitionConfig,
      @Nullable Schema schema) {
    _column = columnName;
    FieldSpec fieldSpec = schema != null ? schema.getFieldSpecFor(columnName) : null;
    _isBytesMode = columnPartitionConfig.getFunctionExpr() != null && fieldSpec != null
        && fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.BYTES;
    _partitionFunction = PartitionFunctionFactory.getPartitionFunction(columnName, columnPartitionConfig, fieldSpec);
  }

  @Override
  public String getPartition(GenericRow genericRow) {
    return computePartition(genericRow.getValue(_column));
  }

  @Override
  public String[] getPartitionColumns() {
    return new String[]{_column};
  }

  @Override
  public String getPartitionFromColumns(Object[] columnValues) {
    if (columnValues.length != 1) {
      throw new IllegalArgumentException(
          "TableConfigPartitioner expects exactly 1 column value, got " + columnValues.length);
    }
    return computePartition(columnValues[0]);
  }

  private String computePartition(@Nullable Object value) {
    // Null input → expression-mode pipelines would fail mid-chain (or return null) and yield NULL_RESULT_PARTITION_ID.
    // Map both the null input case and the null-result case to a designated bucket name distinct from any numeric
    // partition id, so downstream consumers don't mistake a null-row marker for partition -1.
    if (value == null) {
      return NULL_PARTITION;
    }
    int partitionId = (_isBytesMode && value instanceof byte[])
        ? _partitionFunction.getPartition((byte[]) value)
        : _partitionFunction.getPartition(FieldSpec.getStringValue(value));
    if (partitionId == PartitionPipelineFunction.NULL_RESULT_PARTITION_ID) {
      return NULL_PARTITION;
    }
    return String.valueOf(partitionId);
  }
}
