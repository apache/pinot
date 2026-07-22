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
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Partitioner which computes partition values based on the ColumnPartitionConfig from the table config
 */
public class TableConfigPartitioner implements Partitioner {
  private final String _column;
  private final PartitionFunction _partitionFunction;
  /// Non-null when the column's logical type is known. Used to render values via
  /// {@link DataType#toString(Object)} so UUID columns produce the canonical RFC 4122 form
  /// (matching MutableSegmentImpl's runtime partition path and the {@code Uuid} partition function's
  /// expectation) instead of the bare-hex string that {@link FieldSpec#getStringValue} would emit.
  @Nullable
  private final DataType _dataType;

  public TableConfigPartitioner(String columnName, ColumnPartitionConfig columnPartitionConfig) {
    this(columnName, columnPartitionConfig, null);
  }

  public TableConfigPartitioner(String columnName, ColumnPartitionConfig columnPartitionConfig,
      @Nullable DataType dataType) {
    _column = columnName;
    _partitionFunction = PartitionFunctionFactory.getPartitionFunction(columnPartitionConfig);
    _dataType = dataType;
  }

  @Override
  public String getPartition(GenericRow genericRow) {
    return String.valueOf(_partitionFunction.getPartition(toPartitionString(genericRow.getValue(_column))));
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
    return String.valueOf(_partitionFunction.getPartition(toPartitionString(columnValues[0])));
  }

  private String toPartitionString(Object value) {
    return _dataType != null ? _dataType.toString(value) : FieldSpec.getStringValue(value);
  }
}
