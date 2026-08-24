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
package org.apache.pinot.query.runtime.blocks;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.spi.query.QueryThreadContext;


/// A block that contains data in row heap format.
/// This means that the data is stored as list of rows, where each row is an array of objects and each object is a
/// reference to the cell value.
/// This representation is expensive in terms of memory and should be avoided if possible, although it is used almost
/// everytime a [org.apache.pinot.query.runtime.operator.MultiStageOperator] is used.
///
/// This class is a subclass of [MseBlock.Data] and is used to store data in row heap format.
/// This is probably the less efficient way to store data, but it is also the easiest to work with.
/// At the time of writing, this class is used almost every time we need to read or create data blocks.
/// The only place where this class is not used is when we need to shuffle data through the network, in which case
/// we use [SerializedDataBlock].
public class RowHeapDataBlock implements MseBlock.Data {
  private final DataSchema _dataSchema;
  private final List<Object[]> _rows;
  /// This is a hack we created to support different hash exchange distributions.
  /// We should find a new way to keep this information that is more solid than the current one.
  /// This information is only set in [org.apache.pinot.query.runtime.operator.LeafOperator] and
  /// [org.apache.pinot.query.runtime.operator.AggregateOperator] and consumed in
  /// [org.apache.pinot.query.runtime.operator.exchange.HashExchange] when we need to shuffle data.
  /// This means that the value of this attribute is the same for all blocks in the stage, so we should be able to
  /// pass this static information from the operator to the exchange without the need to store it in the block.
  /// Also, some operators in the middle (like [org.apache.pinot.query.runtime.operator.FilterOperator]) may remove
  /// the aggregation functions from the block, which may be problematic.
  @Nullable
  @Deprecated
  @SuppressWarnings("rawtypes")
  private final AggregationFunction[] _aggFunctions;

  /// Creates a new block with the given rows and schema.
  /// @param rows The rows in the block. Once received, the list should not be mutated from outside this class.
  /// @param dataSchema The schema of the data in the block.
  public RowHeapDataBlock(List<Object[]> rows, DataSchema dataSchema) {
    this(rows, dataSchema, null);
  }

  /// Creates a new block with the given rows, schema and aggregation functions.
  /// @param rows The rows in the block. Once received, the list should not be mutated from outside this class.
  /// @param dataSchema The schema of the data in the block.
  @SuppressWarnings("rawtypes")
  public RowHeapDataBlock(List<Object[]> rows, DataSchema dataSchema,
      @Nullable AggregationFunction[] aggFunctions) {
    _dataSchema = dataSchema;
    _rows = rows;
    _aggFunctions = aggFunctions;
  }

  @Override
  public int getNumRows() {
    return _rows.size();
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  /// Returns the rows in the block.
  /// The returned list should be considered immutable.
  public List<Object[]> getRows() {
    return _rows;
  }

  @Deprecated
  @Nullable
  @SuppressWarnings("rawtypes")
  public AggregationFunction[] getAggFunctions() {
    return _aggFunctions;
  }

  /// Returns this same object.
  @Override
  public RowHeapDataBlock asRowHeap() {
    return this;
  }

  /// Returns a copy of this block that does not share any mutable cell values with this block.
  ///
  /// Cells of [OBJECT][ColumnDataType#OBJECT] columns hold aggregation intermediate results (e.g. priority queues or
  /// sketches) that downstream operators mutate in place when merging them or extracting final results. This method
  /// copies such cells by round-tripping them through the corresponding aggregation function's intermediate result
  /// serde, which requires the aggregation functions to be attached to this block whenever a non-null OBJECT cell is
  /// present (the same requirement [DataBlockBuilder] imposes to serialize such blocks). Cells of all other column
  /// types are effectively immutable and are shared with the returned block.
  @SuppressWarnings({"rawtypes", "unchecked"})
  public RowHeapDataBlock copy() {
    ColumnDataType[] storedTypes = _dataSchema.getStoredColumnDataTypes();
    int numColumns = storedTypes.length;
    List<Object[]> copiedRows = new ArrayList<>(_rows.size());
    int numCellsProcessed = 0;
    for (Object[] row : _rows) {
      Object[] copiedRow = row.clone();
      for (int colId = 0; colId < numColumns; colId++) {
        Object value = copiedRow[colId];
        if (value != null && storedTypes[colId] == ColumnDataType.OBJECT) {
          Preconditions.checkState(_aggFunctions != null,
              "Cannot copy OBJECT column: %s without aggregation functions", _dataSchema.getColumnName(colId));
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numCellsProcessed++, "RowHeapDataBlock#copy");
          // NOTE: The first (numColumns - numAggFunctions) columns are key columns
          AggregationFunction aggFunction = _aggFunctions[colId + _aggFunctions.length - numColumns];
          AggregationFunction.SerializedIntermediateResult serialized = aggFunction.serializeIntermediateResult(value);
          copiedRow[colId] = aggFunction.deserializeIntermediateResult(
              new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
        }
      }
      copiedRows.add(copiedRow);
    }
    return new RowHeapDataBlock(copiedRows, _dataSchema, _aggFunctions);
  }

  @Override
  public SerializedDataBlock asSerialized() {
    try {
      return new SerializedDataBlock(DataBlockBuilder.buildFromRows(_rows, _dataSchema, _aggFunctions));
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  @Override
  public boolean isRowHeap() {
    return true;
  }

  @Override
  public <R, A> R accept(Data.Visitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  @Override
  public String toString() {
    return "{\"type\": \"rowHeap\", \"numRows\": " + getNumRows() + "}";
  }
}
