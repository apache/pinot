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
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.Table;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.spi.query.QueryThreadContext;


/// A block that contains data in row heap format.
/// Each row is an array of objects and each object is a reference to the cell value. Rows are stored in a List
/// or retained in an owned, finished table that can be consumed through [#getRowIterator()].
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
  @Nullable
  private volatile List<Object[]> _rows;
  @Nullable
  private Table _table;
  private final int _numRows;
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
    _numRows = rows.size();
    _aggFunctions = aggFunctions;
  }

  /// Creates a block that owns a finished table and exposes its existing row arrays through a cursor.
  /// The table and its records must not be updated or finished again after handoff. The table remains reachable
  /// until this block is released or [#getRows()] materializes the List fallback.
  @SuppressWarnings("rawtypes")
  public RowHeapDataBlock(Table table, DataSchema dataSchema, @Nullable AggregationFunction[] aggFunctions) {
    _dataSchema = dataSchema;
    _table = table;
    _numRows = table.size();
    _aggFunctions = aggFunctions;
  }

  @Override
  public int getNumRows() {
    List<Object[]> rows = _rows;
    return rows != null ? rows.size() : _numRows;
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  /// Returns the rows in the block, materializing and caching a List for table-backed blocks.
  /// The returned list should be considered immutable.
  public List<Object[]> getRows() {
    List<Object[]> rows = _rows;
    if (rows != null) {
      return rows;
    }
    synchronized (this) {
      if (_rows == null) {
        rows = new ArrayList<>(_numRows);
        Iterator<Object[]> iterator = getRowIterator();
        while (iterator.hasNext()) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(rows.size(), "RowHeapDataBlock#getRows");
          rows.add(iterator.next());
        }
        _rows = rows;
        _table = null;
      }
      return _rows;
    }
  }

  /// Returns whether rows can still be consumed directly from the owned table, without a List fallback.
  public boolean isTableBacked() {
    return _rows == null;
  }

  /// Returns a fresh cursor over the owned row arrays. Row arrays and mutable aggregation results are shared
  /// with [#getRows()], with the same ownership rules as a block constructed from a List.
  public Iterator<Object[]> getRowIterator() {
    List<Object[]> rows = _rows;
    if (rows != null) {
      return rows.iterator();
    }
    synchronized (this) {
      if (_rows != null) {
        return _rows.iterator();
      }
      assert _table != null;
      Iterator<Record> records = _table.iterator();
      return new Iterator<>() {
        @Override
        public boolean hasNext() {
          return records.hasNext();
        }

        @Override
        public Object[] next() {
          return records.next().getValues();
        }
      };
    }
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

  /// Returns whether this block contains [OBJECT][ColumnDataType#OBJECT] columns, which hold mutable aggregation
  /// intermediate results — the only cell values downstream operators mutate in place. Blocks that contain them must
  /// not be shared by reference across receivers; see [#copyObjectColumns()]. Extend both methods if operators ever
  /// start to mutate other cell types.
  public boolean containsObjectColumns() {
    for (ColumnDataType storedType : _dataSchema.getStoredColumnDataTypes()) {
      if (storedType == ColumnDataType.OBJECT) {
        return true;
      }
    }
    return false;
  }

  /// Returns a copy of this block whose [OBJECT][ColumnDataType#OBJECT] cells do not share state with this block.
  ///
  /// OBJECT cells hold aggregation intermediate results (e.g. priority queues or sketches) that downstream operators
  /// mutate in place when they merge them or extract final results, so the same instance must not reach two
  /// receivers. This method copies them by round-tripping them through the corresponding aggregation function's
  /// intermediate result serde, which requires the aggregation functions to be attached to this block whenever a
  /// non-null OBJECT cell is present (the same requirement [DataBlockBuilder] imposes to serialize such blocks).
  ///
  /// The row arrays are cloned, but cells of all other column types are shared with the returned block, even though
  /// some of them are backed by mutable objects (maps, arrays, byte arrays): downstream operators do not mutate
  /// them. Extend this method if that ever changes.
  @SuppressWarnings({"rawtypes", "unchecked"})
  public RowHeapDataBlock copyObjectColumns() {
    ColumnDataType[] storedTypes = _dataSchema.getStoredColumnDataTypes();
    int numColumns = storedTypes.length;
    List<Object[]> copiedRows = new ArrayList<>(getNumRows());
    int numCellsProcessed = 0;
    Iterator<Object[]> iterator = getRowIterator();
    while (iterator.hasNext()) {
      Object[] row = iterator.next();
      Object[] copiedRow = row.clone();
      for (int colId = 0; colId < numColumns; colId++) {
        Object value = copiedRow[colId];
        if (value != null && storedTypes[colId] == ColumnDataType.OBJECT) {
          Preconditions.checkState(_aggFunctions != null,
              "Cannot copy OBJECT column: %s without aggregation functions", _dataSchema.getColumnName(colId));
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numCellsProcessed++,
              "RowHeapDataBlock#copyObjectColumns");
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
      return new SerializedDataBlock(DataBlockBuilder.buildFromRows(getRows(), _dataSchema, _aggFunctions));
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
