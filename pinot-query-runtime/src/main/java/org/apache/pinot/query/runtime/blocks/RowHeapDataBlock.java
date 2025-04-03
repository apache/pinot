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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;

/// A block that contains data in row heap format.
///
/// This class is a subclass of [MseBlock.Data] and is used to store data in row heap format.
/// This is probably the less efficient way to store data, but it is also the easiest to work with.
/// At the time of writing, this class is used almost every time we need to read or create data blocks.
/// The only place where this class is not used is when we need to shuffle data through the network, in which case
/// we use [SerializedDataBlock].
public class RowHeapDataBlock implements MseBlock, MseBlock.Data {
  private final DataSchema _dataSchema;
  private final List<Object[]> _rows;
  @Nullable
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
