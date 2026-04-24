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

import java.util.Arrays;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/**
 * An {@link MseBlock.Data} backed by an Apache Arrow {@link VectorSchemaRoot}.
 *
 * <p>This is the columnar block type for the Multi-Stage Query Engine when Arrow is enabled. The
 * {@link #asRowHeap()} and {@link #asSerialized()} methods provide a fallback path for operators that
 * don't yet consume Arrow blocks directly — they materialize the off-heap columnar data into the legacy
 * row-heap format. These fallback conversions are expensive (every primitive gets boxed, every string
 * allocated on heap); they exist as a compatibility bridge, not as the intended hot path.
 *
 * <p><b>Lifetime:</b> the block's off-heap buffers are owned by the {@code BufferAllocator} used to
 * construct it (typically a per-query or per-stage child of the root allocator). When that allocator
 * closes, every block it produced is freed atomically. Individual blocks <em>do not</em> need to be
 * released by operator code — the allocator is the unit of ownership.
 *
 * <p>{@link #close()} is provided for explicit early disposal (e.g. in tests, or when a caller wants to
 * free a block before its allocator closes). It is <b>not</b> reference-counted; calling it twice will
 * attempt to close the underlying {@link ArrowDataBlock} twice.
 *
 * <p>To move a block across allocator scopes (e.g. across stage boundaries or onto the wire), use
 * Arrow's {@code TransferPair} API — this is zero-copy and preserves the allocator-ownership invariant.
 */
public class ArrowBlock implements MseBlock.Data, AutoCloseable {
  private final ArrowDataBlock _dataBlock;

  public ArrowBlock(ArrowDataBlock dataBlock) {
    _dataBlock = dataBlock;
  }

  public ArrowDataBlock getDataBlock() {
    return _dataBlock;
  }

  // ----- MseBlock.Data -----

  @Override
  public int getNumRows() {
    return _dataBlock.getNumberOfRows();
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataBlock.getDataSchema();
  }

  @Override
  public boolean isRowHeap() {
    return false;
  }

  @Override
  public boolean isSerialized() {
    return false;
  }

  @Override
  public boolean isArrow() {
    return true;
  }

  @Override
  public ArrowBlock asArrow() {
    return this;
  }

  /**
   * Materializes the Arrow columnar data into a row-heap block. This is the fallback path for operators
   * that don't yet consume Arrow blocks directly; it is expensive (every primitive gets boxed, every string
   * allocated on heap).
   *
   * <p>The returned {@link RowHeapDataBlock} is independent of this block's off-heap buffers — the caller
   * may close this block (or let its allocator close) without affecting the row-heap copy. This block is
   * <em>not</em> closed as a side effect.
   *
   * <p>TODO: remove this method once all operators consume {@link ArrowBlock} directly.
   */
  @Override
  public RowHeapDataBlock asRowHeap() {
    int numRows = getNumRows();
    int numCols = _dataBlock.getNumberOfColumns();
    DataSchema schema = getDataSchema();
    // Dispatch on Pinot's stored column type (not on the Arrow vector's  class) so the row-heap
    // cells carry the types downstream expects
    ColumnDataType[] storedTypes = schema.getStoredColumnDataTypes();
    VectorSchemaRoot root = _dataBlock.getRoot();
    Object[][] rows = new Object[numRows][numCols];
    for (int colIdx = 0; colIdx < numCols; colIdx++) {
      FieldVector vector = root.getVector(colIdx);
      ColumnDataType storedType = storedTypes[colIdx];
      for (int row = 0; row < numRows; row++) {
        if (vector.isNull(row)) {
          continue;
        }
        switch (storedType) {
          case INT:
            rows[row][colIdx] = _dataBlock.getInt(row, colIdx);
            break;
          case LONG:
            rows[row][colIdx] = _dataBlock.getLong(row, colIdx);
            break;
          case FLOAT:
            rows[row][colIdx] = _dataBlock.getFloat(row, colIdx);
            break;
          case DOUBLE:
            rows[row][colIdx] = _dataBlock.getDouble(row, colIdx);
            break;
          case STRING:
            rows[row][colIdx] = _dataBlock.getString(row, colIdx);
            break;
          case BYTES:
            rows[row][colIdx] = _dataBlock.getBytes(row, colIdx);
            break;
          default:
            throw new UnsupportedOperationException(
                "ArrowBlock.asRowHeap does not support column stored type " + storedType
                    + " (column '" + schema.getColumnName(colIdx) + "')");
        }
      }
    }
    return new RowHeapDataBlock(Arrays.asList(rows), schema);
  }

  /**
   * Materializes the Arrow columnar data into a serialized block by first converting to row-heap. Fallback
   * path for operators that need serialized output; inherits the cost of {@link #asRowHeap()}.
   *
   * <p>TODO: remove this method once all operators consume {@link ArrowBlock} directly.
   */
  @Override
  public SerializedDataBlock asSerialized() {
    return asRowHeap().asSerialized();
  }

  @Override
  public <R, A> R accept(MseBlock.Data.Visitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  @Override
  public String toString() {
    return "{\"type\": \"arrow\", \"numRows\": " + getNumRows() + "}";
  }

  @Override
  public void close() {
    _dataBlock.close();
  }
}
