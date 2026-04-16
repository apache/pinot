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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.common.utils.DataSchema;


/**
 * An {@link MseBlock.Data} backed by an Apache Arrow {@link VectorSchemaRoot}.
 *
 * <p>This is the primary columnar block type for the Multi-Stage Query Engine. All operators that can produce
 * Arrow data should return an {@code ArrowBlock}; conversion to {@link RowHeapDataBlock} or
 * {@link SerializedDataBlock} is available via the deprecated methods for backward-compatibility with operators
 * that have not yet been Arrow-ified.
 *
 * <p>Reference counting: the underlying {@link ArrowDataBlock} is reference-counted. Call {@link #retain()} to
 * increment the count before sharing a block across threads or data structures; call {@link #release()} (or
 * {@link #close()}) when done. The off-heap buffers are freed when the count reaches zero.
 */
public class ArrowBlock implements MseBlock.Data, AutoCloseable {
  private final ArrowDataBlock _dataBlock;
  private final AtomicInteger _refCount = new AtomicInteger(1);

  public ArrowBlock(VectorSchemaRoot root) {
    _dataBlock = new ArrowDataBlock(root);
  }

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
  public ArrowBlock asArrow() {
    return this;
  }

  /**
   * Materializes the Arrow columnar data into a row-heap block. This is an expensive operation and should only
   * be used for operators that have not yet been Arrow-ified.
   */
  @Deprecated
  @Override
  public RowHeapDataBlock asRowHeap() {
    int numRows = getNumRows();
    int numCols = _dataBlock.getNumberOfColumns();
    VectorSchemaRoot root = _dataBlock.getRoot();
    Object[][] rows = new Object[numRows][numCols];
    for (int colIdx = 0; colIdx < numCols; colIdx++) {
      FieldVector vector = root.getVector(colIdx);
      if (vector instanceof NullVector) {
        continue;
      }
      DictionaryEncoding encoding = vector.getField().getDictionary();
      if (encoding != null && _dataBlock.getDictionaryProvider() != null) {
        Dictionary dict = _dataBlock.getDictionaryProvider().lookup(encoding.getId());
        FieldVector dictVector = dict.getVector();
        IntVector indexVector = (IntVector) vector;
        for (int row = 0; row < numRows; row++) {
          if (!indexVector.isNull(row)) {
            rows[row][colIdx] = dictVector.getObject(indexVector.get(row)).toString();
          }
        }
      } else if (vector instanceof VarCharVector) {
        for (int row = 0; row < numRows; row++) {
          Object value = vector.getObject(row);
          if (value != null) {
            rows[row][colIdx] = value.toString();
          }
        }
      } else {
        for (int row = 0; row < numRows; row++) {
          rows[row][colIdx] = vector.getObject(row);
        }
      }
    }
    release();
    return new RowHeapDataBlock(Arrays.asList(rows), getDataSchema());
  }

  @Deprecated
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

  // ----- reference counting -----

  /** Increments the reference count. Must be paired with a subsequent {@link #release()}. */
  public void retain() {
    while (true) {
      int count = _refCount.get();
      if (count <= 0) {
        throw new IllegalStateException("ArrowBlock has already been released");
      }
      if (_refCount.compareAndSet(count, count + 1)) {
        return;
      }
    }
  }

  /** Decrements the reference count, closing the underlying buffers when it reaches zero. */
  public void release() {
    if (_refCount.decrementAndGet() == 0) {
      _dataBlock.close();
    }
  }

  @Override
  public void close() {
    release();
  }
}
