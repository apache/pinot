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

import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * An {@link MseBlock.Data} backed by an Apache Arrow {@link VectorSchemaRoot}.
 *
 * <p>This is the columnar block type for the Multi-Stage Query Engine when Arrow is enabled. The
 * {@link #asRowHeap()} and {@link #asSerialized()} methods provide a fallback path for operators that
 * don't yet consume Arrow blocks directly — they materialize the off-heap columnar data into the legacy
 * row-heap format. These fallback conversions are expensive (every primitive gets boxed, every string
 * allocated on heap); they exist as a compatibility bridge, not as the intended hot path.
 *
 * <p><b>Lifetime — reference counting.</b> The block's column data lives off-heap and is invisible to the
 * JVM garbage collector, so it must be reclaimed explicitly. Reclamation is governed by a block-level
 * reference count, manipulated only through {@link #retain()} and {@link #release()}:
 * <ul>
 *   <li>A freshly constructed block starts at refcount 1; the constructor's caller is the initial holder.</li>
 *   <li>{@link #retain()} increments the count — call it once per additional holder before sharing a block
 *       (e.g. enqueuing the same block onto multiple local mailbox queues for a broadcast exchange).</li>
 *   <li>{@link #release()} decrements the count and frees the underlying {@link VectorSchemaRoot} once it
 *       reaches 0. Every holder must {@code release()} exactly once when done — including on exception
 *       paths.</li>
 * </ul>
 * The invariant is: refcount == number of live holders. Transferring a block to a single new holder
 * (handing off ownership without keeping it yourself) leaves the count unchanged — no retain, no release.
 *
 * <p>This class deliberately does <b>not</b> implement {@link AutoCloseable}. {@code close()} means
 * "destroy unconditionally", which collides with reference counting: a try-with-resources block would free
 * the buffers at scope exit even while other holders are still reading them. Exposing only explicit
 * {@code retain()}/{@code release()} makes each lifetime event visible at the call site. (This mirrors
 * Netty's {@code ReferenceCounted}, which avoids {@code AutoCloseable} for the same reason.)
 *
 * <p>{@link #asRowHeap()} and {@link #asSerialized()} produce independent heap copies and do <b>not</b>
 * change this block's refcount — the caller still owns its reference and must {@link #release()} it.
 *
 * <p><b>Thread-safety:</b> {@code retain()}/{@code release()} are atomic. Two distinct ordering guarantees
 * are in play, and they come from different places:
 * <ul>
 *   <li>Safe publication of the column <em>buffer contents</em> from a producer thread to a consumer thread
 *       is supplied by the handoff mechanism (the mailbox queue's publication semantics), <em>not</em> by
 *       this refcount.</li>
 *   <li>Safe reclamation is supplied by the {@link AtomicInteger}: the terminal {@code 1 -> 0} transition
 *       that runs {@code _dataBlock.close()} happens-after every other holder's {@code retain()}/{@code
 *       release()} — and therefore after each holder's buffer reads, which precede its own release in
 *       program order — so the buffers are never freed while a holder is still reading them.</li>
 * </ul>
 * No extra synchronization on the block itself is required.
 */
public final class ArrowBlock implements MseBlock.Data {
  private final ArrowDataBlock _dataBlock;
  private final AtomicInteger _refCount = new AtomicInteger(1);

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
   * <p>The returned {@link RowHeapDataBlock} is an independent heap copy of this block's off-heap buffers.
   * This block's refcount is <em>unchanged</em> — the caller still owns its reference and must
   * {@link #release()} it; the row-heap copy outlives any later release of this block.
   *
   * <p>TODO: remove this method once all operators consume {@link ArrowBlock} directly.
   */
  @Override
  public RowHeapDataBlock asRowHeap() {
    int numRows = getNumRows();
    int numCols = _dataBlock.getNumberOfColumns();
    DataSchema schema = getDataSchema();
    // Dispatch on Pinot's stored column type (not the Arrow vector class) so the row-heap cells carry the
    // types downstream expects, and decide the type once per column. Each case then runs a tight per-row
    // loop that reads the typed vector directly, instead of re-evaluating the switch and re-fetching the
    // vector for every cell (the row-oriented shape this used to have).
    ColumnDataType[] storedTypes = schema.getStoredColumnDataTypes();
    VectorSchemaRoot root = _dataBlock.getRoot();
    Object[][] rows = new Object[numRows][numCols];
    for (int colIdx = 0; colIdx < numCols; colIdx++) {
      FieldVector vector = root.getVector(colIdx);
      switch (storedTypes[colIdx]) {
        case INT:
          // BOOLEAN is stored as INT but backed by a BitVector; plain INT uses an IntVector. Both get()s
          // return int, boxed to Integer.
          if (vector instanceof BitVector) {
            fillColumn(rows, colIdx, vector, numRows, ((BitVector) vector)::get);
          } else {
            fillColumn(rows, colIdx, vector, numRows, ((IntVector) vector)::get);
          }
          break;
        case LONG:
          // TIMESTAMP is also stored as LONG (a BigIntVector).
          fillColumn(rows, colIdx, vector, numRows, ((BigIntVector) vector)::get);
          break;
        case FLOAT:
          fillColumn(rows, colIdx, vector, numRows, ((Float4Vector) vector)::get);
          break;
        case DOUBLE:
          fillColumn(rows, colIdx, vector, numRows, ((Float8Vector) vector)::get);
          break;
        case STRING:
          writeStringColumn(rows, colIdx, vector, numRows);
          break;
        case BYTES: {
          VarBinaryVector v = (VarBinaryVector) vector;
          fillColumn(rows, colIdx, vector, numRows, row -> new ByteArray(v.get(row)));
          break;
        }
        default:
          throw new UnsupportedOperationException(
              "ArrowBlock.asRowHeap does not support column stored type " + storedTypes[colIdx]
                  + " (column '" + schema.getColumnName(colIdx) + "')");
      }
    }
    return new RowHeapDataBlock(Arrays.asList(rows), schema);
  }

  private static void fillColumn(Object[][] rows, int colIdx, FieldVector vector, int numRows,
      IntFunction<Object> reader) {
    for (int row = 0; row < numRows; row++) {
      if (!vector.isNull(row)) {
        rows[row][colIdx] = reader.apply(row);
      }
    }
  }

  /**
   * Materializes a STRING/JSON column into {@code rows}. Both layouts {@link ArrowDataBlock} can produce are
   * handled: a plain {@link VarCharVector}, or a dictionary-encoded integer index vector whose values live in
   * the block's dictionary provider.
   */
  private void writeStringColumn(Object[][] rows, int colIdx, FieldVector vector, int numRows) {
    DictionaryEncoding encoding = vector.getField().getDictionary();
    if (encoding == null) {
      VarCharVector v = (VarCharVector) vector;
      fillColumn(rows, colIdx, vector, numRows, row -> new String(v.get(row), StandardCharsets.UTF_8));
      return;
    }
    MapDictionaryProvider provider = _dataBlock.getDictionaryProvider();
    if (provider == null) {
      throw new IllegalStateException(
          "Column at index " + colIdx + " is dictionary-encoded but the block carries no dictionary provider");
    }
    IntVector indices = (IntVector) vector;
    VarCharVector dictionary = (VarCharVector) provider.lookup(encoding.getId()).getVector();
    fillColumn(rows, colIdx, vector, numRows,
        row -> new String(dictionary.get(indices.get(row)), StandardCharsets.UTF_8));
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

  // ----- Reference-counted lifetime (see class Javadoc) -----

  /**
   * Increments the reference count to register an additional holder. Call this once per extra holder
   * before sharing the same block (e.g. broadcasting it to multiple local mailbox queues).
   *
   * @throws IllegalStateException if the block has already been freed (refcount 0) — retaining a freed
   *     block would resurrect a dangling {@link VectorSchemaRoot}.
   */
  public void retain() {
    while (true) {
      int current = _refCount.get();
      if (current == 0) {
        throw new IllegalStateException("Cannot retain an ArrowBlock that has already been freed (refcount=0)");
      }
      if (_refCount.compareAndSet(current, current + 1)) {
        return;
      }
    }
  }

  /**
   * Decrements the reference count, freeing the underlying {@link VectorSchemaRoot} when it reaches 0.
   * Every holder must call this exactly once for its reference when done with the block — including on
   * exception paths.
   *
   * <p><b>Release exactly once per reference.</b> Do not call {@code release()} for the same reference from
   * both the happy path and a {@code finally}/{@code catch} — that is a double-free, throws here, and can
   * mask the original exception. Structure cleanup so each held reference is released on exactly one path.
   *
   * @throws IllegalStateException if the block has already been freed (refcount 0) — a second release is a
   *     double-free.
   */
  public void release() {
    while (true) {
      int current = _refCount.get();
      if (current == 0) {
        throw new IllegalStateException("Cannot release an ArrowBlock that has already been freed (double-free)");
      }
      if (_refCount.compareAndSet(current, current - 1)) {
        if (current == 1) {
          // Last holder is letting go — free the off-heap buffers now.
          _dataBlock.close();
        }
        return;
      }
    }
  }

  @VisibleForTesting
  int refCount() {
    return _refCount.get();
  }
}
