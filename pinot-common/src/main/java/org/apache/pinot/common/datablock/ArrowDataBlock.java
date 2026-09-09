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
package org.apache.pinot.common.datablock;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferLedger;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * A {@link DataBlock} backed by an Apache Arrow {@link VectorSchemaRoot}.
 *
 * <p>Primitive types use their native Arrow vector types (e.g. {@code IntVector}, {@code BigIntVector}).
 * STRING/JSON columns are dictionary-encoded: the stored column is an integer index {@link IntVector}
 * whose field carries a {@link DictionaryEncoding}, and the distinct values live in a separate dictionary
 * vector held by {@code _dictionaryProvider}. High-cardinality (all-distinct) string columns fall back to
 * a plain {@code VarCharVector} (no encoding); {@link #getString} branches on the field's encoding to read
 * either layout.
 *
 * <p><b>Lifetime:</b> this is the low-level columnar container; block-level reference counting lives one
 * layer up on {@code org.apache.pinot.query.runtime.blocks.ArrowBlock}, which wraps this type for the
 * multi-stage runtime. {@link #close()} unconditionally frees the underlying {@link VectorSchemaRoot} and
 * the dictionary vectors (both of which decrement Arrow's buffer-level refcounts); it is the primitive
 * that {@code ArrowBlock.release()} invokes once its reference count reaches 0, and must be called at
 * most once.
 *
 * <p>{@link #getStringDictionary()}, {@link #getFixedData()}, and {@link #getVarSizeData()} throw
 * {@link UnsupportedOperationException} — they're part of {@link DataBlock}'s legacy abstraction that assumes
 * a flat-byte-buffer layout and have no equivalent in Arrow's columnar vector layout.
 */
public class ArrowDataBlock implements DataBlock, AutoCloseable {
  private final VectorSchemaRoot _root;
  private final Map<Integer, String> _errCodeToExceptionMap;
  private final DataSchema _dataSchema;
  // Holds the dictionary vectors for any dictionary-encoded (STRING/JSON) columns; null when no column is
  // dictionary-encoded. The dictionary vectors are off-heap and are freed by close().
  @Nullable
  private final MapDictionaryProvider _dictionaryProvider;
  @Nullable
  private final BufferAllocator _ownedAllocator;

  public ArrowDataBlock(VectorSchemaRoot root, DataSchema schema) {
    this(root, schema, null);
  }

  public ArrowDataBlock(VectorSchemaRoot root, DataSchema schema, @Nullable MapDictionaryProvider dictionaryProvider) {
    this(root, schema, dictionaryProvider, null);
  }

  private ArrowDataBlock(VectorSchemaRoot root, DataSchema schema,
      @Nullable MapDictionaryProvider dictionaryProvider, @Nullable BufferAllocator ownedAllocator) {
    _root = root;
    _dataSchema = schema;
    _dictionaryProvider = dictionaryProvider;
    _ownedAllocator = ownedAllocator;
    _errCodeToExceptionMap = new HashMap<>();
  }

  public Schema getSchema() {
    return _root.getSchema();
  }

  public VectorSchemaRoot getRoot() {
    return _root;
  }

  /**
   * Returns the per-column dictionary provider for the dictionary-encoded (STRING/JSON) columns, or
   * {@code null} if no column in this block is dictionary-encoded. Exposed for the wrapping
   * {@code ArrowBlock} so a column-oriented reader (e.g. {@code asRowHeap}) can resolve a column's
   * dictionary once instead of per row.
   */
  @Nullable
  public MapDictionaryProvider getDictionaryProvider() {
    return _dictionaryProvider;
  }

  /**
   * Retains an independently closeable view in an allocator with the same Arrow root. The source is not cleared;
   * both blocks must remain read-only while their buffers are shared, and either block may be closed first.
   * Each view reserves its full physical buffer footprint in a child of the target until closed, even while
   * another receiver owns those allocations. Reservations can conservatively count shared memory more than once.
   */
  public ArrowDataBlock retainTo(BufferAllocator allocator) {
    long reservation = retainedAllocationSize(allocator);
    BufferAllocator child = allocator.newChildAllocator("retained-arrow-block", reservation, Long.MAX_VALUE);
    ArrowDataBlock retained = null;
    try {
      retained = retainedCopy(_root, _dataSchema, _dictionaryProvider, _errCodeToExceptionMap, child, child);
      if (retained.retainedAllocationSize(child) > reservation) {
        throw new OutOfMemoryException("Retaining Arrow block exceeds its reserved physical buffer footprint");
      }
      return retained;
    } catch (RuntimeException | Error e) {
      try {
        if (retained != null) {
          retained.close();
        } else {
          child.close();
        }
      } catch (RuntimeException | Error closeError) {
        e.addSuppressed(closeError);
      }
      throw e;
    }
  }

  private long retainedAllocationSize(BufferAllocator allocator) {
    Map<Object, Boolean> allocations = new IdentityHashMap<>();
    long size = 0;
    for (FieldVector vector : _root.getFieldVectors()) {
      size = Math.addExact(size, retainedAllocationSize(vector, _root.getRowCount(), allocator, allocations));
    }
    if (_dictionaryProvider != null) {
      for (long id : _dictionaryProvider.getDictionaryIds()) {
        FieldVector vector = _dictionaryProvider.lookup(id).getVector();
        size = Math.addExact(size, retainedAllocationSize(vector, vector.getValueCount(), allocator, allocations));
      }
    }
    return size;
  }

  private static long retainedAllocationSize(FieldVector vector, int rowCount, BufferAllocator allocator,
      Map<Object, Boolean> allocations) {
    if (rowCount == 0) {
      return 0;
    }
    long size = 0;
    for (ArrowBuf buffer : vector.getFieldBuffers()) {
      ReferenceManager manager = buffer.getReferenceManager();
      Object allocation = manager instanceof BufferLedger ? ((BufferLedger) manager).getAllocationManager() : manager;
      if (allocations.put(allocation, Boolean.TRUE) == null) {
        size = Math.addExact(size, manager.getSize());
      }
    }
    if (vector instanceof BaseVariableWidthVector && vector.getOffsetBuffer().getInt(0) != 0) {
      // A rebased variable-width slice allocates an offset buffer in addition to retaining the original allocation.
      size = Math.addExact(size, allocator.getRoundingPolicy().getRoundedSize((rowCount + 1L) * Integer.BYTES));
    }
    for (FieldVector child : vector.getChildrenFromFields()) {
      size = Math.addExact(size, retainedAllocationSize(child, child.getValueCount(), allocator, allocations));
    }
    return size;
  }

  // Also used to detach a reader's borrowed vectors before the reader advances or closes.
  static ArrowDataBlock retainedCopy(VectorSchemaRoot root, DataSchema schema,
      @Nullable DictionaryProvider dictionaries, Map<Integer, String> exceptions, BufferAllocator allocator) {
    return retainedCopy(root, schema, dictionaries, exceptions, allocator, null);
  }

  private static ArrowDataBlock retainedCopy(VectorSchemaRoot root, DataSchema schema,
      @Nullable DictionaryProvider dictionaries, Map<Integer, String> exceptions, BufferAllocator allocator,
      @Nullable BufferAllocator ownedAllocator) {
    List<FieldVector> ownedVectors = new ArrayList<>();
    try {
      List<FieldVector> vectors = new ArrayList<>(root.getFieldVectors().size());
      for (FieldVector vector : root.getFieldVectors()) {
        vectors.add(retainVector(vector, root.getRowCount(), allocator, ownedVectors));
      }
      MapDictionaryProvider retainedDictionaries = null;
      if (dictionaries != null && !dictionaries.getDictionaryIds().isEmpty()) {
        retainedDictionaries = new MapDictionaryProvider();
        for (long id : dictionaries.getDictionaryIds()) {
          Dictionary dictionary = dictionaries.lookup(id);
          FieldVector vector = dictionary.getVector();
          retainedDictionaries.put(new Dictionary(
              retainVector(vector, vector.getValueCount(), allocator, ownedVectors), dictionary.getEncoding()));
        }
      }
      ArrowDataBlock retained = new ArrowDataBlock(
          new VectorSchemaRoot(root.getSchema(), vectors, root.getRowCount()), schema.clone(), retainedDictionaries,
          ownedAllocator);
      retained._errCodeToExceptionMap.putAll(exceptions);
      return retained;
    } catch (RuntimeException | Error e) {
      for (FieldVector vector : ownedVectors) {
        try {
          vector.close();
        } catch (RuntimeException | Error closeError) {
          e.addSuppressed(closeError);
        }
      }
      throw e;
    }
  }

  private static FieldVector retainVector(FieldVector vector, int rowCount, BufferAllocator allocator,
      List<FieldVector> ownedVectors) {
    TransferPair pair = vector.getTransferPair(vector.getField(), allocator);
    FieldVector retained = (FieldVector) pair.getTo();
    ownedVectors.add(retained);
    if (rowCount == 0) {
      return retained;
    }
    pair.splitAndTransfer(0, rowCount);
    if (retained instanceof BitVector
        && retained.getDataBuffer().getReferenceManager().getAllocator() != allocator) {
      // Arrow 19's BitVector split retains the source ledger. Rehome the private slice, never the shared source.
      TransferPair rehome = retained.getTransferPair(retained.getField(), allocator);
      FieldVector rehomed = (FieldVector) rehome.getTo();
      ownedVectors.add(rehomed);
      rehome.transfer();
      retained.close();
      retained = rehomed;
    }
    // Arrow ownership transfers can exceed an allocator's limit without throwing.
    if (allocator.isOverLimit()) {
      throw new OutOfMemoryException("Retaining Arrow block exceeds allocator limit: " + allocator.getName());
    }
    return retained;
  }

  // ----- DataBlock interface -----

  @Override
  public Map<String, String> getMetadata() {
    return Map.of();
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  @Override
  public int getNumberOfRows() {
    return _root.getRowCount();
  }

  @Override
  public int getNumberOfColumns() {
    return _root.getSchema().getFields().size();
  }

  @Override
  public void addException(int errCode, String errMsg) {
    _errCodeToExceptionMap.put(errCode, errMsg);
  }

  @Override
  public Map<Integer, String> getExceptions() {
    return _errCodeToExceptionMap;
  }

  @Override
  public List<ByteBuffer> serialize()
      throws IOException {
    return DataBlockUtils.serialize(this);
  }

  @Override
  public int getInt(int rowId, int colId) {
    FieldVector vector = _root.getVector(colId);
    if (vector instanceof IntVector) {
      return ((IntVector) vector).get(rowId);
    }
    if (vector instanceof BitVector) {
      return ((BitVector) vector).get(rowId);
    }
    throw new UnsupportedOperationException("Cannot read int from vector type: " + vector.getClass().getSimpleName());
  }

  @Override
  public long getLong(int rowId, int colId) {
    return ((BigIntVector) _root.getVector(colId)).get(rowId);
  }

  @Override
  public float getFloat(int rowId, int colId) {
    return ((Float4Vector) _root.getVector(colId)).get(rowId);
  }

  @Override
  public double getDouble(int rowId, int colId) {
    return ((Float8Vector) _root.getVector(colId)).get(rowId);
  }

  @Override
  public BigDecimal getBigDecimal(int rowId, int colId) {
    Object value = ((VarCharVector) _root.getVector(colId)).getObject(rowId);
    return value == null ? null : new BigDecimal(value.toString());
  }

  @Override
  // TODO: Explore if Text can be used here instead, to evaluate after operator changes.
  public String getString(int rowId, int colId) {
    FieldVector vector = _root.getVector(colId);
    DictionaryEncoding encoding = vector.getField().getDictionary();
    if (encoding == null) {
      // Plain VarCharVector. Read the raw bytes directly (avoids the intermediate Text wrapper that
      // getObject() allocates); get(int) throws on null slots, so null-check first.
      VarCharVector varChar = (VarCharVector) vector;
      return varChar.isNull(rowId) ? null : new String(varChar.get(rowId), StandardCharsets.UTF_8);
    }
    // Dictionary-encoded: the column is an integer index vector; the value lives in the dictionary.
    IntVector indices = (IntVector) vector;
    if (indices.isNull(rowId)) {
      return null;
    }
    VarCharVector dictionary = (VarCharVector) _dictionaryProvider.lookup(encoding.getId()).getVector();
    return new String(dictionary.get(indices.get(rowId)), StandardCharsets.UTF_8);
  }

  @Override
  public ByteArray getBytes(int rowId, int colId) {
    byte[] bytes = ((VarBinaryVector) _root.getVector(colId)).get(rowId);
    return bytes == null ? null : new ByteArray(bytes);
  }

  // TODO: To support array columns in future PRs
  @Override
  public int[] getIntArray(int rowId, int colId) {
    throw new UnsupportedOperationException("Array columns are not supported on ArrowDataBlock yet");
  }

  @Override
  public long[] getLongArray(int rowId, int colId) {
    throw new UnsupportedOperationException("Array columns are not supported on ArrowDataBlock yet");
  }

  @Override
  public float[] getFloatArray(int rowId, int colId) {
    throw new UnsupportedOperationException("Array columns are not supported on ArrowDataBlock yet");
  }

  @Override
  public double[] getDoubleArray(int rowId, int colId) {
    throw new UnsupportedOperationException("Array columns are not supported on ArrowDataBlock yet");
  }

  @Override
  public BigDecimal[] getBigDecimalArray(int rowId, int colId) {
    throw new UnsupportedOperationException("Array columns are not supported on ArrowDataBlock yet");
  }

  @Override
  public String[] getStringArray(int rowId, int colId) {
    throw new UnsupportedOperationException("Array columns are not supported on ArrowDataBlock yet");
  }

  @Override
  public ByteArray[] getBytesArray(int rowId, int colId) {
    throw new UnsupportedOperationException("Array columns are not supported on ArrowDataBlock yet");
  }

  @Override
  public Map<String, Object> getMap(int rowId, int colId) {
    throw new UnsupportedOperationException("Map columns are not supported on ArrowDataBlock yet");
  }

  @Nullable
  @Override
  public CustomObject getCustomObject(int rowId, int colId) {
    throw new UnsupportedOperationException("CustomObject columns are not supported on ArrowDataBlock yet");
  }

  @Nullable
  @Override
  public RoaringBitmap getNullRowIds(int colId) {
    FieldVector vector = _root.getVector(colId);
    if (vector.getNullCount() == 0) {
      return null;
    }
    RoaringBitmap nullIds = new RoaringBitmap();
    int numRows = vector.getValueCount();
    for (int row = 0; row < numRows; row++) {
      if (vector.isNull(row)) {
        nullIds.add(row);
      }
    }
    return nullIds;
  }

  @Override
  public Type getDataBlockType() {
    return Type.ARROW;
  }

  // The three accessors below expose BaseDataBlock's serialization layout — a flat fixed-size buffer, a flat
  // variable-size buffer, and one global String[] dictionary shared across columns. The DataBlock interface
  // itself documents them as abstraction leaks.
  // Arrow has no such layout: data lives in a columnar VectorSchemaRoot (no flat byte buffers), and string
  // dictionaries are per-column Arrow DictionaryEncoding, not a single global String[]. There is therefore
  // no equivalent value to return, so these throw.
  @Nullable
  @Override
  public String[] getStringDictionary() {
    throw new UnsupportedOperationException("getStringDictionary is not supported on ArrowDataBlock");
  }

  @Nullable
  @Override
  public DataBuffer getFixedData() {
    throw new UnsupportedOperationException("getFixedData is not supported on ArrowDataBlock");
  }

  @Nullable
  @Override
  public DataBuffer getVarSizeData() {
    throw new UnsupportedOperationException("getVarSizeData is not supported on ArrowDataBlock");
  }

  @Nullable
  @Override
  public List<DataBuffer> getStatsByStage() {
    return List.of();
  }

  @Override
  public String toString() {
    return "{\"type\": \"arrow\", \"numRows\": " + getNumberOfRows() + ", \"numCols\": " + getNumberOfColumns() + "}";
  }

  @Override
  public void close() {
    // Free the dictionary vectors before the root; both hold off-heap buffers under the query allocator.
    try {
      if (_dictionaryProvider != null) {
        _dictionaryProvider.close();
      }
    } finally {
      try {
        _root.close();
      } finally {
        if (_ownedAllocator != null) {
          _ownedAllocator.close();
        }
      }
    }
  }
}
