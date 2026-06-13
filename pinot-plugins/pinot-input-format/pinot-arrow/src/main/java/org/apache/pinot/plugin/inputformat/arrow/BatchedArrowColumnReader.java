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
package org.apache.pinot.plugin.inputformat.arrow;

import java.io.IOException;
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.MultiValueResult;


/**
 * A {@link ColumnReader} over one column of a {@link BatchedArrowFileSource}. Reads are served by
 * delegating to a per-batch {@link ArrowColumnReader} over the current record batch's column vector;
 * the source loads at most one batch at a time, so peak resident memory is one batch (see
 * {@link BatchedArrowFileSource}).
 *
 * <p>Random access ({@code getXxx(docId)}) seeks to the owning batch and reads it; sequential access
 * ({@code next} / typed {@code nextXxx}) is built on it. Type predicates are derived from the column's
 * (decoded) Arrow {@link Field}, so they need no batch load. The column-major segment build consumes
 * each column to completion sequentially (rewind + next), one column at a time.
 *
 * <p>The per-batch delegate is invalidated when the shared source moves to another batch (whether
 * this reader advanced it or a different one did); {@link #delegate(int)} documents why and rebuilds
 * it. Interleaving readers therefore stays correct (it just re-loads batches); the build never
 * interleaves.
 *
 * <p>This class is not thread-safe. {@code @SuppressWarnings("serial")}: {@link ColumnReader} is
 * {@link java.io.Serializable} by SPI contract, but this reader holds non-serializable Arrow state
 * and is never serialized.
 */
@SuppressWarnings("serial")
public class BatchedArrowColumnReader implements ColumnReader {

  private final BatchedArrowFileSource _source;
  private final String _columnName;
  private final boolean _extractRawTimeValues;
  private final int _totalDocs;
  private final Field _effectiveField;

  private int _nextDocId;
  private int _delegateBatchIdx = -1;
  private ArrowColumnReader _delegate;

  BatchedArrowColumnReader(BatchedArrowFileSource source, String columnName) {
    _source = source;
    _columnName = columnName;
    _extractRawTimeValues = source.extractRawTimeValues();
    _totalDocs = source.getTotalDocs();
    _effectiveField = source.effectiveField(columnName);
  }

  // ---- sequential iteration (advance _nextDocId only after a successful read) ----

  @Override
  public boolean hasNext() {
    return _nextDocId < _totalDocs;
  }

  @Override
  @Nullable
  public Object next()
      throws IOException {
    requireHasNext();
    Object value = getValue(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public boolean isNextNull()
      throws IOException {
    requireHasNext();
    // Peek; does not advance.
    return isNull(_nextDocId);
  }

  @Override
  public void skipNext()
      throws IOException {
    requireHasNext();
    _nextDocId++;
  }

  @Override
  public int nextInt()
      throws IOException {
    requireHasNext();
    int value = getInt(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public long nextLong()
      throws IOException {
    requireHasNext();
    long value = getLong(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public float nextFloat()
      throws IOException {
    requireHasNext();
    float value = getFloat(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public double nextDouble()
      throws IOException {
    requireHasNext();
    double value = getDouble(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public BigDecimal nextBigDecimal()
      throws IOException {
    requireHasNext();
    BigDecimal value = getBigDecimal(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public String nextString()
      throws IOException {
    requireHasNext();
    String value = getString(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public byte[] nextBytes()
      throws IOException {
    requireHasNext();
    byte[] value = getBytes(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public MultiValueResult<int[]> nextIntMV()
      throws IOException {
    requireHasNext();
    MultiValueResult<int[]> value = getIntMV(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public MultiValueResult<long[]> nextLongMV()
      throws IOException {
    requireHasNext();
    MultiValueResult<long[]> value = getLongMV(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public MultiValueResult<float[]> nextFloatMV()
      throws IOException {
    requireHasNext();
    MultiValueResult<float[]> value = getFloatMV(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public MultiValueResult<double[]> nextDoubleMV()
      throws IOException {
    requireHasNext();
    MultiValueResult<double[]> value = getDoubleMV(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public BigDecimal[] nextBigDecimalMV()
      throws IOException {
    requireHasNext();
    BigDecimal[] value = getBigDecimalMV(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public String[] nextStringMV()
      throws IOException {
    requireHasNext();
    String[] value = getStringMV(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public byte[][] nextBytesMV()
      throws IOException {
    requireHasNext();
    byte[][] value = getBytesMV(_nextDocId);
    _nextDocId++;
    return value;
  }

  @Override
  public void rewind() {
    _nextDocId = 0;
    _delegateBatchIdx = -1;
    _delegate = null;
  }

  // ---- random access (seeks to the owning batch) ----

  @Override
  public boolean isNull(int docId) {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    try {
      return delegate(batchIdx).isNull(localRow(docId, batchIdx));
    } catch (IOException e) {
      throw new RuntimeException("Failed reading column " + _columnName + " at doc " + docId, e);
    }
  }

  @Override
  public int getInt(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getInt(localRow(docId, batchIdx));
  }

  @Override
  public long getLong(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getLong(localRow(docId, batchIdx));
  }

  @Override
  public float getFloat(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getFloat(localRow(docId, batchIdx));
  }

  @Override
  public double getDouble(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getDouble(localRow(docId, batchIdx));
  }

  @Override
  public BigDecimal getBigDecimal(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getBigDecimal(localRow(docId, batchIdx));
  }

  @Override
  public String getString(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getString(localRow(docId, batchIdx));
  }

  @Override
  public byte[] getBytes(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getBytes(localRow(docId, batchIdx));
  }

  @Override
  @Nullable
  public Object getValue(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getValue(localRow(docId, batchIdx));
  }

  @Override
  public MultiValueResult<int[]> getIntMV(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getIntMV(localRow(docId, batchIdx));
  }

  @Override
  public MultiValueResult<long[]> getLongMV(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getLongMV(localRow(docId, batchIdx));
  }

  @Override
  public MultiValueResult<float[]> getFloatMV(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getFloatMV(localRow(docId, batchIdx));
  }

  @Override
  public MultiValueResult<double[]> getDoubleMV(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getDoubleMV(localRow(docId, batchIdx));
  }

  @Override
  public BigDecimal[] getBigDecimalMV(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getBigDecimalMV(localRow(docId, batchIdx));
  }

  @Override
  public String[] getStringMV(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getStringMV(localRow(docId, batchIdx));
  }

  @Override
  public byte[][] getBytesMV(int docId)
      throws IOException {
    checkBounds(docId);
    int batchIdx = _source.batchIndexForDoc(docId);
    return delegate(batchIdx).getBytesMV(localRow(docId, batchIdx));
  }

  // ---- metadata / type predicates (derived from the schema field, no batch load) ----

  @Override
  public String getColumnName() {
    return _columnName;
  }

  @Override
  public int getTotalDocs() {
    return _totalDocs;
  }

  @Override
  public boolean isSingleValue() {
    return !isList(_effectiveField.getType());
  }

  @Override
  public boolean isInt() {
    ArrowType type = elementType();
    return (type instanceof ArrowType.Int && ((ArrowType.Int) type).getBitWidth() == 32)
        || type instanceof ArrowType.Bool;
  }

  @Override
  public boolean isLong() {
    ArrowType type = elementType();
    return type instanceof ArrowType.Int && ((ArrowType.Int) type).getBitWidth() == 64;
  }

  @Override
  public boolean isFloat() {
    ArrowType type = elementType();
    return type instanceof ArrowType.FloatingPoint
        && ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.SINGLE;
  }

  @Override
  public boolean isDouble() {
    ArrowType type = elementType();
    return type instanceof ArrowType.FloatingPoint
        && ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.DOUBLE;
  }

  @Override
  public boolean isBigDecimal() {
    return elementType() instanceof ArrowType.Decimal;
  }

  @Override
  public boolean isString() {
    return elementType() instanceof ArrowType.Utf8;
  }

  @Override
  public boolean isBytes() {
    return elementType() instanceof ArrowType.Binary;
  }

  @Override
  public void close() {
    // The shared BatchedArrowFileSource owns the file, reader, and allocator; the owning factory
    // closes it. Closing one column reader must not tear down the source the others still use.
  }

  // ---- helpers ----

  private ArrowColumnReader delegate(int batchIdx)
      throws IOException {
    // Rebuild the per-batch delegate when this reader advances to a new batch, OR when the shared
    // source has since been moved to a different batch (another reader advanced the cursor) — the
    // delegate wraps the source's current-batch vector, which is refilled (raw) or released
    // (decoded) once the source loads a different batch.
    if (_delegate == null || _delegateBatchIdx != batchIdx || _source.loadedBatchIdx() != batchIdx) {
      FieldVector vector = _source.columnVector(_columnName, batchIdx);
      _delegate = new ArrowColumnReader(_columnName, vector, _extractRawTimeValues);
      _delegateBatchIdx = batchIdx;
    }
    return _delegate;
  }

  private int localRow(int docId, int batchIdx) {
    return docId - _source.batchStartDoc(batchIdx);
  }

  private void checkBounds(int docId) {
    if (docId < 0 || docId >= _totalDocs) {
      throw new IndexOutOfBoundsException(
          "docId " + docId + " is out of range [0, " + _totalDocs + ") for column " + _columnName);
    }
  }

  private void requireHasNext() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
  }

  private ArrowType elementType() {
    Field field = _effectiveField;
    if (isList(field.getType())) {
      field = field.getChildren().get(0);
    }
    return field.getType();
  }

  private static boolean isList(ArrowType type) {
    return type instanceof ArrowType.List || type instanceof ArrowType.LargeList
        || type instanceof ArrowType.FixedSizeList;
  }
}
