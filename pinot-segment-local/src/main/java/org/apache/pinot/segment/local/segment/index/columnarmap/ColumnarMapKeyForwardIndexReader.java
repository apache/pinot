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
package org.apache.pinot.segment.local.segment.index.columnarmap;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * A per-key {@link ForwardIndexReader} backed by a {@link ColumnarMapIndexReader}.
 *
 * <p>Each instance is bound to a single key within a MAP column. Reads for document IDs
 * that do not contain the key return the type-appropriate zero/empty default value; the caller can
 * combine this with the presence bitmap ({@link ColumnarMapIndexReader#getPresenceBitmap}) when null
 * semantics are required.
 *
 * <p>When a {@link ColumnarMapKeyDictionary} is provided, this reader supports dictionary-encoded
 * access via {@link #readDictIds}, enabling dictionary-based GROUP BY operations.
 *
 * <p>No context is needed because the {@link ColumnarMapIndexReader} implementations maintain their
 * own internal state; {@link #createContext()} therefore returns {@code null}.
 *
 * <p>Lifecycle: this reader does NOT own the underlying {@link ColumnarMapIndexReader}—closing this
 * reader is a no-op. The owning {@link ColumnarMapDataSource} is responsible for closing the reader.
 */
public class ColumnarMapKeyForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {

  private final ColumnarMapIndexReader _columnarMapIndexReader;
  private final String _key;
  private final DataType _storedType;
  @Nullable
  private final ColumnarMapKeyDictionary _dictionary;
  @Nullable
  private final FixedBitIntReaderWriter _dictIdReader;
  @Nullable
  private final ImmutableRoaringBitmap _presenceBitmap;

  public ColumnarMapKeyForwardIndexReader(ColumnarMapIndexReader columnarMapIndexReader, String key,
      DataType storedType) {
    this(columnarMapIndexReader, key, storedType, null, null, null);
  }

  public ColumnarMapKeyForwardIndexReader(ColumnarMapIndexReader columnarMapIndexReader, String key,
      DataType storedType, @Nullable ColumnarMapKeyDictionary dictionary) {
    this(columnarMapIndexReader, key, storedType, dictionary, null, null);
  }

  public ColumnarMapKeyForwardIndexReader(ColumnarMapIndexReader columnarMapIndexReader, String key,
      DataType storedType, @Nullable ColumnarMapKeyDictionary dictionary,
      @Nullable FixedBitIntReaderWriter dictIdReader) {
    this(columnarMapIndexReader, key, storedType, dictionary, dictIdReader, null);
  }

  public ColumnarMapKeyForwardIndexReader(ColumnarMapIndexReader columnarMapIndexReader, String key,
      DataType storedType, @Nullable ColumnarMapKeyDictionary dictionary,
      @Nullable FixedBitIntReaderWriter dictIdReader,
      @Nullable ImmutableRoaringBitmap presenceBitmap) {
    _columnarMapIndexReader = columnarMapIndexReader;
    _key = key;
    _storedType = storedType;
    _dictionary = dictionary;
    _dictIdReader = dictIdReader;
    _presenceBitmap = presenceBitmap;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return _dictionary != null;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public DataType getStoredType() {
    return _storedType;
  }

  /**
   * Batch-reads dictionary IDs for a sorted array of document IDs.
   *
   * <p>When a sparse dictId forward index is available, this method uses a co-iteration
   * strategy instead of per-document {@code rank()} calls. A single {@code rankLong()} seeds
   * the ordinal for the first docId in the batch, then a {@link PeekableIntIterator} walks
   * the presence bitmap forward alongside the sorted docIds. This reduces per-block cost
   * from O(blockSize × numContainers) to O(numContainers + blockSize).
   *
   * <p><b>Contract:</b> {@code docIds} must be in ascending order (always true for Pinot
   * query blocks).
   */
  @Override
  public void readDictIds(int[] docIds, int length, int[] dictIdBuffer, ForwardIndexReaderContext context) {
    if (_dictionary == null) {
      throw new UnsupportedOperationException("Dictionary not available for key: " + _key);
    }
    if (_dictIdReader != null && _presenceBitmap != null) {
      // Fast path: co-iterate sorted docIds with presence bitmap iterator.
      // Seed ordinal with one rankLong() call for the first docId, then walk forward.
      int firstDocId = docIds[0];
      PeekableIntIterator iter = _presenceBitmap.getIntIterator();
      iter.advanceIfNeeded(firstDocId);
      int ordinal = (firstDocId == 0) ? 0 : (int) _presenceBitmap.rankLong(firstDocId - 1);

      for (int i = 0; i < length; i++) {
        int docId = docIds[i];
        while (iter.hasNext() && iter.peekNext() < docId) {
          iter.next();
          ordinal++;
        }
        if (iter.hasNext() && iter.peekNext() == docId) {
          dictIdBuffer[i] = _dictIdReader.readInt(ordinal);
          iter.next();
          ordinal++;
        } else {
          dictIdBuffer[i] = Dictionary.NULL_VALUE_INDEX;
        }
      }
    } else if (_dictIdReader != null) {
      // Fallback when presence bitmap is not available
      for (int i = 0; i < length; i++) {
        dictIdBuffer[i] = Dictionary.NULL_VALUE_INDEX;
      }
    } else {
      // Slow path: getString + indexOf for mutable segments
      for (int i = 0; i < length; i++) {
        String rawValue = _columnarMapIndexReader.getString(docIds[i], _key);
        if (rawValue == null || rawValue.isEmpty()) {
          dictIdBuffer[i] = Dictionary.NULL_VALUE_INDEX;
        } else {
          dictIdBuffer[i] = _dictionary.indexOf(rawValue);
        }
      }
    }
  }

  @Override
  public int getInt(int docId, ForwardIndexReaderContext context) {
    return _columnarMapIndexReader.getInt(docId, _key);
  }

  @Override
  public long getLong(int docId, ForwardIndexReaderContext context) {
    return _columnarMapIndexReader.getLong(docId, _key);
  }

  @Override
  public float getFloat(int docId, ForwardIndexReaderContext context) {
    return _columnarMapIndexReader.getFloat(docId, _key);
  }

  @Override
  public double getDouble(int docId, ForwardIndexReaderContext context) {
    return _columnarMapIndexReader.getDouble(docId, _key);
  }

  @Override
  public String getString(int docId, ForwardIndexReaderContext context) {
    return _columnarMapIndexReader.getString(docId, _key);
  }

  @Override
  public byte[] getBytes(int docId, ForwardIndexReaderContext context) {
    return _columnarMapIndexReader.getBytes(docId, _key);
  }

  @Override
  public void close()
      throws IOException {
    // no-op: the underlying reader is owned by ColumnarMapDataSource
  }
}
