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
package org.apache.pinot.segment.local.segment.index.readers;

import java.io.IOException;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Reader for raw value bitmap inverted index.
 * File format:
 * <ul>
 *   <li>Header (36 bytes):</li>
 *   <ul>
 *     <li>version (4 bytes)</li>
 *     <li>cardinality (4 bytes)</li>
 *     <li>maxLength (4 bytes) - for string/bytes data types</li>
 *     <li>dictionaryOffset (8 bytes)</li>
 *     <li>dictionaryLength (8 bytes)</li>
 *     <li>invertedIndexOffset (8 bytes)</li>
 *     <li>invertedIndexLength (8 bytes)</li>
 *   </ul>
 *   <li>Dictionary data</li>
 *   <li>Inverted index data</li>
 * </ul>
 */
public class RawValueBitmapInvertedIndexReader implements InvertedIndexReader<ImmutableRoaringBitmap> {
  private static final int VERSION = 1;
  private static final int HEADER_LENGTH = 36;

  private final PinotDataBuffer _dataBuffer;
  private final DataType _dataType;
  private final Dictionary _dictionary;
  private final BitmapInvertedIndexReader _invertedIndexReader;
  private boolean _isClosed;

  public RawValueBitmapInvertedIndexReader(PinotDataBuffer dataBuffer, DataType dataType) throws IOException {
    _dataBuffer = dataBuffer;
    _dataType = dataType;

    // Read header
    int version = _dataBuffer.getInt(0);
    if (version != VERSION) {
      throw new IllegalStateException("Unsupported version: " + version);
    }
    int cardinality = _dataBuffer.getInt(4);
    int maxLength = _dataBuffer.getInt(8);
    long dictionaryOffset = _dataBuffer.getLong(12);
    long dictionaryLength = _dataBuffer.getLong(20);
    long invertedIndexOffset = _dataBuffer.getLong(28);
    long invertedIndexLength = _dataBuffer.getLong(36);

    // Initialize dictionary
    PinotDataBuffer dictionaryBuffer = _dataBuffer.view(dictionaryOffset, dictionaryOffset + dictionaryLength);
    switch (_dataType.getStoredType()) {
      case INT:
        _dictionary = new IntDictionary(dictionaryBuffer, cardinality);
        break;
      case LONG:
        _dictionary = new LongDictionary(dictionaryBuffer, cardinality);
        break;
      case FLOAT:
        _dictionary = new FloatDictionary(dictionaryBuffer, cardinality);
        break;
      case DOUBLE:
        _dictionary = new DoubleDictionary(dictionaryBuffer, cardinality);
        break;
      case STRING:
        _dictionary = new StringDictionary(dictionaryBuffer, cardinality, maxLength);
        break;
      case BYTES:
        _dictionary = new BytesDictionary(dictionaryBuffer, cardinality, maxLength);
        break;
      default:
        throw new IllegalStateException("Unsupported data type: " + _dataType);
    }

    // Initialize inverted index
    PinotDataBuffer invertedIndexBuffer = _dataBuffer.view(invertedIndexOffset,
        invertedIndexOffset + invertedIndexLength);
    _invertedIndexReader = new BitmapInvertedIndexReader(invertedIndexBuffer, cardinality);
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(int dictId) {
    return _invertedIndexReader.getDocIds(dictId);
  }

  /**
   * Returns the document IDs for the given raw INT value.
   */
  public ImmutableRoaringBitmap getDocIdsForInt(int value) {
    int dictId = _dictionary.indexOf(value);
    return dictId >= 0 ? getDocIds(dictId) : new MutableRoaringBitmap();
  }

  /**
   * Returns the document IDs for the given raw LONG value.
   */
  public ImmutableRoaringBitmap getDocIdsForLong(long value) {
    int dictId = _dictionary.indexOf(value);
    return dictId >= 0 ? getDocIds(dictId) : new MutableRoaringBitmap();
  }

  /**
   * Returns the document IDs for the given raw FLOAT value.
   */
  public ImmutableRoaringBitmap getDocIdsForFloat(float value) {
    int dictId = _dictionary.indexOf(value);
    return dictId >= 0 ? getDocIds(dictId) : new MutableRoaringBitmap();
  }

  /**
   * Returns the document IDs for the given raw DOUBLE value.
   */
  public ImmutableRoaringBitmap getDocIdsForDouble(double value) {
    int dictId = _dictionary.indexOf(value);
    return dictId >= 0 ? getDocIds(dictId) : new MutableRoaringBitmap();
  }

  /**
   * Returns the document IDs for the given raw STRING value.
   */
  public ImmutableRoaringBitmap getDocIdsForString(String value) {
    int dictId = _dictionary.indexOf(value);
    return dictId >= 0 ? getDocIds(dictId) : new MutableRoaringBitmap();
  }

  @Override
  public void close() {
    if (_isClosed) {
      return;
    }
    try {
      _dictionary.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    _invertedIndexReader.close();
    _isClosed = true;
  }
}
