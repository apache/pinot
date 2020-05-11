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
package org.apache.pinot.core.segment.index.readers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RangeIndexReader implements InvertedIndexReader<ImmutableRoaringBitmap> {
  public static final Logger LOGGER = LoggerFactory.getLogger(RangeIndexReader.class);

  private final PinotDataBuffer _buffer;
  private final int _version;
  private final FieldSpec.DataType _valueType;
  private final int _numRanges;
  final long _bitmapIndexOffset;
  private final Number[] _rangeStartArray;
  private final Number _lastRangeEnd;

  private volatile SoftReference<SoftReference<ImmutableRoaringBitmap>[]> _bitmaps = null;

  /**
   * Constructs an inverted index with the specified size.
   * @param indexDataBuffer data buffer for the inverted index.
   */
  public RangeIndexReader(PinotDataBuffer indexDataBuffer) {
    long offset = 0;
    _buffer = indexDataBuffer;
    //READER VERSION
    _version = _buffer.getInt(offset);
    offset += Integer.BYTES;

    //READ THE VALUE TYPE (INT, LONG, DOUBLE, FLOAT)
    int valueTypeBytesLength = _buffer.getInt(offset);
    offset += Integer.BYTES;
    byte[] valueTypeBytes = new byte[valueTypeBytesLength];
    _buffer.copyTo(offset, valueTypeBytes);
    offset += valueTypeBytesLength;
    _valueType = FieldSpec.DataType.valueOf(new String(valueTypeBytes));

    //READ THE NUMBER OF RANGES
    _numRanges = _buffer.getInt(offset);
    offset += Integer.BYTES;
    long rangeArrayStartOffset = offset;

    _rangeStartArray = new Number[_numRanges];
    final long lastOffset = _buffer.getLong(offset + (_numRanges + 1) * _valueType.size() + _numRanges * Long.BYTES);

    _bitmapIndexOffset = offset + (_numRanges + 1) * _valueType.size();

    Preconditions.checkState(lastOffset == _buffer.size(),
        "The last offset should be equal to buffer size! Current lastOffset: " + lastOffset + ", buffer size: "
            + _buffer.size());
    switch (_valueType) {
      case INT:
        for (int i = 0; i < _numRanges; i++) {
          _rangeStartArray[i] = _buffer.getInt(rangeArrayStartOffset + i * Integer.BYTES);
        }
        _lastRangeEnd = _buffer.getInt(rangeArrayStartOffset + _numRanges * Integer.BYTES);
        break;
      case LONG:
        for (int i = 0; i < _numRanges; i++) {
          _rangeStartArray[i] = _buffer.getLong(rangeArrayStartOffset + i * Long.BYTES);
        }
        _lastRangeEnd = _buffer.getLong(rangeArrayStartOffset + _numRanges * Long.BYTES);
        break;
      case FLOAT:
        for (int i = 0; i < _numRanges; i++) {
          _rangeStartArray[i] = _buffer.getFloat(rangeArrayStartOffset + i * Float.BYTES);
        }
        _lastRangeEnd = _buffer.getFloat(rangeArrayStartOffset + _numRanges * Float.BYTES);
        break;
      case DOUBLE:
        for (int i = 0; i < _numRanges; i++) {
          _rangeStartArray[i] = _buffer.getDouble(rangeArrayStartOffset + i * Double.BYTES);
        }
        _lastRangeEnd = _buffer.getDouble(rangeArrayStartOffset + _numRanges * Double.BYTES);
        break;
      default:
        _lastRangeEnd = null;
    }
  }

  @VisibleForTesting
  public Number[] getRangeStartArray() {
    return _rangeStartArray;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableRoaringBitmap getDocIds(int rangeId) {
    SoftReference<ImmutableRoaringBitmap>[] bitmapArrayReference = null;
    // Return the bitmap if it's still on heap
    if (_bitmaps != null) {
      bitmapArrayReference = _bitmaps.get();
      if (bitmapArrayReference != null) {
        SoftReference<ImmutableRoaringBitmap> bitmapReference = bitmapArrayReference[rangeId];
        if (bitmapReference != null) {
          ImmutableRoaringBitmap value = bitmapReference.get();
          if (value != null) {
            return value;
          }
        }
      } else {
        bitmapArrayReference = new SoftReference[_numRanges];
        _bitmaps = new SoftReference<SoftReference<ImmutableRoaringBitmap>[]>(bitmapArrayReference);
      }
    } else {
      bitmapArrayReference = new SoftReference[_numRanges];
      _bitmaps = new SoftReference<SoftReference<ImmutableRoaringBitmap>[]>(bitmapArrayReference);
    }
    synchronized (this) {
      ImmutableRoaringBitmap value;
      if (bitmapArrayReference[rangeId] == null || bitmapArrayReference[rangeId].get() == null) {
        value = buildRoaringBitmapForIndex(rangeId);
        bitmapArrayReference[rangeId] = new SoftReference<ImmutableRoaringBitmap>(value);
      } else {
        value = bitmapArrayReference[rangeId].get();
      }
      return value;
    }
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(Object value) {
    // This should not be called from anywhere. If it happens, there is a bug
    // and that's why we throw illegal state exception
    throw new IllegalStateException("bitmap inverted index reader supports lookup only on dictionary id");
  }

  private synchronized ImmutableRoaringBitmap buildRoaringBitmapForIndex(final int rangeId) {
    final long currentOffset = getOffset(rangeId);
    final long nextOffset = getOffset(rangeId + 1);
    final int bufferLength = (int) (nextOffset - currentOffset);

    // Slice the buffer appropriately for Roaring Bitmap
    ByteBuffer bb = _buffer.toDirectByteBuffer(currentOffset, bufferLength);
    return new ImmutableRoaringBitmap(bb);
  }

  private long getOffset(final int rangeId) {
    return _buffer.getLong(_bitmapIndexOffset + rangeId * Long.BYTES);
  }

  @Override
  public void close()
      throws IOException {
    _buffer.close();
  }

  /**
   * Find the rangeIndex that the value falls in
   * Note this assumes that the value is in one of the range.
   * @param value
   * @return
   */
  public int findRangeId(int value) {
    for (int i = 0; i < _rangeStartArray.length; i++) {
      if (value < _rangeStartArray[i].intValue()) {
        return i - 1;
      }
    }
    if (value <= _lastRangeEnd.intValue()) {
      return _rangeStartArray.length - 1;
    }
    return -1;
  }

  public int findRangeId(long value) {
    for (int i = 0; i < _rangeStartArray.length; i++) {
      if (value < _rangeStartArray[i].longValue()) {
        return i - 1;
      }
    }
    if (value <= _lastRangeEnd.longValue()) {
      return _rangeStartArray.length - 1;
    }
    return -1;
  }

  public int findRangeId(float value) {
    for (int i = 0; i < _rangeStartArray.length; i++) {
      if (value < _rangeStartArray[i].floatValue()) {
        return i - 1;
      }
    }
    if (value <= _lastRangeEnd.floatValue()) {
      return _rangeStartArray.length - 1;
    }
    return -1;
  }

  public int findRangeId(double value) {
    for (int i = 0; i < _rangeStartArray.length; i++) {
      if (value < _rangeStartArray[i].doubleValue()) {
        return i - 1;
      }
    }
    if (value <= _lastRangeEnd.doubleValue()) {
      return _rangeStartArray.length - 1;
    }
    return -1;
  }
}
