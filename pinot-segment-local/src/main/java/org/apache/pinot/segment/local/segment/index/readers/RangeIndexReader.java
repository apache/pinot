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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.data.FieldSpec.DataType;
import static org.apache.pinot.spi.data.FieldSpec.DataType.valueOf;


public class RangeIndexReader implements InvertedIndexReader<ImmutableRoaringBitmap> {
  public static final Logger LOGGER = LoggerFactory.getLogger(RangeIndexReader.class);

  private final PinotDataBuffer _dataBuffer;
  private final DataType _valueType;
  private final int _numRanges;
  final long _bitmapIndexOffset;
  private final Number[] _rangeStartArray;
  private final Number _lastRangeEnd;

  private volatile SoftReference<SoftReference<ImmutableRoaringBitmap>[]> _bitmaps;

  public RangeIndexReader(PinotDataBuffer dataBuffer) {
    _dataBuffer = dataBuffer;
    long offset = 0;
    //READER VERSION
    int version = dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    //READ THE VALUE TYPE (INT, LONG, DOUBLE, FLOAT)
    int valueTypeBytesLength = dataBuffer.getInt(offset);
    offset += Integer.BYTES;
    byte[] valueTypeBytes = new byte[valueTypeBytesLength];
    dataBuffer.copyTo(offset, valueTypeBytes);
    offset += valueTypeBytesLength;
    _valueType = valueOf(new String(valueTypeBytes));

    //READ THE NUMBER OF RANGES
    _numRanges = dataBuffer.getInt(offset);
    offset += Integer.BYTES;
    long rangeArrayStartOffset = offset;

    _rangeStartArray = new Number[_numRanges];
    final long lastOffset = dataBuffer.getLong(offset + (_numRanges + 1) * _valueType.size() + _numRanges * Long.BYTES);

    _bitmapIndexOffset = offset + (_numRanges + 1) * _valueType.size();

    Preconditions.checkState(lastOffset == dataBuffer.size(),
        "The last offset should be equal to buffer size! Current lastOffset: " + lastOffset + ", buffer size: "
            + dataBuffer.size());
    switch (_valueType) {
      case INT:
        for (int i = 0; i < _numRanges; i++) {
          _rangeStartArray[i] = dataBuffer.getInt(rangeArrayStartOffset + i * Integer.BYTES);
        }
        _lastRangeEnd = dataBuffer.getInt(rangeArrayStartOffset + _numRanges * Integer.BYTES);
        break;
      case LONG:
        for (int i = 0; i < _numRanges; i++) {
          _rangeStartArray[i] = dataBuffer.getLong(rangeArrayStartOffset + i * Long.BYTES);
        }
        _lastRangeEnd = dataBuffer.getLong(rangeArrayStartOffset + _numRanges * Long.BYTES);
        break;
      case FLOAT:
        for (int i = 0; i < _numRanges; i++) {
          _rangeStartArray[i] = dataBuffer.getFloat(rangeArrayStartOffset + i * Float.BYTES);
        }
        _lastRangeEnd = dataBuffer.getFloat(rangeArrayStartOffset + _numRanges * Float.BYTES);
        break;
      case DOUBLE:
        for (int i = 0; i < _numRanges; i++) {
          _rangeStartArray[i] = dataBuffer.getDouble(rangeArrayStartOffset + i * Double.BYTES);
        }
        _lastRangeEnd = dataBuffer.getDouble(rangeArrayStartOffset + _numRanges * Double.BYTES);
        break;
      default:
        throw new RuntimeException("Range Index Unsupported for dataType:" + _valueType);
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

  private synchronized ImmutableRoaringBitmap buildRoaringBitmapForIndex(final int rangeId) {
    final long currentOffset = getOffset(rangeId);
    final long nextOffset = getOffset(rangeId + 1);
    final int bufferLength = (int) (nextOffset - currentOffset);

    // Slice the buffer appropriately for Roaring Bitmap
    ByteBuffer bb = _dataBuffer.toDirectByteBuffer(currentOffset, bufferLength);
    return new ImmutableRoaringBitmap(bb);
  }

  private long getOffset(final int rangeId) {
    return _dataBuffer.getLong(_bitmapIndexOffset + rangeId * Long.BYTES);
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

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
