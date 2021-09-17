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

import com.google.common.base.Preconditions;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.data.FieldSpec.DataType;
import static org.apache.pinot.spi.data.FieldSpec.DataType.valueOf;


public class RangeIndexReaderImpl implements RangeIndexReader<ImmutableRoaringBitmap> {
  public static final Logger LOGGER = LoggerFactory.getLogger(RangeIndexReaderImpl.class);

  private final PinotDataBuffer _dataBuffer;
  private final DataType _valueType;
  private final int _numRanges;
  final long _bitmapIndexOffset;
  private final Number[] _rangeStartArray;
  private final Number _lastRangeEnd;

  private volatile SoftReference<SoftReference<ImmutableRoaringBitmap>[]> _bitmaps;

  public RangeIndexReaderImpl(PinotDataBuffer dataBuffer) {
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
    final long lastOffset = dataBuffer.getLong(offset + (long) (_numRanges + 1) * _valueType.size()
        + (long) _numRanges * Long.BYTES);

    _bitmapIndexOffset = offset + (long) (_numRanges + 1) * _valueType.size();

    Preconditions.checkState(lastOffset == dataBuffer.size(),
        "The last offset should be equal to buffer size! Current lastOffset: " + lastOffset + ", buffer size: "
            + dataBuffer.size());
    switch (_valueType) {
      case INT:
        for (int i = 0; i < _numRanges; i++) {
          _rangeStartArray[i] = dataBuffer.getInt(rangeArrayStartOffset + (long) i * Integer.BYTES);
        }
        _lastRangeEnd = dataBuffer.getInt(rangeArrayStartOffset + (long) _numRanges * Integer.BYTES);
        break;
      case LONG:
        for (int i = 0; i < _numRanges; i++) {
          _rangeStartArray[i] = dataBuffer.getLong(rangeArrayStartOffset + (long) i * Long.BYTES);
        }
        _lastRangeEnd = dataBuffer.getLong(rangeArrayStartOffset + (long) _numRanges * Long.BYTES);
        break;
      case FLOAT:
        for (int i = 0; i < _numRanges; i++) {
          _rangeStartArray[i] = dataBuffer.getFloat(rangeArrayStartOffset + (long) i * Float.BYTES);
        }
        _lastRangeEnd = dataBuffer.getFloat(rangeArrayStartOffset + (long) _numRanges * Float.BYTES);
        break;
      case DOUBLE:
        for (int i = 0; i < _numRanges; i++) {
          _rangeStartArray[i] = dataBuffer.getDouble(rangeArrayStartOffset + (long) i * Double.BYTES);
        }
        _lastRangeEnd = dataBuffer.getDouble(rangeArrayStartOffset + (long) _numRanges * Double.BYTES);
        break;
      default:
        throw new RuntimeException("Range Index Unsupported for dataType:" + _valueType);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nullable
  public ImmutableRoaringBitmap getMatchingDocIds(long min, long max) {
    return getMatchesInRange(findRangeId(min), findRangeId(max));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nullable
  public ImmutableRoaringBitmap getMatchingDocIds(int min, int max) {
    return getMatchesInRange(findRangeId(min), findRangeId(max));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nullable
  public ImmutableRoaringBitmap getMatchingDocIds(double min, double max) {
    return getMatchesInRange(findRangeId(min), findRangeId(max));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nullable
  public ImmutableRoaringBitmap getMatchingDocIds(float min, float max) {
    return getMatchesInRange(findRangeId(min), findRangeId(max));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nullable
  public ImmutableRoaringBitmap getPartiallyMatchingDocIds(long min, long max) {
    return getPartialMatchesInRange(findRangeId(min), findRangeId(max));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nullable
  public ImmutableRoaringBitmap getPartiallyMatchingDocIds(int min, int max) {
    return getPartialMatchesInRange(findRangeId(min), findRangeId(max));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nullable
  public ImmutableRoaringBitmap getPartiallyMatchingDocIds(double min, double max) {
    return getPartialMatchesInRange(findRangeId(min), findRangeId(max));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nullable
  public ImmutableRoaringBitmap getPartiallyMatchingDocIds(float min, float max) {
    return getPartialMatchesInRange(findRangeId(min), findRangeId(max));
  }

  private ImmutableRoaringBitmap getDocIds(int rangeId) {
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
    return _dataBuffer.getLong(_bitmapIndexOffset + (long) rangeId * Long.BYTES);
  }

  /**
   * Find the rangeIndex that the value falls in
   * Note this assumes that the value is in one of the range.
   * @param value
   * @return
   */
  private int findRangeId(int value) {
    for (int i = 0; i < _rangeStartArray.length; i++) {
      if (value < _rangeStartArray[i].intValue()) {
        return i - 1;
      }
    }
    return value <= _lastRangeEnd.intValue() ? _rangeStartArray.length - 1 : _rangeStartArray.length;
  }

  private int findRangeId(long value) {
    for (int i = 0; i < _rangeStartArray.length; i++) {
      if (value < _rangeStartArray[i].longValue()) {
        return i - 1;
      }
    }
    return value <= _lastRangeEnd.longValue() ? _rangeStartArray.length - 1 : _rangeStartArray.length;
  }

  private int findRangeId(float value) {
    for (int i = 0; i < _rangeStartArray.length; i++) {
      if (value < _rangeStartArray[i].floatValue()) {
        return i - 1;
      }
    }
    return value <= _lastRangeEnd.floatValue() ? _rangeStartArray.length - 1 : _rangeStartArray.length;
  }

  private int findRangeId(double value) {
    for (int i = 0; i < _rangeStartArray.length; i++) {
      if (value < _rangeStartArray[i].doubleValue()) {
        return i - 1;
      }
    }
    return value <= _lastRangeEnd.doubleValue() ? _rangeStartArray.length - 1 : _rangeStartArray.length;
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }

  private ImmutableRoaringBitmap getMatchesInRange(int firstRangeId, int lastRangeId) {
    // produce bitmap of all ranges fully covered by buckets.
    // 1. if firstRangeId is -1, the query range covers the first bucket
    // 2. if lastRangeId is _rangeStartArray.length, the query range covers the last bucket
    // 3. the loop isn't entered if the range ids are equal
    MutableRoaringBitmap matching = firstRangeId + 1 < lastRangeId ? new MutableRoaringBitmap() : null;
    for (int rangeId = firstRangeId + 1; rangeId < lastRangeId; rangeId++) {
      matching.or(getDocIds(rangeId));
    }
    return matching;
  }

  private ImmutableRoaringBitmap getPartialMatchesInRange(int firstRangeId, int lastRangeId) {
    if (isOutOfRange(firstRangeId)) {
      return isOutOfRange(lastRangeId) ? null : getDocIds(lastRangeId);
    }
    if (isOutOfRange(lastRangeId)) {
      return getDocIds(firstRangeId);
    }
    return ImmutableRoaringBitmap.or(getDocIds(firstRangeId), getDocIds(lastRangeId));
  }

  private boolean isOutOfRange(int rangeId) {
    return rangeId < 0 || rangeId >= _rangeStartArray.length;
  }
}
