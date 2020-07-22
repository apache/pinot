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

import com.google.common.base.Preconditions;
import java.lang.ref.SoftReference;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BitmapInvertedIndexReader implements InvertedIndexReader<ImmutableRoaringBitmap> {
  public static final Logger LOGGER = LoggerFactory.getLogger(BitmapInvertedIndexReader.class);

  private final PinotDataBuffer _dataBuffer;
  private final int _numBitmaps;

  private volatile SoftReference<SoftReference<ImmutableRoaringBitmap>[]> _bitmaps;

  /**
   * Constructs an inverted index with the specified size.
   * @param dataBuffer data buffer for the inverted index.
   * @param cardinality the number of bitmaps in the inverted index, which should be the same as the
   *          number of values in
   *          the dictionary.
   */
  public BitmapInvertedIndexReader(PinotDataBuffer dataBuffer, int cardinality) {
    _dataBuffer = dataBuffer;
    _numBitmaps = cardinality;

    int lastOffset = _dataBuffer.getInt(_numBitmaps * Integer.BYTES);
    Preconditions.checkState(lastOffset == _dataBuffer.size(),
        "The last offset should be equal to buffer size! Current lastOffset: " + lastOffset + ", buffer size: "
            + _dataBuffer.size());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableRoaringBitmap getDocIds(int dictId) {
    SoftReference<ImmutableRoaringBitmap>[] bitmapArrayReference = null;
    // Return the bitmap if it's still on heap
    if (_bitmaps != null) {
      bitmapArrayReference = _bitmaps.get();
      if (bitmapArrayReference != null) {
        SoftReference<ImmutableRoaringBitmap> bitmapReference = bitmapArrayReference[dictId];
        if (bitmapReference != null) {
          ImmutableRoaringBitmap value = bitmapReference.get();
          if (value != null) {
            return value;
          }
        }
      } else {
        bitmapArrayReference = new SoftReference[_numBitmaps];
        _bitmaps = new SoftReference<SoftReference<ImmutableRoaringBitmap>[]>(bitmapArrayReference);
      }
    } else {
      bitmapArrayReference = new SoftReference[_numBitmaps];
      _bitmaps = new SoftReference<SoftReference<ImmutableRoaringBitmap>[]>(bitmapArrayReference);
    }
    synchronized (this) {
      ImmutableRoaringBitmap value;
      if (bitmapArrayReference[dictId] == null || bitmapArrayReference[dictId].get() == null) {
        value = buildRoaringBitmapForIndex(dictId);
        bitmapArrayReference[dictId] = new SoftReference<ImmutableRoaringBitmap>(value);
      } else {
        value = bitmapArrayReference[dictId].get();
      }
      return value;
    }
  }

  private synchronized ImmutableRoaringBitmap buildRoaringBitmapForIndex(final int index) {
    int currentOffset = getOffset(index);
    int bufferLength = getOffset(index + 1) - currentOffset;
    return new ImmutableRoaringBitmap(_dataBuffer.toDirectByteBuffer(currentOffset, bufferLength));
  }

  private int getOffset(final int index) {
    return _dataBuffer.getInt(index * Integer.BYTES);
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
