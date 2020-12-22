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
package org.apache.pinot.core.segment.index.readers.geospatial;

import java.io.Closeable;
import java.lang.ref.SoftReference;
import org.apache.pinot.core.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.IntDictionary;
import org.apache.pinot.core.segment.index.readers.LongDictionary;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class H3IndexReader implements Closeable {

  public static final Logger LOGGER = LoggerFactory.getLogger(BitmapInvertedIndexReader.class);

  private final PinotDataBuffer _bitmapBuffer;
  private final PinotDataBuffer _offsetBuffer;
  private final int _numBitmaps;
  private final int _bitmapBufferSize;

  private volatile SoftReference<SoftReference<ImmutableRoaringBitmap>[]> _bitmaps;

  private Dictionary _dictionary;

  /**
   * Constructs an inverted index with the specified size.
   * @param dataBuffer data buffer for the inverted index.
   */
  public H3IndexReader(PinotDataBuffer dataBuffer) {
    int version = dataBuffer.getInt(0 * Integer.BYTES);
    _numBitmaps = dataBuffer.getInt(1 * Integer.BYTES);

    int headerSize = 2 * Integer.BYTES;
    //read the dictionary
    int dictionarySize = _numBitmaps * Long.BYTES;
    int offsetsSize = _numBitmaps * Integer.BYTES;
    PinotDataBuffer dictionaryBuffer = dataBuffer.view(headerSize, headerSize + dictionarySize);
    _offsetBuffer = dataBuffer.view(headerSize + dictionarySize, headerSize + dictionarySize + offsetsSize);
    _bitmapBuffer = dataBuffer.view(headerSize + dictionarySize + offsetsSize, dataBuffer.size());
    _dictionary = new LongDictionary(dictionaryBuffer, _numBitmaps);
    _bitmapBufferSize = (int) _bitmapBuffer.size();
  }

  public ImmutableRoaringBitmap getDocIds(long h3IndexId) {
    SoftReference<ImmutableRoaringBitmap>[] bitmapArrayReference = null;
    int dictId = _dictionary.indexOf(String.valueOf(h3IndexId));
    if (dictId < 0) {
      return new MutableRoaringBitmap();
    }
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
    int bufferLength;
    if (index == _numBitmaps - 1) {
      bufferLength = _bitmapBufferSize - currentOffset;
    } else {
      bufferLength = getOffset(index + 1) - currentOffset;
    }
    return new ImmutableRoaringBitmap(_bitmapBuffer.toDirectByteBuffer(currentOffset, bufferLength));
  }

  private int getOffset(final int index) {
    return _offsetBuffer.getInt(index * Integer.BYTES);
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }

  public Dictionary getDictionary() {
    return _dictionary;
  }
}
