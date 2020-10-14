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
package org.apache.pinot.core.realtime.impl.invertedindex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Real-time bitmap based inverted index reader which allows adding values on the fly.
 * <p>This class is thread-safe for single writer multiple readers.
 */
public class RealtimeInvertedIndexReader implements InvertedIndexReader<MutableRoaringBitmap> {
  private final List<ThreadSafeMutableRoaringBitmap> _bitmaps = new ArrayList<>();
  private final ReentrantReadWriteLock.ReadLock _readLock;
  private final ReentrantReadWriteLock.WriteLock _writeLock;

  public RealtimeInvertedIndexReader() {
    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    _readLock = readWriteLock.readLock();
    _writeLock = readWriteLock.writeLock();
  }

  /**
   * Adds the document id to the bitmap of the given dictionary id.
   */
  public void add(int dictId, int docId) {
    if (_bitmaps.size() == dictId) {
      // Bitmap for the dictionary id does not exist, add a new bitmap into the list
      ThreadSafeMutableRoaringBitmap bitmap = new ThreadSafeMutableRoaringBitmap(docId);
      try {
        _writeLock.lock();
        _bitmaps.add(bitmap);
      } finally {
        _writeLock.unlock();
      }
    } else {
      // Bitmap for the dictionary id already exists, check and add document id into the bitmap
      _bitmaps.get(dictId).add(docId);
    }
  }

  @Override
  public MutableRoaringBitmap getDocIds(int dictId) {
    ThreadSafeMutableRoaringBitmap bitmap;
    try {
      _readLock.lock();
      // NOTE: the given dictionary id might not be added to the inverted index yet. We first add the value to the
      // dictionary. Before the value is added to the inverted index, the query might have predicates that match the
      // newly added value. In that case, the given dictionary id does not exist in the inverted index, and we return an
      // empty bitmap. For multi-valued column, the dictionary id might be larger than the bitmap size (not equal).
      if (_bitmaps.size() <= dictId) {
        return new MutableRoaringBitmap();
      }
      bitmap = _bitmaps.get(dictId);
    } finally {
      _readLock.unlock();
    }
    return bitmap.getMutableRoaringBitmap();
  }

  @Override
  public void close() {
  }
}
