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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * DictId-based inverted index for mutable MAP keys. Similar to
 * {@link org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeInvertedIndex}
 * but handles non-sequential dictId insertion (gaps). This is needed because MAP keys
 * pre-index a default value at dictId 0 in the dictionary, but documents may never
 * explicitly contain that default value, causing a gap when the first real value gets
 * dictId 1.
 *
 * <p>Thread-safe for single writer, multiple readers. Per-bitmap mutations are protected
 * by {@link ThreadSafeMutableRoaringBitmap}; the list resize path is protected by the
 * write lock. {@link #getDocIds(int)} returns a synchronized clone, so readers can iterate
 * the result safely while the writer continues to mutate the underlying bitmap.
 */
public class ColumnarMapRealtimeInvertedIndex implements InvertedIndexReader<MutableRoaringBitmap> {
  private final List<ThreadSafeMutableRoaringBitmap> _bitmaps = new ArrayList<>();
  private final ReentrantReadWriteLock.ReadLock _readLock;
  private final ReentrantReadWriteLock.WriteLock _writeLock;

  public ColumnarMapRealtimeInvertedIndex() {
    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    _readLock = readWriteLock.readLock();
    _writeLock = readWriteLock.writeLock();
  }

  public void add(int dictId, int docId) {
    if (_bitmaps.size() > dictId) {
      // Common case: bitmap for this dictId already exists. ThreadSafeMutableRoaringBitmap.add
      // is internally synchronized; no list mutation, so no list lock needed.
      _bitmaps.get(dictId).add(docId);
      return;
    }
    // Need to grow the list. Take the write lock — readers may be iterating the list.
    _writeLock.lock();
    try {
      while (_bitmaps.size() <= dictId) {
        _bitmaps.add(new ThreadSafeMutableRoaringBitmap());
      }
      _bitmaps.get(dictId).add(docId);
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public MutableRoaringBitmap getDocIds(int dictId) {
    ThreadSafeMutableRoaringBitmap bitmap;
    _readLock.lock();
    try {
      if (_bitmaps.size() <= dictId) {
        return new MutableRoaringBitmap();
      }
      bitmap = _bitmaps.get(dictId);
    } finally {
      _readLock.unlock();
    }
    // Returns a synchronized clone — safe for the caller to iterate concurrently with new
    // add() calls from the writer thread.
    return bitmap.getMutableRoaringBitmap();
  }

  @Override
  public void close() {
  }
}
