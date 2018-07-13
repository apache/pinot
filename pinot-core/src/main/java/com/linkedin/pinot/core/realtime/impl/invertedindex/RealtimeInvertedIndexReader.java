/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl.invertedindex;

import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


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
   * Add the document id to the bitmap for the given dictionary id.
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
      _bitmaps.get(dictId).checkAndAdd(docId);
    }
  }

  @Override
  public MutableRoaringBitmap getDocIds(int dictId) {
    ThreadSafeMutableRoaringBitmap bitmap;
    try {
      _readLock.lock();
      bitmap = _bitmaps.get(dictId);
    } finally {
      _readLock.unlock();
    }
    return bitmap.getMutableRoaringBitmap();
  }

  @Override
  public void close() {
  }

  /**
   * Helper wrapper class for {@link MutableRoaringBitmap} to make it thread-safe.
   */
  private static class ThreadSafeMutableRoaringBitmap {
    private MutableRoaringBitmap _mutableRoaringBitmap;

    public ThreadSafeMutableRoaringBitmap(int firstDocId) {
      _mutableRoaringBitmap = new MutableRoaringBitmap();
      _mutableRoaringBitmap.add(firstDocId);
    }

    public void checkAndAdd(int docId) {
      if (!_mutableRoaringBitmap.contains(docId)) {
        synchronized (this) {
          _mutableRoaringBitmap.add(docId);
        }
      }
    }

    public synchronized MutableRoaringBitmap getMutableRoaringBitmap() {
      return _mutableRoaringBitmap.clone();
    }
  }
}
