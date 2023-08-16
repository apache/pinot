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
package org.apache.pinot.segment.local.realtime.impl.vec;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.spi.data.readers.Vector;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A Vector index reader for the real-time Vector index values on the fly.
 * <p>This class is thread-safe for single writer multiple readers.
 */
public class MutableVectorIndex implements VectorIndexReader, MutableIndex {
  private final Map<Vector, ThreadSafeMutableRoaringBitmap> _bitmaps = new ConcurrentHashMap<>();

  private int _nextDocId;

  @Override
  public void add(@Nonnull Object value, int dictId, int docId) {
    try {
      Vector vector = Vector.fromBytes((byte[]) value);
      add(vector);
    } finally {
      _nextDocId++;
    }
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds, int docId) {
    throw new UnsupportedOperationException("Mutable H3 indexes are not supported for multi-valued columns");
  }

  /**
   * Adds the next Vector value.
   */
  public void add(Vector vector) {
    _bitmaps.computeIfAbsent(vector, k -> new ThreadSafeMutableRoaringBitmap()).add(_nextDocId);
  }

  @Override
  public MutableRoaringBitmap getDocIds(Vector vector) {
    ThreadSafeMutableRoaringBitmap bitmap = _bitmaps.get(vector);
    return bitmap != null ? bitmap.getMutableRoaringBitmap() : new MutableRoaringBitmap();
  }

  @Override
  public void close() {
  }
}
