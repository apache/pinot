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
package org.apache.pinot.segment.local.realtime.impl.vector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A Vector index reader for the real-time Vector index values on the fly.
 * <p>This class is thread-safe for single writer multiple readers.
 */
public class MutableVectorIndex implements VectorIndexReader, MutableIndex {
  private final Map<float[], ThreadSafeMutableRoaringBitmap> _bitmaps = new ConcurrentHashMap<>();

  private int _nextDocId;

  @Override
  public void add(@Nonnull Object value, int dictId, int docId) {
    try {
      add((float[]) value);
    } finally {
      _nextDocId++;
    }
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds, int docId) {
    try {
      float[] floatVector = new float[values.length];
      for (int i = 0; i < values.length; i++) {
        floatVector[i] = (float) values[i];
      }
      add(floatVector);
    } finally {
      _nextDocId++;
    }
  }

  /**
   * Adds the next Vector value.
   */
  public void add(float[] vector) {
    _bitmaps.computeIfAbsent(vector, k -> new ThreadSafeMutableRoaringBitmap()).add(_nextDocId);
  }

  @Override
  public MutableRoaringBitmap getDocIds(float[] vector, int topK) {
    ThreadSafeMutableRoaringBitmap bitmap = _bitmaps.get(vector);
    return bitmap != null ? bitmap.getMutableRoaringBitmap() : new MutableRoaringBitmap();
  }

  @Override
  public void close() {
  }
}
