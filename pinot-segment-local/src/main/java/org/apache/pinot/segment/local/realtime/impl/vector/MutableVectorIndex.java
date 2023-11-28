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
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A Vector index reader for the real-time Vector index values on the fly.
 * Since there is no good mutable vector index implementation for topK search, we just do brute force search.
 * <p>This class is thread-safe for single writer multiple readers.
 */
public class MutableVectorIndex implements VectorIndexReader, MutableIndex {
  private final Map<float[], ThreadSafeMutableRoaringBitmap> _bitmaps = new ConcurrentHashMap<>();

  private int _nextDocId;
  private final VectorIndexConfig.VectorDistanceFunction _vectorDistanceFunction;

  public MutableVectorIndex(VectorIndexConfig vectorIndexConfig) {
    _vectorDistanceFunction = vectorIndexConfig.getVectorDistanceFunction();
    _nextDocId = 0;
  }

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
    TreeSet<float[]> topKVectors = new TreeSet<>((o1, o2) -> {
      double distance1 = computeDistance(vector, o1);
      double distance2 = computeDistance(vector, o2);
      return Double.compare(distance1, distance2);
    });
    for (float[] vector1 : _bitmaps.keySet()) {
      topKVectors.add(vector1);
      if (topKVectors.size() > topK) {
        topKVectors.pollLast();
      }
    }
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    for (float[] vector1 : topKVectors) {
      docIds.or(_bitmaps.get(vector1).getMutableRoaringBitmap());
    }
    return docIds;
  }

  private double computeDistance(float[] vector1, float[] vector2) {
    double distance = 0;
    for (int i = 0; i < vector1.length; i++) {
      switch (_vectorDistanceFunction) {
        case COSINE:
          distance += VectorFunctions.cosineDistance(vector1, vector2);
          break;
        case INNER_PRODUCT:
          distance += VectorFunctions.innerProduct(vector1, vector2);
          break;
        case DOT_PRODUCT:
          distance += VectorFunctions.dotProduct(vector1, vector2);
          break;
        case EUCLIDEAN:
          distance += VectorFunctions.euclideanDistance(vector1, vector2);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported vector distance function: " + _vectorDistanceFunction);
      }
    }
    return distance;
  }

  @Override
  public void close() {
  }
}
