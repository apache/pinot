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
package org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;


/// Fixed-capacity mapped neighbor row. Diversity pruning follows Lucene 9.12 NeighborArray.
final class MmapHnswNeighborList {
  static final int EDGE_BYTES = Integer.BYTES + Float.BYTES;

  private final MemorySegment _window;
  private final int _baseOffset;
  private final int _capacity;
  private int _size;
  private int _sortedSize;

  MmapHnswNeighborList(MemorySegment window, int baseOffset, int capacity) {
    _window = window;
    _baseOffset = baseOffset;
    _capacity = capacity;
  }

  int size() {
    return _size;
  }

  int node(int index) {
    return _window.get(ValueLayout.JAVA_INT, offset(index));
  }

  private float score(int index) {
    return _window.get(ValueLayout.JAVA_FLOAT, offset(index) + Integer.BYTES);
  }

  void appendInOrder(int node, float score) {
    assert _size == _sortedSize && (_size == 0 || score <= score(_size - 1));
    append(node, score);
    _sortedSize++;
  }

  void append(int node, float score) {
    if (_size == _capacity) {
      throw new IllegalStateException("HNSW neighbor list exceeds capacity " + _capacity);
    }
    set(_size++, node, score);
  }

  void appendAndPrune(int neighbor, float score, int owner, RandomVectorScorerSupplier supplier)
      throws IOException {
    append(neighbor, score);
    if (_size == _capacity) {
      int remove = findWorstNonDiverse(owner, supplier);
      move(remove + 1, remove, _size - remove - 1);
      _size--;
      _sortedSize--;
    }
  }

  private int findWorstNonDiverse(int owner, RandomVectorScorerSupplier supplier)
      throws IOException {
    int[] unchecked = sortPending(supplier.scorer(owner));
    int cursor = unchecked.length - 1;
    for (int candidate = _size - 1; candidate > 0 && cursor >= 0; candidate--) {
      float minSimilarity = score(candidate);
      RandomVectorScorer scorer = supplier.scorer(node(candidate));
      if (candidate == unchecked[cursor]) {
        for (int i = candidate - 1; i >= 0; i--) {
          if (scorer.score(node(i)) >= minSimilarity) {
            return candidate;
          }
        }
        cursor--;
      } else {
        for (int i = cursor; i >= 0; i--) {
          if (scorer.score(node(unchecked[i])) >= minSimilarity) {
            return candidate;
          }
        }
      }
    }
    return _size - 1;
  }

  private int[] sortPending(RandomVectorScorer scorer)
      throws IOException {
    int[] unchecked = new int[_size - _sortedSize];
    for (int count = 0; _sortedSize < _size; count++) {
      int neighbor = node(_sortedSize);
      float score = score(_sortedSize);
      if (Float.isNaN(score)) {
        score = scorer.score(neighbor);
      }
      int insertion = insertionPoint(score);
      move(insertion, insertion + 1, _sortedSize - insertion);
      set(insertion, neighbor, score);
      _sortedSize++;
      for (int i = 0; i < count; i++) {
        if (unchecked[i] >= insertion) {
          unchecked[i]++;
        }
      }
      unchecked[count] = insertion;
    }
    Arrays.sort(unchecked);
    return unchecked;
  }

  private int insertionPoint(float score) {
    int low = 0;
    int high = _sortedSize;
    while (low < high) {
      int mid = (low + high) >>> 1;
      if (score(mid) < score) {
        high = mid;
      } else {
        low = mid + 1;
      }
    }
    return low;
  }

  private void set(int index, int node, float score) {
    long offset = offset(index);
    _window.set(ValueLayout.JAVA_INT, offset, node);
    _window.set(ValueLayout.JAVA_FLOAT, offset + Integer.BYTES, score);
  }

  private long offset(int index) {
    return _baseOffset + index * EDGE_BYTES;
  }

  private void move(int from, int to, int length) {
    if (length != 0) {
      MemorySegment.copy(_window, offset(from), _window, offset(to), length * EDGE_BYTES);
    }
  }
}
