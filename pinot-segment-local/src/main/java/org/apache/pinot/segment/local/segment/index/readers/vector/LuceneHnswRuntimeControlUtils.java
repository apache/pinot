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
package org.apache.pinot.segment.local.segment.index.readers.vector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.AbstractKnnCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.MultiLeafKnnCollector;
import org.apache.lucene.util.hnsw.BlockingFloatHeap;


/**
 * Shared Lucene HNSW query helpers for Pinot's immutable and mutable HNSW readers.
 *
 * <p>This utility keeps query-time runtime controls behaviorally aligned across the two readers by
 * centralizing the custom {@link KnnFloatVectorQuery} implementation and collector semantics in a
 * single place.</p>
 */
public final class LuceneHnswRuntimeControlUtils {
  private LuceneHnswRuntimeControlUtils() {
  }

  public static KnnFloatVectorQuery createQuery(String column, float[] target, int topK, int efSearch,
      boolean useRelativeDistance, boolean useBoundedQueue, @Nullable Query filterQuery) {
    validateRuntimeControls(column, efSearch, useBoundedQueue);
    if (filterQuery == null) {
      return new RuntimeControlledKnnFloatVectorQuery(column, target, topK, efSearch, useRelativeDistance,
          useBoundedQueue);
    }
    return new RuntimeControlledKnnFloatVectorQuery(column, target, topK, filterQuery, efSearch,
        useRelativeDistance, useBoundedQueue);
  }

  public static void validateRuntimeControls(String column, int efSearch, boolean useBoundedQueue) {
    if (!useBoundedQueue && efSearch <= 0) {
      throw new IllegalArgumentException(
          "vectorUseBoundedQueue=false requires vectorEfSearch to be set for column: " + column);
    }
  }

  private static final class RuntimeControlledKnnFloatVectorQuery extends KnnFloatVectorQuery {
    private final int _efSearch;
    private final boolean _useRelativeDistance;
    private final boolean _useBoundedQueue;

    RuntimeControlledKnnFloatVectorQuery(String field, float[] target, int k, int efSearch,
        boolean useRelativeDistance, boolean useBoundedQueue) {
      super(field, target, k);
      _efSearch = efSearch;
      _useRelativeDistance = useRelativeDistance;
      _useBoundedQueue = useBoundedQueue;
    }

    RuntimeControlledKnnFloatVectorQuery(String field, float[] target, int k, Query filter, int efSearch,
        boolean useRelativeDistance, boolean useBoundedQueue) {
      super(field, target, k, filter);
      _efSearch = efSearch;
      _useRelativeDistance = useRelativeDistance;
      _useBoundedQueue = useBoundedQueue;
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
      return new RuntimeControlledKnnCollectorManager(k, searcher, _efSearch, _useRelativeDistance,
          _useBoundedQueue);
    }
  }

  private static final class RuntimeControlledKnnCollectorManager implements KnnCollectorManager {
    private final int _k;
    private final BlockingFloatHeap _globalScoreQueue;
    private final int _efSearch;
    private final boolean _useRelativeDistance;
    private final boolean _useBoundedQueue;

    RuntimeControlledKnnCollectorManager(int k, IndexSearcher searcher, int efSearch,
        boolean useRelativeDistance, boolean useBoundedQueue) {
      _k = k;
      _efSearch = efSearch;
      _useRelativeDistance = useRelativeDistance;
      _useBoundedQueue = useBoundedQueue;
      _globalScoreQueue = useBoundedQueue && searcher.getIndexReader().leaves().size() > 1
          ? new BlockingFloatHeap(k) : null;
    }

    @Override
    public KnnCollector newCollector(int visitLimit, LeafReaderContext context) {
      int effectiveVisitLimit = _efSearch > 0 ? Math.min(visitLimit, _efSearch) : visitLimit;
      if (_useBoundedQueue) {
        AbstractKnnCollector leafCollector = new TopKnnCollector(_k, effectiveVisitLimit);
        KnnCollector collector = _globalScoreQueue != null
            ? new MultiLeafKnnCollector(_k, _globalScoreQueue, leafCollector)
            : leafCollector;
        return _useRelativeDistance ? collector : new RelativeDistanceDisabledKnnCollector(collector);
      }
      return new UnboundedKnnCollector(_k, effectiveVisitLimit, _useRelativeDistance);
    }
  }

  private static final class RelativeDistanceDisabledKnnCollector implements KnnCollector {
    private final KnnCollector _delegate;

    RelativeDistanceDisabledKnnCollector(KnnCollector delegate) {
      _delegate = delegate;
    }

    @Override
    public boolean earlyTerminated() {
      return _delegate.earlyTerminated();
    }

    @Override
    public void incVisitedCount(int count) {
      _delegate.incVisitedCount(count);
    }

    @Override
    public long visitedCount() {
      return _delegate.visitedCount();
    }

    @Override
    public long visitLimit() {
      return _delegate.visitLimit();
    }

    @Override
    public int k() {
      return _delegate.k();
    }

    @Override
    public boolean collect(int docId, float similarity) {
      return _delegate.collect(docId, similarity);
    }

    @Override
    public float minCompetitiveSimilarity() {
      return Float.NEGATIVE_INFINITY;
    }

    @Override
    public TopDocs topDocs() {
      return _delegate.topDocs();
    }
  }

  private static final class UnboundedKnnCollector extends AbstractKnnCollector {
    private final boolean _useRelativeDistance;
    private final List<ScoreDoc> _scoreDocs = new ArrayList<>();
    private final PriorityQueue<Float> _topKFloor;
    private float _minCompetitiveSimilarity = Float.NEGATIVE_INFINITY;

    UnboundedKnnCollector(int k, int visitLimit, boolean useRelativeDistance) {
      super(k, visitLimit);
      _useRelativeDistance = useRelativeDistance;
      _topKFloor = new PriorityQueue<>(Math.max(1, k));
    }

    @Override
    public boolean collect(int docId, float similarity) {
      _scoreDocs.add(new ScoreDoc(docId, similarity));
      if (!_useRelativeDistance) {
        return false;
      }
      if (_topKFloor.size() < k()) {
        _topKFloor.offer(similarity);
        if (_topKFloor.size() == k()) {
          float previous = _minCompetitiveSimilarity;
          _minCompetitiveSimilarity = _topKFloor.peek();
          return Float.compare(previous, _minCompetitiveSimilarity) != 0;
        }
        return false;
      }
      if (similarity <= _topKFloor.peek()) {
        return false;
      }
      _topKFloor.poll();
      _topKFloor.offer(similarity);
      float previous = _minCompetitiveSimilarity;
      _minCompetitiveSimilarity = _topKFloor.peek();
      return Float.compare(previous, _minCompetitiveSimilarity) != 0;
    }

    @Override
    public int numCollected() {
      return _scoreDocs.size();
    }

    @Override
    public float minCompetitiveSimilarity() {
      return _useRelativeDistance && _topKFloor.size() >= k()
          ? _minCompetitiveSimilarity : Float.NEGATIVE_INFINITY;
    }

    @Override
    public TopDocs topDocs() {
      List<ScoreDoc> sorted = new ArrayList<>(_scoreDocs);
      sorted.sort(Comparator.comparingDouble((ScoreDoc scoreDoc) -> scoreDoc.score).reversed()
          .thenComparingInt(scoreDoc -> scoreDoc.doc));
      int resultSize = Math.min(k(), sorted.size());
      ScoreDoc[] scoreDocs = new ScoreDoc[resultSize];
      for (int i = 0; i < resultSize; i++) {
        scoreDocs[i] = sorted.get(i);
      }
      TotalHits.Relation relation =
          earlyTerminated() ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO;
      return new TopDocs(new TotalHits(_scoreDocs.size(), relation), scoreDocs);
    }
  }
}
