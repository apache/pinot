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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.AbstractKnnCollector;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.search.knn.MultiLeafKnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.BlockingFloatHeap;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;


/// Shared Lucene HNSW query helpers for Pinot's immutable and mutable HNSW readers.
///
/// This utility keeps query-time runtime controls behaviorally aligned across the two readers by
/// centralizing the custom [KnnFloatVectorQuery] implementation and collector semantics in a
/// single place.
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
      return new RuntimeControlledKnnCollectorManager(k, searcher, filter != null, _efSearch, _useRelativeDistance,
          _useBoundedQueue);
    }

    @Override
    protected TopDocs approximateSearch(LeafReaderContext context, AcceptDocs acceptDocs, int visitedLimit,
        KnnCollectorManager collectorManager)
        throws IOException {
      if (_efSearch <= 0) {
        return super.approximateSearch(context, acceptDocs, visitedLimit, collectorManager);
      }
      LeafReader reader = context.reader();
      FloatVectorValues values = reader.getFloatVectorValues(field);
      if (values == null) {
        return super.approximateSearch(context, acceptDocs, visitedLimit, collectorManager);
      }
      KnnCollector collector = collectorManager.newCollector(visitedLimit, getSearchStrategy(), context);
      if (values.size() <= collector.visitLimit()
          || canUseGraph(reader, values.size(), acceptDocs.cost(), collector.k())) {
        reader.searchNearestVectors(field, target, collector, acceptDocs);
        return collector.topDocs();
      }
      // Lucene 10.6 can exhaustively score a batch before checking the visit limit. On leaves where
      // the search can take that path, check the budget between individual vector scores instead.
      VectorScorer scorer = values.scorer(target);
      if (scorer != null) {
        Bits accepted = acceptDocs.bits();
        DocIdSetIterator vectors = scorer.iterator();
        while (!collector.earlyTerminated()) {
          int doc = vectors.nextDoc();
          if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          if (accepted == null || accepted.get(doc)) {
            collector.incVisitedCount(1);
            collector.collect(doc, scorer.score());
          }
        }
      }
      return collector.topDocs();
    }

    /// Mirrors Lucene99HnswVectorsReader's choice between graph search and flat scoring.
    private boolean canUseGraph(LeafReader reader, int numVectors, int acceptedDocCount, int leafK)
        throws IOException {
      return leafK < numVectors
          && HnswGraphSearcher.expectedVisitedNodes(leafK, numVectors) < Math.min(numVectors, acceptedDocCount)
          && reader instanceof CodecReader codecReader
          && codecReader.getVectorReader() instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldReader
          && perFieldReader.getFieldReader(field) instanceof HnswGraphProvider graphProvider
          && graphProvider.getGraph(field).size() > 0;
    }
  }

  private static final class RuntimeControlledKnnCollectorManager implements KnnCollectorManager {
    private final int _k;
    private final BlockingFloatHeap _globalScoreQueue;
    private final boolean _hasFilter;
    private final int _efSearch;
    private final boolean _useRelativeDistance;
    private final boolean _useBoundedQueue;

    RuntimeControlledKnnCollectorManager(int k, IndexSearcher searcher, boolean hasFilter, int efSearch,
        boolean useRelativeDistance, boolean useBoundedQueue) {
      _k = k;
      _hasFilter = hasFilter;
      _efSearch = efSearch;
      _useRelativeDistance = useRelativeDistance;
      _useBoundedQueue = useBoundedQueue;
      _globalScoreQueue = useBoundedQueue && searcher.getIndexReader().leaves().size() > 1
          ? new BlockingFloatHeap(k) : null;
    }

    @Override
    public KnnCollector newCollector(int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context) {
      // A reentrant pass is seeded with the first pass's candidates. Retain that leaf quota when it exceeds k.
      int leafK = searchStrategy instanceof KnnSearchStrategy.Seeded seeded
          ? Math.max(_k, seeded.numberOfEntryPoints()) : _k;
      return newCollector(visitLimit, searchStrategy, leafK);
    }

    @Override
    public KnnCollector newOptimisticCollector(int visitLimit, KnnSearchStrategy searchStrategy,
        LeafReaderContext context, int leafK) {
      return newCollector(visitLimit, searchStrategy, leafK);
    }

    @Override
    public boolean isOptimistic() {
      // Lucene checks its leaf quota before accepting filtered ANN results. A seeded second pass can
      // score all entry points before checking its visit limit, so keep it disabled for explicit budgets.
      return _hasFilter && _efSearch <= 0;
    }

    private KnnCollector newCollector(int visitLimit, KnnSearchStrategy searchStrategy, int leafK) {
      int effectiveVisitLimit = _efSearch > 0 ? Math.min(visitLimit, _efSearch) : visitLimit;
      if (_useBoundedQueue) {
        AbstractKnnCollector leafCollector = new TopKnnCollector(leafK, effectiveVisitLimit, searchStrategy);
        KnnCollector collector = _globalScoreQueue != null
            ? new MultiLeafKnnCollector(leafK, _globalScoreQueue, leafCollector)
            : leafCollector;
        return _useRelativeDistance ? collector : new RelativeDistanceDisabledKnnCollector(collector);
      }
      return new UnboundedKnnCollector(leafK, effectiveVisitLimit, searchStrategy, _useRelativeDistance);
    }
  }

  private static final class RelativeDistanceDisabledKnnCollector extends KnnCollector.Decorator {
    RelativeDistanceDisabledKnnCollector(KnnCollector delegate) {
      super(delegate);
    }

    @Override
    public float minCompetitiveSimilarity() {
      return Float.NEGATIVE_INFINITY;
    }
  }

  private static final class UnboundedKnnCollector extends AbstractKnnCollector {
    private final boolean _useRelativeDistance;
    private final List<ScoreDoc> _scoreDocs = new ArrayList<>();
    private final PriorityQueue<Float> _topKFloor;
    private float _minCompetitiveSimilarity = Float.NEGATIVE_INFINITY;

    UnboundedKnnCollector(int k, int visitLimit, KnnSearchStrategy searchStrategy, boolean useRelativeDistance) {
      super(k, visitLimit, searchStrategy);
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
