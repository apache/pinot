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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
import org.apache.pinot.segment.local.segment.creator.impl.vector.XKnnFloatVectorField;
import org.apache.pinot.segment.local.segment.index.readers.vector.BaseFilterQuery;
import org.apache.pinot.segment.local.segment.index.readers.vector.LuceneHnswRuntimeControlUtils;
import org.apache.pinot.segment.local.segment.store.VectorIndexUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.VectorIndexConfigProvider;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.reader.EfSearchAware;
import org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// A mutable HNSW vector index for real-time (consuming) segments, backed by a Lucene [IndexWriter] over a
/// temp-dir [FSDirectory].
///
/// Every added document stores the supplied Pinot doc id as a [NumericDocValuesField]. The same doc values
/// drive both filtered traversal and the translation of search hits, so no assumption is made that
/// `ScoreDoc.doc == Pinot docId` (Lucene may renumber on merges).

///
/// Filtered search ([#getDocIds(float[], int, ImmutableRoaringBitmap)]) restricts HNSW candidate generation
/// to the given Pinot doc ids (used to enforce the upsert doc-ids snapshot). It searches a near-real-time
/// reader obtained from the writer, so uncommitted rows are visible -- required for upsert correctness,
/// where the newest version of a record is the most recently added and may not be committed yet. The
/// unfiltered path keeps searching the last committed generation (cheaper; commit cadence is controlled by
/// `commitIntervalMs` / `commitDocs`).
///
/// **Cost of that freshness.** A refresh flushes the writer's RAM buffer, which for a vector field writes out
/// the HNSW graph for the pending rows as a new Lucene segment. While a segment is actively consuming, nearly
/// every filtered query finds new rows and therefore triggers one, so segment creation is driven by query rate
/// rather than ingestion rate, and the resulting merges rebuild graphs. Concurrent callers coalesce onto a
/// single refresh rather than each forcing their own. Bounding this properly means reopening on a background
/// thread and having queries wait on a sequence number instead of driving the flush themselves -- see
/// `RealtimeLuceneIndexRefreshManager`, which does that for the text indexes, and Lucene's
/// `ControlledRealTimeReopenThread`. Until then, correctness is preserved at the cost of that flush.
///
/// This class is thread-safe for single writer multiple readers.
public class MutableVectorIndex
    implements FilterAwareVectorIndexReader, MutableIndex, VectorIndexConfigProvider, EfSearchAware {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableVectorIndex.class);
  private static final Comparator<ScoreDoc> LUCENE_DOC_ID_ORDER = Comparator.comparingInt(scoreDoc -> scoreDoc.doc);
  private static final RealtimeLuceneTextIndexSearcherPool SEARCHER_POOL =
      RealtimeLuceneTextIndexSearcherPool.getInstance();
  public static final String VECTOR_INDEX_DOC_ID_COLUMN_NAME = "DocID";
  public static final long DEFAULT_COMMIT_INTERVAL_MS = 10_000L;
  public static final long DEFAULT_COMMIT_DOCS = 1000L;
  private final int _vectorDimension;
  private final VectorIndexConfig _vectorIndexConfig;
  private final VectorSimilarityFunction _vectorSimilarityFunction;
  private final String _vectorColumn;
  private final String _segmentName;
  private final long _commitIntervalMs;
  private final long _commitDocs;
  private final File _indexDir;
  private final FSDirectory _indexDirectory;
  private final IndexWriter _indexWriter;
  // Near-real-time searcher over the writer, used by the filtered search path (upsert doc-ids snapshot
  // enforcement) so uncommitted rows are visible; refreshed on demand, reused across queries
  private final SearcherManager _searcherManager;
  // Coordinates callers that need the same near-real-time generation. One caller refreshes while the others wait
  // for its published watermark instead of serializing on maybeRefreshBlocking or reopening the same generation.
  private final Object _searcherRefreshMonitor = new Object();
  // Guarded by _searcherRefreshMonitor.
  private boolean _searcherRefreshInProgress;
  private volatile long _searcherRefreshCount;
  // Number of documents added so far; used only for the commit cadence, never as a doc id. Written by the indexing
  // thread only; read cross-thread for debug output, where staleness is acceptable.
  private volatile int _numDocsAdded;
  /// Sequence number of the newest row handed to the writer, and the sequence number the shared searcher was
  /// last refreshed through. Together they let a filtered search skip the refresh when nothing has been added
  /// since. Doc ids cannot be used for this: [MutableIndex#add] allows rows in arbitrary doc-id order, so a
  /// doc-id watermark would skip the refresh a lower-numbered but newer row needs. Writer sequence numbers are
  /// monotonic by construction. Written by the single indexing thread, read by query threads.
  private volatile long _lastAddedSequenceNumber = -1;
  private volatile long _searcherRefreshedThroughSequenceNumber = -1;

  private long _lastCommitTime;
  private final ThreadLocal<Integer> _efSearchOverride = new ThreadLocal<>();
  private final ThreadLocal<Boolean> _useRelativeDistanceOverride = new ThreadLocal<>();
  private final ThreadLocal<Boolean> _useBoundedQueueOverride = new ThreadLocal<>();

  public MutableVectorIndex(String segmentName, String vectorColumn, VectorIndexConfig vectorIndexConfig) {
    _vectorColumn = vectorColumn;
    _vectorIndexConfig = vectorIndexConfig;
    _vectorDimension = vectorIndexConfig.getVectorDimension();
    _segmentName = segmentName;
    _commitIntervalMs = Long.parseLong(
        vectorIndexConfig.getProperties().getOrDefault("commitIntervalMs", String.valueOf(DEFAULT_COMMIT_INTERVAL_MS)));
    _commitDocs = Long.parseLong(
        vectorIndexConfig.getProperties().getOrDefault("commitDocs", String.valueOf(DEFAULT_COMMIT_DOCS)));
    _vectorSimilarityFunction = VectorIndexUtils.toSimilarityFunction(vectorIndexConfig.getVectorDistanceFunction());
    // Each column of a segment gets its own directory, so that cleaning up one column does not remove the index of
    // another column of the same segment.
    _indexDir = new File(new File(FileUtils.getTempDirectory(), segmentName),
        _vectorColumn + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);

    FSDirectory indexDirectory = null;
    IndexWriter indexWriter = null;
    SearcherManager searcherManager = null;
    try {
      // segment generation is always in V1 and later we convert (as part of post creation processing)
      // to V3 if segmentVersion is set to V3 in SegmentGeneratorConfig.
      indexDirectory = FSDirectory.open(_indexDir.toPath());
      LOGGER.info("Creating mutable HNSW index for segment: {}, column: {} at path: {} with {}", segmentName,
          vectorColumn, _indexDir.getAbsolutePath(), vectorIndexConfig.getProperties());
      // Always start empty. The directory is temp-scoped but its name is stable across restarts, so an unclean
      // shutdown can leave documents written by an older build. The index is rebuilt from the stream, so appending
      // those stale rows would be both unnecessary and incorrect.
      indexWriter = new IndexWriter(indexDirectory,
          VectorIndexUtils.getIndexWriterConfig(vectorIndexConfig).setOpenMode(IndexWriterConfig.OpenMode.CREATE));
      indexWriter.commit();
      searcherManager = new SearcherManager(indexWriter, false, false, null);
      _lastCommitTime = System.currentTimeMillis();
    } catch (Exception e) {
      // IndexWriter does not close the Directory passed to it, so both need to be closed.
      try {
        IOUtils.close(searcherManager, indexWriter, indexDirectory);
      } catch (Exception closeEx) {
        e.addSuppressed(closeEx);
      }
      deleteIndexDir();
      throw new RuntimeException(
          "Caught exception while instantiating the LuceneTextIndexCreator for column: " + vectorColumn, e);
    }
    _indexDirectory = indexDirectory;
    _indexWriter = indexWriter;
    _searcherManager = searcherManager;
  }

  @Override
  public void add(Object value, int dictId, int docId) {
    throw new UnsupportedOperationException("Mutable Vector indexes are not supported for single-valued columns");
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds, int docId) {
    float[] floatValues = new float[_vectorDimension];
    for (int i = 0; i < values.length; i++) {
      floatValues[i] = (Float) values[i];
    }
    Document docToIndex = new Document();
    XKnnFloatVectorField xKnnFloatVectorField =
        new XKnnFloatVectorField(_vectorColumn, floatValues, _vectorSimilarityFunction);
    docToIndex.add(xKnnFloatVectorField);
    // Store the SUPPLIED Pinot doc id (not an internal counter): the doc value translates search hits
    // back to Pinot doc ids, and the doc-values field lets filtered search test bitmap membership per doc
    docToIndex.add(new NumericDocValuesField(VECTOR_INDEX_DOC_ID_COLUMN_NAME, docId));
    try {
      _lastAddedSequenceNumber = _indexWriter.addDocument(docToIndex);
      _numDocsAdded++;
      if ((_lastCommitTime + _commitIntervalMs < System.currentTimeMillis()) || (_numDocsAdded % _commitDocs == 0)) {
        _indexWriter.commit();
        _lastCommitTime = System.currentTimeMillis();
        LOGGER.debug("Committed index for column: {}, segment: {}", _vectorColumn, _segmentName);
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while adding a new document to the Lucene index for column: " + _vectorColumn, e);
    }
  }

  @Override
  public MutableRoaringBitmap getDocIds(float[] vector, int topK) {
    return submitSearch(vector, topK, null);
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(float[] vector, int topK, ImmutableRoaringBitmap preFilterBitmap) {
    // A null bitmap would fall through to an unfiltered search, returning doc ids outside any filter -- the
    // silent degradation this reader's contract forbids, so fail loudly instead of deep inside Lucene.
    Preconditions.checkNotNull(preFilterBitmap, "Pre-filter bitmap must not be null for filtered vector search");
    if (preFilterBitmap.isEmpty()) {
      return new MutableRoaringBitmap();
    }
    return submitSearch(vector, topK, preFilterBitmap);
  }

  private MutableRoaringBitmap submitSearch(float[] vector, int topK,
      @Nullable ImmutableRoaringBitmap preFilterBitmap) {
    int effectiveEfSearch = getEffectiveEfSearch();
    boolean effectiveUseRelativeDistance = getEffectiveUseRelativeDistance();
    boolean effectiveUseBoundedQueue = getEffectiveUseBoundedQueue();
    // Search is executed in SEARCHER_POOL which is wrapped with contextAwareExecutorService(executor, false).
    // This propagates QueryThreadContext for CPU/memory tracking without registering the task for cancellation,
    // preventing Thread.interrupt() during Lucene search which could corrupt FSDirectory.
    // See https://github.com/apache/lucene/issues/3315 and https://github.com/apache/lucene/issues/9309
    // The pre-filter bitmap is captured by the lambda; callers hand this reader a bitmap that is never
    // mutated after submission (the planner passes a query-scoped defensive copy), so the transfer into the
    // executor thread is safe.
    Future<MutableRoaringBitmap> searchFuture = SEARCHER_POOL.getExecutorService().submit(
        () -> executeVectorSearch(vector, topK, effectiveEfSearch, effectiveUseRelativeDistance,
            effectiveUseBoundedQueue, preFilterBitmap));
    try {
      return searchFuture.get();
    } catch (InterruptedException e) {
      searchFuture.cancel(false);
      throw new RuntimeException("VECTOR_SIMILARITY query interrupted for segment " + _segmentName
          + " column " + _vectorColumn, e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new RuntimeException("Failed while searching vector index for segment " + _segmentName
          + " column " + _vectorColumn, cause);
    } catch (Exception e) {
      throw new RuntimeException("Failed while searching vector index for segment " + _segmentName
          + " column " + _vectorColumn, e);
    }
  }

  @Override
  public VectorIndexConfig getVectorIndexConfig() {
    return _vectorIndexConfig;
  }

  @Override
  public void setEfSearch(int efSearch) {
    if (efSearch < 1) {
      throw new IllegalArgumentException("efSearch must be >= 1, got: " + efSearch);
    }
    _efSearchOverride.set(efSearch);
  }

  @Override
  public void clearEfSearch() {
    _efSearchOverride.remove();
  }

  @Override
  public void setUseRelativeDistance(boolean useRelativeDistance) {
    _useRelativeDistanceOverride.set(useRelativeDistance);
  }

  @Override
  public void clearUseRelativeDistance() {
    _useRelativeDistanceOverride.remove();
  }

  @Override
  public void setUseBoundedQueue(boolean useBoundedQueue) {
    _useBoundedQueueOverride.set(useBoundedQueue);
  }

  @Override
  public void clearUseBoundedQueue() {
    _useBoundedQueueOverride.remove();
  }

  @Override
  public Map<String, Object> getIndexDebugInfo() {
    Map<String, Object> info = new LinkedHashMap<>();
    info.put("backend", "HNSW");
    info.put("column", _vectorColumn);
    info.put("effectiveEfSearch", getEffectiveEfSearch());
    info.put("effectiveHnswUseRelativeDistance", getEffectiveUseRelativeDistance());
    info.put("effectiveHnswUseBoundedQueue", getEffectiveUseBoundedQueue());
    info.put("supportsPreFilter", supportsPreFilter());
    try (DirectoryReader directoryReader = DirectoryReader.open(_indexDirectory)) {
      info.put("numDocs", directoryReader.numDocs());
      info.put("numDeletedDocs", directoryReader.numDeletedDocs());
      info.put("luceneSegments", directoryReader.leaves().size());
    } catch (IOException e) {
      LOGGER.warn("Failed to load mutable HNSW debug stats for segment: {}, column: {}", _segmentName, _vectorColumn,
          e);
      info.put("numDocs", _numDocsAdded);
      info.put("numDeletedDocs", 0);
      info.put("luceneSegments", 0);
    }
    return info;
  }

  private MutableRoaringBitmap executeVectorSearch(float[] vector, int topK, int efSearch,
      boolean useRelativeDistance, boolean useBoundedQueue, @Nullable ImmutableRoaringBitmap preFilterBitmap)
      throws IOException {
    if (preFilterBitmap != null) {
      // Filtered search enforces the query's visible-document set, so it must see every row that set names --
      // including rows still in the writer's RAM buffer. Refreshing flushes the writer, so only refresh when this
      // query can actually see past the last refresh, and coalesce callers waiting for the same generation. The
      // added-doc watermark is read BEFORE refreshing so rows arriving during the refresh are not wrongly claimed
      // as visible.
      long lastAdded = _lastAddedSequenceNumber;
      if (lastAdded > _searcherRefreshedThroughSequenceNumber) {
        refreshSearcherThrough(lastAdded);
      }
      IndexSearcher indexSearcher = _searcherManager.acquire();
      try {
        return search(indexSearcher, vector, topK, efSearch, useRelativeDistance, useBoundedQueue,
            new NumericDocValuesBitmapFilterQuery(preFilterBitmap));
      } finally {
        _searcherManager.release(indexSearcher);
      }
    }
    // The unfiltered path keeps the cheaper last-committed view (bounded by commitIntervalMs / commitDocs)
    try (DirectoryReader directoryReader = DirectoryReader.open(_indexDirectory)) {
      return search(new IndexSearcher(directoryReader), vector, topK, efSearch, useRelativeDistance,
          useBoundedQueue, null);
    }
  }

  /// Refreshes the shared near-real-time searcher through `targetSequenceNumber`, coalescing concurrent callers.
  ///
  /// The winning caller publishes the exact generation it captured before refreshing and wakes the waiters. A
  /// waiter whose target is newer then performs the next refresh. This is necessary because rows added during a
  /// refresh are not guaranteed to be visible in the reopened reader.
  private void refreshSearcherThrough(long targetSequenceNumber)
      throws IOException {
    synchronized (_searcherRefreshMonitor) {
      while (_searcherRefreshInProgress
          && _searcherRefreshedThroughSequenceNumber < targetSequenceNumber) {
        onSearcherRefreshWait();
        try {
          _searcherRefreshMonitor.wait();
        } catch (InterruptedException e) {
          throw new IOException("Interrupted while waiting to refresh vector searcher for segment " + _segmentName
              + " column " + _vectorColumn, e);
        }
      }
      if (_searcherRefreshedThroughSequenceNumber >= targetSequenceNumber) {
        return;
      }
      _searcherRefreshInProgress = true;
    }

    try {
      beforeSearcherRefresh();
      // A false return means another caller already owns SearcherManager's refresh lock. This is benign: use the
      // blocking form so this call still returns a searcher covering the requested writer generation.
      if (!_searcherManager.maybeRefresh()) {
        _searcherManager.maybeRefreshBlocking();
      }
      synchronized (_searcherRefreshMonitor) {
        _searcherRefreshedThroughSequenceNumber =
            Math.max(_searcherRefreshedThroughSequenceNumber, targetSequenceNumber);
        _searcherRefreshCount++;
      }
    } finally {
      synchronized (_searcherRefreshMonitor) {
        _searcherRefreshInProgress = false;
        _searcherRefreshMonitor.notifyAll();
      }
    }
  }

  /// Test hook invoked by the winning refresher after it publishes the in-progress state.
  @VisibleForTesting
  void beforeSearcherRefresh()
      throws IOException {
  }

  /// Test hook invoked when another caller is about to wait for the winning refresher.
  @VisibleForTesting
  void onSearcherRefreshWait() {
  }

  @VisibleForTesting
  long getSearcherRefreshCount() {
    return _searcherRefreshCount;
  }

  private MutableRoaringBitmap search(IndexSearcher indexSearcher, float[] vector, int topK, int efSearch,
      boolean useRelativeDistance, boolean useBoundedQueue, @Nullable Query filterQuery)
      throws IOException {
    KnnFloatVectorQuery query =
        LuceneHnswRuntimeControlUtils.createQuery(_vectorColumn, vector, topK, efSearch,
            useRelativeDistance, useBoundedQueue, filterQuery);
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    ScoreDoc[] scoreDocs = indexSearcher.search(query, topK).scoreDocs;
    if (scoreDocs.length == 0) {
      // Nothing matched, which is also the shape of a reader with no leaves: an index that has not committed
      // yet has no doc values to resolve, and asking for them would fail rather than return no results.
      return docIds;
    }
    // Translate Lucene doc ids to Pinot doc ids through the same doc values that drive filtering. Lucene doc
    // ids are NOT guaranteed to equal Pinot doc ids (merges can renumber), so ScoreDoc.doc must never be used
    // directly. Doc values are resolved per leaf rather than through a merged view over every leaf, and hits
    // are visited in Lucene doc id order because NumericDocValues only advances forward.
    Arrays.sort(scoreDocs, LUCENE_DOC_ID_ORDER);
    List<LeafReaderContext> leaves = indexSearcher.getIndexReader().leaves();
    int leafIndex = -1;
    NumericDocValues pinotDocIds = null;
    int leafDocBase = 0;
    for (ScoreDoc scoreDoc : scoreDocs) {
      int hitLeafIndex = ReaderUtil.subIndex(scoreDoc.doc, leaves);
      if (hitLeafIndex != leafIndex) {
        LeafReaderContext leaf = leaves.get(hitLeafIndex);
        leafIndex = hitLeafIndex;
        leafDocBase = leaf.docBase;
        pinotDocIds = leaf.reader().getNumericDocValues(VECTOR_INDEX_DOC_ID_COLUMN_NAME);
        if (pinotDocIds == null) {
          throw new IllegalStateException("Missing Pinot doc id doc values for column: " + _vectorColumn);
        }
      }
      if (!pinotDocIds.advanceExact(scoreDoc.doc - leafDocBase)) {
        throw new IllegalStateException("Missing Pinot doc id for Lucene document: " + scoreDoc.doc);
      }
      docIds.add(Math.toIntExact(pinotDocIds.longValue()));
    }
    return docIds;
  }

  /// A Lucene query accepting only documents whose stored Pinot doc id is present in the given bitmap.
  /// Membership is tested through the [NumericDocValues] written by [#add(Object[], int[], int)], so it is
  /// correct regardless of how Lucene numbers or renumbers its internal doc ids.
  private static class NumericDocValuesBitmapFilterQuery extends BaseFilterQuery {

    NumericDocValuesBitmapFilterQuery(ImmutableRoaringBitmap bitmap) {
      super(bitmap);
    }

    @Override
    protected DocIdSetIterator createLeafIterator(LeafReaderContext context)
        throws IOException {
      NumericDocValues docIdValues = context.reader().getNumericDocValues(VECTOR_INDEX_DOC_ID_COLUMN_NAME);
      if (docIdValues == null) {
        // Every indexed document carries this field. Its absence indicates a corrupt or incompatible index, not a
        // leaf with no matches; returning null would silently discard the entire leaf.
        throw new IllegalStateException(
            "Missing Pinot doc id doc values for column: " + VECTOR_INDEX_DOC_ID_COLUMN_NAME);
      }
      return new DocIdSetIterator() {
        @Override
        public int docID() {
          return docIdValues.docID();
        }

        @Override
        public int nextDoc()
            throws IOException {
          return skipToAccepted(docIdValues.nextDoc());
        }

        @Override
        public int advance(int target)
            throws IOException {
          return skipToAccepted(docIdValues.advance(target));
        }

        private int skipToAccepted(int doc)
            throws IOException {
          while (doc != NO_MORE_DOCS && !_docIds.contains((int) docIdValues.longValue())) {
            doc = docIdValues.nextDoc();
          }
          return doc;
        }

        @Override
        public long cost() {
          return _docIds.getLongCardinality();
        }
      };
    }
  }

  private int getEffectiveEfSearch() {
    Integer efSearch = _efSearchOverride.get();
    return efSearch != null ? efSearch : 0;
  }

  private boolean getEffectiveUseRelativeDistance() {
    Boolean useRelativeDistance = _useRelativeDistanceOverride.get();
    return useRelativeDistance != null ? useRelativeDistance : true;
  }

  private boolean getEffectiveUseBoundedQueue() {
    Boolean useBoundedQueue = _useBoundedQueueOverride.get();
    return useBoundedQueue != null ? useBoundedQueue : true;
  }

  @Override
  public void close() {
    try {
      _indexWriter.commit();
      // IndexWriter does not close the Directory passed to it, so both need to be closed.
      IOUtils.close(_searcherManager, _indexWriter, _indexDirectory);
    } catch (Exception e) {
      // commit() can also fail unchecked (for example, after a tragic writer event). Close every remaining resource
      // so the SearcherManager cannot pin an NRT reader and its file handles for the life of the process. All close()
      // implementations are idempotent, so this is a no-op for resources that were already closed above.
      IOUtils.closeWhileHandlingException(_searcherManager, _indexWriter, _indexDirectory);
      throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
    } finally {
      deleteIndexDir();
    }
  }

  /// Deletes the temporary index directory of this column, then the segment directory holding it if this was the last
  /// column with an index under it.
  private void deleteIndexDir() {
    FileUtils.deleteQuietly(_indexDir);
    // Only succeeds when no other column of the same segment still has an index directory under it.
    //noinspection ResultOfMethodCallIgnored
    _indexDir.getParentFile().delete();
  }
}
