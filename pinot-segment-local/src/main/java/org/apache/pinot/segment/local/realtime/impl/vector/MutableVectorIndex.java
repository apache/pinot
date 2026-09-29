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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
/// **Cost of that freshness.** A reopen flushes the writer's RAM buffer, which writes a new Lucene segment and
/// builds an HNSW graph over the rows it contains. Query threads never pay that cost directly: a filtered query
/// publishes the writer generation it needs and waits, and a single background thread performs the reopen, so
/// concurrent queries needing the same generation share one flush rather than forcing one each. Reopens happen
/// only while a query is waiting -- never on a timer -- and are spaced at least `refreshMinIntervalMs` apart, so
/// segment creation stays bounded by ingestion rather than by query rate. A query waits at most
/// `refreshWaitTimeoutMs`, then fails rather than answering from a searcher that may not hold its rows.
///
/// This class is thread-safe for a single writer and multiple readers, plus one background reopen thread that
/// this instance owns and [#close()] stops.
public class MutableVectorIndex
    implements FilterAwareVectorIndexReader, MutableIndex, VectorIndexConfigProvider, EfSearchAware {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableVectorIndex.class);
  private static final Comparator<ScoreDoc> LUCENE_DOC_ID_ORDER = Comparator.comparingInt(scoreDoc -> scoreDoc.doc);
  private static final RealtimeLuceneTextIndexSearcherPool SEARCHER_POOL =
      RealtimeLuceneTextIndexSearcherPool.getInstance();
  public static final String VECTOR_INDEX_DOC_ID_COLUMN_NAME = "DocID";
  public static final long DEFAULT_COMMIT_INTERVAL_MS = 10_000L;
  public static final long DEFAULT_COMMIT_DOCS = 1000L;
  public static final String REFRESH_MIN_INTERVAL_MS = "refreshMinIntervalMs";
  public static final String REFRESH_WAIT_TIMEOUT_MS = "refreshWaitTimeoutMs";
  /// Minimum spacing between reopens, and equally the freshness delay a filtered query can pay. Raising it trades
  /// query latency for fewer writer flushes; 0 disables the spacing.
  ///
  /// The default is deliberately small because most of the reduction comes from sharing rather than from spacing:
  /// one reopen serves every query waiting on it, so reopens fall roughly by the number of concurrent readers
  /// whatever this is set to. Measured at 2000 docs/s with 8 reader threads, raising it from 1ms to 10ms cut
  /// reopens a further 4x but cost 4x the throughput and 4.5x the p50, landing well below the per-query-refresh
  /// behaviour it replaces. At 1ms it measured better than that behaviour on throughput, p50, p95 and p99 while
  /// still performing about a quarter of its reopens.
  public static final long DEFAULT_REFRESH_MIN_INTERVAL_MS = 1L;
  /// How long a filtered query waits for a reopen before failing. Bounded on purpose: an unbounded wait would let
  /// a stalled reopen pin query threads of the shared searcher pool indefinitely.
  public static final long DEFAULT_REFRESH_WAIT_TIMEOUT_MS = 5_000L;
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
  // enforcement) so uncommitted rows are visible; reopened by _reopenThread, reused across queries
  private final SearcherManager _searcherManager;
  /// Performs every near-real-time reopen on one background thread. Query threads never flush the writer
  /// themselves: they publish the generation they need and block until this thread has reopened past it, so N
  /// concurrent queries share one reopen instead of forcing N.
  ///
  /// Deliberately hand-rolled rather than delegating to Lucene's `ControlledRealTimeReopenThread`, for two reasons
  /// that both bear on correctness here. That class publishes its searching generation from a
  /// `ReferenceManager.RefreshListener`, which `ReferenceManager#doMaybeRefresh` invokes from a finally block --
  /// so a reopen that *threw* still advertises the generation it merely attempted, and a filtered query would then
  /// search a stale searcher and silently drop rows its filter names. And its reopen loop refreshes on a fixed
  /// cadence whether or not anyone is waiting, which on a consuming segment is never a no-op: rows are always
  /// buffered, so every tick flushes and builds a graph even for a table that runs no filtered query at all.
  /// The loop below publishes only after a reopen returns normally, and runs only when a query is waiting.
  private final Thread _reopenThread;
  private final long _refreshMinIntervalMs;
  private final long _refreshWaitTimeoutMs;
  /// Guards the reopen handshake: the requested and reached generations, the failure record, and the closed flag.
  private final Object _refreshMonitor = new Object();
  /// Highest generation any waiting query has asked for. Guarded by [#_refreshMonitor].
  private long _requestedSequenceNumber = -1;
  /// Highest generation the shared searcher is known to cover. Published only after a reopen returns normally, so
  /// a failed reopen can never make a query believe it is looking at rows the searcher does not have.
  private volatile long _refreshedThroughSequenceNumber = -1;
  /// Last reopen failure and a count of them, so a query blocked across a failure fails instead of waiting out its
  /// timeout. Guarded by [#_refreshMonitor].
  private Throwable _reopenFailure;
  private long _reopenFailureCount;
  private boolean _closed;
  private final AtomicLong _searcherRefreshCount = new AtomicLong();
  private final AtomicLong _searcherRefreshWaitCount = new AtomicLong();
  // Number of documents added so far; used only for the commit cadence, never as a doc id. Written by the indexing
  // thread only; read cross-thread for debug output, where staleness is acceptable.
  private volatile int _numDocsAdded;
  /// Sequence number of the newest row handed to the writer. A filtered search waits for the reopen thread to
  /// pass this value, and skips waiting entirely when the searcher is already past it. Doc ids cannot be used
  /// for this: [MutableIndex#add] allows rows in arbitrary doc-id order, so a doc-id watermark would skip the
  /// wait a lower-numbered but newer row needs. Writer sequence numbers are monotonic by construction, and share
  /// the space that `IndexWriter#getMaxCompletedSequenceNumber` reports, which is what the reopen thread
  /// publishes as its searching generation. Written by the single indexing thread, read by query threads.
  private volatile long _lastAddedSequenceNumber = -1;

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
    _refreshMinIntervalMs = Long.parseLong(vectorIndexConfig.getProperties()
        .getOrDefault(REFRESH_MIN_INTERVAL_MS, String.valueOf(DEFAULT_REFRESH_MIN_INTERVAL_MS)));
    _refreshWaitTimeoutMs = Long.parseLong(vectorIndexConfig.getProperties()
        .getOrDefault(REFRESH_WAIT_TIMEOUT_MS, String.valueOf(DEFAULT_REFRESH_WAIT_TIMEOUT_MS)));
    Preconditions.checkArgument(_refreshMinIntervalMs >= 0, "Require %s >= 0, got %s for column: %s",
        REFRESH_MIN_INTERVAL_MS, _refreshMinIntervalMs, vectorColumn);
    Preconditions.checkArgument(_refreshWaitTimeoutMs > 0, "Require %s > 0, got %s for column: %s",
        REFRESH_WAIT_TIMEOUT_MS, _refreshWaitTimeoutMs, vectorColumn);
    _vectorSimilarityFunction = VectorIndexUtils.toSimilarityFunction(vectorIndexConfig.getVectorDistanceFunction());
    // Each column of a segment gets its own directory, so that cleaning up one column does not remove the index of
    // another column of the same segment.
    _indexDir = new File(new File(FileUtils.getTempDirectory(), segmentName),
        _vectorColumn + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);

    FSDirectory indexDirectory = null;
    IndexWriter indexWriter = null;
    SearcherManager searcherManager = null;
    Thread reopenThread = null;
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
      reopenThread = new Thread(this::reopenLoop, "vector-nrt-reopen-" + segmentName + "-" + vectorColumn);
      reopenThread.setDaemon(true);
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
    _reopenThread = reopenThread;
    // Started last: the loop reads final fields assigned above, so publishing `this` to another thread any
    // earlier would race construction. Guarded because this is where OutOfMemoryError: unable to create native
    // thread lands, and by now nothing else holds a reference that could close the writer -- which would keep
    // write.lock on a directory whose name is deterministic, so every retry of this segment would then fail.
    try {
      _reopenThread.start();
    } catch (Throwable t) {
      IOUtils.closeWhileHandlingException(_searcherManager, _indexWriter, _indexDirectory);
      deleteIndexDir();
      throw t;
    }
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
    info.put(REFRESH_MIN_INTERVAL_MS, _refreshMinIntervalMs);
    info.put(REFRESH_WAIT_TIMEOUT_MS, _refreshWaitTimeoutMs);
    // Reopens vs. the queries that had to wait for one: the gap between them is the sharing this path relies on.
    info.put("searcherRefreshCount", _searcherRefreshCount.get());
    info.put("searcherRefreshWaitCount", _searcherRefreshWaitCount.get());
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
      // including rows still in the writer's RAM buffer. The watermark is read BEFORE waiting, so rows arriving
      // during the reopen are not wrongly claimed as visible.
      awaitSearcherGeneration(_lastAddedSequenceNumber);
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

  /// Blocks until the shared searcher is known to cover `targetSequenceNumber`.
  ///
  /// The reopen itself runs on [#_reopenThread]; this only publishes the generation needed and waits. That is the
  /// point of the indirection -- every concurrent caller needing the same or an older generation is satisfied by
  /// one reopen, and no query thread ever flushes the writer.
  ///
  /// Fails rather than waits when the reopen cannot deliver: a reopen that threw, a closed index, or a wait past
  /// [#_refreshWaitTimeoutMs]. Returning normally without the generation would mean searching a stale searcher and
  /// silently dropping rows the query's filter names, which is the failure this whole path exists to prevent.
  @VisibleForTesting
  void awaitSearcherGeneration(long targetSequenceNumber)
      throws IOException {
    // Plain volatile read, so a query that is already covered adds no synchronization at all.
    if (targetSequenceNumber <= _refreshedThroughSequenceNumber) {
      return;
    }
    _searcherRefreshWaitCount.incrementAndGet();
    long deadlineMs = System.currentTimeMillis() + _refreshWaitTimeoutMs;
    synchronized (_refreshMonitor) {
      long failuresBefore = _reopenFailureCount;
      if (targetSequenceNumber > _requestedSequenceNumber) {
        _requestedSequenceNumber = targetSequenceNumber;
      }
      _refreshMonitor.notifyAll();
      while (targetSequenceNumber > _refreshedThroughSequenceNumber) {
        if (_closed) {
          throw new IOException(describe("Vector index closed while waiting for the searcher to reopen"));
        }
        if (_reopenFailureCount != failuresBefore) {
          throw new IOException(describe("Vector searcher reopen failed"), _reopenFailure);
        }
        long remainingMs = deadlineMs - System.currentTimeMillis();
        if (remainingMs <= 0) {
          throw new IOException(describe(
              "Timed out after " + _refreshWaitTimeoutMs + "ms waiting for the vector searcher to reopen through "
                  + "generation " + targetSequenceNumber));
        }
        try {
          _refreshMonitor.wait(remainingMs);
        } catch (InterruptedException e) {
          // Not re-arming the interrupt flag: the cause is preserved on the IOException, and this runs on a pooled
          // Lucene searcher thread the pool deliberately keeps un-interrupted (see submitSearch).
          throw new IOException(describe("Interrupted while waiting for the vector searcher to reopen"), e);
        }
      }
    }
  }

  /// Reopens the shared searcher whenever a query is waiting for a generation it does not yet cover.
  ///
  /// Runs only on demand. An idle cadence would be pure cost here: a consuming segment always has buffered rows,
  /// so a timed reopen is never the cheap no-op it is for a settled index -- it flushes the writer and builds an
  /// HNSW graph for the flushed rows, even for a table that issues no filtered query at all.
  private void reopenLoop() {
    long lastReopenMs = 0L;
    long failedRequest = Long.MIN_VALUE;
    int consecutiveFailures = 0;
    while (true) {
      long request;
      synchronized (_refreshMonitor) {
        // Park unless someone needs a generation we have not reached AND have not already failed on. Without the
        // failedRequest half, a request whose waiters have all given up would drive an unbounded retry-and-log
        // loop, because a failed reopen never advances _refreshedThroughSequenceNumber.
        while (!_closed && (_requestedSequenceNumber <= _refreshedThroughSequenceNumber
            || _requestedSequenceNumber <= failedRequest)) {
          try {
            _refreshMonitor.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
        if (_closed) {
          return;
        }
        // Spacing waits on the monitor rather than sleeping, so close() can wake it immediately. It has to loop:
        // every arriving query calls notifyAll, and a single timed wait would return on that notification and
        // reopen early, leaving the interval unenforced exactly when load makes it matter.
        long reopenNotBeforeMs = lastReopenMs + Math.max(_refreshMinIntervalMs,
            consecutiveFailures == 0 ? 0L : Math.min(1000L << Math.min(consecutiveFailures - 1, 5), 30_000L));
        while (!_closed) {
          long waitMs = reopenNotBeforeMs - System.currentTimeMillis();
          if (waitMs <= 0) {
            break;
          }
          try {
            _refreshMonitor.wait(waitMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
        if (_closed) {
          return;
        }
        // Read after the wait, so this reopen serves the newest request rather than the one that woke us.
        request = _requestedSequenceNumber;
      }
      // Read the writer's generation BEFORE reopening, so rows arriving during the reopen are not claimed as
      // visible by a reader that may not contain them.
      long generation;
      try {
        generation = _indexWriter.getMaxCompletedSequenceNumber();
        doReopen();
      } catch (Throwable t) {
        consecutiveFailures++;
        // Log the first failure of a run in full; after that only periodically, so a persistently failing writer
        // cannot turn an already-degraded server into a log flood.
        if (consecutiveFailures == 1 || consecutiveFailures % 100 == 0) {
          LOGGER.error("Failed to reopen the vector searcher ({} consecutive) for segment: {}, column: {}",
              consecutiveFailures, _segmentName, _vectorColumn, t);
        }
        lastReopenMs = System.currentTimeMillis();
        synchronized (_refreshMonitor) {
          _reopenFailure = t;
          _reopenFailureCount++;
          // Remember what failed so an abandoned request cannot drive a retry loop. A newer request still gets a
          // fresh attempt, so a transient failure (disk pressure, a slow flush) recovers on the next query.
          failedRequest = request;
          _refreshMonitor.notifyAll();
        }
        continue;
      }
      lastReopenMs = System.currentTimeMillis();
      consecutiveFailures = 0;
      _searcherRefreshCount.incrementAndGet();
      synchronized (_refreshMonitor) {
        // Published only here, after a reopen that returned normally.
        _refreshedThroughSequenceNumber = Math.max(_refreshedThroughSequenceNumber, generation);
        _refreshMonitor.notifyAll();
      }
    }
  }

  /// Seam for tests that need a reopen to fail; the failure handling around it is the part worth covering.
  @VisibleForTesting
  void doReopen()
      throws IOException {
    _searcherManager.maybeRefreshBlocking();
  }

  private String describe(String message) {
    return message + " for segment: " + _segmentName + ", column: " + _vectorColumn;
  }

  @VisibleForTesting
  long getLastAddedSequenceNumber() {
    return _lastAddedSequenceNumber;
  }

  @VisibleForTesting
  long getSearcherRefreshCount() {
    return _searcherRefreshCount.get();
  }

  /// Cumulative number of queries that had to wait for a reopen. Compared against the reopen count, this is what
  /// shows sharing: many waits against one reopen.
  @VisibleForTesting
  long getSearcherRefreshWaitCount() {
    return _searcherRefreshWaitCount.get();
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
    // Stop the reopen loop and release every waiting query before the searcher manager goes away: the loop
    // refreshes through it, and a query blocked on a generation that will now never arrive must not hang.
    synchronized (_refreshMonitor) {
      _closed = true;
      _refreshMonitor.notifyAll();
    }
    try {
      _reopenThread.join(TimeUnit.SECONDS.toMillis(30));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
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
