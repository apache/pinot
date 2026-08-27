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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
import org.apache.pinot.segment.local.segment.creator.impl.vector.XKnnFloatVectorField;
import org.apache.pinot.segment.local.segment.index.readers.vector.LuceneHnswRuntimeControlUtils;
import org.apache.pinot.segment.local.segment.store.VectorIndexUtils;
import org.apache.pinot.segment.spi.index.VectorIndexConfigProvider;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.reader.EfSearchAware;
import org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// A vector index reader for real-time vector values indexed on the fly.
///
/// Searches open a near-real-time Lucene reader from the active writer, so completed additions are visible without a
/// commit. Pinot document IDs are stored explicitly and translated from the same reader generation used for search.
/// Each instance owns a unique temporary directory, preventing Lucene write-lock collisions between replicas hosted
/// in the same JVM.
///
/// This class is thread-safe for a single writer and multiple readers. Lifecycle coordination must prevent [#close()]
/// from racing with active additions or searches.
public class MutableVectorIndex
    implements FilterAwareVectorIndexReader, MutableIndex, VectorIndexConfigProvider, EfSearchAware {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableVectorIndex.class);
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
  private volatile int _numDocs;

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
    _indexDir = createIndexDir(segmentName, vectorColumn);

    FSDirectory indexDirectory = null;
    IndexWriter indexWriter = null;
    try {
      // segment generation is always in V1 and later we convert (as part of post creation processing)
      // to V3 if segmentVersion is set to V3 in SegmentGeneratorConfig.
      indexDirectory = FSDirectory.open(_indexDir.toPath());
      LOGGER.info("Creating mutable HNSW index for segment: {}, column: {} at path: {} with {}", segmentName,
          vectorColumn, _indexDir.getAbsolutePath(), vectorIndexConfig.getProperties());
      indexWriter = new IndexWriter(indexDirectory, VectorIndexUtils.getIndexWriterConfig(vectorIndexConfig));
      indexWriter.commit();
      _lastCommitTime = System.currentTimeMillis();
    } catch (Exception e) {
      // IndexWriter does not close the Directory passed to it, so both need to be closed.
      try {
        IOUtils.close(indexWriter, indexDirectory);
      } catch (Exception closeEx) {
        e.addSuppressed(closeEx);
      }
      deleteIndexDir();
      throw new RuntimeException(
          "Caught exception while instantiating the LuceneTextIndexCreator for column: " + vectorColumn, e);
    }
    _indexDirectory = indexDirectory;
    _indexWriter = indexWriter;
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
    // The numeric doc value drives filtered traversal and translates Lucene hits back to Pinot document IDs from the
    // same near-real-time reader generation.
    docToIndex.add(new NumericDocValuesField(VECTOR_INDEX_DOC_ID_COLUMN_NAME, docId));
    try {
      _indexWriter.addDocument(docToIndex);
      _numDocs++;
      if ((_lastCommitTime + _commitIntervalMs < System.currentTimeMillis()) || (_numDocs % _commitDocs == 0)) {
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
    return submitVectorSearch(Arrays.copyOf(vector, vector.length), topK, null);
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(float[] vector, int topK, ImmutableRoaringBitmap preFilterBitmap) {
    ImmutableRoaringBitmap preFilterCopy =
        preFilterBitmap.toMutableRoaringBitmap().toImmutableRoaringBitmap();
    if (preFilterCopy.isEmpty()) {
      return new MutableRoaringBitmap();
    }
    return submitVectorSearch(Arrays.copyOf(vector, vector.length), topK, preFilterCopy);
  }

  private MutableRoaringBitmap submitVectorSearch(float[] vector, int topK,
      @Nullable ImmutableRoaringBitmap preFilterBitmap) {
    int effectiveEfSearch = getEffectiveEfSearch();
    boolean effectiveUseRelativeDistance = getEffectiveUseRelativeDistance();
    boolean effectiveUseBoundedQueue = getEffectiveUseBoundedQueue();
    // Search is executed in SEARCHER_POOL which is wrapped with contextAwareExecutorService(executor, false).
    // This propagates QueryThreadContext for CPU/memory tracking without registering the task for cancellation,
    // preventing Thread.interrupt() during Lucene search which could corrupt FSDirectory.
    // See https://github.com/apache/lucene/issues/3315 and https://github.com/apache/lucene/issues/9309
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
    info.put("supportsPreFilter", true);
    try (DirectoryReader directoryReader = DirectoryReader.open(_indexWriter)) {
      info.put("numDocs", directoryReader.numDocs());
      info.put("numDeletedDocs", directoryReader.numDeletedDocs());
      info.put("luceneSegments", directoryReader.leaves().size());
    } catch (IOException e) {
      LOGGER.warn("Failed to load mutable HNSW debug stats for segment: {}, column: {}", _segmentName, _vectorColumn,
          e);
      info.put("numDocs", _numDocs);
      info.put("numDeletedDocs", 0);
      info.put("luceneSegments", 0);
    }
    return info;
  }

  private MutableRoaringBitmap executeVectorSearch(float[] vector, int topK, int efSearch,
      boolean useRelativeDistance, boolean useBoundedQueue,
      @Nullable ImmutableRoaringBitmap preFilterBitmap) throws IOException {
    // Open from the writer rather than the directory so completed, uncommitted additions are visible to the query.
    try (DirectoryReader directoryReader = DirectoryReader.open(_indexWriter)) {
      IndexSearcher indexSearcher = new IndexSearcher(directoryReader);
      Query filterQuery = preFilterBitmap != null
          ? new PinotDocIdFilterQuery(preFilterBitmap, VECTOR_INDEX_DOC_ID_COLUMN_NAME) : null;
      KnnFloatVectorQuery query =
          LuceneHnswRuntimeControlUtils.createQuery(_vectorColumn, vector, topK, efSearch,
              useRelativeDistance, useBoundedQueue, filterQuery);
      TopDocs search = indexSearcher.search(query, topK);
      return translateTopDocs(directoryReader, search);
    }
  }

  private static MutableRoaringBitmap translateTopDocs(DirectoryReader directoryReader, TopDocs topDocs)
      throws IOException {
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    NumericDocValues pinotDocIds = MultiDocValues.getNumericValues(directoryReader, VECTOR_INDEX_DOC_ID_COLUMN_NAME);
    if (pinotDocIds == null) {
      throw new IllegalStateException("Missing Pinot document ID doc values");
    }
    ScoreDoc[] scoreDocs = Arrays.copyOf(topDocs.scoreDocs, topDocs.scoreDocs.length);
    Arrays.sort(scoreDocs, Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
    for (ScoreDoc scoreDoc : scoreDocs) {
      if (!pinotDocIds.advanceExact(scoreDoc.doc)) {
        throw new IllegalStateException("Missing Pinot document ID for Lucene document: " + scoreDoc.doc);
      }
      docIds.add(Math.toIntExact(pinotDocIds.longValue()));
    }
    return docIds;
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
      IOUtils.close(_indexWriter, _indexDirectory);
    } catch (IOException e) {
      // Both close() implementations are idempotent, so this is a no-op for whatever was already closed above.
      IOUtils.closeWhileHandlingException(_indexWriter, _indexDirectory);
      throw new RuntimeException(e);
    } finally {
      deleteIndexDir();
    }
  }

  /// Creates a directory private to this instance, so replicas of the same segment hosted in one JVM cannot collide
  /// on a Lucene write lock. The segment and column are kept in the name so a directory left behind by a crashed
  /// process can still be attributed.
  private static File createIndexDir(String segmentName, String vectorColumn) {
    String prefix = sanitizeForFileName(segmentName) + '-' + sanitizeForFileName(vectorColumn) + '-';
    try {
      return Files.createTempDirectory("pinot-mutable-vector-" + prefix).toFile();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temporary directory for mutable vector index", e);
    }
  }

  /// Keeps the name filesystem-safe and bounded; the random suffix supplied by the JDK still guarantees uniqueness.
  private static String sanitizeForFileName(String value) {
    String sanitized = value.replaceAll("[^A-Za-z0-9._-]", "_");
    return sanitized.length() <= 64 ? sanitized : sanitized.substring(0, 64);
  }

  /// Deletes this instance's private temporary index directory.
  private void deleteIndexDir() {
    FileUtils.deleteQuietly(_indexDir);
  }

  /// A Lucene filter query that tests Pinot document IDs stored as numeric doc values against a private bitmap copy.
  /// Numeric doc values are resolved from each leaf of the same near-real-time reader used by the vector query, so the
  /// mapping cannot drift across Lucene generations.
  private static final class PinotDocIdFilterQuery extends Query {
    private final ImmutableRoaringBitmap _bitmap;
    private final String _docIdField;

    private PinotDocIdFilterQuery(ImmutableRoaringBitmap bitmap, String docIdField) {
      _bitmap = bitmap;
      _docIdField = docIdField;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
      return new ConstantScoreWeight(this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          NumericDocValues pinotDocIds = context.reader().getNumericDocValues(_docIdField);
          if (pinotDocIds == null) {
            return null;
          }
          DocIdSetIterator iterator = new FilteredDocIdSetIterator(pinotDocIds, _bitmap);
          float constantScore = score();
          return new Scorer(this) {
            @Override
            public DocIdSetIterator iterator() {
              return iterator;
            }

            @Override
            public float getMaxScore(int upTo) {
              return constantScore;
            }

            @Override
            public float score() {
              return constantScore;
            }

            @Override
            public int docID() {
              return iterator.docID();
            }
          };
        }

        @Override
        public boolean isCacheable(LeafReaderContext context) {
          return false;
        }
      };
    }

    @Override
    public String toString(String field) {
      return "PinotDocIdFilterQuery(cardinality=" + _bitmap.getCardinality() + ')';
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof PinotDocIdFilterQuery)) {
        return false;
      }
      PinotDocIdFilterQuery that = (PinotDocIdFilterQuery) other;
      return _bitmap == that._bitmap && _docIdField.equals(that._docIdField);
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(_bitmap) * 31 + _docIdField.hashCode();
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }
  }

  private static final class FilteredDocIdSetIterator extends DocIdSetIterator {
    private final NumericDocValues _pinotDocIds;
    private final ImmutableRoaringBitmap _bitmap;

    private FilteredDocIdSetIterator(NumericDocValues pinotDocIds, ImmutableRoaringBitmap bitmap) {
      _pinotDocIds = pinotDocIds;
      _bitmap = bitmap;
    }

    @Override
    public int docID() {
      return _pinotDocIds.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return advanceToAllowed(_pinotDocIds.nextDoc());
    }

    @Override
    public int advance(int target) throws IOException {
      return advanceToAllowed(_pinotDocIds.advance(target));
    }

    private int advanceToAllowed(int luceneDocId) throws IOException {
      while (luceneDocId != NO_MORE_DOCS) {
        if (_bitmap.contains((int) _pinotDocIds.longValue())) {
          return luceneDocId;
        }
        luceneDocId = _pinotDocIds.nextDoc();
      }
      return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return Math.min(_pinotDocIds.cost(), _bitmap.getLongCardinality());
    }
  }
}
