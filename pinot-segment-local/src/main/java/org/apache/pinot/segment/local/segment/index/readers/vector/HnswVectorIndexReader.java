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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.vector.HnswVectorIndexCreator;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.EfSearchAware;
import org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.LoggerFactory;


public class HnswVectorIndexReader implements FilterAwareVectorIndexReader, EfSearchAware {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(HnswVectorIndexReader.class);

  private final IndexReader _indexReader;
  private final Directory _indexDirectory;
  private final IndexSearcher _indexSearcher;
  private final String _column;
  private final HnswVectorIndexReader.DocIdTranslator _docIdTranslator;
  private final ThreadLocal<Integer> _efSearchOverride = new ThreadLocal<>();
  private final ThreadLocal<Boolean> _useRelativeDistanceOverride = new ThreadLocal<>();
  private final ThreadLocal<Boolean> _useBoundedQueueOverride = new ThreadLocal<>();

  public HnswVectorIndexReader(String column, File indexDir, int numDocs, VectorIndexConfig config) {
    _column = column;
    try {
      File indexFile = getVectorIndexFile(indexDir);
      _indexDirectory = FSDirectory.open(indexFile.toPath());
      _indexReader = DirectoryReader.open(_indexDirectory);
      _indexSearcher = new IndexSearcher(_indexReader);

      // TODO: consider using a threshold of num docs per segment to decide between building
      // mapping file upfront on segment load v/s on-the-fly during query processing
      _docIdTranslator = new HnswVectorIndexReader.DocIdTranslator(indexDir, _column, numDocs, _indexSearcher);
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate Lucene HNSW index reader for column {}, exception {}", column,
          e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * CASE 1: If IndexLoadingConfig specifies a segment version to load and if it is different then
   * the on-disk version of the segment, then {@link ImmutableSegmentLoader}
   * will take care of up-converting the on-disk segment to v3 before load. The converter
   * already has support for converting v1 vector index to v3. So the vector index can be
   * loaded from segmentIndexDir/v3/ since v3 sub-directory would have already been created
   *
   * CASE 2: However, if IndexLoadingConfig doesn't specify the segment version to load or if the specified
   * version is same as the on-disk version of the segment, then ImmutableSegmentLoader will load
   * whatever the version of segment is on disk.
   * @param segmentIndexDir top-level segment index directory
   * @return vector index file
   */
  private File getVectorIndexFile(File segmentIndexDir) {
    // will return null if file does not exist
    File file = SegmentDirectoryPaths.findVectorIndexIndexFile(segmentIndexDir, _column, VectorBackendType.HNSW);
    if (file == null) {
      throw new IllegalStateException("Failed to find HNSW index file for column: " + _column);
    }
    return file;
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

  /**
   * Returns the efSearch value for debug/explain output, or 0 if not set.
   */
  int getEffectiveEfSearch() {
    Integer efSearch = _efSearchOverride.get();
    return efSearch != null ? efSearch : 0;
  }

  boolean getEffectiveUseRelativeDistance() {
    Boolean useRelativeDistance = _useRelativeDistanceOverride.get();
    return useRelativeDistance != null ? useRelativeDistance : true;
  }

  boolean getEffectiveUseBoundedQueue() {
    Boolean useBoundedQueue = _useBoundedQueueOverride.get();
    return useBoundedQueue != null ? useBoundedQueue : true;
  }

  @Override
  public MutableRoaringBitmap getDocIds(float[] searchQuery, int topK) {
    try {
      return translateTopDocs(search(searchQuery, topK, null));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      String msg = "Caught exception while searching the HNSW index for column: " + _column + ", search query: "
          + Arrays.toString(searchQuery);
      throw new RuntimeException(msg, e);
    }
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(float[] searchQuery, int topK, ImmutableRoaringBitmap preFilterBitmap) {
    try {
      Query filterQuery = new RoaringBitmapFilterQuery(preFilterBitmap, _docIdTranslator, _indexReader.numDocs());
      return translateTopDocs(search(searchQuery, topK, filterQuery));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      String msg = "Caught exception while searching the HNSW index with pre-filter for column: " + _column
          + ", search query: " + Arrays.toString(searchQuery);
      throw new RuntimeException(msg, e);
    }
  }

  /**
   * When we destroy the loaded ImmutableSegment, all the indexes
   * (for each column) are destroyed and as part of that
   * we release the vector index
   * @throws IOException
   */
  @Override
  public Map<String, Object> getIndexDebugInfo() {
    Map<String, Object> info = new LinkedHashMap<>();
    info.put("backend", "HNSW");
    info.put("column", _column);
    info.put("numDocs", _indexReader.numDocs());
    info.put("numDeletedDocs", _indexReader.numDeletedDocs());
    info.put("luceneSegments", _indexReader.leaves().size());
    info.put("effectiveEfSearch", getEffectiveEfSearch());
    info.put("effectiveHnswUseRelativeDistance", getEffectiveUseRelativeDistance());
    info.put("effectiveHnswUseBoundedQueue", getEffectiveUseBoundedQueue());
    info.put("supportsPreFilter", true);
    return info;
  }

  @Override
  public void close()
      throws IOException {
    _indexReader.close();
    _indexDirectory.close();
    _docIdTranslator.close();
  }

  private TopDocs search(float[] searchQuery, int topK, Query filterQuery)
      throws IOException {
    KnnFloatVectorQuery query = LuceneHnswRuntimeControlUtils.createQuery(_column, searchQuery, topK,
        getEffectiveEfSearch(), getEffectiveUseRelativeDistance(), getEffectiveUseBoundedQueue(), filterQuery);
    return _indexSearcher.search(query, topK);
  }

  private MutableRoaringBitmap translateTopDocs(TopDocs topDocs) {
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      docIds.add(_docIdTranslator.getPinotDocId(scoreDoc.doc));
    }
    return docIds;
  }

  /**
   * Lucene docIDs are not same as pinot docIDs. The internal implementation
   * of Lucene can change the docIds and they are not guaranteed to be the
   * same as how we expect -- strictly increasing docIDs as the documents
   * are ingested during segment/index creation.
   * This class is used to map the luceneDocId (returned by the search query
   * to the collector) to corresponding pinotDocId.
   */
  static class DocIdTranslator implements Closeable {
    final PinotDataBuffer _buffer;

    DocIdTranslator(File segmentIndexDir, String column, int numDocs, IndexSearcher indexSearcher)
        throws Exception {
      int length = Integer.BYTES * numDocs;
      File docIdMappingFile = new File(SegmentDirectoryPaths.findSegmentDirectory(segmentIndexDir),
          column + V1Constants.Indexes.VECTOR_HNSW_INDEX_DOCID_MAPPING_FILE_EXTENSION);
      // The mapping is local to a segment. It is created on the server during segment load.
      // Unless we are running Pinot on Solaris/SPARC, the underlying architecture is
      // LITTLE_ENDIAN (Linux/x86). So use that as byte order.
      String desc = "Vector index docId mapping buffer: " + column;
      if (docIdMappingFile.exists()) {
        // we will be here for segment reload and server restart
        // for refresh, we will not be here since segment is deleted/replaced
        // TODO: see if we can prefetch the pages
        _buffer =
            PinotDataBuffer.mapFile(docIdMappingFile, /* readOnly */ true, 0, length, ByteOrder.LITTLE_ENDIAN, desc);
      } else {
        _buffer =
            PinotDataBuffer.mapFile(docIdMappingFile, /* readOnly */ false, 0, length, ByteOrder.LITTLE_ENDIAN, desc);
        for (int i = 0; i < numDocs; i++) {
          try {
            Document document = indexSearcher.doc(i);
            int pinotDocId = Integer.parseInt(document.get(HnswVectorIndexCreator.VECTOR_INDEX_DOC_ID_COLUMN_NAME));
            _buffer.putInt(i * Integer.BYTES, pinotDocId);
          } catch (Exception e) {
            throw new RuntimeException(
                "Caught exception while building doc id mapping for HNSW index column: " + column, e);
          }
        }
      }
    }

    int getPinotDocId(int luceneDocId) {
      return _buffer.getInt(luceneDocId * Integer.BYTES);
    }

    @Override
    public void close()
        throws IOException {
      _buffer.close();
    }
  }

  /**
   * A Lucene {@link Query} that accepts only documents whose Pinot doc IDs are present
   * in a {@link ImmutableRoaringBitmap}. Used to implement pre-filter ANN search by
   * restricting HNSW graph traversal to the pre-filtered document set.
   *
   * <p>Because Lucene uses its own internal doc IDs (which differ from Pinot doc IDs),
   * this query translates Lucene doc IDs to Pinot doc IDs using the {@link DocIdTranslator}
   * before checking membership in the bitmap.</p>
   */
  static class RoaringBitmapFilterQuery extends Query {
    private final ImmutableRoaringBitmap _bitmap;
    private final DocIdTranslator _docIdTranslator;
    private final int _maxDoc;

    RoaringBitmapFilterQuery(ImmutableRoaringBitmap bitmap, DocIdTranslator docIdTranslator, int maxDoc) {
      _bitmap = bitmap;
      _docIdTranslator = docIdTranslator;
      _maxDoc = maxDoc;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
      return new ConstantScoreWeight(this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) {
          int docBase = context.docBase;
          int maxDocInLeaf = context.reader().maxDoc();
          DocIdSetIterator iterator = new BitmapDocIdSetIterator(docBase, maxDocInLeaf);
          float constScore = score();
          return new Scorer(this) {
            @Override
            public DocIdSetIterator iterator() {
              return iterator;
            }

            @Override
            public float getMaxScore(int upTo) {
              return constScore;
            }

            @Override
            public float score() {
              return constScore;
            }

            @Override
            public int docID() {
              return iterator.docID();
            }
          };
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }
      };
    }

    @Override
    public String toString(String field) {
      return "RoaringBitmapFilterQuery(cardinality=" + _bitmap.getCardinality() + ")";
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof RoaringBitmapFilterQuery)) {
        return false;
      }
      RoaringBitmapFilterQuery that = (RoaringBitmapFilterQuery) other;
      return _bitmap == that._bitmap && _docIdTranslator == that._docIdTranslator;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(_bitmap) * 31 + System.identityHashCode(_docIdTranslator);
    }

    @Override
    public void visit(org.apache.lucene.search.QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    /**
     * Iterates over Lucene doc IDs whose corresponding Pinot doc IDs are in the bitmap.
     */
    private class BitmapDocIdSetIterator extends DocIdSetIterator {
      private final int _docBase;
      private final int _maxDocInLeaf;
      private int _doc = -1;

      BitmapDocIdSetIterator(int docBase, int maxDocInLeaf) {
        _docBase = docBase;
        _maxDocInLeaf = maxDocInLeaf;
      }

      @Override
      public int docID() {
        return _doc;
      }

      @Override
      public int nextDoc() {
        _doc++;
        while (_doc < _maxDocInLeaf) {
          int pinotDocId = _docIdTranslator.getPinotDocId(_docBase + _doc);
          if (_bitmap.contains(pinotDocId)) {
            return _doc;
          }
          _doc++;
        }
        _doc = NO_MORE_DOCS;
        return _doc;
      }

      @Override
      public int advance(int target) {
        _doc = target;
        while (_doc < _maxDocInLeaf) {
          int pinotDocId = _docIdTranslator.getPinotDocId(_docBase + _doc);
          if (_bitmap.contains(pinotDocId)) {
            return _doc;
          }
          _doc++;
        }
        _doc = NO_MORE_DOCS;
        return _doc;
      }

      @Override
      public long cost() {
        return _bitmap.getLongCardinality();
      }
    }
  }
}
