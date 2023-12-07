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
import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.segment.creator.impl.vector.XKnnFloatVectorField;
import org.apache.pinot.segment.local.segment.store.VectorIndexUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Vector index reader for the real-time Vector index values on the fly.
 * Since there is no good mutable vector index implementation for topK search, we just do brute force search.
 * <p>This class is thread-safe for single writer multiple readers.
 */
public class MutableVectorIndex implements VectorIndexReader, MutableIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableVectorIndex.class);
  public static final String VECTOR_INDEX_DOC_ID_COLUMN_NAME = "DocID";
  public static final long DEFAULT_COMMIT_INTERVAL_MS = 10_000L;
  public static final long DEFAULT_COMMIT_DOCS = 1000L;
  private final int _vectorDimension;
  private final VectorSimilarityFunction _vectorSimilarityFunction;
  private final IndexWriter _indexWriter;
  private final String _vectorColumn;
  private final String _segmentName;
  private final long _commitIntervalMs;
  private final long _commitDocs;
  private final File _indexDir;

  private final FSDirectory _indexDirectory;
  private int _nextDocId;

  private long _lastCommitTime;

  public MutableVectorIndex(String segmentName, String vectorColumn, VectorIndexConfig vectorIndexConfig) {
    _vectorColumn = vectorColumn;
    _vectorDimension = vectorIndexConfig.getVectorDimension();
    _segmentName = segmentName;
    _commitIntervalMs = Long.parseLong(
        vectorIndexConfig.getProperties().getOrDefault("commitIntervalMs", String.valueOf(DEFAULT_COMMIT_INTERVAL_MS)));
    _commitDocs = Long.parseLong(
        vectorIndexConfig.getProperties().getOrDefault("commitDocs", String.valueOf(DEFAULT_COMMIT_DOCS)));
    _vectorSimilarityFunction = VectorIndexUtils.toSimilarityFunction(vectorIndexConfig.getVectorDistanceFunction());
    try {
      // segment generation is always in V1 and later we convert (as part of post creation processing)
      // to V3 if segmentVersion is set to V3 in SegmentGeneratorConfig.
      _indexDir = new File(FileUtils.getTempDirectory(), segmentName);
      _indexDirectory = FSDirectory.open(
          new File(_indexDir, _vectorColumn + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION).toPath());
      LOGGER.info("Creating mutable HNSW index for segment: {}, column: {} at path: {} with {}", segmentName,
          vectorColumn, _indexDir.getAbsolutePath(), vectorIndexConfig.getProperties());
      _indexWriter = new IndexWriter(_indexDirectory, VectorIndexUtils.getIndexWriterConfig(vectorIndexConfig));
      _indexWriter.commit();
      _lastCommitTime = System.currentTimeMillis();
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while instantiating the LuceneTextIndexCreator for column: " + vectorColumn, e);
    }
  }

  @Override
  public void add(@Nonnull Object value, int dictId, int docId) {
    throw new UnsupportedOperationException("Mutable Vector indexes are not supported for single-valued columns");
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds, int docId) {
    float[] floatValues = new float[_vectorDimension];
    for (int i = 0; i < values.length; i++) {
      floatValues[i] = (Float) values[i];
    }
    Document docToIndex = new Document();
    XKnnFloatVectorField xKnnFloatVectorField =
        new XKnnFloatVectorField(_vectorColumn, floatValues, _vectorSimilarityFunction);
    docToIndex.add(xKnnFloatVectorField);
    docToIndex.add(new StoredField(VECTOR_INDEX_DOC_ID_COLUMN_NAME, _nextDocId++));
    try {
      _indexWriter.addDocument(docToIndex);
      if ((_lastCommitTime + _commitIntervalMs < System.currentTimeMillis()) || (_nextDocId % _commitDocs == 0)) {
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
    MutableRoaringBitmap docIds;
    try {
      IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(_indexDirectory));
      Query query = new KnnFloatVectorQuery(_vectorColumn, vector, topK);
      docIds = new MutableRoaringBitmap();
      TopDocs search = indexSearcher.search(query, topK);
      Arrays.stream(search.scoreDocs).map(scoreDoc -> scoreDoc.doc).forEach(docIds::add);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return docIds;
  }

  @Override
  public void close() {
    try {
      _indexWriter.commit();
      _indexWriter.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      // Delete the temporary index directory.
      FileUtils.deleteQuietly(_indexDir);
    }
  }
}
