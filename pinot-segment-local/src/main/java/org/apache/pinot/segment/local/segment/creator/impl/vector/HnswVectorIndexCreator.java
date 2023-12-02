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
package org.apache.pinot.segment.local.segment.creator.impl.vector;

import java.io.File;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndex;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.vector.lucene95.Lucene95Codec;
import org.apache.pinot.segment.local.segment.creator.impl.vector.lucene95.Lucene95HnswVectorsFormat;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is used to create Lucene based text index.
 * Used for both offline from {@link SegmentColumnarIndexCreator}
 * and realtime from {@link RealtimeLuceneTextIndex}
 */
public class HnswVectorIndexCreator implements VectorIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(HnswVectorIndexCreator.class);
  public static final String VECTOR_INDEX_DOC_ID_COLUMN_NAME = "DocID";

  private final Directory _indexDirectory;
  private final IndexWriter _indexWriter;
  private final String _vectorColumn;
  private final VectorSimilarityFunction _vectorSimilarityFunction;
  private final int _vectorDimension;

  private int _nextDocId = 0;

  public HnswVectorIndexCreator(String column, File segmentIndexDir, VectorIndexConfig vectorIndexConfig) {
    _vectorColumn = column;
    _vectorDimension = vectorIndexConfig.getVectorDimension();
    VectorIndexConfig.VectorDistanceFunction vectorDistanceFunction = vectorIndexConfig.getVectorDistanceFunction();
    switch (vectorDistanceFunction) {
      case COSINE:
        _vectorSimilarityFunction = VectorSimilarityFunction.COSINE;
        break;
      case EUCLIDEAN:
        _vectorSimilarityFunction = VectorSimilarityFunction.EUCLIDEAN;
        break;
      case DOT_PRODUCT:
        _vectorSimilarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
        break;
      case INNER_PRODUCT:
        _vectorSimilarityFunction = VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported vector distance function: " + vectorDistanceFunction);
    }
    try {
      // segment generation is always in V1 and later we convert (as part of post creation processing)
      // to V3 if segmentVersion is set to V3 in SegmentGeneratorConfig.
      File indexFile = getV1VectorIndexFile(segmentIndexDir);
      _indexDirectory = FSDirectory.open(indexFile.toPath());
      LOGGER.info("Creating HNSW index for column: {} at path: {} with {} for segment: {}", column,
          indexFile.getAbsolutePath(), vectorIndexConfig.getProperties(), segmentIndexDir.getAbsolutePath());
      _indexWriter = new IndexWriter(_indexDirectory, getIndexWriterConfig(vectorIndexConfig));
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while instantiating the LuceneTextIndexCreator for column: " + column, e);
    }
  }

  private IndexWriterConfig getIndexWriterConfig(VectorIndexConfig vectorIndexConfig) {
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig();

    double maxBufferSizeMB = Double.parseDouble(vectorIndexConfig.getProperties()
        .getOrDefault("maxBufferSizeMB", String.valueOf(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB)));
    boolean commit = Boolean.parseBoolean(vectorIndexConfig.getProperties()
        .getOrDefault("commit", String.valueOf(IndexWriterConfig.DEFAULT_COMMIT_ON_CLOSE)));
    boolean useCompoundFile = Boolean.parseBoolean(vectorIndexConfig.getProperties()
        .getOrDefault("useCompoundFile", String.valueOf(IndexWriterConfig.DEFAULT_USE_COMPOUND_FILE_SYSTEM)));
    indexWriterConfig.setRAMBufferSizeMB(maxBufferSizeMB);
    indexWriterConfig.setCommitOnClose(commit);
    indexWriterConfig.setUseCompoundFile(useCompoundFile);

    int maxCon = Integer.parseInt(vectorIndexConfig.getProperties()
        .getOrDefault("maxCon", String.valueOf(Lucene95HnswVectorsFormat.DEFAULT_MAX_CONN)));
    int beamWidth = Integer.parseInt(vectorIndexConfig.getProperties()
        .getOrDefault("beamWidth", String.valueOf(Lucene95HnswVectorsFormat.DEFAULT_BEAM_WIDTH)));
    int maxDimensions = Integer.parseInt(vectorIndexConfig.getProperties()
        .getOrDefault("maxDimensions", String.valueOf(Lucene95HnswVectorsFormat.DEFAULT_MAX_DIMENSIONS)));

    Lucene95HnswVectorsFormat knnVectorsFormat =
        new Lucene95HnswVectorsFormat(maxCon, beamWidth, maxDimensions);

    Lucene95Codec.Mode mode = Lucene95Codec.Mode.valueOf(vectorIndexConfig.getProperties()
        .getOrDefault("mode", Lucene95Codec.Mode.BEST_SPEED.name()));
    indexWriterConfig.setCodec(new Lucene95Codec(mode, knnVectorsFormat));
    return indexWriterConfig;
  }

  @Override
  public void add(@Nonnull Object value, int dictId)
      throws IOException {
    add((float[]) value);
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds) {
    Object value0 = values[0];
    if (value0 instanceof Float) {
      float[] floatValues = new float[_vectorDimension];
      for (int i = 0; i < values.length; i++) {
        floatValues[i] = (Float) values[i];
      }
      add(floatValues);
    } else if (value0 instanceof Float[]) {
      for (Object value : values) {
        float[] floatValues = new float[_vectorDimension];
        for (int j = 0; j < ((Float[]) value).length; j++) {
          floatValues[j] = ((Float[]) value)[j];
        }
        add(floatValues);
      }
    } else {
      throw new UnsupportedOperationException(
          "Unsupported value class: " + value0.getClass() + " for column: " + _vectorColumn);
    }
  }

  @Override
  public void add(float[] document) {
    // text index on SV column
    Document docToIndex = new Document();
    XKnnFloatVectorField xKnnFloatVectorField =
        new XKnnFloatVectorField(_vectorColumn, document, _vectorSimilarityFunction);
    docToIndex.add(xKnnFloatVectorField);
    docToIndex.add(new StoredField(VECTOR_INDEX_DOC_ID_COLUMN_NAME, _nextDocId++));
    try {
      _indexWriter.addDocument(docToIndex);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while adding a new document to the Lucene index for column: " + _vectorColumn, e);
    }
  }

  @Override
  public void seal() {
    try {
      LOGGER.info("Sealing HNSW index for column: " + _vectorColumn);
      _indexWriter.forceMerge(1);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while sealing the Lucene index for column: " + _vectorColumn, e);
    }
  }

  @Override
  public void close()
      throws IOException {
    try {
      // based on the commit flag set in IndexWriterConfig, this will decide to commit or not
      _indexWriter.close();
      _indexDirectory.close();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while closing the HNSW index for column: " + _vectorColumn, e);
    }
  }

  private File getV1VectorIndexFile(File indexDir) {
    String luceneIndexDirectory = _vectorColumn + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION;
    return new File(indexDir, luceneIndexDirectory);
  }
}
