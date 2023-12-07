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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.segment.local.segment.store.VectorIndexUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is used to create Lucene based HNSW index.
 * Used for both offline from {@link SegmentColumnarIndexCreator}
 * and realtime from {@link org.apache.pinot.segment.local.realtime.impl.vector.MutableVectorIndex}
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
    _vectorSimilarityFunction = VectorIndexUtils.toSimilarityFunction(vectorIndexConfig.getVectorDistanceFunction());
    try {
      // segment generation is always in V1 and later we convert (as part of post creation processing)
      // to V3 if segmentVersion is set to V3 in SegmentGeneratorConfig.
      File indexFile = new File(segmentIndexDir, _vectorColumn + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);
      _indexDirectory = FSDirectory.open(indexFile.toPath());
      LOGGER.info("Creating HNSW index for column: {} at path: {} with {} for segment: {}", column,
          indexFile.getAbsolutePath(), vectorIndexConfig.getProperties(), segmentIndexDir.getAbsolutePath());
      _indexWriter = new IndexWriter(_indexDirectory, VectorIndexUtils.getIndexWriterConfig(vectorIndexConfig));
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while instantiating the HnswVectorIndexCreator for column: " + column, e);
    }
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds) {
    float[] floatValues = new float[_vectorDimension];
    for (int i = 0; i < values.length; i++) {
      floatValues[i] = (Float) values[i];
    }
    add(floatValues);
  }

  @Override
  public void add(float[] document) {
    Document docToIndex = new Document();
    XKnnFloatVectorField xKnnFloatVectorField =
        new XKnnFloatVectorField(_vectorColumn, document, _vectorSimilarityFunction);
    docToIndex.add(xKnnFloatVectorField);
    docToIndex.add(new StoredField(VECTOR_INDEX_DOC_ID_COLUMN_NAME, _nextDocId++));
    try {
      _indexWriter.addDocument(docToIndex);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while adding a new document to the HNSW index for column: " + _vectorColumn, e);
    }
  }

  @Override
  public void seal() {
    try {
      LOGGER.info("Sealing HNSW index for column: " + _vectorColumn);
      _indexWriter.forceMerge(1);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while sealing the HNSW index for column: " + _vectorColumn, e);
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
}
