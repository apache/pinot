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

  private int _nextDocId = 0;

  public HnswVectorIndexCreator(String column, File segmentIndexDir, boolean commit, boolean useCompoundFile,
      int maxBufferSizeMB, VectorIndexConfig vectorIndexConfig) {
    _vectorColumn = column;
    try {
      // segment generation is always in V1 and later we convert (as part of post creation processing)
      // to V3 if segmentVersion is set to V3 in SegmentGeneratorConfig.
      File indexFile = getV1VectorIndexFile(segmentIndexDir);
      _indexDirectory = FSDirectory.open(indexFile.toPath());
      LOGGER.info("Creating HNSW index for column: " + column + " at path: " + indexFile.getAbsolutePath());

      IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
      indexWriterConfig.setRAMBufferSizeMB(maxBufferSizeMB);
      indexWriterConfig.setCommitOnClose(commit);
      indexWriterConfig.setUseCompoundFile(useCompoundFile);
      indexWriterConfig.setCodec(new Lucene95Codec());

      _indexWriter = new IndexWriter(_indexDirectory, indexWriterConfig);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while instantiating the LuceneTextIndexCreator for column: " + column, e);
    }
  }

  public IndexWriter getIndexWriter() {
    return _indexWriter;
  }

  @Override
  public void add(@Nonnull Object value, int dictId)
      throws IOException {
    add((float[]) value);
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds) {
    if (values instanceof Float[]) {
      float[] floatValues = new float[values.length];
      for (int i = 0; i < values.length; i++) {
        floatValues[i] = (Float) values[i];
      }
      add(floatValues);
    } else if (values instanceof Float[][]) {
      add((Float[][]) values, values.length);
    } else if (values instanceof float[][]) {
      add((float[][]) values, values.length);
    } else {
      float[][] vectors = new float[values.length][];
      for (int i = 0; i < values.length; i++) {
        vectors[i] = (float[]) values[i];
      }
      add(vectors, values.length);
    }
  }

  @Override
  public void add(float[] document) {
    // text index on SV column
    Document docToIndex = new Document();
    docToIndex.add(new XKnnFloatVectorField(_vectorColumn, document, VectorSimilarityFunction.COSINE));
    docToIndex.add(new StoredField(VECTOR_INDEX_DOC_ID_COLUMN_NAME, _nextDocId++));
    try {
      _indexWriter.addDocument(docToIndex);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while adding a new document to the Lucene index for column: " + _vectorColumn, e);
    }
  }

  public void add(float[][] documents, int length) {
    Document docToIndex = new Document();

    // Whenever multiple fields with the same name appear in one document, both the
    // inverted index and term vectors will logically append the tokens of the
    // field to one another, in the order the fields were added.
    for (int i = 0; i < length; i++) {
      docToIndex.add(new XKnnFloatVectorField(_vectorColumn, documents[i], VectorSimilarityFunction.COSINE));
    }
    docToIndex.add(new StoredField(VECTOR_INDEX_DOC_ID_COLUMN_NAME, _nextDocId++));

    try {
      _indexWriter.addDocument(docToIndex);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while adding a new document to the Lucene index for column: " + _vectorColumn, e);
    }
  }


  public void add(Float[][] documents, int length) {
    Document docToIndex = new Document();

    // Whenever multiple fields with the same name appear in one document, both the
    // inverted index and term vectors will logically append the tokens of the
    // field to one another, in the order the fields were added.
    for (int i = 0; i < length; i++) {
      float[] document = new float[documents[i].length];
      for (int j = 0; j < documents[i].length; j++) {
        document[j] = documents[i][j];
      }
      docToIndex.add(new XKnnFloatVectorField(_vectorColumn, document, VectorSimilarityFunction.COSINE));
    }
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
