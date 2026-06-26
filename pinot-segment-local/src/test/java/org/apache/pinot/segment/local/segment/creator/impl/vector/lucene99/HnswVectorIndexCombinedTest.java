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
package org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.pinot.segment.local.segment.creator.impl.vector.HnswVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.vector.HnswVectorIndexBufferReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Round-trip test: creates a small HNSW index via {@link HnswVectorIndexCreator} with
 * {@code storeInSegmentFile=true}, verifies the combined file is produced, mmaps it, opens a
 * {@link Directory} via {@link HnswVectorIndexBufferReader}, and runs a {@link KnnFloatVectorQuery}.
 */
public class HnswVectorIndexCombinedTest {

  private static final String COLUMN = "vec";
  private static final int DIMENSION = 4;
  private static final int NUM_DOCS = 8;

  private File _segmentDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _segmentDir = new File(FileUtils.getTempDirectory(),
        "hnsw-combined-test-" + System.nanoTime());
    FileUtils.forceMkdir(_segmentDir);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_segmentDir);
  }

  @Test
  public void testCombineAndRead()
      throws Exception {
    // Build the index with storeInSegmentFile=true
    VectorIndexConfig config = buildConfig(true);
    float[][] vectors = buildVectors(NUM_DOCS, DIMENSION);
    buildIndex(vectors, config);

    // The combined file must exist after close(); the Lucene directory must be gone
    File combinedFile = new File(_segmentDir,
        COLUMN + V1Constants.Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
    Assert.assertTrue(combinedFile.exists(), "Combined file must be present after creator.close()");
    File hnswDir = new File(_segmentDir,
        COLUMN + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    Assert.assertFalse(hnswDir.exists(), "Lucene directory must be cleaned up after packing");

    // Mmap the combined file and open via the buffer reader
    PinotDataBuffer buffer = PinotDataBuffer.mapFile(combinedFile, /* readOnly */ true, 0, combinedFile.length(),
        ByteOrder.LITTLE_ENDIAN, "hnsw-combined-test");
    try {
      Directory dir = HnswVectorIndexBufferReader.createLuceneDirectory(buffer, COLUMN);
      DirectoryReader indexReader = DirectoryReader.open(dir);
      try {
        Assert.assertEquals(indexReader.numDocs(), NUM_DOCS);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        // Query with the first vector; we expect at least one result
        KnnFloatVectorQuery query = new KnnFloatVectorQuery(COLUMN, vectors[0], 3);
        TopDocs results = searcher.search(query, 3);
        Assert.assertTrue(results.totalHits.value > 0,
            "KNN query on buffer-backed directory must return at least one result");
      } finally {
        indexReader.close();
        dir.close();
      }
    } finally {
      buffer.close();
    }
  }

  @Test
  public void testCombineAndExtractRoundTrip()
      throws Exception {
    VectorIndexConfig config = buildConfig(false);
    float[][] vectors = buildVectors(NUM_DOCS, DIMENSION);
    buildIndex(vectors, config);

    // The Lucene directory must exist; no combined file
    File hnswDir = new File(_segmentDir,
        COLUMN + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    Assert.assertTrue(hnswDir.exists() && hnswDir.isDirectory(),
        "Lucene directory must be present when storeInSegmentFile=false");
    File combinedFile = new File(_segmentDir,
        COLUMN + V1Constants.Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
    Assert.assertFalse(combinedFile.exists(),
        "Combined file must not be present when storeInSegmentFile=false");

    // Pack manually and unpack into a new directory
    HnswVectorIndexCombined.combineHnswIndexFiles(hnswDir, combinedFile.getAbsolutePath(), null, null);
    Assert.assertTrue(combinedFile.exists() && combinedFile.length() > 0, "Combined file must be non-empty");

    File extractDir = new File(_segmentDir, "extracted-" + COLUMN);
    HnswVectorIndexCombined.extractHnswIndexFiles(combinedFile, extractDir);
    Assert.assertTrue(extractDir.isDirectory(), "Extract directory must be created");

    // Re-open from the extracted directory and verify it's a valid Lucene index
    org.apache.lucene.store.FSDirectory fsDir = org.apache.lucene.store.FSDirectory.open(extractDir.toPath());
    DirectoryReader reader = DirectoryReader.open(fsDir);
    try {
      Assert.assertEquals(reader.numDocs(), NUM_DOCS);
    } finally {
      reader.close();
      fsDir.close();
    }
  }

  @Test
  public void testExtractDocIdMappingBufferAbsentWhenNotPacked()
      throws Exception {
    VectorIndexConfig config = buildConfig(false);
    buildIndex(buildVectors(NUM_DOCS, DIMENSION), config);

    File hnswDir = new File(_segmentDir, COLUMN + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    File combinedFile = new File(_segmentDir, COLUMN + V1Constants.Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
    HnswVectorIndexCombined.combineHnswIndexFiles(hnswDir, combinedFile.getAbsolutePath(), null, null);

    PinotDataBuffer buffer = PinotDataBuffer.mapFile(combinedFile, /* readOnly */ true, 0, combinedFile.length(),
        ByteOrder.LITTLE_ENDIAN, "hnsw-combined-mapping-test");
    try {
      // No mapping was packed (segmentIndexDir=null in combineHnswIndexFiles above)
      PinotDataBuffer mappingBuffer = HnswVectorIndexBufferReader.extractDocIdMappingBuffer(buffer, COLUMN);
      Assert.assertNull(mappingBuffer, "Mapping buffer must be null when no mapping was packed");
    } finally {
      buffer.close();
    }
  }

  /// Regression for the consolidated-HNSW docId-mapping byte order. The mapping is packed verbatim
  /// from the little-endian file-backed sidecar, but the enclosing {@code columns.psf} buffer is
  /// big-endian. {@code extractDocIdMappingBuffer} must hand back a LITTLE_ENDIAN view so
  /// {@code DocIdTranslator} reads the Lucene→Pinot doc ids unswapped.
  @Test
  public void testExtractDocIdMappingBufferIsLittleEndian()
      throws Exception {
    buildIndex(buildVectors(NUM_DOCS, DIMENSION), buildConfig(false));
    File hnswDir = new File(_segmentDir, COLUMN + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);

    // Write a docId mapping file (Lucene doc id -> Pinot doc id) little-endian, the order the
    // file-backed DocIdTranslator uses.
    int[] expected = {5, 3, 7, 1, 6, 2, 4, 0};
    File mappingFile =
        new File(_segmentDir, COLUMN + V1Constants.Indexes.VECTOR_HNSW_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    ByteBuffer bb = ByteBuffer.allocate(expected.length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int v : expected) {
      bb.putInt(v);
    }
    FileUtils.writeByteArrayToFile(mappingFile, bb.array());

    // Pack including the mapping file.
    File combinedFile = new File(_segmentDir, COLUMN + V1Constants.Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
    HnswVectorIndexCombined.combineHnswIndexFiles(hnswDir, combinedFile.getAbsolutePath(), _segmentDir, COLUMN);

    // Mmap the combined file BIG_ENDIAN to mirror the columns.psf buffer order. Without the
    // little-endian view in extractDocIdMappingBuffer, getInt() would byte-swap every doc id.
    PinotDataBuffer buffer = PinotDataBuffer.mapFile(combinedFile, /* readOnly */ true, 0, combinedFile.length(),
        ByteOrder.BIG_ENDIAN, "hnsw-combined-endianness-test");
    try {
      PinotDataBuffer mappingBuffer = HnswVectorIndexBufferReader.extractDocIdMappingBuffer(buffer, COLUMN);
      Assert.assertNotNull(mappingBuffer, "Mapping buffer must be present when packed");
      for (int i = 0; i < expected.length; i++) {
        Assert.assertEquals(mappingBuffer.getInt(i * Integer.BYTES), expected[i],
            "docId mapping must read little-endian regardless of the enclosing buffer order");
      }
    } finally {
      buffer.close();
    }
  }

  // ---- helpers ----

  private VectorIndexConfig buildConfig(boolean storeInSegmentFile) {
    Map<String, String> props = new java.util.HashMap<>();
    props.put(VectorIndexConfig.STORE_IN_SEGMENT_FILE, String.valueOf(storeInSegmentFile));
    // Commit must be true for files to land; useCompoundFile=false avoids single-file mode.
    props.put("commit", "true");
    props.put("useCompoundFile", "false");
    return new VectorIndexConfig(false, null, DIMENSION, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, props);
  }

  private static float[][] buildVectors(int numDocs, int dimension) {
    float[][] vectors = new float[numDocs][dimension];
    for (int i = 0; i < numDocs; i++) {
      for (int j = 0; j < dimension; j++) {
        vectors[i][j] = (i * dimension + j) * 0.1f;
      }
    }
    return vectors;
  }

  private void buildIndex(float[][] vectors, VectorIndexConfig config)
      throws IOException {
    try (HnswVectorIndexCreator creator = new HnswVectorIndexCreator(COLUMN, _segmentDir, config)) {
      for (float[] v : vectors) {
        creator.add(v);
      }
      creator.seal();
    }
  }
}
