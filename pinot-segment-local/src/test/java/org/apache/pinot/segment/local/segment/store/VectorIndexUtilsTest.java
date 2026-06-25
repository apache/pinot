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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for {@link VectorIndexUtils}.
 */
public class VectorIndexUtilsTest {
  private static final String COLUMN = "embedding";

  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "vector-index-utils-test-" + System.nanoTime());
    Assert.assertTrue(_tempDir.mkdirs());
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testCleanupVectorIndexRemovesKnownArtifacts()
      throws IOException {
    touch(COLUMN + Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_V99_HNSW_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_HNSW_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_V99_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_V912_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION);

    Assert.assertTrue(VectorIndexUtils.hasVectorIndex(_tempDir, COLUMN));

    VectorIndexUtils.cleanupVectorIndex(_tempDir, COLUMN);

    Assert.assertFalse(VectorIndexUtils.hasVectorIndex(_tempDir, COLUMN));
  }

  @Test
  public void testDetectVectorIndexBackendPrefersIvfPq()
      throws IOException {
    touch(COLUMN + Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION);

    Assert.assertEquals(VectorIndexUtils.detectVectorIndexBackend(_tempDir, COLUMN), VectorBackendType.IVF_PQ);
  }

  @Test
  public void testHasVectorIndexExcludesCombinedForm()
      throws IOException {
    // The combined-form extension is the transient consolidated file meant to be packed into
    // columns.psf by the V2→V3 converter — not preserved as a sibling. hasVectorIndex (used by
    // the converter's "skip standard copy" gate) must return false when only the combined form
    // exists so the bytes get packed instead of dropped.
    touch(COLUMN + Indexes.VECTOR_IVF_FLAT_COMBINED_INDEX_FILE_EXTENSION);
    Assert.assertFalse(VectorIndexUtils.hasVectorIndex(_tempDir, COLUMN));
    Assert.assertTrue(VectorIndexUtils.hasCombinedFormVectorIndex(_tempDir, COLUMN));
  }

  @Test
  public void testHasCombinedFormVectorIndexDetectsBothBackends()
      throws IOException {
    Assert.assertFalse(VectorIndexUtils.hasCombinedFormVectorIndex(_tempDir, COLUMN));
    touch(COLUMN + Indexes.VECTOR_IVF_PQ_COMBINED_INDEX_FILE_EXTENSION);
    Assert.assertTrue(VectorIndexUtils.hasCombinedFormVectorIndex(_tempDir, COLUMN));
  }

  @Test
  public void testGetIndexFileExtensionMatchesBackend() {
    Assert.assertEquals(VectorIndexUtils.getIndexFileExtension(VectorBackendType.HNSW),
        Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    Assert.assertEquals(VectorIndexUtils.getIndexFileExtension(VectorBackendType.IVF_FLAT),
        Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(VectorIndexUtils.getIndexFileExtension(VectorBackendType.IVF_PQ),
        Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION);
  }

  @Test
  public void testGetIndexFileExtensionCombinedReturnsHnswCombined() {
    Assert.assertEquals(VectorIndexUtils.getIndexFileExtension(VectorBackendType.HNSW, /* combined */ true),
        Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
    // Non-combined HNSW must still return the legacy extension.
    Assert.assertEquals(VectorIndexUtils.getIndexFileExtension(VectorBackendType.HNSW, /* combined */ false),
        Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
  }

  @Test
  public void testHasCombinedFormVectorIndexDetectsHnswCombined()
      throws IOException {
    Assert.assertFalse(VectorIndexUtils.hasCombinedFormVectorIndex(_tempDir, COLUMN));
    touch(COLUMN + Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
    Assert.assertTrue(VectorIndexUtils.hasCombinedFormVectorIndex(_tempDir, COLUMN),
        "hasCombinedFormVectorIndex must return true when only HNSW combined file exists");
    // hasVectorIndex must return false — the combined form is transient, not a sidecar.
    Assert.assertFalse(VectorIndexUtils.hasVectorIndex(_tempDir, COLUMN),
        "hasVectorIndex must not report the HNSW combined form as a sidecar");
  }

  @Test
  public void testToSimilarityFunctionSupportsDistanceAliases() {
    Assert.assertEquals(VectorIndexUtils.toSimilarityFunction(VectorIndexConfig.VectorDistanceFunction.COSINE),
        VectorSimilarityFunction.COSINE);
    Assert.assertEquals(VectorIndexUtils.toSimilarityFunction(VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT),
        VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
    Assert.assertEquals(VectorIndexUtils.toSimilarityFunction(VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT),
        VectorSimilarityFunction.DOT_PRODUCT);
    Assert.assertEquals(VectorIndexUtils.toSimilarityFunction(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN),
        VectorSimilarityFunction.EUCLIDEAN);
    Assert.assertEquals(VectorIndexUtils.toSimilarityFunction(VectorIndexConfig.VectorDistanceFunction.L2),
        VectorSimilarityFunction.EUCLIDEAN);
  }

  private void touch(String fileName)
      throws IOException {
    FileUtils.touch(new File(_tempDir, fileName));
  }
}
