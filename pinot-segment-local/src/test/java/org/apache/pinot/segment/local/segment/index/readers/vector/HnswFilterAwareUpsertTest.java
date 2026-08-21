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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.vector.HnswVectorIndexCreator;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/// Tests that the immutable HNSW reader's filtered search ([HnswVectorIndexReader#getDocIds(float[], int,
/// ImmutableRoaringBitmap)]) excludes disallowed documents from candidate generation -- the property the
/// FULL-upsert integration relies on: upsert-obsoleted rows that are physically nearest to the query must
/// not consume top-K candidate slots when the doc-ids snapshot excludes them.
///
/// The corpus uses deterministic 2-D vectors with distinct distances (no ties), so identity assertions on
/// the returned doc ids are stable.
public class HnswFilterAwareUpsertTest {
  private static final String COLUMN_NAME = "embedding";
  // Distances (squared L2) from the query vector {1, 0}:
  //   doc 0: 0.0     (obsolete)
  //   doc 1: 0.0002  (obsolete)
  //   doc 2: 0.5     (valid)
  //   doc 3: 2.0     (valid)
  //   doc 4: 4.0     (valid)
  private static final float[][] VECTORS = {
      {1.0f, 0.0f},
      {0.99f, 0.01f},
      {0.5f, 0.5f},
      {0.0f, 1.0f},
      {-1.0f, 0.0f},
  };
  private static final float[] QUERY_VECTOR = {1.0f, 0.0f};

  private File _indexDir;
  private VectorIndexConfig _config;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _indexDir = new File(FileUtils.getTempDirectory(), "hnsw_filter_aware_upsert_test_" + System.nanoTime());
    FileUtils.forceMkdir(_indexDir);

    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "2");
    _config = new VectorIndexConfig(properties);

    try (HnswVectorIndexCreator creator = new HnswVectorIndexCreator(COLUMN_NAME, _indexDir, _config)) {
      for (float[] vector : VECTORS) {
        creator.add(vector);
      }
      creator.seal();
    }
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_indexDir);
  }

  @Test
  public void testFilteredSearchExcludesNearestDisallowedDocs()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader(COLUMN_NAME, _indexDir, VECTORS.length, _config)) {
      // Sanity: unfiltered top-2 returns the obsolete docs, proving they are physically nearest
      ImmutableRoaringBitmap unfiltered = reader.getDocIds(QUERY_VECTOR, 2);
      Assert.assertEquals(unfiltered, ImmutableRoaringBitmap.bitmapOf(0, 1));

      // Filtered top-2 restricted to the valid docs must return the two nearest valid docs
      ImmutableRoaringBitmap allowed = ImmutableRoaringBitmap.bitmapOf(2, 3, 4);
      ImmutableRoaringBitmap filtered = reader.getDocIds(QUERY_VECTOR, 2, allowed);
      Assert.assertEquals(filtered, ImmutableRoaringBitmap.bitmapOf(2, 3),
          "Filtered search must return K allowed docs, not obsolete physical neighbors");
    }
  }

  @Test
  public void testFilteredSearchReturnsAllAllowedWhenFewerThanTopK()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader(COLUMN_NAME, _indexDir, VECTORS.length, _config)) {
      ImmutableRoaringBitmap allowed = ImmutableRoaringBitmap.bitmapOf(3, 4);
      ImmutableRoaringBitmap filtered = reader.getDocIds(QUERY_VECTOR, 10, allowed);
      Assert.assertEquals(filtered, ImmutableRoaringBitmap.bitmapOf(3, 4));
    }
  }

  @Test
  public void testFilteredSearchWithEmptyBitmapReturnsEmpty()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader(COLUMN_NAME, _indexDir, VECTORS.length, _config)) {
      ImmutableRoaringBitmap filtered = reader.getDocIds(QUERY_VECTOR, 2, new MutableRoaringBitmap());
      Assert.assertEquals(filtered.getCardinality(), 0);
    }
  }
}
