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
package org.apache.pinot.segment.local.segment.index.columnarmap;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.ColumnarMapIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Verifies that sparse-tier per-doc JSON blobs round-trip through the SPMX file
 * without using a separate .columnarmap.sparse sidecar.
 */
public class ColumnarMapSparseRoundTripTest {

  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "ColumnarMapSparseRoundTripTest");
  private static final String COLUMN_NAME = "sparseCol";

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  private static ComplexFieldSpec buildMapFieldSpec(String columnName) {
    Map<String, FieldSpec> childFieldSpecs = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    return new ComplexFieldSpec(columnName, FieldSpec.DataType.MAP, true, childFieldSpecs);
  }

  private File createIndex(ComplexFieldSpec fieldSpec, ColumnarMapIndexConfig config,
      Map<String, FieldSpec.DataType> keyTypes, Map<String, Object>[] docs)
      throws IOException {
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, COLUMN_NAME, fieldSpec, config,
            keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    assertTrue(indexFile.exists(), "Index file should exist after sealing");
    return indexFile;
  }

  @Test
  public void testSparseDataPackedInSPMX()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("color", "red", "rare_flag", "true", "debug", "x"),  // doc 0
        Map.of("color", "blue"),                                      // doc 1
        Map.of("rare_flag", "false"),                                 // doc 2
        Map.of("color", "red"),                                       // doc 3
    };

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    // denseKeyThreshold=1.0 disables auto-detection; only "color" is dense via denseKeys
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, null, 1000,
        Set.of("color"), 1.0);
    File indexFile = createIndex(fieldSpec, config, keyTypes, docs);

    // Sparse data must be inside SPMX, not a separate sidecar file
    File sidecarFile = new File(INDEX_DIR, COLUMN_NAME + ".columnarmap.sparse");
    assertFalse(sidecarFile.exists(), "Sparse data must be inside SPMX, not a separate sidecar file");

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      assertEquals(reader.getTierFlag("color"), ImmutableColumnarMapIndexReader.TIER_DENSE);
      assertEquals(reader.getTierFlag("rare_flag"), ImmutableColumnarMapIndexReader.TIER_SPARSE);
      assertEquals(reader.getTierFlag("debug"), ImmutableColumnarMapIndexReader.TIER_SPARSE);

      // Dense key still works
      assertEquals(reader.getString(0, "color"), "red");
      assertEquals(reader.getString(1, "color"), "blue");

      // Sparse key values round-trip via getSparseJsonBlob
      String doc0Blob = reader.getSparseJsonBlob(0);
      assertNotNull(doc0Blob, "doc 0 has sparse keys");
      assertTrue(doc0Blob.contains("\"rare_flag\":\"true\""), "got: " + doc0Blob);
      assertTrue(doc0Blob.contains("\"debug\":\"x\""), "got: " + doc0Blob);

      assertNull(reader.getSparseJsonBlob(1), "doc 1 has no sparse keys");

      String doc2Blob = reader.getSparseJsonBlob(2);
      assertNotNull(doc2Blob, "doc 2 has sparse keys");
      assertTrue(doc2Blob.contains("\"rare_flag\":\"false\""), "got: " + doc2Blob);

      assertNull(reader.getSparseJsonBlob(3), "doc 3 has no sparse keys");
    }
  }

  @Test
  public void testNoSparseSidecarWhenAllKeysDense()
      throws IOException {
    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("a", "1"),
        Map.of("a", "2"),
    };

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    // denseKeyThreshold=0.0 makes all keys dense
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, null, 1000,
        null, 0.0);
    File indexFile = createIndex(fieldSpec, config, null, docs);

    File sidecarFile = new File(INDEX_DIR, COLUMN_NAME + ".columnarmap.sparse");
    assertFalse(sidecarFile.exists(), "No sparse sidecar — never created in any case");

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {
      assertNull(reader.getSparseJsonBlob(0), "no sparse data when all keys dense");
      assertNull(reader.getSparseJsonBlob(1), "no sparse data when all keys dense");
    }
  }
}
