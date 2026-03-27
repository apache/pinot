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
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.ColumnarMapIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * End-to-end test for COLUMNAR_MAP index creation, sealing, and reading.
 * Validates the complete lifecycle: creating a COLUMNAR_MAP index with OnHeapColumnarMapIndexCreator,
 * writing to disk, and reading back via ImmutableColumnarMapIndexReader.
 */
public class ColumnarMapIndexEndToEndTest {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ColumnarMapIndexEndToEndTest");
  private static final String COLUMN = "userMetrics";

  private static ComplexFieldSpec buildMapFieldSpec(String columnName) {
    Map<String, FieldSpec> childFieldSpecs = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    return new ComplexFieldSpec(columnName, FieldSpec.DataType.MAP, true, childFieldSpecs);
  }

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

  /**
   * Tests a realistic scenario: multiple users with sparse feature maps.
   * Each user has a subset of possible features, with typed values.
   * Validates full map reconstruction, presence bitmaps, and inverted index lookups.
   */
  @Test
  public void testUserMetricsScenario()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("clicks", FieldSpec.DataType.LONG);
    keyTypes.put("spend", FieldSpec.DataType.DOUBLE);
    keyTypes.put("country", FieldSpec.DataType.STRING);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN);

    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 100);

    // Build test data: 6 documents, sparse keys
    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("clicks", 100L, "spend", 5.50, "country", "US"),  // doc 0: all 3 keys
        Map.of("clicks", 200L, "country", "CA"),                  // doc 1: no spend
        Map.of("spend", 12.75, "country", "UK"),                  // doc 2: no clicks
        Map.of("country", "US"),                                  // doc 3: country only
        new HashMap<>(),                                           // doc 4: empty map
        Map.of("clicks", 100L, "spend", 5.50, "country", "US")   // doc 5: duplicate of doc 0
    };

    File indexFile = new File(INDEX_DIR, COLUMN + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, COLUMN, fieldSpec, config,
            keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }
    assertTrue(indexFile.exists());

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      // Verify known keys exist
      assertTrue(reader.getKeys().contains("clicks"));
      assertTrue(reader.getKeys().contains("spend"));
      assertTrue(reader.getKeys().contains("country"));

      // Verify key types
      assertEquals(reader.getKeyValueType("clicks"), FieldSpec.DataType.LONG);
      assertEquals(reader.getKeyValueType("spend"), FieldSpec.DataType.DOUBLE);
      assertEquals(reader.getKeyValueType("country"), FieldSpec.DataType.STRING);

      // Verify doc counts
      assertEquals(reader.getNumDocsWithKey("clicks"), 3);  // docs 0, 1, 5
      assertEquals(reader.getNumDocsWithKey("spend"), 3);   // docs 0, 2, 5
      assertEquals(reader.getNumDocsWithKey("country"), 5); // docs 0, 1, 2, 3, 5

      // Verify presence bitmaps
      ImmutableRoaringBitmap clicksBitmap = reader.getPresenceBitmap("clicks");
      assertTrue(clicksBitmap.contains(0));
      assertTrue(clicksBitmap.contains(1));
      assertFalse(clicksBitmap.contains(2));
      assertFalse(clicksBitmap.contains(3));
      assertFalse(clicksBitmap.contains(4));
      assertTrue(clicksBitmap.contains(5));

      ImmutableRoaringBitmap spendBitmap = reader.getPresenceBitmap("spend");
      assertTrue(spendBitmap.contains(0));
      assertFalse(spendBitmap.contains(1));
      assertTrue(spendBitmap.contains(2));
      assertFalse(spendBitmap.contains(3));
      assertFalse(spendBitmap.contains(4));
      assertTrue(spendBitmap.contains(5));

      // Verify typed value access
      assertEquals(reader.getLong(0, "clicks"), 100L);
      assertEquals(reader.getLong(1, "clicks"), 200L);
      assertEquals(reader.getLong(5, "clicks"), 100L);

      assertEquals(reader.getDouble(0, "spend"), 5.50, 0.001);
      assertEquals(reader.getDouble(2, "spend"), 12.75, 0.001);

      assertEquals(reader.getString(0, "country"), "US");
      assertEquals(reader.getString(1, "country"), "CA");
      assertEquals(reader.getString(2, "country"), "UK");
      assertEquals(reader.getString(3, "country"), "US");

      // Verify inverted index: docs with country = "US"
      ImmutableRoaringBitmap usDocs = reader.getDocsWithKeyValue("country", "US");
      assertNotNull(usDocs);
      assertTrue(usDocs.contains(0));
      assertFalse(usDocs.contains(1));
      assertFalse(usDocs.contains(2));
      assertTrue(usDocs.contains(3));
      assertFalse(usDocs.contains(4));
      assertTrue(usDocs.contains(5));

      // Verify inverted index: docs with clicks = 100
      ImmutableRoaringBitmap clicks100 = reader.getDocsWithKeyValue("clicks", "100");
      assertNotNull(clicks100);
      assertTrue(clicks100.contains(0));
      assertFalse(clicks100.contains(1)); // has 200 clicks, not 100
      assertTrue(clicks100.contains(5));

      // Non-existent value in inverted index
      assertNull(reader.getDocsWithKeyValue("country", "AU"));

      // Full map reconstruction
      Map<String, Object> doc0Map = reader.getMap(0);
      assertEquals(((Number) doc0Map.get("clicks")).longValue(), 100L);
      assertEquals(((Number) doc0Map.get("spend")).doubleValue(), 5.50, 0.001);
      assertEquals(doc0Map.get("country"), "US");

      Map<String, Object> doc4Map = reader.getMap(4);
      assertTrue(doc4Map.isEmpty(), "Empty document should produce empty map");

      Map<String, Object> doc3Map = reader.getMap(3);
      assertEquals(doc3Map.size(), 1);
      assertEquals(doc3Map.get("country"), "US");
    }
  }

  /**
   * Tests index with default value type fallback for undeclared keys.
   * Validates that undeclared keys are stored as the defaultValueType.
   */
  @Test
  public void testUndeclaredKeyUsesDefaultType()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("score", FieldSpec.DataType.FLOAT);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN);

    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 100);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("score", 9.5f, "tag", "vip"),
        Map.of("score", 7.0f, "tag", "standard", "region", "west")
    };

    File indexFile = new File(INDEX_DIR, COLUMN + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, COLUMN, fieldSpec, config,
            keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      // score is declared as FLOAT
      assertEquals(reader.getKeyValueType("score"), FieldSpec.DataType.FLOAT);
      assertEquals(reader.getFloat(0, "score"), 9.5f, 0.001f);
      assertEquals(reader.getFloat(1, "score"), 7.0f, 0.001f);

      // tag is undeclared, stored as STRING (defaultValueType)
      assertEquals(reader.getKeyValueType("tag"), FieldSpec.DataType.STRING);
      assertEquals(reader.getString(0, "tag"), "vip");
      assertEquals(reader.getString(1, "tag"), "standard");

      // region appears only in doc 1
      assertEquals(reader.getNumDocsWithKey("region"), 1);
      assertEquals(reader.getString(1, "region"), "west");
    }
  }

  /**
   * Tests round-trip from MutableColumnarMapIndex to immutable index file.
   * Creates data via mutable index, then serializes and reads back.
   */
  @Test
  public void testMutableToImmutableRoundTrip()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("value", FieldSpec.DataType.INT);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN);

    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 100);
    org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext context =
        new org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext(
            fieldSpec, -1, false, "testSegment", null, 100, false, 100, 1000, 1, null);

    // Step 1: Add to mutable index
    MutableColumnarMapIndexImpl mutableIdx = new MutableColumnarMapIndexImpl(context, config,
        keyTypes, FieldSpec.DataType.STRING);
    mutableIdx.add(Map.of("value", 10), -1, 0);
    mutableIdx.add(Map.of("value", 20, "label", "alpha"), -1, 1);
    mutableIdx.add(Map.of("label", "beta"), -1, 2);
    mutableIdx.add(Map.of("value", 10), -1, 3);

    // Verify mutable reads
    assertEquals(mutableIdx.getInt(0, "value"), 10);
    assertEquals(mutableIdx.getInt(1, "value"), 20);
    assertEquals(mutableIdx.getInt(3, "value"), 10);
    assertEquals(mutableIdx.getString(1, "label"), "alpha");
    assertEquals(mutableIdx.getString(2, "label"), "beta");

    // Step 2: Persist to immutable index using OnHeapColumnarMapIndexCreator
    File indexFile = new File(INDEX_DIR, COLUMN + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    List<Map<String, Object>> allDocs = List.of(
        Map.of("value", 10),
        Map.of("value", 20, "label", "alpha"),
        Map.of("label", "beta"),
        Map.of("value", 10)
    );

    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, COLUMN, fieldSpec, config,
            keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : allDocs) {
        creator.add(doc);
      }
      creator.seal();
    }

    // Step 3: Read back immutable index and verify matches mutable
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader immutableReader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      assertEquals(immutableReader.getInt(0, "value"), 10);
      assertEquals(immutableReader.getInt(1, "value"), 20);
      assertEquals(immutableReader.getInt(3, "value"), 10);
      assertEquals(immutableReader.getString(1, "label"), "alpha");
      assertEquals(immutableReader.getString(2, "label"), "beta");

      ImmutableRoaringBitmap value10Docs = immutableReader.getDocsWithKeyValue("value", "10");
      assertNotNull(value10Docs);
      assertTrue(value10Docs.contains(0));
      assertFalse(value10Docs.contains(1));
      assertFalse(value10Docs.contains(2));
      assertTrue(value10Docs.contains(3));
    }

    mutableIdx.close();
  }

  @Test
  public void testKeyUnionAcrossSegments() throws Exception {
    // Simulate merging two segments by re-ingesting both sets of docs
    // through a single OnHeapColumnarMapIndexCreator.
    //
    // Segment A docs: keys {price, quantity}
    // Segment B docs: keys {color, quantity}   <- "quantity" overlaps
    // Merged result: keys {price, quantity, color}, 5 docs total

    String colName = "merge_test_col";
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("price", FieldSpec.DataType.INT);
    keyTypes.put("quantity", FieldSpec.DataType.INT);
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 10);
    OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config,
            keyTypes, FieldSpec.DataType.STRING);

    // "Segment A" docs: 0, 1 — have price and quantity
    creator.add(Map.of("price", 10, "quantity", 5));   // doc 0
    creator.add(Map.of("price", 20, "quantity", 8));   // doc 1

    // "Segment B" docs: 2, 3, 4 — have color and quantity
    creator.add(Map.of("color", "red", "quantity", 3));  // doc 2
    creator.add(Map.of("color", "blue", "quantity", 7));  // doc 3
    creator.add(Map.of("color", "green", "quantity", 2));  // doc 4

    creator.seal();
    creator.close();

    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    // Union of keys across both "segments"
    assertEquals(reader.getKeys().size(), 3);
    assertTrue(reader.getKeys().contains("price"));
    assertTrue(reader.getKeys().contains("quantity"));
    assertTrue(reader.getKeys().contains("color"));

    // "price" present only in docs 0 and 1 (segment A); absent from segment B docs
    assertTrue(reader.getPresenceBitmap("price").contains(0));
    assertTrue(reader.getPresenceBitmap("price").contains(1));
    assertFalse(reader.getPresenceBitmap("price").contains(2));
    assertFalse(reader.getPresenceBitmap("price").contains(3));
    assertFalse(reader.getPresenceBitmap("price").contains(4));
    assertEquals(reader.getPresenceBitmap("price").getCardinality(), 2);

    // "color" present only in docs 2, 3, 4 (segment B); absent from segment A docs
    assertFalse(reader.getPresenceBitmap("color").contains(0));
    assertFalse(reader.getPresenceBitmap("color").contains(1));
    assertTrue(reader.getPresenceBitmap("color").contains(2));
    assertTrue(reader.getPresenceBitmap("color").contains(3));
    assertTrue(reader.getPresenceBitmap("color").contains(4));
    assertEquals(reader.getPresenceBitmap("color").getCardinality(), 3);

    // "quantity" present in all 5 docs (both segments)
    assertTrue(reader.getPresenceBitmap("quantity").contains(0));
    assertTrue(reader.getPresenceBitmap("quantity").contains(4));
    assertEquals(reader.getPresenceBitmap("quantity").getCardinality(), 5);

    // Spot-check values to confirm no corruption during merge re-ingestion
    assertEquals(reader.getInt(0, "price"), 10);
    assertEquals(reader.getInt(1, "price"), 20);
    assertEquals(reader.getString(2, "color"), "red");
    assertEquals(reader.getString(4, "color"), "green");

    buf.close();
  }
}
