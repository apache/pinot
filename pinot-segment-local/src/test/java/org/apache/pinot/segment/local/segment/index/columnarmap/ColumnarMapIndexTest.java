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
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
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
 * Unit tests for {@link OnHeapColumnarMapIndexCreator} and {@link ImmutableColumnarMapIndexReader}.
 */
public class ColumnarMapIndexTest {

  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "ColumnarMapIndexTest");
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

  // ---- Helper to build a ComplexFieldSpec for MAP ----

  private static ComplexFieldSpec buildMapFieldSpec(String columnName) {
    Map<String, FieldSpec> childFieldSpecs = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    return new ComplexFieldSpec(columnName, FieldSpec.DataType.MAP, true, childFieldSpecs);
  }

  // ---- Helper to create index file ----

  private File createIndex(Map<String, FieldSpec.DataType> keyTypes, Map<String, Object>[] docs)
      throws IOException {
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 1000);
    return createIndex(fieldSpec, config, keyTypes, docs);
  }

  private File createIndex(ComplexFieldSpec fieldSpec, ColumnarMapIndexConfig config, Map<String, Object>[] docs)
      throws IOException {
    return createIndex(fieldSpec, config, null, docs);
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
  public void testBasicIntAndStringKeys()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("age", FieldSpec.DataType.INT);
    keyTypes.put("name", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("age", 25, "name", "alice"),
        Map.of("age", 30),
        Map.of("name", "charlie"),
        Map.of("age", 25, "name", "dave")
    };

    File indexFile = createIndex(keyTypes, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      // Verify keys
      assertTrue(reader.getKeys().contains("age"));
      assertTrue(reader.getKeys().contains("name"));

      // Verify key types
      assertEquals(reader.getKeyValueType("age"), FieldSpec.DataType.INT);
      assertEquals(reader.getKeyValueType("name"), FieldSpec.DataType.STRING);

      // Verify presence bitmaps
      ImmutableRoaringBitmap ageBitmap = reader.getPresenceBitmap("age");
      assertTrue(ageBitmap.contains(0));
      assertTrue(ageBitmap.contains(1));
      assertFalse(ageBitmap.contains(2));
      assertTrue(ageBitmap.contains(3));

      ImmutableRoaringBitmap nameBitmap = reader.getPresenceBitmap("name");
      assertTrue(nameBitmap.contains(0));
      assertFalse(nameBitmap.contains(1));
      assertTrue(nameBitmap.contains(2));
      assertTrue(nameBitmap.contains(3));

      // Verify typed values
      assertEquals(reader.getInt(0, "age"), 25);
      assertEquals(reader.getInt(1, "age"), 30);
      assertEquals(reader.getInt(3, "age"), 25);

      assertEquals(reader.getString(0, "name"), "alice");
      assertEquals(reader.getString(2, "name"), "charlie");
      assertEquals(reader.getString(3, "name"), "dave");

      // Verify doc counts
      assertEquals(reader.getNumDocsWithKey("age"), 3);
      assertEquals(reader.getNumDocsWithKey("name"), 3);
    }
  }

  @Test
  public void testGetMap()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("score", FieldSpec.DataType.DOUBLE);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("score", 1.5, "tag", "a"),
        Map.of("score", 2.0),
        new HashMap<>()
    };

    File indexFile = createIndex(keyTypes, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      Map<String, Object> doc0 = reader.getMap(0);
      assertTrue(doc0.containsKey("score"));
      assertEquals((double) doc0.get("score"), 1.5, 0.001);
      assertTrue(doc0.containsKey("tag")); // stored as STRING (default)
      assertEquals(doc0.get("tag"), "a");

      Map<String, Object> doc1 = reader.getMap(1);
      assertTrue(doc1.containsKey("score"));
      assertEquals((double) doc1.get("score"), 2.0, 0.001);
      assertFalse(doc1.containsKey("tag"));

      Map<String, Object> doc2 = reader.getMap(2);
      assertTrue(doc2.isEmpty());
    }
  }

  @Test
  public void testAbsentKeyReturnsEmptyPresenceBitmap()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("x", FieldSpec.DataType.INT);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{Map.of("x", 1)};

    File indexFile = createIndex(keyTypes, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      ImmutableRoaringBitmap missingBitmap = reader.getPresenceBitmap("nonexistent");
      assertTrue(missingBitmap.isEmpty());

      assertNull(reader.getKeyValueType("nonexistent"));
      assertEquals(reader.getNumDocsWithKey("nonexistent"), 0);
    }
  }

  @Test
  public void testWithInvertedIndex()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("color", "red"),
        Map.of("color", "blue"),
        Map.of("color", "red"),
        Map.of("color", "green")
    };

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      ImmutableRoaringBitmap reds = reader.getDocsWithKeyValue("color", "red");
      assertNotNull(reds);
      assertTrue(reds.contains(0));
      assertFalse(reds.contains(1));
      assertTrue(reds.contains(2));
      assertFalse(reds.contains(3));

      ImmutableRoaringBitmap blues = reader.getDocsWithKeyValue("color", "blue");
      assertNotNull(blues);
      assertFalse(blues.contains(0));
      assertTrue(blues.contains(1));

      ImmutableRoaringBitmap nullResult = reader.getDocsWithKeyValue("color", "purple");
      assertNull(nullResult);
    }
  }

  @Test
  public void testMutableIndexBasicOperations()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("price", FieldSpec.DataType.FLOAT);
    keyTypes.put("brand", FieldSpec.DataType.STRING);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 100);

    try (MutableColumnarMapIndexImpl mutableIndex = new MutableColumnarMapIndexImpl(
        buildMutableContext(fieldSpec), config, keyTypes, FieldSpec.DataType.STRING)) {

      mutableIndex.add(Map.of("price", 9.99f, "brand", "acme"), -1, 0);
      mutableIndex.add(Map.of("price", 14.99f), -1, 1);
      mutableIndex.add(Map.of("brand", "foo"), -1, 2);
      mutableIndex.add(Map.of("price", 9.99f, "brand", "acme"), -1, 3);

      // Test presence bitmaps
      ImmutableRoaringBitmap priceBitmap = mutableIndex.getPresenceBitmap("price");
      assertTrue(priceBitmap.contains(0));
      assertTrue(priceBitmap.contains(1));
      assertFalse(priceBitmap.contains(2));
      assertTrue(priceBitmap.contains(3));

      ImmutableRoaringBitmap brandBitmap = mutableIndex.getPresenceBitmap("brand");
      assertTrue(brandBitmap.contains(0));
      assertFalse(brandBitmap.contains(1));
      assertTrue(brandBitmap.contains(2));
      assertTrue(brandBitmap.contains(3));

      // Test typed reads
      assertEquals(mutableIndex.getFloat(0, "price"), 9.99f, 0.001f);
      assertEquals(mutableIndex.getFloat(1, "price"), 14.99f, 0.001f);
      assertEquals(mutableIndex.getString(0, "brand"), "acme");
      assertEquals(mutableIndex.getString(2, "brand"), "foo");

      // Test getString on numeric key — must not throw ClassCastException
      assertEquals(mutableIndex.getString(0, "price"), "9.99");
      assertEquals(mutableIndex.getString(1, "price"), "14.99");

      // Test getMap
      Map<String, Object> doc0 = mutableIndex.getMap(0);
      assertEquals(doc0.get("brand"), "acme");

      // Test getNumDocsWithKey
      assertEquals(mutableIndex.getNumDocsWithKey("price"), 3);
      assertEquals(mutableIndex.getNumDocsWithKey("brand"), 3);
      assertEquals(mutableIndex.getNumDocsWithKey("nonexistent"), 0);
    }
  }

  @Test
  public void testMaxKeysDropsExcessKeysImmutable()
      throws Exception {
    String colName = "maxkeys_imm_test";
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 2); // maxKeys=2
    OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config);

    // Add a doc with 3 keys — only 2 should be stored
    Map<String, Object> doc = new HashMap<>();
    doc.put("alpha", 1);
    doc.put("beta", 2);
    doc.put("gamma", 3);
    creator.add(doc);
    creator.seal();
    creator.close();

    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    assertEquals(2, reader.getKeys().size());
    buf.close();
  }

  @Test
  public void testMaxKeysDropsExcessKeysMutable()
      throws Exception {
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 2); // maxKeys=2
    ComplexFieldSpec fieldSpec = buildMapFieldSpec("mutable_maxkeys_test");

    try (MutableColumnarMapIndexImpl mutableIndex = new MutableColumnarMapIndexImpl(
        buildMutableContext(fieldSpec), config)) {

      // Add doc 0 with 3 keys — only 2 should be stored
      Map<String, Object> doc = new HashMap<>();
      doc.put("alpha", 1);
      doc.put("beta", 2);
      doc.put("gamma", 3);
      mutableIndex.add(doc, -1, 0);

      assertEquals(2, mutableIndex.getKeys().size());
    }
  }

  @Test
  public void testNullValueTreatedAsAbsentImmutable()
      throws Exception {
    String colName = "null_absent_imm_test";
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 10);
    OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config);

    Map<String, Object> doc = new HashMap<>();
    doc.put("key", null);   // explicit null — must be treated as absent
    creator.add(doc);
    creator.seal();
    creator.close();

    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    // Key must not appear in the index at all
    assertFalse(reader.getPresenceBitmap("key").contains(0));
    assertEquals(0, reader.getKeys().size());
    buf.close();
  }

  @Test
  public void testNullValueTreatedAsAbsentMutable()
      throws Exception {
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 10);
    ComplexFieldSpec fieldSpec = buildMapFieldSpec("mutable_null_absent_test");

    try (MutableColumnarMapIndexImpl mutableIndex = new MutableColumnarMapIndexImpl(
        buildMutableContext(fieldSpec), config)) {

      // add doc 0 with Map {"key": null}
      Map<String, Object> doc = new HashMap<>();
      doc.put("key", null);
      mutableIndex.add(doc, -1, 0);

      // key must not appear in the presence bitmap or key set
      assertFalse(mutableIndex.getPresenceBitmap("key").contains(0));
      assertFalse(mutableIndex.getKeys().contains("key"));
    }
  }

  private static org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext buildMutableContext(
      FieldSpec fieldSpec) {
    return new org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext(
        fieldSpec, -1, false, "testSegment", null, 1000, false, 100, 1000, 1, null);
  }

  @Test
  public void testInvalidMagicBytesThrows()
      throws Exception {
    // Build a minimal valid index
    String colName = "magic_test";
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 10);
    OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config);
    creator.add(Map.of("k", 42));
    creator.seal();
    creator.close();

    // Corrupt magic bytes
    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    try (RandomAccessFile raf = new RandomAccessFile(indexFile, "rw")) {
      raf.seek(0);
      raf.writeInt(0xDEADBEEF);
    }

    // Verify reader throws
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    assertThrows(IOException.class, () -> new ImmutableColumnarMapIndexReader(buf, null));
    buf.close();
  }

  @Test
  public void testConcurrentAddAndReadDoesNotThrow()
      throws Exception {
    ComplexFieldSpec fieldSpec = buildMapFieldSpec("concurrent_test");
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 100);
    MutableColumnarMapIndexImpl idx;
    try (MutableColumnarMapIndexImpl tmp = new MutableColumnarMapIndexImpl(buildMutableContext(fieldSpec), config)) {
      idx = tmp;
      // Note: MutableColumnarMapIndexImpl.close() is a no-op, so this is safe

      ExecutorService pool = Executors.newFixedThreadPool(4);
      AtomicBoolean failed = new AtomicBoolean(false);

      // 2 writer threads
      for (int t = 0; t < 2; t++) {
        final int thread = t;
        pool.submit(() -> {
          for (int i = 0; i < 200; i++) {
            try {
              idx.add(Map.of("k" + (thread * 200 + i), i), -1, thread * 200 + i);
            } catch (Exception e) {
              failed.set(true);
            }
          }
        });
      }

      // 2 reader threads
      for (int t = 0; t < 2; t++) {
        final int thread = t;
        pool.submit(() -> {
          for (int i = 0; i < 200; i++) {
            try {
              idx.getMap(thread * 200 + i);
              idx.getKeys();
            } catch (Exception e) {
              failed.set(true);
            }
          }
        });
      }

      pool.shutdown();
      pool.awaitTermination(15, TimeUnit.SECONDS);
      assertFalse(failed.get(), "Concurrent add/read threw an exception");
    }
  }

  @Test
  public void testMaxKeysRetainsExactlyMaxKeysKeys()
      throws Exception {
    String colName = "maxkeys_exact_test";
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 2);
    OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config);

    // Add keys one at a time, in known order, across multiple documents
    // "alpha" first (doc 0), "beta" second (doc 1), "gamma" third (doc 2, should be dropped)
    creator.add(Map.of("alpha", 1));  // doc 0
    creator.add(Map.of("beta", 2));   // doc 1
    creator.add(Map.of("gamma", 3));  // doc 2 — gamma should be dropped (maxKeys=2)
    creator.seal();
    creator.close();

    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    assertEquals(reader.getKeys().size(), 2, "Index must hold exactly maxKeys=2 keys");
    assertTrue(reader.getKeys().contains("alpha"), "alpha was first key, must be retained");
    assertTrue(reader.getKeys().contains("beta"), "beta was second key, must be retained");
    assertFalse(reader.getKeys().contains("gamma"), "gamma was third key and must be dropped");

    // Presence bitmaps must be correct
    assertTrue(reader.getPresenceBitmap("alpha").contains(0));
    assertTrue(reader.getPresenceBitmap("beta").contains(1));
    assertEquals(reader.getPresenceBitmap("gamma").getCardinality(), 0); // empty — key not indexed

    buf.close();
  }

  @Test
  public void testSealAfterManyAddsProducesValidIndex()
      throws Exception {
    String colName = "many_docs_test";
    Map<String, FieldSpec.DataType> keyTypes = Collections.singletonMap("count", FieldSpec.DataType.INT);
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 10);
    OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config,
        keyTypes, FieldSpec.DataType.STRING);

    int numDocs = 500;
    for (int i = 0; i < numDocs; i++) {
      if (i % 3 == 0) {
        creator.add(Map.of("count", i));  // every 3rd doc has "count"
      } else {
        creator.add(Collections.emptyMap());  // absent
      }
    }
    creator.seal();
    creator.close();

    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    // Exactly numDocs/3 (rounded up) docs have "count"
    int expectedCount = (numDocs + 2) / 3;  // ceil(500/3) = 167
    assertEquals(reader.getPresenceBitmap("count").getCardinality(), expectedCount);

    // Spot check: doc 0 has count=0, doc 3 has count=3, doc 1 is absent
    assertTrue(reader.getPresenceBitmap("count").contains(0));
    assertTrue(reader.getPresenceBitmap("count").contains(3));
    assertFalse(reader.getPresenceBitmap("count").contains(1));
    assertEquals(reader.getInt(0, "count"), 0);
    assertEquals(reader.getInt(3, "count"), 3);

    buf.close();
  }

  @Test
  public void testByteOrderRoundTripAllTypes()
      throws Exception {
    String colName = "byteorder_test";
    Map<String, FieldSpec.DataType> keyTypes = Map.of(
        "intKey", FieldSpec.DataType.INT,
        "longKey", FieldSpec.DataType.LONG,
        "floatKey", FieldSpec.DataType.FLOAT,
        "doubleKey", FieldSpec.DataType.DOUBLE
    );
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);

    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 10);
    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      creator.add(Map.of(
          "intKey", 0x12345678,
          "longKey", 0x123456789ABCDEF0L,
          "floatKey", 3.14f,
          "doubleKey", 2.718281828
      ));
      creator.seal();
    }

    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    assertEquals(reader.getInt(0, "intKey"), 0x12345678);
    assertEquals(reader.getLong(0, "longKey"), 0x123456789ABCDEF0L);
    assertEquals(reader.getFloat(0, "floatKey"), 3.14f, 0.0001f);
    assertEquals(reader.getDouble(0, "doubleKey"), 2.718281828, 0.000000001);

    reader.close();
    buf.close();
  }

  // ---- Per-key inverted index tests ----

  @Test
  public void testPerKeyInvertedIndexSelectiveKeys()
      throws IOException {
    // enableInvertedIndexForAll=false + invertedIndexKeys=["color"] → only "color" gets inverted index
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);
    keyTypes.put("size", FieldSpec.DataType.STRING);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config =
        new ColumnarMapIndexConfig(true, null, false, Set.of("color"), 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("color", "red", "size", "small"),
        Map.of("color", "blue", "size", "large"),
        Map.of("color", "red", "size", "medium")
    };

    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, COLUMN_NAME, fieldSpec, config,
            keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {
      // color has inverted index
      ImmutableRoaringBitmap reds = reader.getDocsWithKeyValue("color", "red");
      assertNotNull(reds, "color should have inverted index");
      assertTrue(reds.contains(0));
      assertTrue(reds.contains(2));
      assertFalse(reds.contains(1));

      // size does NOT have inverted index
      ImmutableRoaringBitmap smalls = reader.getDocsWithKeyValue("size", "small");
      assertNull(smalls, "size should NOT have inverted index");
    }
  }

  @Test
  public void testPerKeyInvertedIndexAllOverridesSelectiveKeys()
      throws IOException {
    // enableInvertedIndexForAll=true + invertedIndexKeys=["color"] → ALL keys get inverted index (list ignored)
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);
    keyTypes.put("size", FieldSpec.DataType.STRING);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config =
        new ColumnarMapIndexConfig(true, null, true, Set.of("color"), 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("color", "red", "size", "small"),
        Map.of("color", "blue", "size", "large")
    };

    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, COLUMN_NAME, fieldSpec, config,
            keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {
      // Both keys should have inverted indexes
      assertNotNull(reader.getDocsWithKeyValue("color", "red"));
      assertNotNull(reader.getDocsWithKeyValue("size", "small"));
    }
  }

  @Test
  public void testPerKeyInvertedIndexNoKeysSpecified()
      throws IOException {
    // enableInvertedIndexForAll=false + no invertedIndexKeys → no inverted indexes (existing behavior)
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{Map.of("color", "red")};

    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, COLUMN_NAME, fieldSpec, config,
            keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {
      assertNull(reader.getDocsWithKeyValue("color", "red"), "No inverted index should exist");
    }
  }

  @Test
  public void testPerKeyInvertedIndexMutableSelectiveKeys()
      throws IOException {
    // Mutable index: enableInvertedIndexForAll=false + invertedIndexKeys=["brand"] → only brand gets inverted index
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("brand", FieldSpec.DataType.STRING);
    keyTypes.put("sku", FieldSpec.DataType.STRING);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config =
        new ColumnarMapIndexConfig(true, null, false, Set.of("brand"), 100);

    try (MutableColumnarMapIndexImpl mutableIndex = new MutableColumnarMapIndexImpl(
        buildMutableContext(fieldSpec), config, keyTypes, FieldSpec.DataType.STRING)) {
      mutableIndex.add(Map.of("brand", "acme", "sku", "A1"), -1, 0);
      mutableIndex.add(Map.of("brand", "acme", "sku", "B2"), -1, 1);
      mutableIndex.add(Map.of("brand", "beta", "sku", "A1"), -1, 2);

      // brand has inverted index
      ImmutableRoaringBitmap acmeDocs = mutableIndex.getDocsWithKeyValue("brand", "acme");
      assertNotNull(acmeDocs, "brand should have inverted index");
      assertTrue(acmeDocs.contains(0));
      assertTrue(acmeDocs.contains(1));
      assertFalse(acmeDocs.contains(2));

      // sku does NOT have inverted index
      assertNull(mutableIndex.getDocsWithKeyValue("sku", "A1"),
          "sku should NOT have inverted index");
    }
  }

  @Test
  public void testConfigShouldEnableInvertedIndexForKey() {
    // Unit test for ColumnarMapIndexConfig.shouldEnableInvertedIndexForKey
    ColumnarMapIndexConfig allEnabled = new ColumnarMapIndexConfig(true, null, true, null, 100);
    assertTrue(allEnabled.shouldEnableInvertedIndexForKey("anyKey"));
    assertTrue(allEnabled.shouldEnableInvertedIndexForKey("anotherKey"));

    ColumnarMapIndexConfig selectiveEnabled =
        new ColumnarMapIndexConfig(true, null, false, Set.of("k1", "k2"), 100);
    assertTrue(selectiveEnabled.shouldEnableInvertedIndexForKey("k1"));
    assertTrue(selectiveEnabled.shouldEnableInvertedIndexForKey("k2"));
    assertFalse(selectiveEnabled.shouldEnableInvertedIndexForKey("k3"));

    ColumnarMapIndexConfig noneEnabled = new ColumnarMapIndexConfig(true, null, false, null, 100);
    assertFalse(noneEnabled.shouldEnableInvertedIndexForKey("anyKey"));

    // enableInvertedIndexForAll=true overrides invertedIndexKeys
    ColumnarMapIndexConfig allWithList =
        new ColumnarMapIndexConfig(true, null, true, Set.of("k1"), 100);
    assertTrue(allWithList.shouldEnableInvertedIndexForKey("k1"));
    assertTrue(allWithList.shouldEnableInvertedIndexForKey("k999"));
  }

  // ---- Dictionary-based GROUP BY tests ----

  @Test
  public void testColumnarMapKeyDictionary() {
    String[] values = {"apple", "banana", "cherry"};
    ColumnarMapKeyDictionary dict = new ColumnarMapKeyDictionary(FieldSpec.DataType.STRING, values);

    // length
    assertEquals(dict.length(), 3);

    // indexOf
    assertEquals(dict.indexOf("apple"), 0);
    assertEquals(dict.indexOf("banana"), 1);
    assertEquals(dict.indexOf("cherry"), 2);
    assertEquals(dict.indexOf("missing"), org.apache.pinot.segment.spi.index.reader.Dictionary.NULL_VALUE_INDEX);

    // get / getStringValue
    assertEquals(dict.get(0), "apple");
    assertEquals(dict.getStringValue(1), "banana");

    // isSorted
    assertTrue(dict.isSorted());

    // getValueType
    assertEquals(dict.getValueType(), FieldSpec.DataType.STRING);

    // getMinVal / getMaxVal
    assertEquals(dict.getMinVal(), "apple");
    assertEquals(dict.getMaxVal(), "cherry");

    // insertionIndexOf
    assertEquals(dict.insertionIndexOf("banana"), 1); // exact match
    assertTrue(dict.insertionIndexOf("blueberry") < 0); // not found, returns negative insertion point
  }

  @Test
  public void testColumnarMapKeyDictionaryIntType() {
    String[] values = {"10", "20", "30"};
    ColumnarMapKeyDictionary dict = new ColumnarMapKeyDictionary(FieldSpec.DataType.INT, values);

    assertEquals(dict.getIntValue(0), 10);
    assertEquals(dict.getLongValue(1), 20L);
    assertEquals(dict.getFloatValue(2), 30.0f, 0.001f);
    assertEquals(dict.getDoubleValue(0), 10.0, 0.001);
    assertEquals(dict.getMinVal(), 10);
    assertEquals(dict.getMaxVal(), 30);

    // indexOf with typed values
    assertEquals(dict.indexOf("20"), 1);
  }

  @Test
  public void testImmutableDistinctValues()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);
    keyTypes.put("size", FieldSpec.DataType.INT);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("color", "red", "size", 10),
        Map.of("color", "blue", "size", 20),
        Map.of("color", "red", "size", 30),
        Map.of("color", "green", "size", 10)
    };

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      // hasInvertedIndex
      assertTrue(reader.hasInvertedIndex("color"));
      assertTrue(reader.hasInvertedIndex("size"));

      // getDistinctValuesForKey - color (sorted)
      String[] colorValues = reader.getDistinctValuesForKey("color");
      assertNotNull(colorValues);
      assertEquals(colorValues.length, 4);
      // Values should be sorted (includes default value "")
      assertEquals(colorValues[0], "");
      assertEquals(colorValues[1], "blue");
      assertEquals(colorValues[2], "green");
      assertEquals(colorValues[3], "red");

      // getDistinctValuesForKey - size (sorted, includes default "0")
      String[] sizeValues = reader.getDistinctValuesForKey("size");
      assertNotNull(sizeValues);
      assertEquals(sizeValues.length, 4);
      assertEquals(sizeValues[0], "0");
      assertEquals(sizeValues[1], "10");
      assertEquals(sizeValues[2], "20");
      assertEquals(sizeValues[3], "30");
    }
  }

  @Test
  public void testImmutableDistinctValuesNoInvertedIndex()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("name", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("name", "alice"),
        Map.of("name", "bob")
    };

    // No inverted index
    File indexFile = createIndex(keyTypes, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      assertFalse(reader.hasInvertedIndex("name"));
      assertNull(reader.getDistinctValuesForKey("name"));
    }
  }

  @Test
  public void testMutableDistinctValues()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("brand", FieldSpec.DataType.STRING);
    keyTypes.put("count", FieldSpec.DataType.INT);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 100);

    try (MutableColumnarMapIndexImpl mutableIndex = new MutableColumnarMapIndexImpl(
        buildMutableContext(fieldSpec), config, keyTypes, FieldSpec.DataType.STRING)) {

      mutableIndex.add(Map.of("brand", "acme", "count", 5), -1, 0);
      mutableIndex.add(Map.of("brand", "beta", "count", 10), -1, 1);
      mutableIndex.add(Map.of("brand", "acme", "count", 5), -1, 2);

      // hasInvertedIndex
      assertTrue(mutableIndex.hasInvertedIndex("brand"));
      assertTrue(mutableIndex.hasInvertedIndex("count"));

      // getDistinctValuesForKey - brand (TreeMap → sorted)
      String[] brandValues = mutableIndex.getDistinctValuesForKey("brand");
      assertNotNull(brandValues);
      assertEquals(brandValues.length, 2);
      assertEquals(brandValues[0], "acme");
      assertEquals(brandValues[1], "beta");

      // getDistinctValuesForKey - count
      String[] countValues = mutableIndex.getDistinctValuesForKey("count");
      assertNotNull(countValues);
      assertEquals(countValues.length, 2);
      assertEquals(countValues[0], "10"); // TreeMap sorts lexicographically for strings
      assertEquals(countValues[1], "5");
    }
  }

  @Test
  public void testKeyDataSourceWithDictionary()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("status", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("status", "active"),
        Map.of("status", "inactive"),
        Map.of("status", "active")
    };

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      org.apache.pinot.segment.spi.datasource.DataSource ds =
          new ColumnarMapDataSource(buildColumnMetadata(fieldSpec, 3), reader).getKeyDataSource("status");
      assertNotNull(ds);

      // Dictionary contains actual values plus default: "", "active", "inactive" (sorted)
      org.apache.pinot.segment.spi.index.reader.Dictionary dict = ds.getDictionary();
      assertNotNull(dict, "Dictionary should be present when inverted index is available");
      assertEquals(dict.length(), 3); // "", "active", "inactive"
      assertTrue(dict.indexOf("active") >= 0);
      assertTrue(dict.indexOf("inactive") >= 0);

      // Forward index should report dictionary-encoded
      org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> fwd = ds.getForwardIndex();
      assertTrue(fwd.isDictionaryEncoded());
    }
  }

  @Test
  public void testKeyDataSourceWithoutDictionary()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("name", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("name", "alice"),
        Map.of("name", "bob")
    };

    // Force raw encoding via noDictionaryKeys
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, Set.of("name"), 1000);
    File indexFile = createIndex(fieldSpec, config, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      org.apache.pinot.segment.spi.datasource.DataSource ds =
          new ColumnarMapDataSource(buildColumnMetadata(fieldSpec(keyTypes), 2), reader).getKeyDataSource("name");
      assertNotNull(ds);

      // No dictionary when noDictionaryKeys is set
      assertNull(ds.getDictionary(), "Dictionary should be null when noDictionaryKeys forces raw");

      // Forward index should NOT report dictionary-encoded
      org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> fwd = ds.getForwardIndex();
      assertFalse(fwd.isDictionaryEncoded());
    }
  }

  @Test
  public void testDictIdForwardIndex()
      throws IOException {
    // Verify that bit-packed dictIds are written and readable at segment level
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("color", "red"),     // doc 0
        Map.of("color", "blue"),    // doc 1
        Map.of("color", "red"),     // doc 2
        new HashMap<>(),            // doc 3 — absent, gets default ""
        Map.of("color", "green")    // doc 4
    };

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      // Verify dictId reader is available
      FixedBitIntReaderWriter dictIdReader = reader.getDictIdReader("color");
      assertNotNull(dictIdReader, "dictId reader should be available for key with inverted index");

      // Verify key dictionary is available
      ColumnarMapKeyDictionary dict = reader.getKeyDictionary("color");
      assertNotNull(dict, "Key dictionary should be available");

      // Dictionary contains actual values plus default value: "", "blue", "green", "red" (sorted)
      assertEquals(dict.length(), 4);
      assertEquals(dict.get(0), "");
      assertEquals(dict.get(1), "blue");
      assertEquals(dict.get(2), "green");
      assertEquals(dict.get(3), "red");

      // DictId forward index is sparse: 4 entries for docs 0,1,2,4 (doc 3 absent)
      int redId = dict.indexOf("red");
      int blueId = dict.indexOf("blue");
      int greenId = dict.indexOf("green");

      assertEquals(dictIdReader.readInt(0), redId);     // ordinal 0 (doc 0) = red
      assertEquals(dictIdReader.readInt(1), blueId);    // ordinal 1 (doc 1) = blue
      assertEquals(dictIdReader.readInt(2), redId);     // ordinal 2 (doc 2) = red
      assertEquals(dictIdReader.readInt(3), greenId);   // ordinal 3 (doc 4) = green
    }
  }

  @Test
  public void testReadDictIdsFastPath()
      throws IOException {
    // Verify readDictIds uses bit-packed fast path and returns correct values
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("status", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("status", "active"),     // doc 0
        Map.of("status", "inactive"),   // doc 1
        Map.of("status", "active"),     // doc 2
        new HashMap<>(),                // doc 3 — absent
        Map.of("status", "pending")     // doc 4
    };

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      // Build DataSource and get per-key DataSource
      org.apache.pinot.segment.spi.datasource.DataSource ds =
          new ColumnarMapDataSource(buildColumnMetadata(fieldSpec, 5), reader).getKeyDataSource("status");
      assertNotNull(ds);

      org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> fwd = ds.getForwardIndex();
      assertTrue(fwd.isDictionaryEncoded());

      org.apache.pinot.segment.spi.index.reader.Dictionary dict = ds.getDictionary();
      assertNotNull(dict);

      // Read dictIds including absent doc 3
      int[] docIds = {0, 1, 2, 3, 4};
      int[] dictIdBuffer = new int[5];
      fwd.readDictIds(docIds, 5, dictIdBuffer, null);

      // Verify via dictionary lookup
      assertEquals(dict.getStringValue(dictIdBuffer[0]), "active");
      assertEquals(dict.getStringValue(dictIdBuffer[1]), "inactive");
      assertEquals(dict.getStringValue(dictIdBuffer[2]), "active");
      // Absent doc gets default value's dictId (not NULL_VALUE_INDEX)
      int defaultDictId = dict.indexOf(ColumnarMapKeyDictionary.getDefaultValueString(FieldSpec.DataType.STRING));
      assertTrue(defaultDictId >= 0, "Default value should be in dictionary");
      assertEquals(dictIdBuffer[3], defaultDictId, "Absent doc should get default value dictId");
      assertEquals(dict.getStringValue(dictIdBuffer[4]), "pending");

      // Verify same dictIds for same values
      assertEquals(dictIdBuffer[0], dictIdBuffer[2], "Same value 'active' should have same dictId");
    }
  }

  @Test
  public void testReadDictIdsWithGaps()
      throws IOException {
    // Verify co-iterator readDictIds works correctly with non-sequential docIds (filtered GROUP BY)
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);

    // 20 docs, key present in ~60%: docs 0,1,3,5,6,8,10,12,14,16,17,19
    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[20];
    String[] colors = {"red", "blue", "green", "red", "blue"};
    int[] presentDocs = {0, 1, 3, 5, 6, 8, 10, 12, 14, 16, 17, 19};
    Set<Integer> presentSet = new HashSet<>();
    for (int d : presentDocs) {
      presentSet.add(d);
    }
    int colorIdx = 0;
    for (int i = 0; i < 20; i++) {
      if (presentSet.contains(i)) {
        docs[i] = Map.of("color", colors[colorIdx % colors.length]);
        colorIdx++;
      } else {
        docs[i] = new HashMap<>();
      }
    }

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      org.apache.pinot.segment.spi.datasource.DataSource ds =
          new ColumnarMapDataSource(buildColumnMetadata(fieldSpec, 20), reader).getKeyDataSource("color");
      assertNotNull(ds);

      org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> fwd = ds.getForwardIndex();
      assertTrue(fwd.isDictionaryEncoded());
      org.apache.pinot.segment.spi.index.reader.Dictionary dict = ds.getDictionary();
      assertNotNull(dict);

      // Case 1: sparse docIds with gaps (simulates filtered GROUP BY)
      int[] sparseDocIds = {0, 3, 7, 15, 19};
      int[] dictIdBuffer = new int[5];
      fwd.readDictIds(sparseDocIds, 5, dictIdBuffer, null);

      // Absent docs get default value's dictId
      int defaultDictId = dict.indexOf(ColumnarMapKeyDictionary.getDefaultValueString(FieldSpec.DataType.STRING));
      assertTrue(defaultDictId >= 0, "Default value should be in dictionary");

      assertEquals(dict.getStringValue(dictIdBuffer[0]), "red");    // doc 0 present
      assertEquals(dict.getStringValue(dictIdBuffer[1]), "green");  // doc 3 present
      assertEquals(dictIdBuffer[2], defaultDictId);                 // doc 7 absent
      assertEquals(dictIdBuffer[3], defaultDictId);                 // doc 15 absent
      assertEquals(dict.getStringValue(dictIdBuffer[4]), "blue");   // doc 19 present

      // Case 2: all docIds absent
      int[] absentDocIds = {2, 4, 7, 9, 11};
      int[] absentBuffer = new int[5];
      fwd.readDictIds(absentDocIds, 5, absentBuffer, null);
      for (int i = 0; i < 5; i++) {
        assertEquals(absentBuffer[i], defaultDictId,
            "Doc " + absentDocIds[i] + " should be absent");
      }

      // Case 3: all docIds present
      int[] allPresentDocIds = {0, 1, 3, 5, 6};
      int[] presentBuffer = new int[5];
      fwd.readDictIds(allPresentDocIds, 5, presentBuffer, null);
      for (int i = 0; i < 5; i++) {
        assertNotEquals(presentBuffer[i], Dictionary.NULL_VALUE_INDEX,
            "Doc " + allPresentDocIds[i] + " should be present");
      }

      // Case 4: single doc
      int[] singleDoc = {10};
      int[] singleBuffer = new int[1];
      fwd.readDictIds(singleDoc, 1, singleBuffer, null);
      assertNotEquals(singleBuffer[0], Dictionary.NULL_VALUE_INDEX);

      // Case 5: late docIds (simulates blocks from end of segment)
      int[] lateDocIds = {16, 17, 19};
      int[] lateBuffer = new int[3];
      fwd.readDictIds(lateDocIds, 3, lateBuffer, null);
      assertNotEquals(lateBuffer[0], Dictionary.NULL_VALUE_INDEX); // doc 16 present
      assertNotEquals(lateBuffer[1], Dictionary.NULL_VALUE_INDEX); // doc 17 present
      assertNotEquals(lateBuffer[2], Dictionary.NULL_VALUE_INDEX); // doc 19 present
    }
  }

  @Test
  public void testDictIdForwardIndexAvailableByDefault()
      throws IOException {
    // Dictionary encoding is now the default for all keys (even without inverted index)
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("name", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("name", "alice"),
        Map.of("name", "bob")
    };

    File indexFile = createIndex(keyTypes, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {
      assertNotNull(reader.getDictIdReader("name"), "DictId reader should be available by default");
      assertNotNull(reader.getKeyDictionary("name"), "Key dictionary should be available by default");

      assertEquals(reader.getString(0, "name"), "alice");
      assertEquals(reader.getString(1, "name"), "bob");
    }
  }

  @Test
  public void testBytesDictionaryRoundTrip()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("data", FieldSpec.DataType.BYTES);

    byte[] val1 = new byte[]{(byte) 0xFF, 0x00, 0x42};
    byte[] val2 = new byte[]{0x01, 0x02, 0x03};

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("data", val1),
        Map.of("data", val2),
        Map.of("data", val1)
    };

    File indexFile = createIndex(keyTypes, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {
      assertEquals(reader.getBytes(0, "data"), val1);
      assertEquals(reader.getBytes(1, "data"), val2);
      assertEquals(reader.getBytes(2, "data"), val1);

      // Verify dictionary round-trip
      ColumnarMapKeyDictionary dict = reader.getKeyDictionary("data");
      assertNotNull(dict);
      for (int i = 0; i < dict.length(); i++) {
        byte[] bytesVal = dict.getBytesValue(i);
        String hexStr = dict.getStringValue(i);
        assertEquals(bytesVal, org.apache.pinot.spi.utils.BytesUtils.toBytes(hexStr),
            "getBytesValue must decode hex back to raw bytes");
      }
    }
  }

  private static ComplexFieldSpec fieldSpec(Map<String, FieldSpec.DataType> keyTypes) {
    return buildMapFieldSpec(COLUMN_NAME);
  }

  private static org.apache.pinot.segment.spi.ColumnMetadata buildColumnMetadata(
      FieldSpec fieldSpec, int numDocs) {
    return new org.apache.pinot.segment.spi.ColumnMetadata() {
      @Override
      public FieldSpec getFieldSpec() {
        return fieldSpec;
      }

      @Override
      public int getTotalDocs() {
        return numDocs;
      }

      @Override
      public int getTotalNumberOfEntries() {
        return numDocs;
      }

      @Override
      public int getCardinality() {
        return 0;
      }

      @Override
      public boolean isSorted() {
        return false;
      }

      @Override
      public int getBitsPerElement() {
        return 0;
      }

      @Override
      public int getColumnMaxLength() {
        return 0;
      }

      @Override
      public boolean hasDictionary() {
        return false;
      }

      @Override
      public org.apache.pinot.segment.spi.partition.PartitionFunction getPartitionFunction() {
        return null;
      }

      @Override
      public Set<Integer> getPartitions() {
        return null;
      }

      @Override
      public Comparable getMinValue() {
        return null;
      }

      @Override
      public Comparable getMaxValue() {
        return null;
      }

      @Override
      public boolean isMinMaxValueInvalid() {
        return true;
      }

      @Override
      public boolean isAutoGenerated() {
        return false;
      }

      @Override
      public int getMaxNumberOfMultiValues() {
        return 0;
      }

      @Override
      public java.util.Map<org.apache.pinot.segment.spi.index.IndexType<?, ?, ?>, Long> getIndexSizeMap() {
        return Collections.emptyMap();
      }
    };
  }

  /// Verifies that {@link ColumnarMapDataSource#getKeyDataSource(String)} returns a valid
  /// DataSource for keys not yet ingested into a mutable (consuming) segment, rather than
  /// returning null and causing an NPE in {@code ItemTransformFunction.init()}.
  @Test
  public void testMutableKeyDataSourceForUnseenKey()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("status", FieldSpec.DataType.STRING);
    keyTypes.put("count", FieldSpec.DataType.INT);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 100);

    try (MutableColumnarMapIndexImpl mutableIndex = new MutableColumnarMapIndexImpl(
        buildMutableContext(fieldSpec), config, keyTypes, FieldSpec.DataType.STRING)) {

      // Only ingest docs with "status" — "count" is never seen
      mutableIndex.add(Map.of("status", "active"), -1, 0);
      mutableIndex.add(Map.of("status", "inactive"), -1, 1);

      ColumnarMapDataSource mapDS = new ColumnarMapDataSource(fieldSpec, 2, mutableIndex);

      // Key with data — should work as before
      org.apache.pinot.segment.spi.datasource.DataSource statusDS = mapDS.getKeyDataSource("status");
      assertNotNull(statusDS, "DataSource for ingested key should not be null");
      assertEquals(statusDS.getDataSourceMetadata().getDataType(), FieldSpec.DataType.STRING);

      // Key with explicit type but no docs ingested — must not return null
      org.apache.pinot.segment.spi.datasource.DataSource countDS = mapDS.getKeyDataSource("count");
      assertNotNull(countDS, "DataSource for unseen key with explicit type should not be null");
      assertEquals(countDS.getDataSourceMetadata().getDataType(), FieldSpec.DataType.INT);

      // Completely unknown key — gets default type, must not return null
      org.apache.pinot.segment.spi.datasource.DataSource unknownDS = mapDS.getKeyDataSource("never_heard_of");
      assertNotNull(unknownDS, "DataSource for unknown key should not be null (uses default type)");
      assertEquals(unknownDS.getDataSourceMetadata().getDataType(), FieldSpec.DataType.STRING);

      // Forward index should return default values for all docs on unseen keys
      org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> fwd = countDS.getForwardIndex();
      assertNotNull(fwd);
      assertEquals(fwd.getString(0, null), "");
      assertEquals(fwd.getString(1, null), "");
    }
  }
}
