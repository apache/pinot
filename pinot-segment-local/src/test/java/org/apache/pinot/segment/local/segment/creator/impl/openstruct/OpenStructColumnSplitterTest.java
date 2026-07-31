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
package org.apache.pinot.segment.local.segment.creator.impl.openstruct;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.segment.local.segment.index.readers.json.ImmutableJsonIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class OpenStructColumnSplitterTest {

  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("OpenStructColumnSplitterTest").toFile();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(_tempDir);
  }

  private ComplexFieldSpec spec() {
    return new ComplexFieldSpec("metrics", DataType.OPEN_STRUCT, true, Map.of());
  }

  private OpenStructIndexConfig config(double minFillRate, int maxDenseKeys, Set<String> denseKeys) {
    return new OpenStructIndexConfig(false, null, maxDenseKeys, denseKeys, minFillRate, null, null);
  }

  @Test
  public void testClassifyByFillRate()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    for (int d = 0; d < 10; d++) {
      Map<String, Object> doc = d < 7 ? Map.of("clicks", (long) d) : Map.of();
      s.add(doc, d);
    }
    Set<String> dense = s.classify();
    assertTrue(dense.contains("clicks"));
  }

  @Test
  public void testExplicitDenseKeysAlwaysMaterialized()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.99, -1, Set.of("rare")));
    s.add(Map.of("rare", "x"), 0);
    for (int d = 1; d < 100; d++) {
      s.add(Map.of(), d);
    }
    Set<String> dense = s.classify();
    assertTrue(dense.contains("rare"));
  }

  @Test
  public void testRareKeyDroppedFromDense()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    s.add(Map.of("rare", "x"), 0);
    for (int d = 1; d < 100; d++) {
      s.add(Map.of(), d);
    }
    Set<String> dense = s.classify();
    assertFalse(dense.contains("rare"));
  }

  @Test
  public void testMaxDenseKeysCap()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.1, 1, null));
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("a", "x", "b", "y", "c", "z"), d);
    }
    Set<String> dense = s.classify();
    assertEquals(dense.size(), 1);
  }

  @Test
  public void testZeroDocsIsNoop()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    s.seal();
    assertTrue(s.getResolvedDenseKeys().isEmpty());
  }

  @Test
  public void testSealEmitsParentMetadataForDense()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("clicks", (long) d), d);
    }
    s.seal();
    String denseCol = OpenStructNaming.materializedColumnName("metrics", "clicks");
    Map<String, PropertiesConfiguration> meta = s.getMaterializedColumnMetadata();
    PropertiesConfiguration denseProps = meta.get(denseCol);
    assertEquals(denseProps.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.PARENT_COLUMN)), "metrics");

    PropertiesConfiguration parentProps = meta.get("metrics");
    assertNotNull(parentProps);
    assertEquals(parentProps.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        "metrics", V1Constants.MetadataKeys.Column.DATA_TYPE)), "OPEN_STRUCT");
    assertEquals(parentProps.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        "metrics", V1Constants.MetadataKeys.Column.COLUMN_TYPE)), "COMPLEX");
    assertEquals(parentProps.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        "metrics", V1Constants.MetadataKeys.Column.HAS_SPARSE_COLUMN)), "false");
  }

  @Test
  public void testDenseColumnMetadataKeysPresent()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("clicks", (long) d), d);
    }
    s.seal();
    String denseCol = OpenStructNaming.materializedColumnName("metrics", "clicks");
    PropertiesConfiguration p = s.getMaterializedColumnMetadata().get(denseCol);
    assertNotNull(p);
    assertEquals(p.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.DATA_TYPE)), "LONG");
    assertEquals(p.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.COLUMN_TYPE)), "DIMENSION");
    assertEquals(p.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.HAS_DICTIONARY)), "true");
    assertEquals(p.getInt(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.TOTAL_DOCS)), 10);
    assertEquals(p.getInt(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.CARDINALITY)), 10);
    assertEquals(p.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.PARENT_COLUMN)), "metrics");
    assertEquals(p.getString(V1Constants.MetadataKeys.Column.getKeyFor(denseCol, "hasNullValue")), "true");
  }

  @Test
  public void testSparseJsonColumnWritten()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.9, -1, null));
    s.add(Map.of("rare", "x"), 0);
    for (int d = 1; d < 10; d++) {
      s.add(Map.of(), d);
    }
    s.seal();
    String sparseCol = OpenStructNaming.sparseColumnName("metrics");
    assertTrue(s.getMaterializedColumnMetadata().containsKey(sparseCol));
  }

  @Test
  public void testBigDecimalDictionaryRoundTrip()
      throws Exception {
    // Regression: an untyped key whose value is a BigDecimal used to crash seal() with
    // IllegalStateException("Unsupported OPEN_STRUCT stored type for dictionary build: BIG_DECIMAL").
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("amount", new BigDecimal("12.34").add(BigDecimal.valueOf(d))), d);
    }
    s.seal();

    String denseCol = OpenStructNaming.materializedColumnName("metrics", "amount");
    PropertiesConfiguration props = s.getMaterializedColumnMetadata().get(denseCol);
    assertNotNull(props);
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.DATA_TYPE)), "BIG_DECIMAL");
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.HAS_DICTIONARY)), "true");
    assertTrue(new File(_tempDir, denseCol + V1Constants.Dict.FILE_EXTENSION).exists());
  }

  @Test
  public void testBigDecimalScaleDistinctValuesNotCollapsed()
      throws Exception {
    // 1.0 and 1.00 are equal by compareTo but distinct by equals; they must stay separate dictionary
    // entries. Doc 2 is absent, so the default (BigDecimal.ZERO) is also collected -> 3 distinct values.
    // A compareTo-based dedup would wrongly collapse 1.0/1.00 and yield 2.
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    s.add(Map.of("amount", new BigDecimal("1.0")), 0);
    s.add(Map.of("amount", new BigDecimal("1.00")), 1);
    s.add(Map.of(), 2);
    s.seal();

    String denseCol = OpenStructNaming.materializedColumnName("metrics", "amount");
    PropertiesConfiguration props = s.getMaterializedColumnMetadata().get(denseCol);
    assertNotNull(props);
    assertEquals(props.getInt(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.CARDINALITY)), 3);
  }

  @Test
  public void testBigDecimalExplicitChildSpec()
      throws Exception {
    // A key declared BIG_DECIMAL in the schema bypasses inferDataType but must still seal.
    Map<String, FieldSpec> children = Map.of(
        "amount", new DimensionFieldSpec("amount", DataType.BIG_DECIMAL, true));
    ComplexFieldSpec specWithChild = new ComplexFieldSpec("metrics", DataType.OPEN_STRUCT, true, children);
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", specWithChild,
        config(0.5, -1, null));
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("amount", new BigDecimal("100.5")), d);
    }
    s.seal();

    String denseCol = OpenStructNaming.materializedColumnName("metrics", "amount");
    PropertiesConfiguration props = s.getMaterializedColumnMetadata().get(denseCol);
    assertNotNull(props);
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.DATA_TYPE)), "BIG_DECIMAL");
  }

  @Test
  public void testBigDecimalRawForwardIndex()
      throws Exception {
    // RAW-encoded BIG_DECIMAL key must take the raw var-byte forward index path, not the dictionary.
    FieldConfig rawConfig = new FieldConfig.Builder("amount")
        .withEncodingType(FieldConfig.EncodingType.RAW).build();
    OpenStructIndexConfig cfg = new OpenStructIndexConfig(
        false, null, -1, null, 0.5, List.of(rawConfig), null);
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(), cfg);
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("amount", new BigDecimal("7.5").add(BigDecimal.valueOf(d))), d);
    }
    s.seal();

    String denseCol = OpenStructNaming.materializedColumnName("metrics", "amount");
    PropertiesConfiguration props = s.getMaterializedColumnMetadata().get(denseCol);
    assertNotNull(props);
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.HAS_DICTIONARY)), "false");
    assertFalse(new File(_tempDir, denseCol + V1Constants.Dict.FILE_EXTENSION).exists());
    assertTrue(new File(_tempDir,
        denseCol + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION).exists());
  }

  @Test
  public void testBigDecimalSparseKey()
      throws Exception {
    // A BIG_DECIMAL key below the fill-rate threshold goes to the sparse JSON column without crashing.
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.9, -1, null));
    s.add(Map.of("rare", new BigDecimal("3.14159")), 0);
    for (int d = 1; d < 10; d++) {
      s.add(Map.of(), d);
    }
    s.seal();
    assertTrue(s.getMaterializedColumnMetadata().containsKey(OpenStructNaming.sparseColumnName("metrics")));
  }

  @Test
  public void testAbsentDocUsesDimensionNullDefault()
      throws Exception {
    // Absent docs now store the standard Pinot dimension null value (INT -> Integer.MIN_VALUE),
    // so the column min reflects that default rather than the old metric-style 0.
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    for (int d = 0; d < 5; d++) {
      s.add(Map.of("clicks", 10 + d), d);   // present: 10..14
    }
    for (int d = 5; d < 10; d++) {
      s.add(Map.of(), d);                    // absent
    }
    s.seal();

    String denseCol = OpenStructNaming.materializedColumnName("metrics", "clicks");
    PropertiesConfiguration props = s.getMaterializedColumnMetadata().get(denseCol);
    assertNotNull(props);
    // Default (non-RAW) numeric key is dictionary-encoded.
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.HAS_DICTIONARY)), "true");
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.MIN_VALUE)), String.valueOf(Integer.MIN_VALUE));
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.MAX_VALUE)), "14");
  }

  @Test
  public void testDenseDefaultKeyWritesDictionaryAndInvertedIndex()
      throws Exception {
    // Default keys are dictionary-encoded with an inverted index (both default on), now written via the
    // standard ForwardIndexCreator and inverted index creator.
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("tag", "v" + (d % 3)), d);
    }
    s.seal();

    String denseCol = OpenStructNaming.materializedColumnName("metrics", "tag");
    PropertiesConfiguration props = s.getMaterializedColumnMetadata().get(denseCol);
    assertNotNull(props);
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.HAS_DICTIONARY)), "true");
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(denseCol, "hasInvertedIndex")),
        "true");
    assertTrue(new File(_tempDir, denseCol + V1Constants.Dict.FILE_EXTENSION).exists());
    assertTrue(new File(_tempDir,
        denseCol + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION).exists());
  }

  @Test
  public void testRawStringForwardIndexViaStandardCreator()
      throws Exception {
    // A RAW-configured key takes the standard raw var-byte forward index path (no dictionary).
    FieldConfig rawConfig = new FieldConfig.Builder("note")
        .withEncodingType(FieldConfig.EncodingType.RAW).build();
    OpenStructIndexConfig cfg = new OpenStructIndexConfig(false, null, -1, null, 0.5, List.of(rawConfig), null);
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(), cfg);
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("note", "n" + d), d);
    }
    s.seal();

    String denseCol = OpenStructNaming.materializedColumnName("metrics", "note");
    PropertiesConfiguration props = s.getMaterializedColumnMetadata().get(denseCol);
    assertNotNull(props);
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        denseCol, V1Constants.MetadataKeys.Column.HAS_DICTIONARY)), "false");
    assertFalse(new File(_tempDir, denseCol + V1Constants.Dict.FILE_EXTENSION).exists());
    assertTrue(new File(_tempDir,
        denseCol + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION).exists());
  }

  @Test
  public void testRangeAndBloomIndexesWrittenForKey()
      throws Exception {
    // An INT key configured with range + bloom must produce those index buffers via the generic loop.
    JsonNode indexes = JsonUtils.stringToJsonNode("{\"range\": {}, \"bloom\": {}}");
    FieldConfig keyConfig = new FieldConfig.Builder("clicks").withIndexes(indexes).build();
    OpenStructIndexConfig cfg = new OpenStructIndexConfig(false, null, -1, null, 0.5, List.of(keyConfig), null);
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(), cfg);
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("clicks", d), d);
    }
    s.seal();

    String denseCol = OpenStructNaming.materializedColumnName("metrics", "clicks");
    assertTrue(new File(_tempDir, denseCol + V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION).exists(),
        "range index buffer should be written");
    assertTrue(new File(_tempDir, denseCol + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION).exists(),
        "bloom filter buffer should be written");
  }

  @Test
  public void testRangeOnRawNonNumericKeyFailsWithCanonicalGuard()
      throws Exception {
    // A STRING key with RAW encoding + range resolves to raw (range does not require a dictionary), which the
    // range creator cannot build. The splitter must surface the canonical RangeIndexType.validate guard
    // (IllegalStateException) at build time rather than crashing opaquely inside the creator.
    JsonNode indexes = JsonUtils.stringToJsonNode("{\"range\": {}}");
    FieldConfig keyConfig = new FieldConfig.Builder("tag")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexes(indexes)
        .build();
    OpenStructIndexConfig cfg = new OpenStructIndexConfig(false, null, -1, null, 0.5, List.of(keyConfig), null);
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(), cfg);
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("tag", "v" + d), d);
    }

    assertThrows(IllegalStateException.class, s::seal);
  }

  @Test
  public void testParentMetadataCarriesSparseKeyManifest()
      throws Exception {
    // maxDenseKeys=0 forces every key sparse regardless of fill rate.
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, 0, null));
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("region", "us", "latencyMs", (long) d), d);
    }
    s.seal();

    PropertiesConfiguration parentProps = s.getMaterializedColumnMetadata().get("metrics");
    assertNotNull(parentProps);
    String jsonManifest = parentProps.getString(
        V1Constants.MetadataKeys.Column.getKeyFor("metrics", V1Constants.MetadataKeys.Column.SPARSE_KEYS));
    assertNotNull(jsonManifest);
    Set<String> sparseKeys = new HashSet<>(
        JsonUtils.stringToObject(jsonManifest, new com.fasterxml.jackson.core.type.TypeReference<List<String>>() { }));
    assertEquals(sparseKeys, Set.of("region", "latencyMs"));
  }

  @Test
  public void testParentMetadataManifestExcludesDenseKeys()
      throws Exception {
    // "clicks" is present on every doc (dense); "rare" is present on one doc (sparse). The manifest
    // must list only the sparse key, not the dense one.
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    s.add(Map.of("clicks", 1L, "rare", "x"), 0);
    for (int d = 1; d < 10; d++) {
      s.add(Map.of("clicks", (long) d), d);
    }
    s.seal();

    PropertiesConfiguration parentProps = s.getMaterializedColumnMetadata().get("metrics");
    assertNotNull(parentProps);
    String jsonManifest = parentProps.getString(
        V1Constants.MetadataKeys.Column.getKeyFor("metrics", V1Constants.MetadataKeys.Column.SPARSE_KEYS));
    assertNotNull(jsonManifest);
    Set<String> sparseKeys = new HashSet<>(
        JsonUtils.stringToObject(jsonManifest, new com.fasterxml.jackson.core.type.TypeReference<List<String>>() { }));
    assertEquals(sparseKeys, Set.of("rare"));
  }

  @Test
  public void testParentMetadataOmitsManifestWhenNoSparseKeys()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, -1, null));
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("clicks", (long) d), d);
    }
    s.seal();

    PropertiesConfiguration parentProps = s.getMaterializedColumnMetadata().get("metrics");
    assertNotNull(parentProps);
    assertFalse(parentProps.containsKey(
        V1Constants.MetadataKeys.Column.getKeyFor("metrics", V1Constants.MetadataKeys.Column.SPARSE_KEYS)));
    assertEquals(parentProps.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        "metrics", V1Constants.MetadataKeys.Column.HAS_SPARSE_COLUMN)), "false");
  }

  @Test
  public void testParentMetadataManifestIncludesCommaKey()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(),
        config(0.5, 0, null));
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("region", "us", "weird,key", "x"), d);
    }
    s.seal();

    PropertiesConfiguration parentProps = s.getMaterializedColumnMetadata().get("metrics");
    assertNotNull(parentProps);
    assertTrue(parentProps.containsKey(
        V1Constants.MetadataKeys.Column.getKeyFor("metrics", V1Constants.MetadataKeys.Column.SPARSE_KEYS)));
    assertEquals(parentProps.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        "metrics", V1Constants.MetadataKeys.Column.HAS_SPARSE_COLUMN)), "true");
  }

  @Test
  public void testSparseJsonIndexBuiltWhenEnabled()
      throws Exception {
    // Pins bare key form (not "$."-prefixed) — MapFilterOperator fast path relies on this.
    OpenStructIndexConfig cfg = new OpenStructIndexConfig(false, null, 0, null, null, null, true);
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(), cfg);
    s.add(Map.of("region", "us"), 0);
    s.add(Map.of(), 1);
    s.add(Map.of("region", "eu"), 2);
    s.seal();

    String sparseCol = OpenStructNaming.sparseColumnName("metrics");
    File indexFile = new File(_tempDir, sparseCol + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    assertTrue(indexFile.exists());

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
      ImmutableJsonIndexReader reader = new ImmutableJsonIndexReader(buffer, 3);

      MutableRoaringBitmap us = reader.getMatchingDocIds(FilterContext.forPredicate(
          new EqPredicate(ExpressionContext.forIdentifier("region"), "us")));
      assertEquals(us.getCardinality(), 1);
      assertTrue(us.contains(0));
      assertFalse(us.contains(1));
      assertFalse(us.contains(2));

      MutableRoaringBitmap eu = reader.getMatchingDocIds(FilterContext.forPredicate(
          new EqPredicate(ExpressionContext.forIdentifier("region"), "eu")));
      assertEquals(eu.getCardinality(), 1);
      assertTrue(eu.contains(2));
      assertFalse(eu.contains(0));
      assertFalse(eu.contains(1));
    }
  }

  @Test
  public void testSparseJsonIndexAbsentByDefault()
      throws Exception {
    OpenStructIndexConfig cfg = new OpenStructIndexConfig(false, null, 0, null, null, null, null);
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", spec(), cfg);
    s.add(Map.of("region", "us"), 0);
    s.add(Map.of(), 1);
    s.add(Map.of("region", "eu"), 2);
    s.seal();

    String sparseCol = OpenStructNaming.sparseColumnName("metrics");
    File indexFile = new File(_tempDir, sparseCol + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    assertFalse(indexFile.exists());
  }
}
