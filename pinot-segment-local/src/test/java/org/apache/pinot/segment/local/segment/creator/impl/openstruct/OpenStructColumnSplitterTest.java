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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV4;
import org.apache.pinot.segment.local.segment.index.readers.json.ImmutableJsonIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
    return config(minFillRate, maxDenseKeys, denseKeys, false);
  }

  private OpenStructIndexConfig config(double minFillRate, int maxDenseKeys, Set<String> denseKeys,
      boolean perKeyMetricsEnabled) {
    return new OpenStructIndexConfig(false, null, maxDenseKeys, denseKeys, minFillRate, null, null,
        perKeyMetricsEnabled);
  }

  @Test
  public void testClassifyByFillRate()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
        config(0.5, -1, null));
    s.seal();
    assertTrue(s.getResolvedDenseKeys().isEmpty());
  }

  @Test
  public void testSealEmitsParentMetadataForDense()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
        config(0.9, -1, null));
    s.add(Map.of("rare", "x"), 0);
    for (int d = 1; d < 10; d++) {
      s.add(Map.of(), d);
    }
    s.seal();
    String sparseCol = OpenStructNaming.sparseColumnName("metrics");
    PropertiesConfiguration props = s.getMaterializedColumnMetadata().get(sparseCol);
    assertNotNull(props);

    // Regression: the sparse column's metadata used to omit several properties ColumnMetadataImpl.
    // fromPropertiesConfiguration() reads via config.getInt() with no default (e.g. CARDINALITY). This branch's
    // reader still tolerates a missing bitsPerElement/lengthOfEachEntry/maxNumberOfMultiValues via UNAVAILABLE
    // defaults, but the sibling early-release branch's stricter reader throws NoSuchElementException on the same
    // gap, so keep this column on the standard addColumnMetadataInfo() path and verify it round-trips cleanly.
    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(props, 10, sparseCol);
    assertEquals(metadata.getFieldSpec().getDataType(), DataType.STRING);
    assertFalse(metadata.hasDictionary());
  }

  @Test
  public void testBigDecimalDictionaryRoundTrip()
      throws Exception {
    // Regression: an untyped key whose value is a BigDecimal used to crash seal() with
    // IllegalStateException("Unsupported OPEN_STRUCT stored type for dictionary build: BIG_DECIMAL").
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", specWithChild,
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(), cfg);
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(), cfg);
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(), cfg);
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(), cfg);
    for (int d = 0; d < 10; d++) {
      s.add(Map.of("tag", "v" + d), d);
    }

    assertThrows(IllegalStateException.class, s::seal);
  }

  @Test
  public void testParentMetadataCarriesSparseKeyManifest()
      throws Exception {
    // maxDenseKeys=0 forces every key sparse regardless of fill rate.
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
        JsonUtils.stringToObject(jsonManifest, new TypeReference<List<String>>() { }));
    assertEquals(sparseKeys, Set.of("region", "latencyMs"));
  }

  @Test
  public void testParentMetadataManifestExcludesDenseKeys()
      throws Exception {
    // "clicks" is present on every doc (dense); "rare" is present on one doc (sparse). The manifest
    // must list only the sparse key, not the dense one.
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
        JsonUtils.stringToObject(jsonManifest, new TypeReference<List<String>>() { }));
    assertEquals(sparseKeys, Set.of("rare"));
  }

  @Test
  public void testParentMetadataOmitsManifestWhenNoSparseKeys()
      throws Exception {
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(), cfg);
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
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(), cfg);
    s.add(Map.of("region", "us"), 0);
    s.add(Map.of(), 1);
    s.add(Map.of("region", "eu"), 2);
    s.seal();

    String sparseCol = OpenStructNaming.sparseColumnName("metrics");
    File indexFile = new File(_tempDir, sparseCol + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    assertFalse(indexFile.exists());
  }

  @Test
  public void testSparseValuesLandOnTheirOwnDocs()
      throws Exception {
    // maxDenseKeys = 0 forces every key into the sparse tier; sparseJsonIndex = true lets the
    // written content be read back and asserted per key.
    OpenStructIndexConfig cfg = new OpenStructIndexConfig(false, null, 0, null, null, null, true, null, null);
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(), cfg);
    // Interleave which keys are present per doc, and give each key different values on different
    // docs, so an off-by-one in any key's value ordinal shows up as a value on the wrong doc.
    s.add(Map.of("b", "b0", "a", "a0"), 0);
    s.add(Map.of("c", "c1", "a", "a1"), 1);
    s.add(Map.of("b", "b2"), 2);
    s.add(Map.of(), 3);
    s.seal();

    String sparseCol = OpenStructNaming.sparseColumnName("metrics");
    File indexFile = new File(_tempDir, sparseCol + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    assertTrue(indexFile.exists());

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile)) {
      ImmutableJsonIndexReader reader = new ImmutableJsonIndexReader(buffer, 4);
      assertMatches(reader, "a", "a0", 0);
      assertMatches(reader, "a", "a1", 1);
      assertMatches(reader, "b", "b0", 0);
      assertMatches(reader, "b", "b2", 2);
      assertMatches(reader, "c", "c1", 1);
    }

    // The JSON index above is inherently key-order-independent, so it cannot catch a regression in
    // per-document key ordering. Read the raw sparse forward-index column directly for doc 0 (which
    // carries two sparse keys, "a" and "b") and assert the exact serialised JSON string, so the
    // scatter's key-iteration order is enforced byte-for-byte, not just its per-key value placement.
    File fwdIndexFile = new File(_tempDir, sparseCol + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    assertTrue(fwdIndexFile.exists());
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(fwdIndexFile);
        VarByteChunkForwardIndexReaderV4 fwdReader =
            new VarByteChunkForwardIndexReaderV4(buffer, DataType.STRING, true);
        VarByteChunkForwardIndexReaderV4.ReaderContext context = fwdReader.createContext()) {
      assertEquals(fwdReader.getString(0, context), "{\"a\":\"a0\",\"b\":\"b0\"}");
    }
  }

  /// The sparse scatter walks documents in windows to cap how many per-document maps are live at
  /// once, so every key's value ordinal has to keep counting across window boundaries: resetting it
  /// per window would pair a later document with an earlier document's value, and consuming the
  /// first docId of the next window while bounding a window would drop that value entirely. This
  /// case derives its document count from [OpenStructColumnSplitter]'s
  /// `SPARSE_SCATTER_WINDOW_SIZE` so it keeps crossing a window boundary even if that constant
  /// changes.
  @Test
  public void testSparseValuesSurviveScatterWindowBoundaries()
      throws Exception {
    int windowSize = OpenStructColumnSplitter.SPARSE_SCATTER_WINDOW_SIZE;
    int numDocs = windowSize + 4096;
    // Straddle the boundary from both sides, and place one document carrying both keys exactly on
    // it, so a dropped or misordered boundary document shows up as a wrong value or wrong key order.
    Set<Integer> aDocs = Set.of(0, windowSize - 1, windowSize, windowSize + 1, numDocs - 1);
    Set<Integer> bDocs = Set.of(1, windowSize, numDocs - 1);

    OpenStructIndexConfig cfg = new OpenStructIndexConfig(false, null, 0, null, null, null, true, null, null);
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(), cfg);
    for (int d = 0; d < numDocs; d++) {
      Map<String, Object> row = new HashMap<>();
      if (aDocs.contains(d)) {
        row.put("a", "a" + d);
      }
      if (bDocs.contains(d)) {
        row.put("b", "b" + d);
      }
      s.add(row, d);
    }
    s.seal();

    String sparseCol = OpenStructNaming.sparseColumnName("metrics");
    File fwdIndexFile = new File(_tempDir, sparseCol + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    assertTrue(fwdIndexFile.exists());
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(fwdIndexFile);
        VarByteChunkForwardIndexReaderV4 fwdReader =
            new VarByteChunkForwardIndexReaderV4(buffer, DataType.STRING, true);
        VarByteChunkForwardIndexReaderV4.ReaderContext context = fwdReader.createContext()) {
      for (int d = 0; d < numDocs; d++) {
        StringBuilder expected = new StringBuilder();
        if (aDocs.contains(d)) {
          expected.append("\"a\":\"a").append(d).append('"');
        }
        if (bDocs.contains(d)) {
          if (expected.length() > 0) {
            expected.append(',');
          }
          expected.append("\"b\":\"b").append(d).append('"');
        }
        // Documents with no sparse entry store the empty placeholder, not an empty JSON object.
        String expectedJson = expected.length() == 0 ? "" : "{" + expected + "}";
        assertEquals(fwdReader.getString(d, context), expectedJson, "Wrong sparse content at docId " + d);
      }
    }
  }

  private static void assertMatches(ImmutableJsonIndexReader reader, String key, String value, int expectedDocId) {
    MutableRoaringBitmap matched = reader.getMatchingDocIds(FilterContext.forPredicate(
        new EqPredicate(ExpressionContext.forIdentifier(key), value)));
    assertEquals(matched.getCardinality(), 1, key + "=" + value + " matched " + matched);
    assertTrue(matched.contains(expectedDocId), key + "=" + value + " should be on doc " + expectedDocId);
  }

  /// The inferred type of a key is cached on first sighting, so counting inference failures inside
  /// that caching step would record 1 failure per key no matter how many values actually failed.
  /// Every value that takes the STRING fallback must be counted.
  @Test
  public void testInferenceFailuresCountedPerValueNotPerKey()
      throws Exception {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
          config(0.5, -1, null));
      // 'payload' has no child spec and a Map value, which OpenStructTypeInference cannot map to a DataType.
      for (int d = 0; d < 4; d++) {
        s.add(Map.of("payload", Map.of("nested", d)), d);
      }
      s.seal();

      verify(metrics).addMeteredTableValue("testTable_OFFLINE", "metrics",
          ServerMeter.OPEN_STRUCT_TYPE_INFERENCE_FAILURES, 4L);
    } finally {
      ServerMetrics.deregister();
    }
  }

  @Test
  public void testIgnoredKeyNeverMaterializedDenseOrSparse()
      throws Exception {
    // "debug" has a fill rate far below the default denseKeyMinFillRate (0.5), so if it were not
    // dropped by ignoredKeys it would land in the sparse manifest (see testRareKeyDroppedFromDense).
    // This is what makes the sparse-manifest assertion below meaningful rather than vacuous.
    OpenStructIndexConfig cfg = new OpenStructIndexConfig(false, null, -1, null, 0.5, null, null, null,
        Set.of("debug"));
    OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(), cfg);
    s.add(Map.of("debug", "noise", "clicks", 0L), 0);
    for (int d = 1; d < 100; d++) {
      s.add(Map.of("clicks", (long) d), d);
    }
    s.seal();

    Set<String> dense = s.getResolvedDenseKeys();
    assertFalse(dense.contains("debug"));
    assertTrue(dense.contains("clicks"));

    PropertiesConfiguration parentProps = s.getMaterializedColumnMetadata().get("metrics");
    assertNotNull(parentProps);
    assertFalse(parentProps.containsKey(
        V1Constants.MetadataKeys.Column.getKeyFor("metrics", V1Constants.MetadataKeys.Column.SPARSE_KEYS)));
  }

  @Test
  public void testIgnoredKeyMeteredOnSeal()
      throws Exception {
    OpenStructIndexConfig cfg = JsonUtils.stringToObject(
        "{\"ignoredKeys\": [\"debug\"]}", OpenStructIndexConfig.class);
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
          cfg);
      s.add(Map.of("debug", "a"), 0);
      s.add(Map.of("debug", "b", "clicks", 1L), 1);
      s.seal();

      verify(metrics, times(1)).addMeteredTableValue("testTable_OFFLINE", "metrics",
          ServerMeter.OPEN_STRUCT_IGNORED_KEY_DROPS, 2L);
    } finally {
      ServerMetrics.deregister();
    }
  }

  @Test
  public void testIgnoredKeyWithNullValueNotMetered()
      throws Exception {
    OpenStructIndexConfig cfg = JsonUtils.stringToObject(
        "{\"ignoredKeys\": [\"debug\"]}", OpenStructIndexConfig.class);
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
          cfg);
      Map<String, Object> row = new HashMap<>();
      row.put("debug", null);
      s.add(row, 0);
      s.seal();

      verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
          eq(ServerMeter.OPEN_STRUCT_IGNORED_KEY_DROPS), anyLong());
    } finally {
      ServerMetrics.deregister();
    }
  }

  /// A value that cannot be mapped to a DataType but lands on a key whose type is already
  /// established as something other than STRING is dropped by coercion, not stored as STRING. It
  /// must be counted once, against the coercion meter only — counting it as an inference failure
  /// too would record one dropped value against two meters and make the seal-time log claim it
  /// "fell back to STRING" when it did not.
  @Test
  public void testUnmappableValueOnTypedKeyCountsOnlyAsCoercionFailure()
      throws Exception {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
          config(0.5, -1, null));
      // First value fixes the key's type as LONG; the next two cannot be coerced to it.
      s.add(Map.of("clicks", 1L), 0);
      s.add(Map.of("clicks", Map.of("a", 1)), 1);
      s.add(Map.of("clicks", List.of(1, 2)), 2);
      s.seal();

      verify(metrics).addMeteredTableValue("testTable_OFFLINE", "metrics",
          ServerMeter.OPEN_STRUCT_TYPE_COERCION_FAILURES, 2L);
      verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
          eq(ServerMeter.OPEN_STRUCT_TYPE_INFERENCE_FAILURES), anyLong());
    } finally {
      ServerMetrics.deregister();
    }
  }

  /// Fill rate is emitted as two raw counts rather than one percentage gauge: integer division
  /// truncates a dense key present in a handful of docs to 0, which is indistinguishable from no
  /// data and is exactly the case worth alerting on. Pins both numerator and denominator for a key
  /// whose fill rate would truncate to 0.
  @Test
  public void testGaugesEmitRawDocCountsForTruncatingFillRate()
      throws Exception {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      // 'rare' is forced dense despite appearing in 3 of 200 docs (1.5%, which truncates to 0%).
      OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
          config(0.5, -1, Set.of("rare")));
      for (int d = 0; d < 200; d++) {
        s.add(d < 3 ? Map.of("rare", (long) d, "host", "h") : Map.of("host", "h"), d);
      }
      s.classify();
      s.seal();

      verify(metrics).setOrUpdateTableGauge("testTable_OFFLINE", "metrics",
          ServerGauge.OPEN_STRUCT_LAST_SEGMENT_DOC_COUNT, 200L);
      verify(metrics).setOrUpdateTableGauge("testTable_OFFLINE",
          OpenStructNaming.materializedColumnName("metrics", "rare"),
          ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT, 3L);
      verify(metrics).setOrUpdateTableGauge("testTable_OFFLINE", "metrics",
          ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_COUNT, 2L);
      verify(metrics).setOrUpdateTableGauge("testTable_OFFLINE", "metrics",
          ServerGauge.OPEN_STRUCT_LAST_SEGMENT_DENSE_KEY_COUNT, 2L);
      verify(metrics).setOrUpdateTableGauge("testTable_OFFLINE", "metrics",
          ServerGauge.OPEN_STRUCT_LAST_SEGMENT_SPARSE_KEY_COUNT, 0L);
    } finally {
      ServerMetrics.deregister();
    }
  }

  /// Default: flag off, no configured denseKeys → no per-key gauge at all.
  @Test
  public void testPerKeyGaugeOffByDefaultWithNoDenseKeys()
      throws Exception {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
          config(0.5, -1, null));
      for (int d = 0; d < 10; d++) {
        s.add(Map.of("host", "h", "clicks", (long) d), d);
      }
      s.classify();
      s.seal();

      verify(metrics, never()).setOrUpdateTableGauge(anyString(), anyString(),
          eq(ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT), anyLong());
      // Column-level gauges still emit.
      verify(metrics).setOrUpdateTableGauge("testTable_OFFLINE", "metrics",
          ServerGauge.OPEN_STRUCT_LAST_SEGMENT_DENSE_KEY_COUNT, 2L);
    } finally {
      ServerMetrics.deregister();
    }
  }

  /// Flag on: every discovered key emits, including sparse ones.
  @Test
  public void testPerKeyGaugeCoversAllKeysWhenEnabled()
      throws Exception {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
          config(0.5, -1, null, true));
      // 'host' fills every doc (dense); 'rare' fills 2 of 10 (sparse).
      for (int d = 0; d < 10; d++) {
        s.add(d < 2 ? Map.of("host", "h", "rare", (long) d) : Map.of("host", "h"), d);
      }
      s.classify();
      s.seal();

      verify(metrics).setOrUpdateTableGauge("testTable_OFFLINE", OpenStructNaming.metricKey("metrics", "host"),
          ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT, 10L);
      verify(metrics).setOrUpdateTableGauge("testTable_OFFLINE", OpenStructNaming.metricKey("metrics", "rare"),
          ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT, 2L);
      verify(metrics).setOrUpdateTableGauge("testTable_OFFLINE", "metrics",
          ServerGauge.OPEN_STRUCT_LAST_SEGMENT_SPARSE_KEY_COUNT, 1L);
    } finally {
      ServerMetrics.deregister();
    }
  }

  /// A configured key cut from the dense set by the maxDenseKeys cap still reports its doc count -- a
  /// configured key that did not earn a materialized column is the case worth seeing. This is the branch
  /// where iterating the keys present in the segment diverges from iterating `_resolvedDenseKeys`.
  @Test
  public void testPerKeyGaugeCoversConfiguredKeyCutByMaxDenseKeysCap()
      throws Exception {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      // Both keys qualify on fill rate (100%), but the cap admits only one to the dense set.
      OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
          config(0.5, 1, Set.of("alpha", "beta")));
      for (int d = 0; d < 10; d++) {
        s.add(Map.of("alpha", (long) d, "beta", (long) d), d);
      }
      // Which of the two wins the single slot depends on Set.of iteration order, so assert the count
      // rather than the identity; the gauge assertions below hold either way.
      assertEquals(s.classify().size(), 1);
      s.seal();

      for (String key : List.of("alpha", "beta")) {
        verify(metrics).setOrUpdateTableGauge("testTable_OFFLINE",
            OpenStructNaming.materializedColumnName("metrics", key),
            ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT, 10L);
      }
    } finally {
      ServerMetrics.deregister();
    }
  }

  /// A configured key that never appears in the segment has no presence bitmap and is skipped rather
  /// than reported as 0, which would be indistinguishable from a key that was ingested into no docs.
  @Test
  public void testPerKeyGaugeSkipsConfiguredKeyAbsentFromSegment()
      throws Exception {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
          config(0.5, -1, Set.of("host", "never-ingested")));
      for (int d = 0; d < 10; d++) {
        s.add(Map.of("host", "h"), d);
      }
      s.classify();
      s.seal();

      verify(metrics, never()).setOrUpdateTableGauge(
          eq("testTable_OFFLINE"), eq(OpenStructNaming.metricKey("metrics", "never-ingested")),
          eq(ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT), anyLong());
      // The configured key that was ingested still reports, so the assertion above is about
      // absence, not about the gauge being off entirely.
      verify(metrics).setOrUpdateTableGauge("testTable_OFFLINE", OpenStructNaming.metricKey("metrics", "host"),
          ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT, 10L);
    } finally {
      ServerMetrics.deregister();
    }
  }

  /// Pins the emission path to `metricKey`, not `materializedColumnName`. OPEN_STRUCT keys come from
  /// user JSON, and a '"' in one is backslash-escaped by ObjectName.quote on the way to JMX, which stops
  /// the exported name matching the per-key scrape rule at all -- the key silently loses its column/key
  /// labels. Every other per-key test above uses a clean key, for which the two helpers agree, so this is
  /// the only case here that fails if the call site regresses.
  @Test
  public void testPerKeyGaugeEscapesKeyForMetricName()
      throws Exception {
    String rawKey = "promo\"code";
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
          config(0.5, -1, Set.of(rawKey)));
      for (int d = 0; d < 10; d++) {
        s.add(Map.of("host", "h", rawKey, (long) d), d);
      }
      s.classify();
      s.seal();

      verify(metrics).setOrUpdateTableGauge("testTable_OFFLINE",
          OpenStructNaming.metricKey("metrics", rawKey),
          ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT, 10L);
      // The unescaped form is what the old call site produced; it must not be emitted.
      verify(metrics, never()).setOrUpdateTableGauge(
          eq("testTable_OFFLINE"), eq(OpenStructNaming.materializedColumnName("metrics", rawKey)),
          eq(ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT), anyLong());
    } finally {
      ServerMetrics.deregister();
    }
  }

  @Test
  public void testNoIgnoredKeyDropsNotMetered()
      throws Exception {
    ServerMetrics metrics = mock(ServerMetrics.class);
    assertTrue(ServerMetrics.register(metrics), "another ServerMetrics is already registered");
    try {
      OpenStructColumnSplitter s = new OpenStructColumnSplitter(_tempDir, "metrics", "testTable_OFFLINE", spec(),
          config(0.5, -1, null));
      s.add(Map.of("clicks", 1L), 0);
      s.seal();

      verify(metrics, never()).addMeteredTableValue(anyString(), anyString(),
          eq(ServerMeter.OPEN_STRUCT_IGNORED_KEY_DROPS), anyLong());
    } finally {
      ServerMetrics.deregister();
    }
  }
}
