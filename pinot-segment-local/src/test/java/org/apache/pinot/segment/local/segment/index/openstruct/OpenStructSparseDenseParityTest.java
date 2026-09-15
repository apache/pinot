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
package org.apache.pinot.segment.local.segment.index.openstruct;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Builds the same dataset as both fully-dense (all materialized) and fully-sparse (all in blob),
/// loads the segments, and asserts projection/null-bitmap/filter parity between the two tiers.
public class OpenStructSparseDenseParityTest {
  private static final String METRICS = "metrics";
  private static final int NUM_DOCS = 200;
  private static final File TMP_DIR =
      new File(FileUtils.getTempDirectory(), OpenStructSparseDenseParityTest.class.getName());

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TMP_DIR);
    TMP_DIR.mkdirs();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TMP_DIR);
  }

  private static ComplexFieldSpec spec() {
    Map<String, FieldSpec> children = new HashMap<>();
    children.put("region", new DimensionFieldSpec("region", FieldSpec.DataType.STRING, true));
    children.put("latencyMs", new DimensionFieldSpec("latencyMs", FieldSpec.DataType.LONG, true));
    // Declared STRING, only populated by testNonScalarValuesStoredAsJsonStrings.
    children.put("attrs", new DimensionFieldSpec("attrs", FieldSpec.DataType.STRING, true));
    children.put("tags", new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, true));
    // Declared INT, only populated by testTypeMismatchedValueDroppedFromBothTiers.
    children.put("count", new DimensionFieldSpec("count", FieldSpec.DataType.INT, true));
    return new ComplexFieldSpec(METRICS, FieldSpec.DataType.OPEN_STRUCT, true, children);
  }

  private static Map<String, Object> metricsForDoc(int docId) {
    Map<String, Object> m = new HashMap<>();
    if (docId % 3 == 0) {
      m.put("region", docId % 6 == 0 ? "us" : "eu");
    }
    if (docId % 5 == 0) {
      m.put("latencyMs", (long) docId);
    }
    if (docId % 10 == 0 && docId > 0) {
      m.put("freeform", 42);
    }
    return m;
  }

  private ImmutableSegment buildSegment(OpenStructIndexConfig osConfig, String segmentName,
      List<GenericRow> rows)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testSparseDenseParity")
        .addField(spec())
        .build();
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("open_struct", JsonUtils.objectToJsonNode(osConfig));
    FieldConfig metricsCfg = new FieldConfig.Builder(METRICS).withIndexes(indexes).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testSparseDenseParity")
        .setFieldConfigList(List.of(metricsCfg))
        .setNullHandlingEnabled(true)
        .build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    File outDir = new File(TMP_DIR, segmentName);
    config.setOutDir(outDir.getAbsolutePath());
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return ImmutableSegmentLoader.load(driver.getOutputDirectory(), ReadMode.mmap);
  }

  private ImmutableSegment buildSegment(OpenStructIndexConfig osConfig, String segmentName)
      throws Exception {
    List<GenericRow> rows = new ArrayList<>(NUM_DOCS);
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      GenericRow row = new GenericRow();
      row.putValue(METRICS, metricsForDoc(docId));
      rows.add(row);
    }
    return buildSegment(osConfig, segmentName, rows);
  }

  @Test
  public void testProjectionNullBitmapAndTypeParity()
      throws Exception {
    OpenStructIndexConfig denseConfig =
        new OpenStructIndexConfig(false, null, -1, null, 0.0, null, null);
    OpenStructIndexConfig sparseConfig =
        new OpenStructIndexConfig(false, null, 0, null, null, null, null);

    ImmutableSegment dense = buildSegment(denseConfig, "dense");
    ImmutableSegment sparse = buildSegment(sparseConfig, "sparse");
    try {
      OpenStructDataSource denseDs = (OpenStructDataSource) dense.getDataSource(METRICS);
      OpenStructDataSource sparseDs = (OpenStructDataSource) sparse.getDataSource(METRICS);

      assertForwardValuesParity(denseDs, sparseDs, "region", FieldSpec.DataType.STRING);
      assertNullBitmapParity(denseDs, sparseDs, "region");
      assertForwardValuesParity(denseDs, sparseDs, "latencyMs", FieldSpec.DataType.LONG);
      assertNullBitmapParity(denseDs, sparseDs, "latencyMs");

      // Undeclared key: dense infers INT, sparse defaults to STRING.
      DataSource denseFreeform = denseDs.getDataSource("freeform");
      DataSource sparseFreeform = sparseDs.getDataSource("freeform");
      assertNotNull(denseFreeform);
      assertNotNull(sparseFreeform);
      assertEquals(denseFreeform.getDataSourceMetadata().getDataType().getStoredType(), FieldSpec.DataType.INT);
      assertEquals(sparseFreeform.getDataSourceMetadata().getDataType().getStoredType(), FieldSpec.DataType.STRING);

      @SuppressWarnings("rawtypes")
      ForwardIndexReader denseFwd = denseFreeform.getForwardIndex();
      @SuppressWarnings("rawtypes")
      ForwardIndexReader sparseFwd = sparseFreeform.getForwardIndex();
      ForwardIndexReaderContext denseCtx = denseFwd.createContext();
      ForwardIndexReaderContext sparseCtx = sparseFwd.createContext();
      if (denseFreeform.getDictionary() != null) {
        assertEquals(denseFreeform.getDictionary().get(denseFwd.getDictId(10, denseCtx)), 42);
      } else {
        assertEquals(denseFwd.getInt(10, denseCtx), 42);
      }
      assertEquals(sparseFwd.getString(10, sparseCtx), "42");
    } finally {
      dense.destroy();
      sparse.destroy();
    }
  }

  private void assertForwardValuesParity(OpenStructDataSource denseDs, OpenStructDataSource sparseDs,
      String key, FieldSpec.DataType storedType) {
    DataSource denseKey = denseDs.getDataSource(key);
    DataSource sparseKey = sparseDs.getDataSource(key);
    assertNotNull(denseKey, "dense DataSource for " + key);
    assertNotNull(sparseKey, "sparse DataSource for " + key);

    @SuppressWarnings("rawtypes")
    ForwardIndexReader denseFwd = denseKey.getForwardIndex();
    @SuppressWarnings("rawtypes")
    ForwardIndexReader sparseFwd = sparseKey.getForwardIndex();
    ForwardIndexReaderContext denseCtx = denseFwd.createContext();
    ForwardIndexReaderContext sparseCtx = sparseFwd.createContext();
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      Object denseVal = readValue(denseKey, denseFwd, docId, storedType, denseCtx);
      Object sparseVal = readValue(sparseKey, sparseFwd, docId, storedType, sparseCtx);
      assertEquals(sparseVal, denseVal, key + " docId=" + docId);
    }
  }

  @SuppressWarnings("rawtypes")
  private static Object readValue(DataSource ds, ForwardIndexReader fwd, int docId,
      FieldSpec.DataType storedType, ForwardIndexReaderContext ctx) {
    if (ds.getDictionary() != null) {
      return ds.getDictionary().get(fwd.getDictId(docId, ctx));
    }
    switch (storedType) {
      case INT:
        return fwd.getInt(docId, ctx);
      case LONG:
        return fwd.getLong(docId, ctx);
      case FLOAT:
        return fwd.getFloat(docId, ctx);
      case DOUBLE:
        return fwd.getDouble(docId, ctx);
      case STRING:
        return fwd.getString(docId, ctx);
      default:
        throw new IllegalArgumentException("Unsupported type: " + storedType);
    }
  }

  private void assertNullBitmapParity(OpenStructDataSource denseDs, OpenStructDataSource sparseDs,
      String key) {
    NullValueVectorReader denseNull = denseDs.getDataSource(key).getNullValueVector();
    NullValueVectorReader sparseNull = sparseDs.getDataSource(key).getNullValueVector();
    assertNotNull(denseNull, "dense null vector for " + key);
    assertNotNull(sparseNull, "sparse null vector for " + key);
    assertEquals(sparseNull.getNullBitmap(), denseNull.getNullBitmap(), key + " null bitmap");
  }

  /// A Map- or List-valued key can never reach the sparse blob as a JSON container.
  /// `OpenStructColumnSplitter#addMap` coerces every value to the key's stored type before it is
  /// serialized, and for a STRING key that coercion is `PinotDataType.STRING.convert`, i.e.
  /// `sourceType.toString(value)` — so the blob holds `{"k":"<text>"}`, never `{"k":{...}}`.
  ///
  /// The assertion is discriminating: a container node would make `JsonNode#asText()` return the
  /// empty string, so reading back the exact serialized text proves the stored form is a JSON
  /// string. That is also what the opt-in sparse JSON index indexes, which is why the index fast
  /// path and the scan cannot disagree for non-scalar values.
  @Test
  public void testNonScalarValuesStoredAsJsonStrings()
      throws Exception {
    // Index on, so the assertions below cover both the scan and the index view of the same blob.
    OpenStructIndexConfig sparseConfig =
        new OpenStructIndexConfig(false, null, 0, null, null, null, true);
    int numDocs = 4;

    // Insertion order b,a — MapUtils.toString sorts keys, so the stored text must come back sorted.
    Map<String, Object> attrs = new LinkedHashMap<>();
    attrs.put("b", 2);
    attrs.put("a", 1);

    List<GenericRow> rows = new ArrayList<>(numDocs);
    for (int docId = 0; docId < numDocs; docId++) {
      GenericRow row = new GenericRow();
      Map<String, Object> m = new HashMap<>();
      m.put("attrs", attrs);
      m.put("tags", List.of(1, 2));
      row.putValue(METRICS, m);
      rows.add(row);
    }

    ImmutableSegment sparse = buildSegment(sparseConfig, "sparse-non-scalar", rows);
    try {
      OpenStructDataSource ds = (OpenStructDataSource) sparse.getDataSource(METRICS);
      assertFalse(ds.isMaterialized("attrs"), "attrs should be sparse");
      assertFalse(ds.isMaterialized("tags"), "tags should be sparse");

      // Map → PinotDataType.MAP.toString → MapUtils.toString → compact JSON, keys sorted.
      assertSparseStringValue(ds, "attrs", "{\"a\":1,\"b\":2}", numDocs);
      // List → PinotDataType.OBJECT.toString → Java List#toString, which is not JSON. Pinned as the
      // current stored form, not endorsed: changing it is safe from a tier-skew angle, since
      // OpenStructColumnSplitter hands one serialized string to both the forward and JSON index
      // creators, but it does change segment contents, so the test should fail loudly if it moves.
      assertSparseStringValue(ds, "tags", "[1, 2]", numDocs);

      // The index is built from that same serialized text, so an EQ on it must match every doc.
      // This is the half that would break if a container ever reached the blob: the scan would
      // read "" from asText() while the index flattened the container into per-element postings.
      assertIndexMatchesAll(ds, "attrs", "{\"a\":1,\"b\":2}", numDocs);
      assertIndexMatchesAll(ds, "tags", "[1, 2]", numDocs);
    } finally {
      sparse.destroy();
    }
  }

  private static void assertIndexMatchesAll(OpenStructDataSource ds, String key, String value, int numDocs) {
    JsonIndexReader jsonIndex = ds.getSparseJsonIndex();
    assertNotNull(jsonIndex, "sparse JSON index");
    ImmutableRoaringBitmap matching = jsonIndex.getMatchingDocIds(
        FilterContext.forPredicate(new EqPredicate(ExpressionContext.forIdentifier(key), value)));
    assertEquals(matching.getCardinality(), numDocs, "index EQ on the stored text for " + key);
  }

  private void assertSparseStringValue(OpenStructDataSource ds, String key, String expected, int numDocs) {
    DataSource keyDs = ds.getDataSource(key);
    assertNotNull(keyDs, "sparse DataSource for " + key);
    assertEquals(keyDs.getDataSourceMetadata().getDataType().getStoredType(), FieldSpec.DataType.STRING);

    @SuppressWarnings("rawtypes")
    ForwardIndexReader fwd = keyDs.getForwardIndex();
    ForwardIndexReaderContext ctx = fwd.createContext();
    for (int docId = 0; docId < numDocs; docId++) {
      String actual = fwd.getString(docId, ctx);
      assertFalse(actual.isEmpty(),
          key + " read back empty at docId=" + docId + " — the blob holds a JSON container, not a string");
      assertEquals(actual, expected, key + " docId=" + docId);
    }
  }

  /// Same config (fillRate=0.5), different data: "region" is dense in seg1 (present on 90% of docs)
  /// and sparse in seg2 (present on 10%). Both segments must return the same values for the
  /// overlapping docs, same defaults for absent docs, and same declared type (STRING).
  @Test
  public void testSameKeyCrossSegmentDenseAndSparse()
      throws Exception {
    OpenStructIndexConfig config = new OpenStructIndexConfig(false, null, -1, null, 0.5, null, null);
    int numDocs = 100;

    // Segment 1: "region" on 90% of docs → dense
    List<GenericRow> seg1Rows = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      GenericRow row = new GenericRow();
      Map<String, Object> m = new HashMap<>();
      if (i % 10 != 0) {
        m.put("region", "val-" + (i % 5));
      }
      row.putValue(METRICS, m);
      seg1Rows.add(row);
    }

    // Segment 2: "region" on 10% of docs → sparse
    List<GenericRow> seg2Rows = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      GenericRow row = new GenericRow();
      Map<String, Object> m = new HashMap<>();
      if (i % 10 == 0) {
        m.put("region", "val-" + (i % 5));
      }
      row.putValue(METRICS, m);
      seg2Rows.add(row);
    }

    ImmutableSegment seg1 = buildSegment(config, "seg1-dense-region", seg1Rows);
    ImmutableSegment seg2 = buildSegment(config, "seg2-sparse-region", seg2Rows);
    try {
      OpenStructDataSource ds1 = (OpenStructDataSource) seg1.getDataSource(METRICS);
      OpenStructDataSource ds2 = (OpenStructDataSource) seg2.getDataSource(METRICS);

      assertTrue(ds1.isMaterialized("region"), "seg1 should have region as dense");
      assertFalse(ds2.isMaterialized("region"), "seg2 should have region as sparse");

      DataSource regionDs1 = ds1.getDataSource("region");
      DataSource regionDs2 = ds2.getDataSource("region");
      assertNotNull(regionDs1);
      assertNotNull(regionDs2);

      // Both report STRING
      assertEquals(regionDs1.getDataSourceMetadata().getDataType(), FieldSpec.DataType.STRING);
      assertEquals(regionDs2.getDataSourceMetadata().getDataType(), FieldSpec.DataType.STRING);

      // Values match expected: present docs get their value, absent docs get the default.
      @SuppressWarnings("rawtypes")
      ForwardIndexReader fwd1 = regionDs1.getForwardIndex();
      @SuppressWarnings("rawtypes")
      ForwardIndexReader fwd2 = regionDs2.getForwardIndex();
      ForwardIndexReaderContext ctx1 = fwd1.createContext();
      ForwardIndexReaderContext ctx2 = fwd2.createContext();

      String defaultVal = FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
      for (int i = 0; i < numDocs; i++) {
        Object v1 = readValue(regionDs1, fwd1, i, FieldSpec.DataType.STRING, ctx1);
        Object v2 = readValue(regionDs2, fwd2, i, FieldSpec.DataType.STRING, ctx2);
        // seg1: present when i%10 != 0
        assertEquals(v1, i % 10 != 0 ? "val-" + (i % 5) : defaultVal, "seg1 doc " + i);
        // seg2: present when i%10 == 0
        assertEquals(v2, i % 10 == 0 ? "val-" + (i % 5) : defaultVal, "seg2 doc " + i);
      }
    } finally {
      seg1.destroy();
      seg2.destroy();
    }
  }

  /// A value that cannot be coerced to its key's declared type never reaches either tier.
  /// `OpenStructColumnSplitter#addMap` catches the `PinotDataType.convert` failure, rolls the doc
  /// back out of the key's presence bitmap and stores nothing — so `writeDenseKeyColumn` and
  /// `writeSparseJsonColumn` both see that doc as ABSENT. The drop must therefore be invisible to
  /// the tier: null in both null vectors, and the INT default from both forward readers.
  @Test
  public void testTypeMismatchedValueDroppedFromBothTiers()
      throws Exception {
    OpenStructIndexConfig denseConfig =
        new OpenStructIndexConfig(false, null, -1, null, 0.0, null, null);
    OpenStructIndexConfig sparseConfig =
        new OpenStructIndexConfig(false, null, 0, null, null, null, null);

    // "count" is declared INT. Even docs supply a well-typed int; odd docs supply "abc", which
    // PinotDataType.INTEGER.convert cannot parse.
    List<GenericRow> rows = new ArrayList<>(NUM_DOCS);
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      GenericRow row = new GenericRow();
      Map<String, Object> m = new HashMap<>();
      m.put("count", docId % 2 == 0 ? docId : "abc");
      row.putValue(METRICS, m);
      rows.add(row);
    }

    ImmutableSegment dense = buildSegment(denseConfig, "dense-coercion-failure", rows);
    ImmutableSegment sparse = buildSegment(sparseConfig, "sparse-coercion-failure", rows);
    try {
      OpenStructDataSource denseDs = (OpenStructDataSource) dense.getDataSource(METRICS);
      OpenStructDataSource sparseDs = (OpenStructDataSource) sparse.getDataSource(METRICS);
      assertTrue(denseDs.isMaterialized("count"), "count should be dense");
      assertFalse(sparseDs.isMaterialized("count"), "count should be sparse");

      assertForwardValuesParity(denseDs, sparseDs, "count", FieldSpec.DataType.INT);
      assertNullBitmapParity(denseDs, sparseDs, "count");

      // Pin the absolute answer, not just dense == sparse: well-typed docs keep their value,
      // dropped docs read as null plus the INT default in both tiers.
      DataSource denseKey = denseDs.getDataSource("count");
      DataSource sparseKey = sparseDs.getDataSource("count");
      @SuppressWarnings("rawtypes")
      ForwardIndexReader denseFwd = denseKey.getForwardIndex();
      @SuppressWarnings("rawtypes")
      ForwardIndexReader sparseFwd = sparseKey.getForwardIndex();
      ForwardIndexReaderContext denseCtx = denseFwd.createContext();
      ForwardIndexReaderContext sparseCtx = sparseFwd.createContext();
      NullValueVectorReader denseNull = denseKey.getNullValueVector();
      NullValueVectorReader sparseNull = sparseKey.getNullValueVector();
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        boolean dropped = docId % 2 != 0;
        Object expected = dropped ? FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT : docId;
        assertEquals(readValue(denseKey, denseFwd, docId, FieldSpec.DataType.INT, denseCtx), expected,
            "dense count docId=" + docId);
        assertEquals(readValue(sparseKey, sparseFwd, docId, FieldSpec.DataType.INT, sparseCtx), expected,
            "sparse count docId=" + docId);
        assertEquals(denseNull.isNull(docId), dropped, "dense null docId=" + docId);
        assertEquals(sparseNull.isNull(docId), dropped, "sparse null docId=" + docId);
      }
    } finally {
      dense.destroy();
      sparse.destroy();
    }
  }

  /// The opt-in sparse JSON index must change only how a sparse-key filter is computed, never the
  /// answer. Same dataset built twice — index on and off — and for every EQ / IN shape the
  /// index-derived doc set must equal a straight scan of the sparse key's forward reader, which is
  /// what the filter path falls back to when the index is absent.
  ///
  /// Only the positive shapes are checked. `MapFilterOperator#trySparseJsonIndex` answers NOT_EQ /
  /// NOT_IN by wrapping the same positive `JsonMatchFilterOperator` in a `NotFilterOperator`, so
  /// both sides here would complement over the identical `[0, numDocs)` universe and the assertion
  /// could not fail independently of its positive counterpart. The operator lives in pinot-core and
  /// cannot be driven from this module; its translation, negation and refusal guards are covered by
  /// `MapFilterOperatorOpenStructTest` against a mocked index. This test is the other half — a real
  /// index, checked against a real scan.
  @Test
  public void testSparseJsonIndexAgreesWithScan()
      throws Exception {
    OpenStructIndexConfig indexOn = new OpenStructIndexConfig(false, null, 0, null, null, null, true);
    OpenStructIndexConfig indexOff = new OpenStructIndexConfig(false, null, 0, null, null, null, false);

    ImmutableSegment withIndex = buildSegment(indexOn, "sparse-json-index-on");
    ImmutableSegment withoutIndex = buildSegment(indexOff, "sparse-json-index-off");
    try {
      OpenStructDataSource onDs = (OpenStructDataSource) withIndex.getDataSource(METRICS);
      OpenStructDataSource offDs = (OpenStructDataSource) withoutIndex.getDataSource(METRICS);

      assertFalse(onDs.isMaterialized("region"), "region should be sparse");
      assertNotNull(onDs.getSparseJsonIndex(), "sparseJsonIndex=true should build the index");
      assertNull(offDs.getSparseJsonIndex(), "the sparse JSON index is opt-in");

      // Identical stored values either way — the index only changes how they are searched.
      assertForwardValuesParity(onDs, offDs, "region", FieldSpec.DataType.STRING);

      // "nope" is absent from the data, exercising the missing-dictId branch on both sides.
      List<List<String>> valueSets =
          List.of(List.of("us"), List.of("nope"), List.of("us", "eu"), List.of("eu", "nope"));
      // Guard against a vacuous pass, where both sides agree only because both match nothing.
      assertFalse(jsonIndexMatches(onDs, "region", List.of("us")).isEmpty(), "EQ 'us' matched nothing");
      for (List<String> values : valueSets) {
        assertEquals(jsonIndexMatches(onDs, "region", values), scanMatches(offDs, "region", values),
            (values.size() == 1 ? "EQ" : "IN") + values);
      }

      // The one shape where the two legitimately disagree, and the reason MapFilterOperator refuses
      // the index for it. An absent key reads back as the schema's defaultNullValue — for STRING
      // that is the literal "null", the same sentinel every Pinot STRING column uses, since a
      // forward index has no null slot. So the scan matches exactly the docs missing the key, while
      // the index has no posting for that literal. Real nullability lives in the null vector.
      List<String> sentinel = List.of(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
      List<Integer> absentDocIds = new ArrayList<>();
      for (int docId : offDs.getDataSource("region").getNullValueVector().getNullBitmap().toArray()) {
        absentDocIds.add(docId);
      }
      assertEquals(scanMatches(offDs, "region", sentinel), absentDocIds,
          "EQ on the sentinel makes the scan match exactly the absent docs");
      assertTrue(jsonIndexMatches(onDs, "region", sentinel).isEmpty(),
          "the index must not match the absent-value sentinel");
    } finally {
      withIndex.destroy();
      withoutIndex.destroy();
    }
  }

  private static List<Integer> jsonIndexMatches(OpenStructDataSource ds, String key, List<String> values) {
    JsonIndexReader jsonIndex = ds.getSparseJsonIndex();
    assertNotNull(jsonIndex, "sparse JSON index");
    ExpressionContext lhs = ExpressionContext.forIdentifier(key);
    Predicate predicate =
        values.size() == 1 ? new EqPredicate(lhs, values.get(0)) : new InPredicate(lhs, values);
    ImmutableRoaringBitmap matching = jsonIndex.getMatchingDocIds(FilterContext.forPredicate(predicate));
    List<Integer> docIds = new ArrayList<>(matching.getCardinality());
    for (int docId : matching.toArray()) {
      docIds.add(docId);
    }
    return docIds;
  }

  private static List<Integer> scanMatches(OpenStructDataSource ds, String key, List<String> values) {
    DataSource keyDs = ds.getDataSource(key);
    assertNotNull(keyDs, "sparse DataSource for " + key);
    @SuppressWarnings("rawtypes")
    ForwardIndexReader fwd = keyDs.getForwardIndex();
    ForwardIndexReaderContext ctx = fwd.createContext();
    List<Integer> docIds = new ArrayList<>();
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      if (values.contains(fwd.getString(docId, ctx))) {
        docIds.add(docId);
      }
    }
    return docIds;
  }
}
