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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
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
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;


/// Pins consuming-vs-sealed semantics for an OPEN_STRUCT key absent from some docs: with null
/// handling off, both tiers must read the default null value for absent docs, so scans,
/// projections and MIN/MAX/DISTINCTCOUNT agree across the seal boundary.
public class OpenStructConsumingSealedParityTest {
  private static final String METRICS = "metrics";
  private static final String KEY = "views";
  private static final int NUM_DOCS = 10;
  // Key present in docs 0,1,2,5 — middle hole (3,4) and absent tail (6..9).
  private static final Map<Integer, Long> PRESENT = Map.of(0, 10L, 1, 20L, 2, 30L, 5, 60L);
  private static final File TMP_DIR =
      new File(FileUtils.getTempDirectory(), OpenStructConsumingSealedParityTest.class.getName());

  private PinotDataBufferMemoryManager _mm;

  @BeforeMethod
  public void setUp() {
    _mm = new DirectMemoryManager(getClass().getName());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mm.close();
    FileUtils.deleteDirectory(TMP_DIR);
  }

  private ComplexFieldSpec spec() {
    Map<String, FieldSpec> children = new HashMap<>();
    children.put(KEY, new DimensionFieldSpec(KEY, FieldSpec.DataType.LONG, true));
    children.put("host", new DimensionFieldSpec("host", FieldSpec.DataType.STRING, true));
    return new ComplexFieldSpec(METRICS, FieldSpec.DataType.OPEN_STRUCT, true, children);
  }

  private static Map<String, Object> metricsForDoc(int docId) {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put("host", "host-" + docId);
    Long viewsValue = PRESENT.get(docId);
    if (viewsValue != null) {
      metrics.put(KEY, viewsValue);
    }
    return metrics;
  }

  private static List<Object> readAllValues(DataSource dataSource) {
    ForwardIndexReader<?> fwd = dataSource.getForwardIndex();
    List<Object> values = new ArrayList<>(NUM_DOCS);
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      values.add(dataSource.getDictionary().get(fwd.getDictId(docId, null)));
    }
    return values;
  }

  @Test
  public void testConsumingMatchesSealedForPartiallyPresentKey()
      throws Exception {
    // --- Consuming side ---
    List<Object> consumingValues;
    MutableRoaringBitmap consumingDefaultDocIds;
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex(METRICS, "testTable_REALTIME", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, NUM_DOCS)) {
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        idx.index(docId, metricsForDoc(docId));
      }
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, NUM_DOCS);
      DataSource views = ds.getDataSource(KEY);
      assertNotNull(views);
      consumingValues = readAllValues(views);
      consumingDefaultDocIds = (MutableRoaringBitmap) views.getInvertedIndex().getDocIds(0);
    }

    // --- Sealed side (same rows through the offline build) ---
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testOpenStructParity")
        .addField(spec())
        .build();

    OpenStructIndexConfig osConfig =
        new OpenStructIndexConfig(false, null, -1, Set.of(KEY, "host"), 0.5, List.of(), null);

    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("open_struct", JsonUtils.objectToJsonNode(osConfig));
    FieldConfig metricsCfg = new FieldConfig.Builder(METRICS).withIndexes(indexes).build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testOpenStructParity")
        .setFieldConfigList(List.of(metricsCfg)).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TMP_DIR.getAbsolutePath());
    config.setSegmentName("testSegmentParity");

    List<GenericRow> rows = new ArrayList<>(NUM_DOCS);
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      GenericRow row = new GenericRow();
      row.putValue(METRICS, metricsForDoc(docId));
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    ImmutableSegment sealed = ImmutableSegmentLoader.load(driver.getOutputDirectory(), ReadMode.mmap);
    try {
      // Materialized OPEN_STRUCT children are grouped under the parent column; per-key access goes
      // through OpenStructDataSource#getDataSource, mirroring the mutable side.
      OpenStructDataSource sealedMetrics = (OpenStructDataSource) sealed.getDataSource(METRICS);
      DataSource sealedViews = sealedMetrics.getDataSource(KEY);
      assertNotNull(sealedViews);
      List<Object> sealedValues = readAllValues(sealedViews);

      // Projection/scan parity: identical values doc-by-doc, absent docs = LONG default.
      assertEquals(consumingValues, sealedValues);
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        Object expected = PRESENT.containsKey(docId) ? PRESENT.get(docId) : Long.MIN_VALUE;
        assertEquals(consumingValues.get(docId), expected, "docId " + docId);
      }

      // Aggregation parity with null handling off: MIN/MAX/DISTINCTCOUNT over scanned values,
      // pinned to independently computed constants on both tiers rather than only asserting the
      // tiers match each other (a bug shared by both tiers wouldn't be caught by cross-tier
      // equality alone). Values across docs 0-9: {10,20,30,MIN_VALUE,MIN_VALUE,60,MIN_VALUE x4}.
      assertEquals(min(consumingValues), Long.MIN_VALUE);
      assertEquals(min(sealedValues), Long.MIN_VALUE);
      assertEquals(max(consumingValues), 60L);
      assertEquals(max(sealedValues), 60L);
      assertEquals(new HashSet<>(consumingValues).size(), 5);
      assertEquals(new HashSet<>(sealedValues).size(), 5);

      // EQ/NOT_EQ parity: the consuming per-key inverted index folds absent docs into dictId 0's
      // postings, so its docIds must exactly match the docs where the sealed segment resolved the
      // default (derived from the already-read sealedValues, since sealed segments don't build an
      // inverted index for OPEN_STRUCT keys in this config).
      MutableRoaringBitmap expectedDefaultDocIds = new MutableRoaringBitmap();
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        if (sealedValues.get(docId).equals(Long.MIN_VALUE)) {
          expectedDefaultDocIds.add(docId);
        }
      }
      assertEquals(consumingDefaultDocIds, expectedDefaultDocIds);
    } finally {
      sealed.destroy();
    }
  }

  /// A key with no declared child spec whose value is a Map or a List: `OpenStructTypeInference`
  /// maps neither to a Pinot DataType, so both tiers must fall back to STRING and store the
  /// serialized form. Before this was aligned, the consuming tier dropped the entry while the
  /// sealed tier kept it, so the same row read differently either side of the seal boundary (and
  /// the REALTIME and OFFLINE halves of a hybrid table disagreed).
  @Test
  public void testConsumingMatchesSealedForUninferrableValues()
      throws Exception {
    String nestedKey = "payload";
    Map<Integer, Object> raw = new HashMap<>();
    raw.put(0, Map.of("b", 2, "a", 1));
    raw.put(1, List.of(1, 2, 3));
    for (int docId = 2; docId < NUM_DOCS; docId++) {
      raw.put(docId, "plain-" + docId);
    }

    // 'payload' is deliberately absent from the child specs so that type inference runs for it.
    ComplexFieldSpec inferredSpec = new ComplexFieldSpec(METRICS, FieldSpec.DataType.OPEN_STRUCT, true,
        Map.of("host", new DimensionFieldSpec("host", FieldSpec.DataType.STRING, true)));

    List<Object> consumingValues;
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex(METRICS, "testTable_REALTIME", inferredSpec,
        OpenStructIndexConfig.DEFAULT, _mm, NUM_DOCS)) {
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        idx.index(docId, Map.of("host", "host-" + docId, nestedKey, raw.get(docId)));
      }
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(inferredSpec, idx, NUM_DOCS);
      DataSource payload = ds.getDataSource(nestedKey);
      assertNotNull(payload, "uninferrable key must still be materialized on the consuming side");
      consumingValues = readAllValues(payload);
    }

    Schema schema = new Schema.SchemaBuilder().setSchemaName("testOpenStructInferParity")
        .addField(inferredSpec)
        .build();
    OpenStructIndexConfig osConfig =
        new OpenStructIndexConfig(false, null, -1, Set.of(nestedKey, "host"), 0.5, List.of(), null);
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("open_struct", JsonUtils.objectToJsonNode(osConfig));
    FieldConfig metricsCfg = new FieldConfig.Builder(METRICS).withIndexes(indexes).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testOpenStructInferParity")
        .setFieldConfigList(List.of(metricsCfg)).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TMP_DIR.getAbsolutePath());
    config.setSegmentName("testSegmentInferParity");

    List<GenericRow> rows = new ArrayList<>(NUM_DOCS);
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      GenericRow row = new GenericRow();
      row.putValue(METRICS, Map.of("host", "host-" + docId, nestedKey, raw.get(docId)));
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    ImmutableSegment sealed = ImmutableSegmentLoader.load(driver.getOutputDirectory(), ReadMode.mmap);
    try {
      OpenStructDataSource sealedMetrics = (OpenStructDataSource) sealed.getDataSource(METRICS);
      DataSource sealedPayload = sealedMetrics.getDataSource(nestedKey);
      assertNotNull(sealedPayload, "uninferrable key must still be materialized on the sealed side");
      List<Object> sealedValues = readAllValues(sealedPayload);

      assertEquals(consumingValues, sealedValues);
      // Pin the serialized form so a change in either tier's fallback is caught, not just divergence.
      // MapUtils.toString sorts by key, so the input order {"b","a"} must come back out as {"a","b"}.
      assertEquals(consumingValues.get(0), "{\"a\":1,\"b\":2}");
      assertEquals(consumingValues.get(1), "[1, 2, 3]");
      assertEquals(consumingValues.get(2), "plain-2");
    } finally {
      sealed.destroy();
    }
  }

  /// A key whose values are lists is a multi-value column on both tiers, and must read back element for element
  /// either side of the seal boundary. The two build it by different routes -- a mutable MV forward index of
  /// dictIds during consumption, the standard MV creators at seal -- so a divergence here would make the REALTIME
  /// and OFFLINE halves of a hybrid table disagree on the same row.
  @Test
  public void testConsumingMatchesSealedForMultiValueKey()
      throws Exception {
    String mvKey = "tags";
    ComplexFieldSpec mvSpec = new ComplexFieldSpec(METRICS, FieldSpec.DataType.OPEN_STRUCT, true, Map.of());
    OpenStructIndexConfig osConfig =
        new OpenStructIndexConfig(false, null, -1, Set.of(mvKey), 0.5, List.of(), null);

    List<List<Object>> consumingValues;
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex(METRICS, "testTable_REALTIME", mvSpec,
        osConfig, _mm, NUM_DOCS)) {
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        idx.index(docId, mvDoc(docId));
      }
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(mvSpec, idx, NUM_DOCS);
      DataSource tags = ds.getDataSource(mvKey);
      assertNotNull(tags, "a multi-value key must be materialized on the consuming side");
      // The planner reads shape off the metadata, not the forward index, so the two must agree.
      assertFalse(tags.getDataSourceMetadata().getFieldSpec().isSingleValueField(),
          "consuming metadata must report the key as multi-value");
      consumingValues = readAllMultiValues(tags);
    }

    Schema schema = new Schema.SchemaBuilder().setSchemaName("testOpenStructMvParity").addField(mvSpec).build();
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("open_struct", JsonUtils.objectToJsonNode(osConfig));
    FieldConfig metricsCfg = new FieldConfig.Builder(METRICS).withIndexes(indexes).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testOpenStructMvParity")
        .setFieldConfigList(List.of(metricsCfg)).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TMP_DIR.getAbsolutePath());
    config.setSegmentName("testSegmentMvParity");

    List<GenericRow> rows = new ArrayList<>(NUM_DOCS);
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      GenericRow row = new GenericRow();
      row.putValue(METRICS, mvDoc(docId));
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    ImmutableSegment sealed = ImmutableSegmentLoader.load(driver.getOutputDirectory(), ReadMode.mmap);
    try {
      OpenStructDataSource sealedMetrics = (OpenStructDataSource) sealed.getDataSource(METRICS);
      DataSource sealedTags = sealedMetrics.getDataSource(mvKey);
      assertNotNull(sealedTags, "a multi-value key must be materialized on the sealed side");
      assertFalse(sealedTags.getDataSourceMetadata().getFieldSpec().isSingleValueField(),
          "sealed metadata must report the key as multi-value");
      assertEquals(consumingValues, readAllMultiValues(sealedTags));

      // Pin the values themselves, not just cross-tier equality: a bug shared by both tiers would survive an
      // equality-only assertion. Docs 0-4 carry two tags, docs 5-9 carry none.
      for (int docId = 0; docId < 5; docId++) {
        assertEquals(consumingValues.get(docId), List.of("t" + docId, "shared"), "docId " + docId);
      }
      for (int docId = 5; docId < NUM_DOCS; docId++) {
        assertEquals(consumingValues.get(docId).size(), 1, "an absent doc holds one default element");
      }
    } finally {
      sealed.destroy();
    }
  }

  private static Map<String, Object> mvDoc(int docId) {
    Map<String, Object> document = new HashMap<>();
    if (docId < 5) {
      document.put("tags", List.of("t" + docId, "shared"));
    }
    document.put("host", "host-" + docId);
    return document;
  }

  @SuppressWarnings("unchecked")
  private static List<List<Object>> readAllMultiValues(DataSource dataSource)
      throws Exception {
    ForwardIndexReader<ForwardIndexReaderContext> fwd =
        (ForwardIndexReader<ForwardIndexReaderContext>) dataSource.getForwardIndex();
    assertFalse(fwd.isSingleValue(), "expected a multi-value forward index");
    List<List<Object>> values = new ArrayList<>(NUM_DOCS);
    int[] buffer = new int[MutableKeyColumn.MAX_NUM_MULTI_VALUES];
    try (ForwardIndexReaderContext context = fwd.createContext()) {
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        int length = fwd.getDictIdMV(docId, buffer, context);
        List<Object> row = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
          row.add(dataSource.getDictionary().get(buffer[i]));
        }
        values.add(row);
      }
    }
    return values;
  }

  /// A nested value addressed by its path key (`device.os`) must resolve identically either side of
  /// the seal boundary, and so must the container it was split out of. The two tiers reach the same
  /// key by different routes -- the consuming tier flattens into its own mutable columns, the sealed
  /// tier flattens into the splitter's dense/sparse classification -- so a divergence here would make
  /// the REALTIME and OFFLINE halves of a hybrid table disagree on the same row.
  @Test
  public void testConsumingMatchesSealedForNestedPathKey()
      throws Exception {
    String pathKey = "device.os";
    String containerKey = "device";
    ComplexFieldSpec nestedSpec = new ComplexFieldSpec(METRICS, FieldSpec.DataType.OPEN_STRUCT, true, Map.of());
    // maxNestedKeyDepth = 2 makes 'device.os' a key; 'device' is pinned dense so both tiers
    // materialize the container rather than one of them routing it to the sparse blob.
    OpenStructIndexConfig osConfig = new OpenStructIndexConfig(false, null, -1,
        Set.of(pathKey, containerKey), 0.5, List.of(), null, null, null, 2);

    List<Object> consumingValues;
    List<Object> consumingContainers;
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex(METRICS, "testTable_REALTIME", nestedSpec,
        osConfig, _mm, NUM_DOCS)) {
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        idx.index(docId, nestedDoc(docId));
      }
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(nestedSpec, idx, NUM_DOCS);
      DataSource os = ds.getDataSource(pathKey);
      assertNotNull(os, "nested leaf must be addressable by its path key on the consuming side");
      consumingValues = readAllValues(os);
      DataSource container = ds.getDataSource(containerKey);
      assertNotNull(container, "container must remain a key of its own");
      consumingContainers = readAllValues(container);
    }

    Schema schema = new Schema.SchemaBuilder().setSchemaName("testOpenStructNestedParity")
        .addField(nestedSpec)
        .build();
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("open_struct", JsonUtils.objectToJsonNode(osConfig));
    FieldConfig metricsCfg = new FieldConfig.Builder(METRICS).withIndexes(indexes).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testOpenStructNestedParity")
        .setFieldConfigList(List.of(metricsCfg)).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TMP_DIR.getAbsolutePath());
    config.setSegmentName("testSegmentNestedParity");

    List<GenericRow> rows = new ArrayList<>(NUM_DOCS);
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      GenericRow row = new GenericRow();
      row.putValue(METRICS, nestedDoc(docId));
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    ImmutableSegment sealed = ImmutableSegmentLoader.load(driver.getOutputDirectory(), ReadMode.mmap);
    try {
      OpenStructDataSource sealedMetrics = (OpenStructDataSource) sealed.getDataSource(METRICS);
      DataSource sealedOs = sealedMetrics.getDataSource(pathKey);
      assertNotNull(sealedOs, "nested leaf must be addressable by its path key on the sealed side");
      assertEquals(consumingValues, readAllValues(sealedOs));

      DataSource sealedContainer = sealedMetrics.getDataSource(containerKey);
      assertNotNull(sealedContainer);
      assertEquals(consumingContainers, readAllValues(sealedContainer));

      // Pin the values themselves, not just cross-tier equality: a bug shared by both tiers would
      // survive an equality-only assertion.
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        assertEquals(consumingValues.get(docId), docId % 2 == 0 ? "ios" : "android", "docId " + docId);
      }
      assertEquals(consumingContainers.get(0), "{\"os\":\"ios\"}");
    } finally {
      sealed.destroy();
    }
  }

  private static Map<String, Object> nestedDoc(int docId) {
    Map<String, Object> device = new HashMap<>();
    device.put("os", docId % 2 == 0 ? "ios" : "android");
    Map<String, Object> document = new HashMap<>();
    document.put("device", device);
    return document;
  }

  private static long min(List<Object> values) {
    return values.stream().mapToLong(v -> ((Number) v).longValue()).min().orElseThrow();
  }

  private static long max(List<Object> values) {
    return values.stream().mapToLong(v -> ((Number) v).longValue()).max().orElseThrow();
  }
}
