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
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex(METRICS, spec(),
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

  private static long min(List<Object> values) {
    return values.stream().mapToLong(v -> ((Number) v).longValue()).min().orElseThrow();
  }

  private static long max(List<Object> values) {
    return values.stream().mapToLong(v -> ((Number) v).longValue()).max().orElseThrow();
  }
}
