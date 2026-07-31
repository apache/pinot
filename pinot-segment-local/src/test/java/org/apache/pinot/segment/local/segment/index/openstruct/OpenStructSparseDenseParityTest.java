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
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
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
}
