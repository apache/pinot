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
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.datasource.NullDataSource;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class ImmutableOpenStructDataSourceTest {

  private static ComplexFieldSpec openStructSpec(String name) {
    ComplexFieldSpec spec = new ComplexFieldSpec(name, DataType.OPEN_STRUCT, true, Map.of());
    return spec;
  }

  @Test
  public void testGetDataSourceReturnsPerKeyDataSource() {
    DataSource clicksDs = mock(DataSource.class);
    DataSource sparseDs = mock(DataSource.class);
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of("clicks", clicksDs),
        sparseDs,
        meta,
        container,
        null);

    assertSame(ds.getDataSource("clicks"), clicksDs);
    // Absent key with mock sparse (no real forward index) resolves to an all-null STRING source
    DataSource unknownDs = ds.getDataSource("unknown");
    assertTrue(unknownDs instanceof NullDataSource);
    assertEquals(unknownDs.getDataSourceMetadata().getDataType(), DataType.STRING);
  }

  /// A key absent from the whole segment is described by its declared child field spec, including a custom default
  /// null value.
  @Test
  public void testAbsentDeclaredKeyReadsDeclaredDefault() {
    ComplexFieldSpec spec = new ComplexFieldSpec("event", DataType.OPEN_STRUCT, true,
        Map.of("score", new DimensionFieldSpec("score", DataType.STRING, true, "N/A")));
    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(spec, Map.of(), null, 7, null);

    DataSource scoreDs = ds.getDataSource("score");
    assertTrue(scoreDs instanceof NullDataSource);
    assertEquals(scoreDs.getDataSourceMetadata().getDataType(), DataType.STRING);
    assertEquals(scoreDs.getDataSourceMetadata().getNumDocs(), 7);
    assertEquals(scoreDs.getDictionary().get(0), "N/A");
    assertEquals(scoreDs.getNullValueVector().getNullBitmap().getCardinality(), 7);
  }

  @Test
  public void testIsMaterializedTrueOnlyForMaterializedKeys() {
    DataSource clicksDs = mock(DataSource.class);
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of("clicks", clicksDs),
        null,
        meta,
        container,
        null);

    assertTrue(ds.isMaterialized("clicks"));
    assertFalse(ds.isMaterialized("absent"));
  }

  @Test
  public void testIsFullyMaterializedTrueWhenNoSparse() {
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of(),
        null,
        meta,
        container,
        null);

    assertTrue(ds.isFullyMaterialized());
  }

  @Test
  public void testIsFullyMaterializedFalseWhenSparsePresent() {
    DataSource sparseDs = mock(DataSource.class);
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of(),
        sparseDs,
        meta,
        container,
        null);

    assertFalse(ds.isFullyMaterialized());
  }

  @Test
  public void testGetFieldSpecReturnsOpenStructView() {
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of(),
        null,
        meta,
        container,
        null);

    ComplexFieldSpec fieldSpec = ds.getFieldSpec();
    assertNotNull(fieldSpec);
    assertEquals(fieldSpec.getName(), "event");
  }

  @Test
  public void testGetDataSourceMetadataByKeyReturnsDelegated() {
    DataSource clicksDs = mock(DataSource.class);
    DataSourceMetadata clicksMeta = mock(DataSourceMetadata.class);
    DataSourceMetadata topMeta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    org.mockito.Mockito.when(clicksDs.getDataSourceMetadata()).thenReturn(clicksMeta);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of("clicks", clicksDs),
        null,
        topMeta,
        container,
        null);

    assertSame(ds.getDataSourceMetadata("clicks"), clicksMeta);
    assertEquals(ds.getDataSourceMetadata("absent").getDataType(), DataType.STRING);
  }

  @Test
  public void testGetDataSourcesReturnsPerKeyMap() {
    DataSource clicksDs = mock(DataSource.class);
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);
    Map<String, DataSource> perKeyMap = Map.of("clicks", clicksDs);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        perKeyMap,
        null,
        meta,
        container,
        null);

    assertEquals(ds.getDataSources(), perKeyMap);
  }

  @Test
  public void testTopLevelMetadataAndContainerDelegated() {
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of(),
        null,
        meta,
        container,
        null);

    assertSame(ds.getDataSourceMetadata(), meta);
    assertSame(ds.getIndexContainer(), container);
  }

  @Test
  public void testConvenienceConstructorSynthesizesMetadata() {
    DataSource clicksDs = mock(DataSource.class);
    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of("clicks", clicksDs),
        null,
        42,
        null);

    DataSourceMetadata meta = ds.getDataSourceMetadata();
    assertNotNull(meta);
    assertEquals(meta.getNumDocs(), 42);
    assertEquals(meta.getNumValues(), 42);
    assertEquals(meta.getFieldSpec().getDataType(), DataType.OPEN_STRUCT);
    assertNotNull(ds.getIndexContainer());
    assertTrue(ds.isFullyMaterialized());
    assertSame(ds.getDataSource("clicks"), clicksDs);
  }

  private static DataSource mockSparseDataSource(String[] blobs) {
    DataSource ds = mock(DataSource.class);
    doReturn(new FakeStringForwardIndex(blobs)).when(ds).getForwardIndex();
    doReturn(FakeStringForwardIndex.nullVector(blobs)).when(ds).getNullValueVector();
    return ds;
  }

  /// Dense child columns are written dictionary-encoded (`OpenStructColumnSplitter#writeColumnIndexes` builds the
  /// dictionary and the forward index from the same `useDictionary` flag), so the mock must report
  /// `isDictionaryEncoded()` — not merely expose a dictionary — to match a real segment.
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static DataSource mockDenseDataSource(DataType storedType, Object valueAtDoc0, boolean nullAtDoc1) {
    DataSource ds = mock(DataSource.class);
    ForwardIndexReader fwdReader = mock(ForwardIndexReader.class);
    ForwardIndexReaderContext ctx = mock(ForwardIndexReaderContext.class);
    when(fwdReader.createContext()).thenReturn(ctx);
    when(fwdReader.getStoredType()).thenReturn(storedType);
    when(fwdReader.isSingleValue()).thenReturn(true);
    when(fwdReader.isDictionaryEncoded()).thenReturn(true);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(storedType);
    when(fwdReader.getDictId(eq(0), eq(ctx))).thenReturn(42);
    when(dictionary.get(42)).thenReturn(valueAtDoc0);
    when(ds.getDictionary()).thenReturn(dictionary);

    when(ds.getForwardIndex()).thenReturn(fwdReader);

    NullValueVectorReader nullReader = mock(NullValueVectorReader.class);
    when(nullReader.isNull(0)).thenReturn(false);
    when(nullReader.isNull(1)).thenReturn(nullAtDoc1);
    when(ds.getNullValueVector()).thenReturn(nullReader);

    return ds;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static DataSource mockSparseDataSource(String jsonAtDoc0, boolean nullAtDoc1) {
    DataSource ds = mock(DataSource.class);
    ForwardIndexReader fwdReader = mock(ForwardIndexReader.class);
    ForwardIndexReaderContext ctx = mock(ForwardIndexReaderContext.class);
    when(fwdReader.createContext()).thenReturn(ctx);
    when(fwdReader.getStoredType()).thenReturn(DataType.STRING);
    when(fwdReader.isSingleValue()).thenReturn(true);
    when(fwdReader.isDictionaryEncoded()).thenReturn(false);
    when(fwdReader.getString(eq(0), eq(ctx))).thenReturn(jsonAtDoc0);
    when(fwdReader.getString(eq(1), eq(ctx))).thenReturn("");
    when(ds.getForwardIndex()).thenReturn(fwdReader);
    when(ds.getDictionary()).thenReturn(null);

    NullValueVectorReader nullReader = mock(NullValueVectorReader.class);
    when(nullReader.isNull(0)).thenReturn(false);
    when(nullReader.isNull(1)).thenReturn(nullAtDoc1);
    when(ds.getNullValueVector()).thenReturn(nullReader);

    return ds;
  }

  @Test
  public void testManifestKeyGetsVirtualSparseDataSource() {
    String[] blobs = {"{\"region\":\"us\"}", null};
    DataSource sparseDs = mockSparseDataSource(blobs);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of(),
        sparseDs,
        blobs.length,
        List.of("region"));

    DataSource regionDs = ds.getDataSource("region");
    assertNotNull(regionDs);
    assertTrue(regionDs instanceof SparseKeyDataSource);
    assertEquals(regionDs.getForwardIndex().getString(0, null), "us");
    assertSame(ds.getDataSource("region"), regionDs);
    assertTrue(ds.getDataSource("nope") instanceof NullDataSource);
    assertFalse(ds.isMaterialized("region"));
    assertFalse(ds.isFullyMaterialized());
  }

  @Test
  public void testNoManifestFallsBackToVirtualForAnyKey() {
    String[] blobs = {"{\"region\":\"us\"}", null};
    DataSource sparseDs = mockSparseDataSource(blobs);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of(),
        sparseDs,
        blobs.length,
        null);

    DataSource anyDs = ds.getDataSource("anything");
    assertNotNull(anyDs);
    assertTrue(anyDs instanceof SparseKeyDataSource);
  }

  @Test
  public void testDeclaredChildSpecDrivesVirtualType() {
    String[] blobs = {"{\"latencyMs\":42}", null};
    DataSource sparseDs = mockSparseDataSource(blobs);

    ComplexFieldSpec spec = new ComplexFieldSpec("event", DataType.OPEN_STRUCT, true,
        Map.of("latencyMs", new DimensionFieldSpec("latencyMs", DataType.LONG, true)));

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        spec,
        Map.of(),
        sparseDs,
        blobs.length,
        List.of("latencyMs"));

    DataSource latDs = ds.getDataSource("latencyMs");
    assertNotNull(latDs);
    assertEquals(latDs.getDataSourceMetadata().getFieldSpec().getDataType(), DataType.LONG);
    assertEquals(latDs.getForwardIndex().getLong(0, null), 42L);
  }

  @Test
  public void testGetSparseJsonIndexDelegatesToSparseDataSource() {
    String[] blobs = {"{\"x\":1}", null};
    DataSource sparseDs = mockSparseDataSource(blobs);
    JsonIndexReader mockJsonIdx = mock(JsonIndexReader.class);
    doReturn(mockJsonIdx).when(sparseDs).getJsonIndex();

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), Map.of(), sparseDs, blobs.length, null);
    assertSame(ds.getSparseJsonIndex(), mockJsonIdx);
  }

  @Test
  public void testGetSparseJsonIndexNullWhenNoSparseColumn() {
    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), Map.of(), null, 5, null);
    assertNull(ds.getSparseJsonIndex());
  }

  @Test
  public void testGetMapValueDenseOnly() {
    DataSource clicksDs = mockDenseDataSource(DataType.INT, 10, true);
    DataSource nameDs = mockDenseDataSource(DataType.STRING, "hello", false);

    Map<String, DataSource> perKey = new HashMap<>();
    perKey.put("clicks", clicksDs);
    perKey.put("name", nameDs);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), perKey, null, 2, null);

    Map<String, Object> doc0 = ds.getMapValue(0);
    assertNotNull(doc0);
    assertEquals(doc0.get("clicks"), 10);
    assertEquals(doc0.get("name"), "hello");
  }

  /// Raw (no-dictionary) dense child columns are a real, config-selectable encoding
  /// (`OpenStructColumnSplitter#writeColumnIndexes` with `useDictionary == false`), and they take a different
  /// read path than the dictionary-encoded case: the per-type dispatch inside the forward-index read rather
  /// than a dictId lookup. Cover every stored type OPEN_STRUCT can materialize.
  @DataProvider(name = "rawStoredTypes")
  public static Object[][] rawStoredTypes() {
    return new Object[][]{
        {DataType.INT, 7},
        {DataType.LONG, 7L},
        {DataType.FLOAT, 1.5f},
        {DataType.DOUBLE, 2.5d},
        {DataType.BIG_DECIMAL, new BigDecimal("3.25")},
        {DataType.STRING, "raw"},
        {DataType.BYTES, new byte[]{1, 2, 3}}
    };
  }

  @Test(dataProvider = "rawStoredTypes")
  public void testGetMapValueRawDenseColumnPerStoredType(DataType storedType, Object expected) {
    DataSource rawDs = mockRawDenseDataSource(storedType, expected);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), Map.of("k", rawDs), null, 2, null);

    Map<String, Object> doc0 = ds.getMapValue(0);
    assertNotNull(doc0);
    if (storedType == DataType.BYTES) {
      assertEquals((byte[]) doc0.get("k"), (byte[]) expected);
    } else {
      assertEquals(doc0.get("k"), expected);
    }
  }

  /// No dictionary and a raw forward index — the combination `mockDenseDataSource` never produces.
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static DataSource mockRawDenseDataSource(DataType storedType, Object valueAtDoc0) {
    DataSource ds = mock(DataSource.class);
    ForwardIndexReader fwdReader = mock(ForwardIndexReader.class);
    ForwardIndexReaderContext ctx = mock(ForwardIndexReaderContext.class);
    when(fwdReader.createContext()).thenReturn(ctx);
    when(fwdReader.getStoredType()).thenReturn(storedType);
    when(fwdReader.isSingleValue()).thenReturn(true);
    when(fwdReader.isDictionaryEncoded()).thenReturn(false);
    when(ds.getDictionary()).thenReturn(null);

    switch (storedType) {
      case INT:
        when(fwdReader.getInt(eq(0), eq(ctx))).thenReturn((Integer) valueAtDoc0);
        break;
      case LONG:
        when(fwdReader.getLong(eq(0), eq(ctx))).thenReturn((Long) valueAtDoc0);
        break;
      case FLOAT:
        when(fwdReader.getFloat(eq(0), eq(ctx))).thenReturn((Float) valueAtDoc0);
        break;
      case DOUBLE:
        when(fwdReader.getDouble(eq(0), eq(ctx))).thenReturn((Double) valueAtDoc0);
        break;
      case BIG_DECIMAL:
        when(fwdReader.getBigDecimal(eq(0), eq(ctx))).thenReturn((BigDecimal) valueAtDoc0);
        break;
      case STRING:
        when(fwdReader.getString(eq(0), eq(ctx))).thenReturn((String) valueAtDoc0);
        break;
      case BYTES:
        when(fwdReader.getBytes(eq(0), eq(ctx))).thenReturn((byte[]) valueAtDoc0);
        break;
      default:
        throw new IllegalArgumentException("Unhandled stored type in test fixture: " + storedType);
    }

    when(ds.getForwardIndex()).thenReturn(fwdReader);

    NullValueVectorReader nullReader = mock(NullValueVectorReader.class);
    when(nullReader.isNull(0)).thenReturn(false);
    when(nullReader.isNull(1)).thenReturn(true);
    when(ds.getNullValueVector()).thenReturn(nullReader);

    return ds;
  }

  @Test
  public void testGetMapValueNullDoc() {
    DataSource clicksDs = mockDenseDataSource(DataType.INT, 10, true);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), Map.of("clicks", clicksDs), null, 2, null);

    // doc 1 has null for clicks → no keys → null map
    Map<String, Object> doc1 = ds.getMapValue(1);
    assertNull(doc1);
  }

  @Test
  public void testGetMapValueWithSparse() {
    DataSource clicksDs = mockDenseDataSource(DataType.INT, 10, true);
    DataSource sparseDs = mockSparseDataSource("{\"rare_key\":\"val\"}", true);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), Map.of("clicks", clicksDs), sparseDs, 2, null);

    Map<String, Object> doc0 = ds.getMapValue(0);
    assertNotNull(doc0);
    assertEquals(doc0.get("clicks"), 10);
    assertEquals(doc0.get("rare_key"), "val");
  }

  @Test
  public void testGetMapValueSparseOnlyNullDoc() {
    DataSource sparseDs = mockSparseDataSource("{\"rare_key\":\"val\"}", true);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), Map.of(), sparseDs, 2, null);

    // doc 1: sparse is null
    Map<String, Object> doc1 = ds.getMapValue(1);
    assertNull(doc1);
  }

  @Test
  public void testGetMapValueEmptySegment() {
    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), Map.of(), null, 0, null);

    assertNull(ds.getMapValue(0));
  }

  /// Builds and seals a real OPEN_STRUCT segment (one dense key, one sparse key) and reads it back
  /// with [PinotSegmentRecordReader], which drives [ImmutableOpenStructDataSource#getMapValue]
  /// through [org.apache.pinot.segment.spi.IndexSegment#getPhysicalColumnNames], the schema-derived
  /// column set every real caller uses — never the materialized child columns
  /// (`event$clicks`/`event$__sparse__`) by name. Every other test in this class drives
  /// `getMapValue`/`openMapValueReader` against mocked [DataSource]s; this is the one real
  /// sealed-segment round trip, so a regression in how `ImmutableSegmentImpl` wires the parent
  /// [DataSource] or in the [ImmutableOpenStructDataSource.CachingMapValueReader] reader cache can't
  /// hide behind a mock that always answers the way the test expects (e.g. in the per-key reader
  /// cache `openMapValueReader()` builds for a sequential scan).
  @Test
  public void testSealedSegmentRoundTripThroughRecordReader()
      throws Exception {
    File tempDir = Files.createTempDirectory("ImmutableOpenStructDataSourceTest").toFile();
    try {
      Map<String, FieldSpec> children = new HashMap<>();
      children.put("clicks", new DimensionFieldSpec("clicks", DataType.LONG, true));
      ComplexFieldSpec fieldSpec = new ComplexFieldSpec("event", DataType.OPEN_STRUCT, true, children);
      Schema schema = new Schema.SchemaBuilder().setSchemaName("testRoundTrip").addField(fieldSpec).build();

      // "clicks" present on 9/10 docs -> dense; "region" present on 1/10 -> sparse.
      OpenStructIndexConfig osConfig = new OpenStructIndexConfig(false, null, -1, null, 0.5, null, null);
      ObjectNode indexes = JsonUtils.newObjectNode();
      indexes.set("open_struct", JsonUtils.objectToJsonNode(osConfig));
      FieldConfig eventFieldConfig = new FieldConfig.Builder("event").withIndexes(indexes).build();
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
          .setTableName("testRoundTrip")
          .setFieldConfigList(List.of(eventFieldConfig))
          .setNullHandlingEnabled(true)
          .build();

      int numDocs = 10;
      List<GenericRow> rows = new ArrayList<>(numDocs);
      List<Map<String, Object>> expected = new ArrayList<>(numDocs);
      for (int docId = 0; docId < numDocs; docId++) {
        Map<String, Object> value = new HashMap<>();
        if (docId != 9) {
          value.put("clicks", (long) docId);
        }
        if (docId == 3) {
          value.put("region", "us");
        }
        GenericRow row = new GenericRow();
        row.putValue("event", value);
        rows.add(row);
        expected.add(value.isEmpty() ? null : value);
      }

      SegmentGeneratorConfig segmentConfig = new SegmentGeneratorConfig(tableConfig, schema);
      segmentConfig.setOutDir(tempDir.getAbsolutePath());
      segmentConfig.setSegmentName("testRoundTripSegment");
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(segmentConfig, new GenericRowRecordReader(rows));
      driver.build();

      ImmutableSegment segment = ImmutableSegmentLoader.load(driver.getOutputDirectory(), ReadMode.mmap);
      try {
        assertTrue(segment.getDataSource("event") instanceof ImmutableOpenStructDataSource);
        try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
          recordReader.init(segment);
          for (int docId = 0; docId < numDocs; docId++) {
            GenericRow row = new GenericRow();
            recordReader.next(row);
            assertEquals(row.getValue("event"), expected.get(docId), "docId=" + docId);
          }
        }
      } finally {
        segment.destroy();
      }
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }
  }
}
