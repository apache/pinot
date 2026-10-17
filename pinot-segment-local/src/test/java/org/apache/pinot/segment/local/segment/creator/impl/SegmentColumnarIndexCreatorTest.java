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
package org.apache.pinot.segment.local.segment.creator.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.utils.DataTypeTransformerUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.MaxLengthExceedStrategy;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.SchemaValidatorFactory;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.ColumnReaderFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.PinotDataType;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedStatic;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class SegmentColumnarIndexCreatorTest implements PinotBuffersAfterClassCheckRule {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentColumnarIndexCreatorTest");
  private static final File CONFIG_FILE = new File(TEMP_DIR, "config");
  private static final String COLUMN_NAME = "testColumn";
  private static final String COLUMN_PROPERTY_KEY_PREFIX = Column.COLUMN_PROPS_KEY_PREFIX + COLUMN_NAME + ".";

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testRemoveColumnMetadataInfo()
      throws Exception {
    PropertiesConfiguration configuration = CommonsConfigurationUtils.fromFile(CONFIG_FILE);
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "a", "foo");
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "b", "bar");
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "c", "foobar");
    CommonsConfigurationUtils.saveToFile(configuration, CONFIG_FILE);

    configuration = CommonsConfigurationUtils.fromFile(CONFIG_FILE);
    assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
    SegmentColumnarIndexCreator.removeColumnMetadataInfo(configuration, COLUMN_NAME);
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
    CommonsConfigurationUtils.saveToFile(configuration, CONFIG_FILE);

    configuration = CommonsConfigurationUtils.fromFile(CONFIG_FILE);
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
  }

  @Test
  public void testTimeColumnInMetadata()
      throws Exception {
    long withTZ =
        getStartTimeInSegmentMetadata("1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ssZ", "2021-07-21T06:48:51Z");
    long withoutTZ =
        getStartTimeInSegmentMetadata("1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss", "2021-07-21T06:48:51");
    assertEquals(withTZ, 1626850131000L); // as UTC timestamp.
    assertEquals(withTZ, withoutTZ);
  }

  @DataProvider
  public Object[][] providerJsonModes() {
    return new Object[][]{{false, false}, {true, false}, {true, true}};
  }

  @Test(dataProvider = "providerJsonModes")
  public void testRecordReaderColumnProviderPreservesValuesStatisticsAndIndexes(boolean canonicalJson,
      boolean dictionaryJson)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("arbitraryJson", DataType.JSON)
        .addDimensionField("message", DataType.STRING, field -> field.setMaxLength(12))
        .addDimensionField("label", DataType.STRING, field -> field.setMaxLength(4))
        .addSingleValueDimension("physicalTarget", DataType.STRING).addSingleValueDimension("count", DataType.INT)
        .build();
    TableConfig table = providerTable(schema, false);
    if (dictionaryJson) {
      table.getIndexingConfig().getNoDictionaryColumns().remove("arbitraryJson");
      table.setFieldConfigList(table.getFieldConfigList().stream()
          .filter(field -> !field.getName().equals("arbitraryJson")).toList());
    }
    IngestionConfig ingestion = new IngestionConfig();
    ingestion.setTransformConfigs(List.of(new TransformConfig("physicalTarget", "aliasSource")));
    table.setIngestionConfig(ingestion);
    List<GenericRow> rows = List.of(
        row(Map.of("arbitraryJson", Map.of("z", "é\"", "a", "first"), "message", "needle\0discard", "label", "alpha",
            "physicalTarget", "physical-A", "aliasSource", "wrong-A", "count", "7")),
        row(Map.of("arbitraryJson", Map.of("a", "first", "z", "é\""), "message", "other", "label", "beta",
            "physicalTarget", "physical-B", "aliasSource", "wrong-B", "count", "2")),
        row(Map.of("arbitraryJson", "{\"number\":1.50}", "message", "needle rest", "label", "gamm",
            "physicalTarget", "physical-C", "aliasSource", "wrong-C", "count", "7")),
        row(Map.of("arbitraryJson", Map.of("nested", Map.of("enabled", true)), "message", "unrelated", "label", "delt",
            "physicalTarget", "physical-D", "aliasSource", "wrong-D", "count", "2")));

    File inputFile = new File(TEMP_DIR, "provider-input.data");
    FileUtils.touch(inputFile);
    String inputPath = inputFile.getAbsolutePath();
    SegmentGeneratorConfig rowConfig = providerConfig(table, schema, "provider-row");
    rowConfig.setInputFilePath(inputPath);
    SegmentIndexCreationDriverImpl rowDriver = new SegmentIndexCreationDriverImpl();
    rowDriver.init(rowConfig, new GenericRowRecordReader(rows));
    rowDriver.build();

    ColumnReaderFactory factory = columnFactory(schema, rows);
    if (canonicalJson) {
      List<String> converted = rows.stream().map(value -> (String) DataTypeTransformerUtils.transformValue(
          "arbitraryJson", value.getValue("arbitraryJson"), PinotDataType.JSON)).toList();
      ColumnReader json = factory.getAllColumnReaders().get("arbitraryJson");
      when(json.isCanonicalJson()).thenReturn(true);
      when(json.getValueType()).thenReturn(PinotDataType.STRING);
      when(json.getValue(anyInt())).thenAnswer(invocation -> converted.get(invocation.getArgument(0)));
      when(json.getString(anyInt())).thenAnswer(invocation -> converted.get(invocation.getArgument(0)));
    }
    ProviderRecordReader reader = new ProviderRecordReader(rows, factory);
    SegmentGeneratorConfig config = providerConfig(table, schema, "provider-column");
    config.setSegmentName("provider-row");
    config.setInputFilePath(inputPath);
    SegmentIndexCreationDriverImpl columnDriver = new SegmentIndexCreationDriverImpl();
    try (MockedStatic<SchemaValidatorFactory> validators = mockStatic(SchemaValidatorFactory.class)) {
      columnDriver.init(config, reader);
      validators.verify(() -> SchemaValidatorFactory.getSchemaValidator(schema, ProviderRecordReader.class.getName(),
          inputPath));
    }
    assertNull(columnDriver.getRecordReader());
    assertEquals(reader._providerCalls, 1);
    assertEquals(reader._closeCalls, 1);
    assertEquals(reader._nextCalls, 0);
    verify(factory, times(1)).init(schema);
    try (MockedStatic<DataTypeTransformerUtils> conversion = mockStatic(DataTypeTransformerUtils.class,
        CALLS_REAL_METHODS)) {
      columnDriver.build();
      if (canonicalJson) {
        conversion.verify(() -> DataTypeTransformerUtils.transformValue(eq("arbitraryJson"), any(),
            eq(PinotDataType.JSON)), times(0));
      }
    }
    verify(factory, times(1)).close();
    assertEquals(reader._closeCalls, 1);
    assertEquals(reader._nextCalls, 0);
    assertEquals(rowDriver.getSanitizedRowsFound(), 1);
    assertEquals(columnDriver.getSanitizedRowsFound(), 1, "Two sanitized columns in one document count once");

    ImmutableSegment rowSegment = ImmutableSegmentLoader.load(rowDriver.getOutputDirectory(), ReadMode.heap);
    ImmutableSegment columnSegment = ImmutableSegmentLoader.load(columnDriver.getOutputDirectory(), ReadMode.heap);
    try {
      assertSegmentsEquivalent(rowSegment, columnSegment);
      try (PinotSegmentColumnReader target = new PinotSegmentColumnReader(columnSegment, "physicalTarget");
          PinotSegmentColumnReader json = new PinotSegmentColumnReader(columnSegment, "arbitraryJson")) {
        assertEquals(target.getValue(0), "physical-A", "Existing physical values must win over identifier aliases");
        assertEquals(json.getValue(0), "{\"a\":\"first\",\"z\":\"é\\\"\"}");
      }
      assertNotNull(columnSegment.getDataSource("message").getTextIndex());
      for (String query : List.of("needle", "discard", "other")) {
        assertEquals(columnSegment.getDataSource("message").getTextIndex().getDocIds(query, null).toArray(),
            rowSegment.getDataSource("message").getTextIndex().getDocIds(query, null).toArray());
      }
      assertEquals(columnSegment.getDataSource("message").getTextIndex().getDocIds("needle", null).toArray(),
          new int[]{0, 2});
      int labelId = columnSegment.getDataSource("label").getDictionary().indexOf("alph");
      assertEquals(((ImmutableRoaringBitmap) columnSegment.getDataSource("label").getInvertedIndex()
          .getDocIds(labelId)).toArray(), new int[]{0});
      assertNotNull(columnSegment.getDataSource("count").getRangeIndex());
      assertEquals(columnSegment.getDataSource("count").getRangeIndex().getNumMatchingDocs(0, 0),
          rowSegment.getDataSource("count").getRangeIndex().getNumMatchingDocs(0, 0));
    } finally {
      rowSegment.destroy();
      columnSegment.destroy();
    }
  }

  @Test
  public void testCanonicalJsonReaderStillSanitizesAndSubstitutesNulls()
      throws IOException {
    Schema schema = new Schema.SchemaBuilder().addDimensionField("json", DataType.JSON, field -> {
      field.setMaxLength(8);
      field.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.TRIM_LENGTH);
    }).build();
    ColumnReader source = mock(ColumnReader.class);
    when(source.getColumnName()).thenReturn("json");
    when(source.getTotalDocs()).thenReturn(2);
    when(source.getValueType()).thenReturn(PinotDataType.STRING);
    when(source.isCanonicalJson()).thenReturn(true);
    when(source.getValue(0)).thenReturn("{\"long\":\"value\"}");
    when(source.getString(0)).thenReturn("{\"long\":\"value\"}");
    BitSet sanitized = new BitSet();
    try (NormalizedColumnReader reader = new NormalizedColumnReader(source, providerTable(schema, true),
        schema.getFieldSpecFor("json"), schema, sanitized)) {
      reader.enableSanitizationTracking();
      assertEquals(reader.getValue(0), "{\"long\":");
      assertEquals(reader.getValue(1), schema.getFieldSpecFor("json").getDefaultNullValue());
      assertTrue(sanitized.get(0));
      assertFalse(sanitized.get(1));
      reader.rewind();
      assertEquals(reader.getValue(0), "{\"long\":");
    }
  }

  @DataProvider
  public Object[][] providerRawRowCounts() {
    return new Object[][]{{2, false}, {0, false}, {0, true}};
  }

  @Test(dataProvider = "providerRawRowCounts")
  public void testAllRawProviderHandlesEmptyStatisticsSchema(int totalDocs, boolean failOnEmpty)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("rawJson", DataType.JSON)
        .addSingleValueDimension("rawString", DataType.STRING).build();
    TableConfig table = providerTable(schema, true);
    List<GenericRow> rows = new ArrayList<>();
    for (int doc = 0; doc < totalDocs; doc++) {
      rows.add(row(Map.of("rawJson", Map.of("value", "doc" + doc), "rawString", "row" + doc)));
    }
    ColumnReaderFactory factory = columnFactory(schema, rows);
    ProviderRecordReader reader = new ProviderRecordReader(rows, factory);
    SegmentGeneratorConfig config = providerConfig(table, schema, "provider-all-raw-" + totalDocs + "-" + failOnEmpty);
    config.setFailOnEmptySegment(failOnEmpty);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    if (failOnEmpty) {
      expectThrows(IllegalStateException.class, () -> driver.init(config, reader));
    } else {
      driver.init(config, reader);
      driver.build();
      ImmutableSegment segment = ImmutableSegmentLoader.load(driver.getOutputDirectory(), ReadMode.heap);
      try {
        assertEquals(segment.getSegmentMetadata().getTotalDocs(), totalDocs);
        for (String column : schema.getPhysicalColumnNames()) {
          assertFalse(segment.getSegmentMetadata().getColumnMetadataFor(column).hasDictionary());
          assertEquals(driver.getSegmentStats().getColumnProfileFor(column).getTotalNumberOfEntries(), totalDocs);
        }
        if (totalDocs > 0) {
          try (PinotSegmentColumnReader value = new PinotSegmentColumnReader(segment, "rawJson")) {
            assertEquals(value.getValue(0), "{\"value\":\"doc0\"}");
          }
        }
      } finally {
        segment.destroy();
      }
    }
    verify(factory, times(1)).init(schema);
    verify(factory, times(1)).close();
    assertEquals(reader._closeCalls, 1);
    assertEquals(reader._nextCalls, 0);
  }

  @DataProvider
  public Object[][] providerFallbacks() {
    return new Object[][]{{"unsupported"}, {"expression"}, {"jsonAlias"}, {"disabled"}};
  }

  @Test(dataProvider = "providerFallbacks")
  public void testUnsupportedColumnProviderLeavesRowReaderUntouched(String fallback)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("result", fallback.equals("jsonAlias") ? DataType.JSON : DataType.STRING).build();
    TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName("provider")
        .setColumnMajorSegmentBuilderEnabled(!fallback.equals("disabled")).build();
    GenericRow row = row(Map.of("source", "raw"));
    if (fallback.equals("expression") || fallback.equals("jsonAlias")) {
      IngestionConfig ingestion = new IngestionConfig();
      ingestion.setTransformConfigs(List.of(new TransformConfig("result",
          fallback.equals("expression") ? "concat(source, '_suffix')" : "source")));
      table.setIngestionConfig(ingestion);
      if (fallback.equals("jsonAlias")) {
        row.putValue("result", Map.of("original", "physical"));
        row.putValue("source", "{\"alias\":\"selected\"}");
      }
    } else {
      row.putValue("result", "raw");
    }
    ColumnReaderFactory factory = mock(ColumnReaderFactory.class);
    ProviderRecordReader reader =
        new ProviderRecordReader(List.of(row), fallback.equals("unsupported") ? null : factory);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(providerConfig(table, schema, "provider-fallback-" + fallback), reader);
    assertSame(driver.getRecordReader(), reader);
    assertEquals(reader._providerCalls, fallback.equals("unsupported") ? 1 : 0);
    assertEquals(reader._closeCalls, 0);
    assertEquals(reader._nextCalls, 0);
    driver.build();
    assertEquals(reader._closeCalls, 1);
    assertTrue(reader._nextCalls > 0);
    verifyNoInteractions(factory);
    ImmutableSegment segment = ImmutableSegmentLoader.load(driver.getOutputDirectory(), ReadMode.heap);
    try (PinotSegmentColumnReader result = new PinotSegmentColumnReader(segment, "result")) {
      String expected = fallback.equals("expression") ? "raw_suffix"
          : fallback.equals("jsonAlias") ? "{\"alias\":\"selected\"}" : "raw";
      assertEquals(result.getValue(0), expected);
    } finally {
      segment.destroy();
    }
  }

  @DataProvider
  public Object[][] invalidColumnProviders() {
    return new Object[][]{{"missing"}, {"extra"}, {"negative"}, {"inconsistent"}};
  }

  @Test(dataProvider = "invalidColumnProviders")
  public void testColumnProviderRejectsInvalidShapeAndClosesFactory(String invalid)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("a", DataType.STRING)
        .addSingleValueDimension("b", DataType.STRING).build();
    List<GenericRow> rows = List.of(row(Map.of("a", "one", "b", "two")));
    ColumnReaderFactory factory = columnFactory(schema, rows);
    Map<String, ColumnReader> readers = factory.getAllColumnReaders();
    switch (invalid) {
      case "missing":
        readers.remove("b");
        break;
      case "extra":
        readers.put("unexpected", readers.get("a"));
        break;
      case "negative":
        when(readers.get("a").getTotalDocs()).thenReturn(-1);
        break;
      case "inconsistent":
        when(readers.get("b").getTotalDocs()).thenReturn(2);
        break;
      default:
        throw new AssertionError(invalid);
    }
    ProviderRecordReader reader = new ProviderRecordReader(rows, factory);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    expectThrows(IllegalStateException.class,
        () -> driver.init(providerConfig(providerTable(schema, true), schema, "provider-invalid-" + invalid), reader));
    verify(factory, times(1)).init(schema);
    verify(factory, times(1)).close();
    assertEquals(reader._closeCalls, 1);
    assertEquals(reader._nextCalls, 0);
  }

  @DataProvider
  public Object[][] providerFailures() {
    return new Object[][]{{"init"}, {"index"}, {"hook"}};
  }

  @Test(dataProvider = "providerFailures")
  public void testColumnProviderFailuresPropagateAndReleaseOwnership(String phase)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("value", DataType.STRING).build();
    List<GenericRow> rows = List.of(row(Map.of("value", "raw")));
    ColumnReaderFactory factory = columnFactory(schema, rows);
    ProviderRecordReader reader = new ProviderRecordReader(rows, factory);
    IOException failure = new IOException("provider " + phase + " failure");
    if (phase.equals("init")) {
      doThrow(failure).when(factory).init(schema);
    } else if (phase.equals("index")) {
      when(factory.getAllColumnReaders().get("value").getValue(anyInt())).thenThrow(failure);
    } else {
      reader._hookFailure = failure;
    }
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    SegmentGeneratorConfig config = providerConfig(providerTable(schema, true), schema, "provider-failure-" + phase);
    if (phase.equals("index")) {
      driver.init(config, reader);
      assertSame(expectThrows(IOException.class, driver::build), failure);
    } else {
      assertSame(expectThrows(IOException.class, () -> driver.init(config, reader)), failure);
    }
    if (phase.equals("hook")) {
      verify(factory, times(0)).init(schema);
      verify(factory, times(0)).close();
      assertEquals(reader._closeCalls, 1);
    } else {
      verify(factory, times(1)).init(schema);
      verify(factory, times(1)).close();
      assertEquals(reader._closeCalls, 1);
    }
    assertEquals(reader._nextCalls, 0);
  }

  private static GenericRow row(Map<String, Object> values) {
    GenericRow row = new GenericRow();
    values.forEach(row::putValue);
    return row;
  }

  private static TableConfig providerTable(Schema schema, boolean allRaw) {
    List<String> rawColumns = new ArrayList<>();
    List<FieldConfig> fields = new ArrayList<>();
    for (String column : schema.getPhysicalColumnNames()) {
      if (allRaw || schema.getFieldSpecFor(column).getDataType() == DataType.JSON || column.equals("message")) {
        rawColumns.add(column);
        fields.add(new FieldConfig(column, FieldConfig.EncodingType.RAW, null,
            column.equals("message") ? List.of(FieldConfig.IndexType.TEXT) : List.of(),
            FieldConfig.CompressionCodec.LZ4,
            null, JsonUtils.objectToJsonNode(Map.of("forward",
                Map.of("rawIndexWriterVersion", 4, "targetDocsPerChunk", -1))),
            Map.of(FieldConfig.TEXT_INDEX_LUCENE_MAX_BUFFER_SIZE_MB, "16"), null));
      }
    }
    TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE).setTableName("provider")
        .setColumnMajorSegmentBuilderEnabled(true).setOptimizeNoDictStatsCollection(true)
        .setNoDictionaryColumns(rawColumns).setFieldConfigList(fields);
    if (!allRaw) {
      builder.setInvertedIndexColumns(List.of("label")).setRangeIndexColumns(List.of("count"));
    }
    return builder.build();
  }

  private static SegmentGeneratorConfig providerConfig(TableConfig table, Schema schema, String name) {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(table, schema);
    config.setOutDir(new File(TEMP_DIR, name).getAbsolutePath());
    config.setSegmentName(name);
    return config;
  }

  private static ColumnReaderFactory columnFactory(Schema schema, List<GenericRow> rows)
      throws IOException {
    Map<String, ColumnReader> readers = new TreeMap<>();
    for (String column : schema.getPhysicalColumnNames()) {
      ColumnReader reader = mock(ColumnReader.class);
      when(reader.getColumnName()).thenReturn(column);
      when(reader.getTotalDocs()).thenReturn(rows.size());
      when(reader.getValue(anyInt())).thenAnswer(invocation -> {
        int docId = invocation.getArgument(0);
        return rows.get(docId).getValue(column);
      });
      readers.put(column, reader);
    }
    ColumnReaderFactory factory = mock(ColumnReaderFactory.class);
    when(factory.getAllColumnReaders()).thenReturn(readers);
    return factory;
  }

  private static void assertSegmentsEquivalent(ImmutableSegment expected, ImmutableSegment actual)
      throws IOException {
    assertEquals(actual.getSegmentMetadata().getTotalDocs(), expected.getSegmentMetadata().getTotalDocs());
    assertEquals(actual.getSegmentMetadata().getSchema(), expected.getSegmentMetadata().getSchema());
    for (String column : expected.getPhysicalColumnNames()) {
      ColumnMetadata first = expected.getSegmentMetadata().getColumnMetadataFor(column);
      ColumnMetadata second = actual.getSegmentMetadata().getColumnMetadataFor(column);
      assertEquals(second.getFieldSpec(), first.getFieldSpec());
      assertEquals(second.getTotalDocs(), first.getTotalDocs());
      assertEquals(second.getTotalNumberOfEntries(), first.getTotalNumberOfEntries());
      assertEquals(second.getCardinality(), first.getCardinality());
      assertEquals(second.getLengthOfShortestElement(), first.getLengthOfShortestElement());
      assertEquals(second.getLengthOfLongestElement(), first.getLengthOfLongestElement());
      assertEquals(second.getMaxRowLengthInBytes(), first.getMaxRowLengthInBytes());
      assertEquals(second.getMaxNumberOfMultiValues(), first.getMaxNumberOfMultiValues());
      assertEquals(second.isSorted(), first.isSorted());
      assertEquals(second.isAscii(), first.isAscii());
      assertEquals(second.hasDictionary(), first.hasDictionary());
      assertEquals(second.getForwardIndexEncoding(), first.getForwardIndexEncoding());
      assertEquals(second.getBitsPerElement(), first.getBitsPerElement());
      assertEquals(second.getMinValue(), first.getMinValue());
      assertEquals(second.getMaxValue(), first.getMaxValue());
      assertEquals(second.isMinMaxValueInvalid(), first.isMinMaxValueInvalid());
      assertEquals(second.getPartitions(), first.getPartitions());
      assertEquals(second.isNonNull(), first.isNonNull());
      try (PinotSegmentColumnReader firstReader = new PinotSegmentColumnReader(expected, column);
          PinotSegmentColumnReader secondReader = new PinotSegmentColumnReader(actual, column)) {
        for (int doc = 0; doc < first.getTotalDocs(); doc++) {
          assertEquals(secondReader.getValue(doc), firstReader.getValue(doc), column + " doc " + doc);
          assertEquals(secondReader.isNull(doc), firstReader.isNull(doc), column + " null " + doc);
        }
      }
    }
  }

  /// Exposes an optional independent factory while recording whether driver selection consumed the original reader.
  private static final class ProviderRecordReader extends GenericRowRecordReader {
    @Nullable
    private final ColumnReaderFactory _factory;
    @Nullable
    private IOException _hookFailure;
    private int _providerCalls;
    private int _nextCalls;
    private int _closeCalls;

    private ProviderRecordReader(List<GenericRow> rows, @Nullable ColumnReaderFactory factory) {
      super(rows);
      _factory = factory;
    }

    @Nullable
    @Override
    public ColumnReaderFactory tryCreateColumnReaderFactory(Schema targetSchema)
        throws IOException {
      _providerCalls++;
      if (_hookFailure != null) {
        throw _hookFailure;
      }
      return _factory;
    }

    @Override
    public GenericRow next(GenericRow reuse) {
      _nextCalls++;
      return super.next(reuse);
    }

    @Override
    public void close() {
      _closeCalls++;
      super.close();
    }
  }

  private static long getStartTimeInSegmentMetadata(String testDateTimeFormat, String testDateTime)
      throws Exception {
    String timeColumn = "foo";
    Schema schema =
        new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.STRING, testDateTimeFormat, "1:MILLISECONDS")
            .build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(timeColumn).build();

    String segmentName = "testSegment";
    String indexDirPath = new File(TEMP_DIR, segmentName).getAbsolutePath();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(indexDirPath);
    config.setSegmentName(segmentName);
    IndexSegment indexSegment = null;

    try {
      FileUtils.deleteQuietly(new File(indexDirPath));

      GenericRow row = new GenericRow();
      row.putValue(timeColumn, testDateTime);
      List<GenericRow> rows = List.of(row);

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, new GenericRowRecordReader(rows));
      driver.build();
      indexSegment = ImmutableSegmentLoader.load(new File(indexDirPath, segmentName), ReadMode.heap);
      SegmentMetadata md = indexSegment.getSegmentMetadata();
      return md.getStartTime();
    } finally {
      if (indexSegment != null) {
        indexSegment.destroy();
      }
      FileUtils.deleteQuietly(new File(indexDirPath));
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
