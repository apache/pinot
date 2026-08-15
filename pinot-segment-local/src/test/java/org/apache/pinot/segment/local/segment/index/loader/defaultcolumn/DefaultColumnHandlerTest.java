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
package org.apache.pinot.segment.local.segment.index.loader.defaultcolumn;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.BaseDefaultColumnHandler.DefaultColumnAction;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class DefaultColumnHandlerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), DefaultColumnHandlerTest.class.getSimpleName());
  private static final File INDEX_DIR = new File(TEMP_DIR, SEGMENT_NAME);
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final String DERIVED_COLUMN = "derivedColumn";
  private static final String SOURCE_COLUMN = "column1";
  private static final String ORIGINAL_TRANSFORM_FUNCTION = "plus(column1, 1)";
  private static final String UPDATED_TRANSFORM_FUNCTION = "plus(column1, 2)";
  private static final String FINAL_TRANSFORM_FUNCTION = "plus(column1, 3)";

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private Schema _schema;
  private SegmentDirectory _segmentDirectory;
  private SegmentDirectory.Writer _writer;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);

    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());
    _schema = SegmentTestUtils.extractSchemaFromAvroWithoutTime(avroFile);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, _schema);
    config.setInputFilePath(avroFile.getAbsolutePath());
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testComputeDefaultColumnActionMap()
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(INDEX_DIR, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      _segmentDirectory = segmentDirectory;
      _writer = writer;

      // Same schema
      testComputeDefaultColumnActionMap(Map.of());

      // Add single-value dimension in the schema
      _schema.addField(new DimensionFieldSpec("column11", DataType.INT, true));
      testComputeDefaultColumnActionMap(Map.of("column11", DefaultColumnAction.ADD_DIMENSION));
      _schema.removeField("column11");

      // Add multi-value dimension in the schema
      _schema.addField(new DimensionFieldSpec("column11", DataType.INT, false));
      testComputeDefaultColumnActionMap(Map.of("column11", DefaultColumnAction.ADD_DIMENSION));
      _schema.removeField("column11");

      // Add metric in the schema
      _schema.addField(new MetricFieldSpec("column11", DataType.INT));
      testComputeDefaultColumnActionMap(Map.of("column11", DefaultColumnAction.ADD_METRIC));
      _schema.removeField("column11");

      // Add date-time in the schema
      _schema.addField(new DateTimeFieldSpec("column11", DataType.INT, "EPOCH|HOURS", "1:HOURS"));
      testComputeDefaultColumnActionMap(Map.of("column11", DefaultColumnAction.ADD_DATE_TIME));
      _schema.removeField("column11");

      // Do not remove non-autogenerated column in the segmentMetadata
      _schema.removeField("column2");
      testComputeDefaultColumnActionMap(Map.of());

      // Do not update non-autogenerated column in the schema
      _schema.addField(new DimensionFieldSpec("column2", DataType.STRING, true));
      testComputeDefaultColumnActionMap(Map.of());
    }
  }

  @Test
  public void testSegmentGenerationPersistsTransformFunction()
      throws Exception {
    File segmentGenerationTempDir = new File(TEMP_DIR, "segmentGenerationWithTransform");
    File indexDir = buildSegmentWithDerivedColumn(segmentGenerationTempDir, ORIGINAL_TRANSFORM_FUNCTION);

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(segmentMetadata.getColumnMetadataFor(DERIVED_COLUMN).getTransformFunction(),
        ORIGINAL_TRANSFORM_FUNCTION);
    assertFalse(isTransformFunctionBackfilled(indexDir, DERIVED_COLUMN));
  }

  /// Legacy segments carry no transform function in their metadata. Reloading them must NOT regenerate the derived
  /// column values (that would rebuild every legacy derived column in a fleet on the first reload after upgrade), it
  /// must only backfill the configured transform function into the metadata, so that the NEXT change is detected.
  @Test
  public void testLegacyDerivedColumnWithoutStoredTransformFunctionIsBackfilledInsteadOfRegenerated()
      throws Exception {
    Schema schema = getSchemaWithDerivedColumn();
    File indexDir = buildSegmentWithAutoGeneratedDerivedColumn("legacyDerivedTransform", schema);
    removeTransformFunctionFromMetadata(indexDir, DERIVED_COLUMN);

    SegmentMetadataImpl legacySegmentMetadata = new SegmentMetadataImpl(indexDir);
    ColumnMetadata legacyDerivedColumnMetadata = legacySegmentMetadata.getColumnMetadataFor(DERIVED_COLUMN);
    assertNull(legacyDerivedColumnMetadata.getTransformFunction());
    Comparable<?> legacyMinValue = legacyDerivedColumnMetadata.getMinValue();
    Comparable<?> legacyMaxValue = legacyDerivedColumnMetadata.getMaxValue();
    ColumnMetadata sourceColumnMetadata = legacySegmentMetadata.getColumnMetadataFor(SOURCE_COLUMN);

    // First reload after the upgrade: metadata-only backfill, even though the configured transform function differs
    // from the one the stored values were generated with.
    TableConfig tableConfig = getTableConfigWithTransformFunction(UPDATED_TRANSFORM_FUNCTION);
    assertEquals(computeDefaultColumnActionMap(indexDir, tableConfig, schema),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.BACKFILL_TRANSFORM_FUNCTION));
    updateDefaultColumns(indexDir, tableConfig, schema);

    ColumnMetadata backfilledColumnMetadata =
        new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED_COLUMN);
    assertEquals(backfilledColumnMetadata.getTransformFunction(), UPDATED_TRANSFORM_FUNCTION);
    assertTrue(isTransformFunctionBackfilled(indexDir, DERIVED_COLUMN));
    assertEquals(backfilledColumnMetadata.getMinValue(), legacyMinValue);
    assertEquals(backfilledColumnMetadata.getMaxValue(), legacyMaxValue);

    // Reloading again with the same config is a no-op.
    assertEquals(computeDefaultColumnActionMap(indexDir, tableConfig, schema), Map.of());

    // Second reload, this time the transform function actually changed: values are regenerated.
    TableConfig updatedTableConfig = getTableConfigWithTransformFunction(FINAL_TRANSFORM_FUNCTION);
    assertEquals(computeDefaultColumnActionMap(indexDir, updatedTableConfig, schema),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    updateDefaultColumns(indexDir, updatedTableConfig, schema);

    ColumnMetadata derivedColumnMetadata = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED_COLUMN);
    assertEquals(derivedColumnMetadata.getTransformFunction(), FINAL_TRANSFORM_FUNCTION);
    assertFalse(isTransformFunctionBackfilled(indexDir, DERIVED_COLUMN));
    assertEquals(derivedColumnMetadata.getMinValue(), (Integer) sourceColumnMetadata.getMinValue() + 3);
    assertEquals(derivedColumnMetadata.getMaxValue(), (Integer) sourceColumnMetadata.getMaxValue() + 3);
  }

  /// Removing the transform function from the table config is a real change: the derived column is regenerated with
  /// default values, which is what a freshly built segment would contain for that config.
  @Test
  public void testRemovingTransformFunctionFromConfigRegeneratesDefaultValues()
      throws Exception {
    Schema schema = getSchemaWithDerivedColumn();
    File indexDir = buildSegmentWithAutoGeneratedDerivedColumn("removedDerivedTransform", schema);
    TableConfig tableConfig = getTableConfigWithTransformFunction(null);

    assertEquals(computeDefaultColumnActionMap(indexDir, tableConfig, schema),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    updateDefaultColumns(indexDir, tableConfig, schema);

    ColumnMetadata derivedColumnMetadata = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED_COLUMN);
    assertNull(derivedColumnMetadata.getTransformFunction());
    Object defaultNullValue = schema.getFieldSpecFor(DERIVED_COLUMN).getDefaultNullValue();
    assertEquals(derivedColumnMetadata.getMinValue(), defaultNullValue);
    assertEquals(derivedColumnMetadata.getMaxValue(), defaultNullValue);
    // The stale transform function must be gone from the metadata, otherwise every reload would rebuild the column.
    assertEquals(computeDefaultColumnActionMap(indexDir, tableConfig, schema), Map.of());
  }

  /// The backfilled transform function is written into the segment metadata with the same escaping the segment
  /// creator uses. Commas (PropertiesConfiguration list delimiter) and backslashes must survive the round trip.
  @Test
  public void testBackfilledTransformFunctionRoundTripsThroughSegmentMetadata()
      throws Exception {
    String transformFunction = "Groovy({column1 + ',' + column2.replace('\\\\', '/')}, column1, column2)";
    Schema schema = getSchemaWithDerivedColumn();
    File indexDir = buildSegmentWithAutoGeneratedDerivedColumn("backfillTransformRoundTrip", schema);
    removeTransformFunctionFromMetadata(indexDir, DERIVED_COLUMN);

    TableConfig tableConfig = getTableConfigWithTransformFunction(transformFunction);
    assertEquals(computeDefaultColumnActionMap(indexDir, tableConfig, schema),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.BACKFILL_TRANSFORM_FUNCTION));
    updateDefaultColumns(indexDir, tableConfig, schema);

    assertEquals(new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED_COLUMN).getTransformFunction(),
        transformFunction);
    assertTrue(isTransformFunctionBackfilled(indexDir, DERIVED_COLUMN));
    // The recovered value must compare equal to the config, otherwise the next reload would rebuild the column.
    assertEquals(computeDefaultColumnActionMap(indexDir, tableConfig, schema), Map.of());
  }

  @Test
  public void testMissingDerivedColumnArgumentPersistsTransformFunction()
      throws Exception {
    File segmentGenerationTempDir = new File(TEMP_DIR, "missingArgumentDerivedTransform");
    FileUtils.deleteQuietly(segmentGenerationTempDir);
    File indexDir =
        buildSegment(segmentGenerationTempDir, TABLE_CONFIG, Schema.fromString(_schema.toSingleLineJsonString()));
    String transformFunction = "plus(missingColumn, 1)";

    Schema schema = getSchemaWithDerivedColumn();
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      DefaultColumnHandler defaultColumnHandler =
          new V3DefaultColumnHandler(indexDir, segmentDirectory.getSegmentMetadata(),
              new IndexLoadingConfig(getTableConfigWithTransformFunction(transformFunction), schema), writer);
      defaultColumnHandler.updateDefaultColumns();
    }

    SegmentMetadataImpl updatedSegmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(updatedSegmentMetadata.getColumnMetadataFor(DERIVED_COLUMN).getTransformFunction(),
        transformFunction);
  }

  @Test
  public void testComputeDefaultColumnActionMapForTransformFunction() {
    assertEquals(computeDefaultColumnActionMap(ORIGINAL_TRANSFORM_FUNCTION, UPDATED_TRANSFORM_FUNCTION),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    assertEquals(computeDefaultColumnActionMap(ORIGINAL_TRANSFORM_FUNCTION, ORIGINAL_TRANSFORM_FUNCTION), Map.of());
    // Legacy segment: no transform function in the metadata means metadata-only backfill, never a value rebuild.
    assertEquals(computeDefaultColumnActionMap(null, UPDATED_TRANSFORM_FUNCTION),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.BACKFILL_TRANSFORM_FUNCTION));
    assertEquals(computeDefaultColumnActionMap(ORIGINAL_TRANSFORM_FUNCTION, null),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    assertEquals(computeDefaultColumnActionMap(null, null), Map.of());
  }

  @Test
  public void testTransformFunctionIsIgnoredForNonAutoGeneratedColumn() {
    assertEquals(computeDefaultColumnActionMap(ORIGINAL_TRANSFORM_FUNCTION, UPDATED_TRANSFORM_FUNCTION, false),
        Map.of());
    assertEquals(computeDefaultColumnActionMap(null, UPDATED_TRANSFORM_FUNCTION, false), Map.of());
  }

  private void testComputeDefaultColumnActionMap(Map<String, DefaultColumnAction> expected) {
    BaseDefaultColumnHandler defaultColumnHandler =
        new V3DefaultColumnHandler(INDEX_DIR, _segmentDirectory.getSegmentMetadata(),
            new IndexLoadingConfig(TABLE_CONFIG, _schema), _writer);
    assertEquals(defaultColumnHandler.computeDefaultColumnActionMap(), expected);
  }

  private static Map<String, DefaultColumnAction> computeDefaultColumnActionMap(String transformFunctionInMetadata,
      String transformFunctionInTableConfig) {
    return computeDefaultColumnActionMap(transformFunctionInMetadata, transformFunctionInTableConfig, true);
  }

  private static Map<String, DefaultColumnAction> computeDefaultColumnActionMap(String transformFunctionInMetadata,
      String transformFunctionInTableConfig, boolean autoGenerated) {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(DERIVED_COLUMN, DataType.INT)
        .build();
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getColumnName()).thenReturn(DERIVED_COLUMN);
    when(columnMetadata.isAutoGenerated()).thenReturn(autoGenerated);
    when(columnMetadata.getFieldSpec()).thenReturn(schema.getFieldSpecFor(DERIVED_COLUMN));
    when(columnMetadata.getTransformFunction()).thenReturn(transformFunctionInMetadata);
    TreeMap<String, ColumnMetadata> columnMetadataMap = new TreeMap<>();
    columnMetadataMap.put(DERIVED_COLUMN, columnMetadata);

    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getColumnMetadataFor(DERIVED_COLUMN)).thenReturn(columnMetadata);
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(columnMetadataMap);
    BaseDefaultColumnHandler defaultColumnHandler =
        new V3DefaultColumnHandler(new File("."), segmentMetadata,
            new IndexLoadingConfig(getTableConfigWithTransformFunction(transformFunctionInTableConfig), schema),
            mock(SegmentDirectory.Writer.class));
    return defaultColumnHandler.computeDefaultColumnActionMap();
  }

  /// Builds a segment without the derived column, then materializes it through the default column handler so that it
  /// is marked as auto-generated in the segment metadata, the way a reload after a schema change would.
  private File buildSegmentWithAutoGeneratedDerivedColumn(String tempDirName, Schema schema)
      throws Exception {
    File indexDir = buildSegment(new File(TEMP_DIR, tempDirName), TABLE_CONFIG,
        Schema.fromString(_schema.toSingleLineJsonString()));
    updateDefaultColumns(indexDir, getTableConfigWithTransformFunction(ORIGINAL_TRANSFORM_FUNCTION), schema);
    assertEquals(new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED_COLUMN).getTransformFunction(),
        ORIGINAL_TRANSFORM_FUNCTION);
    return indexDir;
  }

  private static Map<String, DefaultColumnAction> computeDefaultColumnActionMap(File indexDir, TableConfig tableConfig,
      Schema schema)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      return new V3DefaultColumnHandler(indexDir, segmentDirectory.getSegmentMetadata(),
          new IndexLoadingConfig(tableConfig, schema), writer).computeDefaultColumnActionMap();
    }
  }

  private static void updateDefaultColumns(File indexDir, TableConfig tableConfig, Schema schema)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      new V3DefaultColumnHandler(indexDir, segmentDirectory.getSegmentMetadata(),
          new IndexLoadingConfig(tableConfig, schema), writer).updateDefaultColumns();
    }
  }

  private static TableConfig getTableConfigWithTransformFunction(String transformFunction) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    if (transformFunction != null) {
      ingestionConfig.setTransformConfigs(List.of(new TransformConfig(DERIVED_COLUMN, transformFunction)));
    } else {
      ingestionConfig.setTransformConfigs(List.of());
    }
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setIngestionConfig(ingestionConfig)
        .build();
  }

  private Schema getSchemaWithDerivedColumn()
      throws Exception {
    Schema schema = Schema.fromString(_schema.toSingleLineJsonString());
    schema.addField(new DimensionFieldSpec(DERIVED_COLUMN, DataType.INT, true));
    return schema;
  }

  private File buildSegmentWithDerivedColumn(File segmentGenerationTempDir, String transformFunction)
      throws Exception {
    return buildSegment(segmentGenerationTempDir, getTableConfigWithTransformFunction(transformFunction),
        getSchemaWithDerivedColumn());
  }

  private File buildSegment(File segmentGenerationTempDir, TableConfig tableConfig, Schema schema)
      throws Exception {
    FileUtils.deleteQuietly(segmentGenerationTempDir);

    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInputFilePath(avroFile.getAbsolutePath());
    config.setOutDir(segmentGenerationTempDir.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    return new File(segmentGenerationTempDir, SEGMENT_NAME);
  }

  private static void removeTransformFunctionFromMetadata(File indexDir, String column)
      throws Exception {
    PropertiesConfiguration segmentProperties = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    segmentProperties.clearProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION));
    SegmentMetadataUtils.savePropertiesConfiguration(segmentProperties, indexDir);
  }

  private static boolean isTransformFunctionBackfilled(File indexDir, String column)
      throws Exception {
    PropertiesConfiguration segmentProperties = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    String backfilledKey = V1Constants.MetadataKeys.Column.getKeyFor(column,
        V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION_BACKFILLED);
    return segmentProperties.getBoolean(backfilledKey, false);
  }
}
