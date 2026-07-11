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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class DefaultColumnHandlerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), DefaultColumnHandlerTest.class.getSimpleName());
  private static final File INDEX_DIR = new File(TEMP_DIR, SEGMENT_NAME);
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final String DERIVED_COLUMN = "derivedColumn";
  private static final String ORIGINAL_TRANSFORM_FUNCTION = "plus(column1, 1)";
  private static final String UPDATED_TRANSFORM_FUNCTION = "plus(column1, 2)";

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
  }

  @Test
  public void testLegacyDerivedColumnWithoutStoredTransformFunctionIsRegenerated()
      throws Exception {
    File segmentGenerationTempDir = new File(TEMP_DIR, "legacyDerivedTransform");
    File indexDir =
        buildSegment(segmentGenerationTempDir, TABLE_CONFIG, Schema.fromString(_schema.toSingleLineJsonString()));
    Schema schema = getSchemaWithDerivedColumn();

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      DefaultColumnHandler defaultColumnHandler =
          new V3DefaultColumnHandler(indexDir, segmentDirectory.getSegmentMetadata(),
              new IndexLoadingConfig(getTableConfigWithTransformFunction(ORIGINAL_TRANSFORM_FUNCTION), schema), writer);
      defaultColumnHandler.updateDefaultColumns();
    }

    SegmentMetadataImpl originalSegmentMetadata = new SegmentMetadataImpl(indexDir);
    assertEquals(originalSegmentMetadata.getColumnMetadataFor(DERIVED_COLUMN).getTransformFunction(),
        ORIGINAL_TRANSFORM_FUNCTION);
    removeTransformFunctionFromMetadata(indexDir, DERIVED_COLUMN);

    SegmentMetadataImpl legacySegmentMetadata = new SegmentMetadataImpl(indexDir);
    assertNull(legacySegmentMetadata.getColumnMetadataFor(DERIVED_COLUMN).getTransformFunction());
    ColumnMetadata sourceColumnMetadata = legacySegmentMetadata.getColumnMetadataFor("column1");

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      DefaultColumnHandler defaultColumnHandler =
          new V3DefaultColumnHandler(indexDir, segmentDirectory.getSegmentMetadata(),
              new IndexLoadingConfig(getTableConfigWithTransformFunction(UPDATED_TRANSFORM_FUNCTION), schema), writer);
      defaultColumnHandler.updateDefaultColumns();
    }

    SegmentMetadataImpl updatedSegmentMetadata = new SegmentMetadataImpl(indexDir);
    ColumnMetadata derivedColumnMetadata = updatedSegmentMetadata.getColumnMetadataFor(DERIVED_COLUMN);
    assertEquals(derivedColumnMetadata.getTransformFunction(), UPDATED_TRANSFORM_FUNCTION);
    assertEquals(derivedColumnMetadata.getMinValue(), (Integer) sourceColumnMetadata.getMinValue() + 2);
    assertEquals(derivedColumnMetadata.getMaxValue(), (Integer) sourceColumnMetadata.getMaxValue() + 2);
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
    assertEquals(computeDefaultColumnActionMap(null, UPDATED_TRANSFORM_FUNCTION),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    assertEquals(computeDefaultColumnActionMap(ORIGINAL_TRANSFORM_FUNCTION, null),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    assertEquals(computeDefaultColumnActionMap(null, null), Map.of());
  }

  private void testComputeDefaultColumnActionMap(Map<String, DefaultColumnAction> expected) {
    BaseDefaultColumnHandler defaultColumnHandler =
        new V3DefaultColumnHandler(INDEX_DIR, _segmentDirectory.getSegmentMetadata(),
            new IndexLoadingConfig(TABLE_CONFIG, _schema), _writer);
    assertEquals(defaultColumnHandler.computeDefaultColumnActionMap(), expected);
  }

  private static Map<String, DefaultColumnAction> computeDefaultColumnActionMap(String transformFunctionInMetadata,
      String transformFunctionInTableConfig) {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(DERIVED_COLUMN, DataType.INT)
        .build();
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getColumnName()).thenReturn(DERIVED_COLUMN);
    when(columnMetadata.isAutoGenerated()).thenReturn(true);
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
}
