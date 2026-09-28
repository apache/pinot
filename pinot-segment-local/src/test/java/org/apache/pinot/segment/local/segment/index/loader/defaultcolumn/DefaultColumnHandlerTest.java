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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.recordtransformer.TransformProvenanceUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.BaseSegmentCreator;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.BaseDefaultColumnHandler.DefaultColumnAction;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


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

  @DataProvider(name = "segmentVersions")
  public Object[][] segmentVersions() {
    return new Object[][]{{SegmentVersion.v1}, {SegmentVersion.v3}};
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
    assertEquals(segmentMetadata.getColumnMetadataFor(DERIVED_COLUMN).getTransformFunctionProvenanceVersion(),
        TransformProvenanceUtils.CURRENT_VERSION);
  }

  /// Legacy segments carry no provenance marker. Reloading them must neither regenerate values nor invent provenance,
  /// because the expression that produced their stored values is unknowable.
  @Test
  public void testLegacyDerivedColumnWithoutProvenanceIsUntouched()
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

    TableConfig tableConfig = getTableConfigWithTransformFunction(UPDATED_TRANSFORM_FUNCTION);
    assertEquals(computeDefaultColumnActionMap(indexDir, tableConfig, schema), Map.of());
    updateDefaultColumns(indexDir, tableConfig, schema);

    ColumnMetadata derivedColumnMetadata = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED_COLUMN);
    assertNull(derivedColumnMetadata.getTransformFunction());
    assertEquals(derivedColumnMetadata.getTransformFunctionProvenanceVersion(), ColumnMetadata.UNAVAILABLE);
    assertEquals(derivedColumnMetadata.getMinValue(), legacyMinValue);
    assertEquals(derivedColumnMetadata.getMaxValue(), legacyMaxValue);
    assertNotNull(sourceColumnMetadata);
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
    assertEquals(updateDefaultColumnsAndGetChangedTransformValues(indexDir, tableConfig, schema, false),
        Set.of(DERIVED_COLUMN));

    ColumnMetadata derivedColumnMetadata = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED_COLUMN);
    assertNull(derivedColumnMetadata.getTransformFunction());
    assertEquals(derivedColumnMetadata.getTransformFunctionProvenanceVersion(),
        TransformProvenanceUtils.CURRENT_VERSION);
    Object defaultNullValue = schema.getFieldSpecFor(DERIVED_COLUMN).getDefaultNullValue();
    assertEquals(derivedColumnMetadata.getMinValue(), defaultNullValue);
    assertEquals(derivedColumnMetadata.getMaxValue(), defaultNullValue);
    // The stale transform function must be gone from the metadata, otherwise every reload would rebuild the column.
    assertEquals(computeDefaultColumnActionMap(indexDir, tableConfig, schema), Map.of());

    // Provenance survives the no-transform state, so adding A again regenerates instead of being mistaken for legacy.
    TableConfig restoredTableConfig = getTableConfigWithTransformFunction(ORIGINAL_TRANSFORM_FUNCTION);
    assertEquals(computeDefaultColumnActionMap(indexDir, restoredTableConfig, schema),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    updateDefaultColumns(indexDir, restoredTableConfig, schema);
    ColumnMetadata restored = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED_COLUMN);
    assertEquals(restored.getTransformFunction(), ORIGINAL_TRANSFORM_FUNCTION);
    assertEquals(restored.getTransformFunctionProvenanceVersion(), 1);
  }

  @Test(dataProvider = "segmentVersions")
  public void testInfeasibleTransformUpdatePreservesExistingColumn(SegmentVersion segmentVersion)
      throws Exception {
    Schema schema = getSchemaWithDerivedColumn();
    File indexDir = buildSegmentWithAutoGeneratedDerivedColumn(
        "infeasibleTransformUpdate-" + segmentVersion, schema, segmentVersion);
    ColumnMetadata originalMetadata = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED_COLUMN);
    TableConfig updateConfig = getTableConfigWithTransformFunction(UPDATED_TRANSFORM_FUNCTION, true);

    assertEquals(computeDefaultColumnActionMap(indexDir, updateConfig, schema),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    assertEquals(updateDefaultColumnsAndGetChangedTransformValues(indexDir, updateConfig, schema, false), Set.of());

    assertDerivedColumnUnchanged(indexDir, originalMetadata);
  }

  @Test(dataProvider = "segmentVersions")
  public void testFailedTransformUpdatePreservesExistingColumn(SegmentVersion segmentVersion)
      throws Exception {
    Schema schema = getSchemaWithDerivedColumn();
    File indexDir = buildSegmentWithAutoGeneratedDerivedColumn(
        "failedTransformUpdate-" + segmentVersion, schema, segmentVersion);
    ColumnMetadata originalMetadata = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED_COLUMN);
    TableConfig updateConfig = getTableConfigWithTransformFunction("reverse(column3)");

    // reverse() returns STRING values which cannot be stored in the INT derived column.
    assertEquals(computeDefaultColumnActionMap(indexDir, updateConfig, schema),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    expectThrows(Exception.class, () -> updateDefaultColumns(indexDir, updateConfig, schema, true));

    assertDerivedColumnUnchanged(indexDir, originalMetadata);
  }

  @Test
  public void testTransformFunctionRoundTripsThroughBase64Metadata()
      throws Exception {
    File indexDir = buildSegmentWithAutoGeneratedDerivedColumn("transformRoundTrip", getSchemaWithDerivedColumn());
    for (String transformFunction : List.of("Groovy({x + '😀'}, x)", "Groovy({x + '${value}'}, x)",
        "Groovy({column1 + ',' + column2.replace('\\\\', ' ')}, column1, column2)")) {
      PropertiesConfiguration properties = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
      BaseSegmentCreator.addTransformFunction(properties, DERIVED_COLUMN, transformFunction);
      SegmentMetadataUtils.savePropertiesConfiguration(properties, indexDir);
      ColumnMetadata metadata = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED_COLUMN);
      assertEquals(metadata.getTransformFunction(), transformFunction);
      assertEquals(metadata.getTransformFunctionProvenanceVersion(), 1);
    }
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
    ColumnMetadata derivedColumnMetadata = updatedSegmentMetadata.getColumnMetadataFor(DERIVED_COLUMN);
    assertEquals(derivedColumnMetadata.getTransformFunction(), transformFunction);
    // Default-column updates cannot certify dependency closure, even when they persist a direct expression.
    assertEquals(derivedColumnMetadata.getTransformFunctionProvenanceVersion(), 1);
    assertNull(derivedColumnMetadata.getTransformFunctionFingerprint());
  }

  @Test
  public void testComputeDefaultColumnActionMapForTransformFunction() {
    assertEquals(computeDefaultColumnActionMap(ORIGINAL_TRANSFORM_FUNCTION, UPDATED_TRANSFORM_FUNCTION),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    assertEquals(computeDefaultColumnActionMap(ORIGINAL_TRANSFORM_FUNCTION, ORIGINAL_TRANSFORM_FUNCTION), Map.of());
    // A known no-transform state differs from a configured expression.
    assertEquals(computeDefaultColumnActionMap(null, UPDATED_TRANSFORM_FUNCTION),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    assertEquals(computeDefaultColumnActionMap(ORIGINAL_TRANSFORM_FUNCTION, null),
        Map.of(DERIVED_COLUMN, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    assertEquals(computeDefaultColumnActionMap(null, null), Map.of());
    // Without provenance, the same apparent null expression belongs to a legacy segment and must stay untouched.
    assertEquals(computeDefaultColumnActionMap(null, UPDATED_TRANSFORM_FUNCTION, true, ColumnMetadata.UNAVAILABLE),
        Map.of());
  }

  @Test
  public void testTransformChainChangesAreDeferredToRecordReplay() {
    String root = "chainRoot";
    String leaf = "chainLeaf";
    String standalone = "standalone";
    Map<String, String> storedTransforms = Map.of(
        root, "plus(column1, 1)",
        leaf, "plus(chainRoot, 100)",
        standalone, "plus(column1, 10)");

    // Changing a root must not regenerate it while leaving its unchanged dependent stale. A standalone direct change
    // remains eligible for the default-column path.
    BaseDefaultColumnHandler currentChainHandler = newHandlerForTransformGraph(storedTransforms, Map.of(
        root, "plus(column1, 2)",
        leaf, "plus(chainRoot, 100)",
        standalone, "plus(column1, 20)"));
    assertEquals(currentChainHandler.computeDefaultColumnActionMap(),
        Map.of(standalone, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    assertEquals(currentChainHandler.getColumnsWithPendingTransformValueChanges(), Set.of(standalone));

    // The old graph is also authoritative for deciding whether an in-place update is safe when a config change removes
    // an edge. Both former chain participants must still be left for dependency-ordered record replay.
    BaseDefaultColumnHandler storedChainHandler = newHandlerForTransformGraph(storedTransforms, Map.of(
        root, "plus(column1, 2)",
        leaf, "plus(column1, 100)",
        standalone, "plus(column1, 20)"));
    assertEquals(storedChainHandler.computeDefaultColumnActionMap(),
        Map.of(standalone, DefaultColumnAction.UPDATE_DIMENSION_TRANSFORM_FUNCTION));
    assertEquals(storedChainHandler.getColumnsWithPendingTransformValueChanges(), Set.of(standalone));

    // An old expression can become unparseable after a UDF is removed. Dependency discovery must fail closed without
    // aborting handler construction or allowing an unrelated partial transform update.
    BaseDefaultColumnHandler unparseableStoredGraphHandler = newHandlerForTransformGraph(Map.of(
        root, "malformed(", standalone, "plus(column1, 10)"), Map.of(
        root, "plus(column1, 2)", standalone, "plus(column1, 20)"));
    assertEquals(unparseableStoredGraphHandler.computeDefaultColumnActionMap(), Map.of());
    assertEquals(unparseableStoredGraphHandler.getColumnsWithPendingTransformValueChanges(), Set.of());
  }

  @Test
  public void testMutableDefaultInputAndDependentTransformAreDeferredToRecordReplay() {
    String defaultInput = "defaultInput";
    String dependent = "dependent";
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addField(new MetricFieldSpec(defaultInput, DataType.INT, 20))
        .addMetric(dependent, DataType.INT)
        .build();

    ColumnMetadata defaultInputMetadata = mock(ColumnMetadata.class);
    when(defaultInputMetadata.getColumnName()).thenReturn(defaultInput);
    when(defaultInputMetadata.isAutoGenerated()).thenReturn(true);
    when(defaultInputMetadata.getFieldSpec()).thenReturn(new MetricFieldSpec(defaultInput, DataType.INT, 10));
    when(defaultInputMetadata.getTransformFunctionProvenanceVersion())
        .thenReturn(TransformProvenanceUtils.CURRENT_VERSION);

    String dependentTransform = "plus(defaultInput, 100)";
    ColumnMetadata dependentMetadata = mock(ColumnMetadata.class);
    when(dependentMetadata.getColumnName()).thenReturn(dependent);
    when(dependentMetadata.isAutoGenerated()).thenReturn(false);
    when(dependentMetadata.getFieldSpec()).thenReturn(schema.getFieldSpecFor(dependent));
    when(dependentMetadata.getTransformFunction()).thenReturn(dependentTransform);
    when(dependentMetadata.getTransformFunctionProvenanceVersion())
        .thenReturn(TransformProvenanceUtils.CURRENT_VERSION);

    TreeMap<String, ColumnMetadata> columnMetadataMap = new TreeMap<>();
    columnMetadataMap.put(defaultInput, defaultInputMetadata);
    columnMetadataMap.put(dependent, dependentMetadata);
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getColumnMetadataFor(defaultInput)).thenReturn(defaultInputMetadata);
    when(segmentMetadata.getColumnMetadataFor(dependent)).thenReturn(dependentMetadata);
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(columnMetadataMap);

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(List.of(new TransformConfig(dependent, dependentTransform)));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setIngestionConfig(ingestionConfig)
        .build();
    BaseDefaultColumnHandler handler = new V3DefaultColumnHandler(new File("."), segmentMetadata,
        new IndexLoadingConfig(tableConfig, schema), mock(SegmentDirectory.Writer.class));

    // Updating the input in place would leave the already persisted dependent at its old value.
    assertEquals(handler.computeDefaultColumnActionMap(), Map.of());
    assertFalse(handler.needStructuralDefaultColumnUpdates());
    assertEquals(handler.getColumnsWithPendingTransformValueChanges(), Set.of());
  }

  @Test
  public void testTransformFunctionIsIgnoredForNonAutoGeneratedColumn() {
    assertEquals(computeDefaultColumnActionMap(ORIGINAL_TRANSFORM_FUNCTION, UPDATED_TRANSFORM_FUNCTION, false),
        Map.of());
    assertEquals(computeDefaultColumnActionMap(null, UPDATED_TRANSFORM_FUNCTION, false), Map.of());
  }

  @Test
  public void testStructuralVsTransformDefaultColumnSignals() {
    BaseDefaultColumnHandler updateHandler = newHandlerForTransformMetadata(ORIGINAL_TRANSFORM_FUNCTION,
        UPDATED_TRANSFORM_FUNCTION, true, 1);
    assertTrue(updateHandler.needUpdateDefaultColumns());
    assertFalse(updateHandler.needStructuralDefaultColumnUpdates());
    assertEquals(updateHandler.getColumnsWithPendingTransformValueChanges(), Set.of(DERIVED_COLUMN));

    BaseDefaultColumnHandler legacyHandler =
        newHandlerForTransformMetadata(null, UPDATED_TRANSFORM_FUNCTION, true, ColumnMetadata.UNAVAILABLE);
    assertFalse(legacyHandler.needUpdateDefaultColumns());
    assertFalse(legacyHandler.needStructuralDefaultColumnUpdates());
    assertEquals(legacyHandler.getColumnsWithPendingTransformValueChanges(), Set.of());

    BaseDefaultColumnHandler addHandler = newHandlerForMissingColumn(UPDATED_TRANSFORM_FUNCTION);
    assertTrue(addHandler.needUpdateDefaultColumns());
    assertTrue(addHandler.needStructuralDefaultColumnUpdates());
    assertEquals(addHandler.getColumnsWithPendingTransformValueChanges(), Set.of());
  }

  @Test
  public void testTransformChangeDetectedAlongsideStructuralUpdate() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(DERIVED_COLUMN, DataType.LONG)
        .build();
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getColumnName()).thenReturn(DERIVED_COLUMN);
    when(columnMetadata.isAutoGenerated()).thenReturn(true);
    when(columnMetadata.getFieldSpec()).thenReturn(new DimensionFieldSpec(DERIVED_COLUMN, DataType.INT, true));
    when(columnMetadata.getTransformFunction()).thenReturn(ORIGINAL_TRANSFORM_FUNCTION);
    when(columnMetadata.getTransformFunctionProvenanceVersion()).thenReturn(1);
    TreeMap<String, ColumnMetadata> columnMetadataMap = new TreeMap<>();
    columnMetadataMap.put(DERIVED_COLUMN, columnMetadata);

    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getColumnMetadataFor(DERIVED_COLUMN)).thenReturn(columnMetadata);
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(columnMetadataMap);
    TableConfig tableConfig = getTableConfigWithTransformFunction(UPDATED_TRANSFORM_FUNCTION);
    tableConfig.getIngestionConfig().setTransformConfigs(List.of(
        new TransformConfig(DERIVED_COLUMN, UPDATED_TRANSFORM_FUNCTION),
        new TransformConfig("dependentColumn", "plus(derivedColumn, 1)")));
    BaseDefaultColumnHandler handler = new V3DefaultColumnHandler(new File("."), segmentMetadata,
        new IndexLoadingConfig(tableConfig, schema),
        mock(SegmentDirectory.Writer.class));

    // Structural and value changes are both deferred because rebuilding only the root would leave its persisted
    // dependent stale.
    assertEquals(handler.computeDefaultColumnActionMap(), Map.of());
    assertEquals(handler.getColumnsWithPendingTransformValueChanges(), Set.of());
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
    return computeDefaultColumnActionMap(transformFunctionInMetadata, transformFunctionInTableConfig, autoGenerated,
        1);
  }

  private static Map<String, DefaultColumnAction> computeDefaultColumnActionMap(String transformFunctionInMetadata,
      String transformFunctionInTableConfig, boolean autoGenerated, int transformFunctionProvenanceVersion) {
    return newHandlerForTransformMetadata(transformFunctionInMetadata, transformFunctionInTableConfig, autoGenerated,
        transformFunctionProvenanceVersion).computeDefaultColumnActionMap();
  }

  private static BaseDefaultColumnHandler newHandlerForTransformMetadata(String transformFunctionInMetadata,
      String transformFunctionInTableConfig, boolean autoGenerated, int transformFunctionProvenanceVersion) {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(DERIVED_COLUMN, DataType.INT)
        .build();
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getColumnName()).thenReturn(DERIVED_COLUMN);
    when(columnMetadata.isAutoGenerated()).thenReturn(autoGenerated);
    when(columnMetadata.getFieldSpec()).thenReturn(schema.getFieldSpecFor(DERIVED_COLUMN));
    when(columnMetadata.getTransformFunction()).thenReturn(transformFunctionInMetadata);
    when(columnMetadata.getTransformFunctionProvenanceVersion()).thenReturn(transformFunctionProvenanceVersion);
    TreeMap<String, ColumnMetadata> columnMetadataMap = new TreeMap<>();
    columnMetadataMap.put(DERIVED_COLUMN, columnMetadata);

    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getColumnMetadataFor(DERIVED_COLUMN)).thenReturn(columnMetadata);
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(columnMetadataMap);
    return new V3DefaultColumnHandler(new File("."), segmentMetadata,
        new IndexLoadingConfig(getTableConfigWithTransformFunction(transformFunctionInTableConfig), schema),
        mock(SegmentDirectory.Writer.class));
  }

  private static BaseDefaultColumnHandler newHandlerForTransformGraph(Map<String, String> storedTransforms,
      Map<String, String> currentTransforms) {
    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME);
    Set<String> columns = new HashSet<>(storedTransforms.keySet());
    columns.addAll(currentTransforms.keySet());
    for (String column : columns) {
      schemaBuilder.addSingleValueDimension(column, DataType.INT);
    }
    Schema schema = schemaBuilder.build();

    TreeMap<String, ColumnMetadata> columnMetadataMap = new TreeMap<>();
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    for (Map.Entry<String, String> entry : storedTransforms.entrySet()) {
      String column = entry.getKey();
      ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
      when(columnMetadata.getColumnName()).thenReturn(column);
      when(columnMetadata.isAutoGenerated()).thenReturn(true);
      when(columnMetadata.getFieldSpec()).thenReturn(schema.getFieldSpecFor(column));
      when(columnMetadata.getTransformFunction()).thenReturn(entry.getValue());
      when(columnMetadata.getTransformFunctionProvenanceVersion()).thenReturn(TransformProvenanceUtils.CURRENT_VERSION);
      when(columnMetadata.getTransformFunctionFingerprint()).thenReturn("stored-" + column);
      columnMetadataMap.put(column, columnMetadata);
      when(segmentMetadata.getColumnMetadataFor(column)).thenReturn(columnMetadata);
    }
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(columnMetadataMap);

    List<TransformConfig> transformConfigs = new ArrayList<>();
    for (Map.Entry<String, String> entry : new TreeMap<>(currentTransforms).entrySet()) {
      transformConfigs.add(new TransformConfig(entry.getKey(), entry.getValue()));
    }
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setIngestionConfig(ingestionConfig)
        .build();
    return new V3DefaultColumnHandler(new File("."), segmentMetadata,
        new IndexLoadingConfig(tableConfig, schema), mock(SegmentDirectory.Writer.class));
  }

  private static BaseDefaultColumnHandler newHandlerForMissingColumn(String transformFunctionInTableConfig) {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(DERIVED_COLUMN, DataType.INT)
        .build();
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getColumnMetadataFor(DERIVED_COLUMN)).thenReturn(null);
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(new TreeMap<>());
    return new V3DefaultColumnHandler(new File("."), segmentMetadata,
        new IndexLoadingConfig(getTableConfigWithTransformFunction(transformFunctionInTableConfig), schema),
        mock(SegmentDirectory.Writer.class));
  }

  /// Builds a segment without the derived column, then materializes it through the default column handler so that it
  /// is marked as auto-generated in the segment metadata, the way a reload after a schema change would.
  private File buildSegmentWithAutoGeneratedDerivedColumn(String tempDirName, Schema schema)
      throws Exception {
    return buildSegmentWithAutoGeneratedDerivedColumn(tempDirName, schema, SegmentVersion.v3);
  }

  private File buildSegmentWithAutoGeneratedDerivedColumn(String tempDirName, Schema schema,
      SegmentVersion segmentVersion)
      throws Exception {
    File indexDir = buildSegment(new File(TEMP_DIR, tempDirName), TABLE_CONFIG,
        Schema.fromString(_schema.toSingleLineJsonString()), segmentVersion);
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
      return ((BaseDefaultColumnHandler) DefaultColumnHandlerFactory.getDefaultColumnHandler(indexDir,
          segmentDirectory.getSegmentMetadata(), new IndexLoadingConfig(tableConfig, schema), writer))
          .computeDefaultColumnActionMap();
    }
  }

  private static void updateDefaultColumns(File indexDir, TableConfig tableConfig, Schema schema)
      throws Exception {
    updateDefaultColumns(indexDir, tableConfig, schema, false);
  }

  private static void updateDefaultColumns(File indexDir, TableConfig tableConfig, Schema schema,
      boolean errorOnColumnBuildFailure)
      throws Exception {
    updateDefaultColumnsAndGetChangedTransformValues(indexDir, tableConfig, schema, errorOnColumnBuildFailure);
  }

  private static Set<String> updateDefaultColumnsAndGetChangedTransformValues(File indexDir, TableConfig tableConfig,
      Schema schema, boolean errorOnColumnBuildFailure)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
      indexLoadingConfig.setErrorOnColumnBuildFailure(errorOnColumnBuildFailure);
      DefaultColumnHandler handler = DefaultColumnHandlerFactory.getDefaultColumnHandler(indexDir,
          segmentDirectory.getSegmentMetadata(), indexLoadingConfig, writer);
      handler.updateDefaultColumns();
      return handler.getColumnsWithChangedTransformValues();
    }
  }

  private static TableConfig getTableConfigWithTransformFunction(String transformFunction) {
    return getTableConfigWithTransformFunction(transformFunction, false);
  }

  private static TableConfig getTableConfigWithTransformFunction(String transformFunction,
      boolean forwardIndexDisabled) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    if (transformFunction != null) {
      ingestionConfig.setTransformConfigs(List.of(new TransformConfig(DERIVED_COLUMN, transformFunction)));
    } else {
      ingestionConfig.setTransformConfigs(List.of());
    }
    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setIngestionConfig(ingestionConfig);
    if (forwardIndexDisabled) {
      tableConfigBuilder.setFieldConfigList(List.of(
          new FieldConfig(DERIVED_COLUMN, FieldConfig.EncodingType.DICTIONARY, List.of(), null,
              Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true"))));
    }
    return tableConfigBuilder.build();
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
    return buildSegment(segmentGenerationTempDir, tableConfig, schema, SegmentVersion.v3);
  }

  private File buildSegment(File segmentGenerationTempDir, TableConfig tableConfig, Schema schema,
      SegmentVersion segmentVersion)
      throws Exception {
    FileUtils.deleteQuietly(segmentGenerationTempDir);

    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInputFilePath(avroFile.getAbsolutePath());
    config.setOutDir(segmentGenerationTempDir.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);
    config.setSegmentVersion(segmentVersion);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    return new File(segmentGenerationTempDir, SEGMENT_NAME);
  }

  private static void assertDerivedColumnUnchanged(File indexDir, ColumnMetadata originalMetadata)
      throws Exception {
    ColumnMetadata currentMetadata = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED_COLUMN);
    assertNotNull(currentMetadata);
    assertEquals(currentMetadata.getTransformFunction(), originalMetadata.getTransformFunction());
    assertEquals(currentMetadata.getTransformFunctionProvenanceVersion(),
        originalMetadata.getTransformFunctionProvenanceVersion());
    assertEquals(currentMetadata.getTransformFunctionFingerprint(),
        originalMetadata.getTransformFunctionFingerprint());
    assertEquals(currentMetadata.getFieldSpec(), originalMetadata.getFieldSpec());
    assertEquals(currentMetadata.getCardinality(), originalMetadata.getCardinality());
    assertEquals(currentMetadata.getMinValue(), originalMetadata.getMinValue());
    assertEquals(currentMetadata.getMaxValue(), originalMetadata.getMaxValue());

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertTrue(reader.hasIndexFor(DERIVED_COLUMN, StandardIndexes.forward()));
      assertTrue(reader.hasIndexFor(DERIVED_COLUMN, StandardIndexes.dictionary()));
    }
    File[] stagingDirectories = indexDir.listFiles(
        file -> file.isDirectory() && file.getName().startsWith(".default-column-"));
    assertNotNull(stagingDirectories);
    assertEquals(stagingDirectories.length, 0);
  }

  private static void removeTransformFunctionFromMetadata(File indexDir, String column)
      throws Exception {
    PropertiesConfiguration segmentProperties = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    segmentProperties.clearProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION));
    segmentProperties.clearProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION_BASE64));
    segmentProperties.clearProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
        V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION_PROVENANCE_VERSION));
    SegmentMetadataUtils.savePropertiesConfiguration(segmentProperties, indexDir);
  }
}
