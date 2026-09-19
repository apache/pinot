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
package org.apache.pinot.plugin.minion.tasks.refreshsegment;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.plugin.minion.tasks.MinionTaskTestUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.recordtransformer.TransformProvenanceUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.DefaultColumnHandlerFactory;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// RefreshSegment omits changed derived outputs during record replay so ingestion transforms recompute them, while
/// legacy columns without provenance remain untouched.
public class RefreshSegmentTaskExecutorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), RefreshSegmentTaskExecutorTest.class.getSimpleName());
  private static final String TABLE_NAME = "refreshDerivedTable";
  private static final String TABLE_NAME_WITH_TYPE = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final String SEGMENT_NAME = "testSegment";
  private static final String SRC = "src";
  private static final String SRC_JSON = "srcJson";
  private static final String DERIVED = "derived";
  private static final String INTERMEDIATE = "intermediate";
  private static final String FINAL = "final";
  private static final String NEW_COL = "newCol";
  private static final String ORIGINAL_TRANSFORM = "plus(src, 1)";
  private static final String UPDATED_TRANSFORM = "plus(src, 2)";
  private static final String ORIGINAL_INTERMEDIATE_TRANSFORM = "plus(derived, 10)";
  private static final String UPDATED_INTERMEDIATE_TRANSFORM = "plus(derived, 20)";
  private static final String FINAL_TRANSFORM = "plus(intermediate, 100)";
  private static final String LEGACY_NULL_TRANSFORM = "jsonPathString(srcJson, '$.missing')";
  private static final String CURRENT_NULL_TRANSFORM = "jsonPathString(srcJson, '$.missing', 'changed')";

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    MinionMetrics.register(mock(MinionMetrics.class));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testRefreshSegmentLeavesLegacyDerivedColumnUntouched()
      throws Exception {
    File indexDir = buildSegmentWithAutoGeneratedDerived("backfillFreeze", ORIGINAL_TRANSFORM);
    removeTransformFunctionFromMetadata(indexDir, DERIVED);
    assertTrue(new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED).isAutoGenerated());

    stubTableAndSchema(tableConfig(ORIGINAL_TRANSFORM), schemaWithDerived());
    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "backfillWorking"));

    assertEquals(result.getFile(), indexDir);
    ColumnMetadata derived = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED);
    assertTrue(derived.isAutoGenerated());
    assertNull(derived.getTransformFunction());
    assertEquals(derived.getTransformFunctionProvenanceVersion(), ColumnMetadata.UNAVAILABLE);
  }

  @Test
  public void testRefreshSegmentRecomputesChangedDerivedColumn()
      throws Exception {
    File indexDir = buildSegmentWithAutoGeneratedDerived("updateFreeze", ORIGINAL_TRANSFORM);
    ColumnMetadata before = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED);
    assertEquals(before.getTransformFunction(), ORIGINAL_TRANSFORM);
    assertEquals(before.getMinValue(), 11);

    stubTableAndSchema(tableConfig(UPDATED_TRANSFORM), schemaWithDerived());
    File workingDir = new File(TEMP_DIR, "updateWorking");
    SegmentConversionResult result = convert(indexDir, workingDir);

    assertNotEquals(result.getFile(), indexDir);
    ColumnMetadata after = new SegmentMetadataImpl(result.getFile()).getColumnMetadataFor(DERIVED);
    assertTrue(after.isAutoGenerated());
    assertEquals(after.getTransformFunction(), UPDATED_TRANSFORM);
    assertEquals(after.getTransformFunctionProvenanceVersion(), TransformProvenanceUtils.CURRENT_VERSION);
    assertEquals(after.getMinValue(), 12);
    assertEquals(after.getMaxValue(), 22);
  }

  @Test
  public void testRefreshSegmentRecomputesChangedNonAutoGeneratedDerivedColumn()
      throws Exception {
    File indexDir = buildSegmentWithIngestionDerived("updateIngestionDerived", ORIGINAL_TRANSFORM);
    ColumnMetadata before = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED);
    assertFalse(before.isAutoGenerated());
    assertEquals(before.getTransformFunction(), ORIGINAL_TRANSFORM);
    assertEquals(before.getMinValue(), 11);

    stubTableAndSchema(tableConfig(UPDATED_TRANSFORM), schemaWithDerived());
    SegmentConversionResult result =
        convert(indexDir, new File(TEMP_DIR, "updateIngestionDerivedWorking"));

    assertNotEquals(result.getFile(), indexDir);
    ColumnMetadata after = new SegmentMetadataImpl(result.getFile()).getColumnMetadataFor(DERIVED);
    assertFalse(after.isAutoGenerated());
    assertEquals(after.getTransformFunction(), UPDATED_TRANSFORM);
    assertEquals(after.getTransformFunctionProvenanceVersion(), TransformProvenanceUtils.CURRENT_VERSION);
    assertEquals(after.getMinValue(), 12);
    assertEquals(after.getMaxValue(), 22);
  }

  @Test
  public void testRefreshSegmentTracksKnownNoTransformState()
      throws Exception {
    File indexDir = buildSegmentWithIngestionDerived("knownNoTransform", ORIGINAL_TRANSFORM);
    TableConfig noTransformTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    stubTableAndSchema(noTransformTableConfig, schemaWithDerived());

    SegmentConversionResult removed = convert(indexDir, new File(TEMP_DIR, "knownNoTransformRemovedWorking"));

    ColumnMetadata withoutTransform = new SegmentMetadataImpl(removed.getFile()).getColumnMetadataFor(DERIVED);
    assertNull(withoutTransform.getTransformFunction());
    assertEquals(withoutTransform.getTransformFunctionProvenanceVersion(), TransformProvenanceUtils.CURRENT_VERSION);

    stubTableAndSchema(tableConfig(ORIGINAL_TRANSFORM), schemaWithDerived());
    SegmentConversionResult restored =
        convert(removed.getFile(), new File(TEMP_DIR, "knownNoTransformRestoredWorking"));

    assertNotEquals(restored.getFile(), removed.getFile());
    ColumnMetadata restoredMetadata = new SegmentMetadataImpl(restored.getFile()).getColumnMetadataFor(DERIVED);
    assertEquals(restoredMetadata.getTransformFunction(), ORIGINAL_TRANSFORM);
    assertEquals(restoredMetadata.getMinValue(), 11);
    assertEquals(restoredMetadata.getMaxValue(), 21);
  }

  @Test
  public void testRefreshSegmentRecomputesTransformChangedWithDataType()
      throws Exception {
    File indexDir = buildSegmentWithAutoGeneratedDerived("updateTransformAndType", ORIGINAL_TRANSFORM);
    Schema updatedSchema = schemaWithDerived(DataType.LONG);
    stubTableAndSchema(tableConfig(UPDATED_TRANSFORM), updatedSchema);

    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "updateTransformAndTypeWorking"));

    assertNotEquals(result.getFile(), indexDir);
    ColumnMetadata after = new SegmentMetadataImpl(result.getFile()).getColumnMetadataFor(DERIVED);
    assertTrue(after.isAutoGenerated());
    assertEquals(after.getFieldSpec().getDataType(), DataType.LONG);
    assertEquals(after.getTransformFunction(), UPDATED_TRANSFORM);
    assertEquals(after.getTransformFunctionProvenanceVersion(), TransformProvenanceUtils.CURRENT_VERSION);
    assertEquals(after.getMinValue(), 12L);
    assertEquals(after.getMaxValue(), 22L);
  }

  @Test
  public void testRefreshSegmentRecomputesUnchangedTransformWhenOutputTypeChanges()
      throws Exception {
    String transform = "Groovy({src + 0.5}, src)";
    Schema originalSchema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric(SRC, DataType.INT)
        .addMetric(DERIVED, DataType.INT)
        .build();
    TableConfig tableConfig = tableConfig(transform);
    File indexDir = buildSegmentWithTransformChain("unchangedTransformType", tableConfig, originalSchema);
    assertEquals(new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED).getMinValue(), 10);

    Schema updatedSchema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric(SRC, DataType.INT)
        .addMetric(DERIVED, DataType.DOUBLE)
        .build();
    stubTableAndSchema(tableConfig, updatedSchema);
    SegmentConversionResult result =
        convert(indexDir, new File(TEMP_DIR, "unchangedTransformTypeWorking"));

    ColumnMetadata after = new SegmentMetadataImpl(result.getFile()).getColumnMetadataFor(DERIVED);
    assertEquals(after.getFieldSpec().getDataType(), DataType.DOUBLE);
    assertEquals(after.getMinValue(), 10.5);
    assertEquals(after.getMaxValue(), 20.5);
  }

  @Test
  public void testRefreshSegmentRecomputesTrustedAncestorForNewDependentTransform()
      throws Exception {
    String derivedTransform = "Groovy({src + 0.5}, src)";
    Schema originalSchema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric(SRC, DataType.INT)
        .addMetric(DERIVED, DataType.INT)
        .build();
    TableConfig originalTableConfig = tableConfig(derivedTransform);
    File indexDir = buildSegmentWithTransformChain("newDependent", originalTableConfig, originalSchema);
    ColumnMetadata beforeDerived = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED);
    assertEquals(beforeDerived.getMinValue(), 10);
    assertEquals(beforeDerived.getTransformFunctionProvenanceVersion(), TransformProvenanceUtils.CURRENT_VERSION);

    Schema updatedSchema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric(SRC, DataType.INT)
        .addMetric(DERIVED, DataType.INT)
        .addMetric(FINAL, DataType.DOUBLE)
        .build();
    TableConfig updatedTableConfig = tableConfigWithTransforms(
        new TransformConfig(DERIVED, derivedTransform),
        new TransformConfig(FINAL, "plus(derived, 100)"));
    stubTableAndSchema(updatedTableConfig, updatedSchema);
    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "newDependentWorking"));

    SegmentMetadataImpl after = new SegmentMetadataImpl(result.getFile());
    assertEquals(after.getColumnMetadataFor(DERIVED).getMinValue(), 10);
    assertEquals(after.getColumnMetadataFor(FINAL).getMinValue(), 110.5);
    assertEquals(after.getColumnMetadataFor(FINAL).getMaxValue(), 120.5);
  }

  @Test
  public void testRefreshSegmentRecomputesDependentOfChangedDefaultColumn()
      throws Exception {
    Schema originalSchema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addField(new MetricFieldSpec(DERIVED, DataType.INT, 10))
        .addMetric(FINAL, DataType.INT)
        .build();
    TableConfig tableConfig = tableConfigWithTransforms(new TransformConfig(FINAL, "plus(derived, 100)"));
    File indexDir = buildSegmentWithDefaultInputChain("changedDefaultInput", tableConfig, originalSchema);
    SegmentMetadataImpl before = new SegmentMetadataImpl(indexDir);
    assertTrue(before.getColumnMetadataFor(DERIVED).isAutoGenerated());
    assertEquals(before.getColumnMetadataFor(DERIVED).getMinValue(), 10);
    assertEquals(before.getColumnMetadataFor(FINAL).getMinValue(), 110);

    Schema updatedSchema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addField(new MetricFieldSpec(DERIVED, DataType.INT, 20))
        .addMetric(FINAL, DataType.INT)
        .build();
    stubTableAndSchema(tableConfig, updatedSchema);
    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "changedDefaultInputWorking"));

    assertNotEquals(result.getFile(), indexDir);
    SegmentMetadataImpl after = new SegmentMetadataImpl(result.getFile());
    assertTrue(after.getColumnMetadataFor(DERIVED).isAutoGenerated());
    assertEquals(after.getColumnMetadataFor(DERIVED).getMinValue(), 20);
    assertEquals(after.getColumnMetadataFor(DERIVED).getMaxValue(), 20);
    assertEquals(after.getColumnMetadataFor(FINAL).getMinValue(), 120);
    assertEquals(after.getColumnMetadataFor(FINAL).getMaxValue(), 120);
  }

  @Test
  public void testRefreshSegmentPersistsNewDefaultInputAcrossRefreshes()
      throws Exception {
    Schema baseSchema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric(SRC, DataType.INT)
        .build();
    TableConfig baseTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    File indexDir = buildSegmentWithTransformChain("newDefaultInput", baseTableConfig, baseSchema);

    TableConfig tableConfig = tableConfigWithTransforms(new TransformConfig(FINAL, "plus(derived, 100)"));
    Schema firstSchema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric(SRC, DataType.INT)
        .addField(new MetricFieldSpec(DERIVED, DataType.INT, 10))
        .addMetric(FINAL, DataType.INT)
        .build();
    stubTableAndSchema(tableConfig, firstSchema);
    SegmentConversionResult firstResult = convert(indexDir, new File(TEMP_DIR, "newDefaultInputFirstWorking"));

    SegmentMetadataImpl firstRefresh = new SegmentMetadataImpl(firstResult.getFile());
    ColumnMetadata firstDefaultInput = firstRefresh.getColumnMetadataFor(DERIVED);
    assertTrue(firstDefaultInput.isAutoGenerated());
    assertNull(firstDefaultInput.getTransformFunction());
    assertEquals(firstDefaultInput.getTransformFunctionProvenanceVersion(), TransformProvenanceUtils.CURRENT_VERSION);
    assertEquals(firstDefaultInput.getMinValue(), 10);
    assertEquals(firstRefresh.getColumnMetadataFor(FINAL).getMinValue(), 110);

    Schema secondSchema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric(SRC, DataType.INT)
        .addField(new MetricFieldSpec(DERIVED, DataType.INT, 20))
        .addMetric(FINAL, DataType.INT)
        .build();
    stubTableAndSchema(tableConfig, secondSchema);
    SegmentConversionResult secondResult =
        convert(firstResult.getFile(), new File(TEMP_DIR, "newDefaultInputSecondWorking"));

    SegmentMetadataImpl secondRefresh = new SegmentMetadataImpl(secondResult.getFile());
    ColumnMetadata secondDefaultInput = secondRefresh.getColumnMetadataFor(DERIVED);
    assertTrue(secondDefaultInput.isAutoGenerated());
    assertEquals(secondDefaultInput.getTransformFunctionProvenanceVersion(), TransformProvenanceUtils.CURRENT_VERSION);
    assertEquals(secondDefaultInput.getMinValue(), 20);
    assertEquals(secondDefaultInput.getMaxValue(), 20);
    assertEquals(secondRefresh.getColumnMetadataFor(FINAL).getMinValue(), 120);
    assertEquals(secondRefresh.getColumnMetadataFor(FINAL).getMaxValue(), 120);
  }

  @Test
  public void testRefreshSegmentRecomputesTransitiveDependentThroughIntermediate()
      throws Exception {
    TableConfig originalTableConfig = tableConfigWithChain(ORIGINAL_TRANSFORM, ORIGINAL_INTERMEDIATE_TRANSFORM);
    File indexDir = buildSegmentWithTransformChain("changedRoot", originalTableConfig);
    SegmentMetadataImpl before = new SegmentMetadataImpl(indexDir);
    ColumnMetadata beforeFinal = before.getColumnMetadataFor(FINAL);
    assertEquals(beforeFinal.getMinValue(), 121);
    assertEquals(beforeFinal.getMaxValue(), 131);
    assertEquals(beforeFinal.getTransformFunctionProvenanceVersion(), TransformProvenanceUtils.CURRENT_VERSION);
    assertNotNull(beforeFinal.getTransformFunctionFingerprint());
    assertNull(before.getColumnMetadataFor(INTERMEDIATE));

    TableConfig updatedTableConfig = tableConfigWithChain(UPDATED_TRANSFORM, ORIGINAL_INTERMEDIATE_TRANSFORM);
    stubTableAndSchema(updatedTableConfig, schemaWithTransformChain());
    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "changedRootWorking"));

    assertNotEquals(result.getFile(), indexDir);
    SegmentMetadataImpl after = new SegmentMetadataImpl(result.getFile());
    assertEquals(after.getColumnMetadataFor(DERIVED).getMinValue(), 12);
    assertEquals(after.getColumnMetadataFor(DERIVED).getMaxValue(), 22);
    ColumnMetadata afterFinal = after.getColumnMetadataFor(FINAL);
    assertEquals(afterFinal.getMinValue(), 122);
    assertEquals(afterFinal.getMaxValue(), 132);
    assertNotEquals(afterFinal.getTransformFunctionFingerprint(), beforeFinal.getTransformFunctionFingerprint());
  }

  @Test
  public void testRefreshSegmentRecomputesV1TransitiveDependentWhenRootChanges()
      throws Exception {
    TableConfig originalTableConfig = tableConfigWithChain(ORIGINAL_TRANSFORM, ORIGINAL_INTERMEDIATE_TRANSFORM);
    File indexDir = buildSegmentWithTransformChain("changedV1Root", originalTableConfig);
    setTransformProvenanceToV1(indexDir, DERIVED);
    setTransformProvenanceToV1(indexDir, FINAL);

    TableConfig updatedTableConfig = tableConfigWithChain(UPDATED_TRANSFORM, ORIGINAL_INTERMEDIATE_TRANSFORM);
    stubTableAndSchema(updatedTableConfig, schemaWithTransformChain());
    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "changedV1RootWorking"));

    SegmentMetadataImpl after = new SegmentMetadataImpl(result.getFile());
    assertEquals(after.getColumnMetadataFor(DERIVED).getMinValue(), 12);
    assertEquals(after.getColumnMetadataFor(DERIVED).getMaxValue(), 22);
    assertEquals(after.getColumnMetadataFor(FINAL).getMinValue(), 122);
    assertEquals(after.getColumnMetadataFor(FINAL).getMaxValue(), 132);
    assertEquals(after.getColumnMetadataFor(FINAL).getTransformFunctionProvenanceVersion(),
        TransformProvenanceUtils.CURRENT_VERSION);
  }

  @Test
  public void testRefreshSegmentRecomputesV1TransitiveDependentWhenRootIsRemoved()
      throws Exception {
    TableConfig originalTableConfig = tableConfigWithChain(ORIGINAL_TRANSFORM, ORIGINAL_INTERMEDIATE_TRANSFORM);
    File indexDir = buildSegmentWithTransformChain("removedV1Root", originalTableConfig);
    setTransformProvenanceToV1(indexDir, DERIVED);
    setTransformProvenanceToV1(indexDir, FINAL);

    TableConfig updatedTableConfig = tableConfigWithTransforms(
        new TransformConfig(INTERMEDIATE, ORIGINAL_INTERMEDIATE_TRANSFORM),
        new TransformConfig(FINAL, FINAL_TRANSFORM));
    Schema schema = schemaWithTransformChain();
    File freshIndexDir = buildSegmentWithTransformChain("removedV1RootFresh", updatedTableConfig, schema);
    SegmentMetadataImpl fresh = new SegmentMetadataImpl(freshIndexDir);
    stubTableAndSchema(updatedTableConfig, schema);
    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "removedV1RootWorking"));

    SegmentMetadataImpl after = new SegmentMetadataImpl(result.getFile());
    assertEquals(after.getColumnMetadataFor(DERIVED).getMinValue(), fresh.getColumnMetadataFor(DERIVED).getMinValue());
    assertEquals(after.getColumnMetadataFor(DERIVED).getMaxValue(), fresh.getColumnMetadataFor(DERIVED).getMaxValue());
    assertEquals(after.getColumnMetadataFor(FINAL).getMinValue(), fresh.getColumnMetadataFor(FINAL).getMinValue());
    assertEquals(after.getColumnMetadataFor(FINAL).getMaxValue(), fresh.getColumnMetadataFor(FINAL).getMaxValue());
    assertEquals(after.getColumnMetadataFor(FINAL).getTransformFunctionProvenanceVersion(),
        TransformProvenanceUtils.CURRENT_VERSION);
  }

  @Test
  public void testRefreshSegmentRecomputesChangedNonSchemaIntermediate()
      throws Exception {
    TableConfig originalTableConfig = tableConfigWithChain(ORIGINAL_TRANSFORM, ORIGINAL_INTERMEDIATE_TRANSFORM);
    File indexDir = buildSegmentWithTransformChain("changedIntermediate", originalTableConfig);
    SegmentMetadataImpl before = new SegmentMetadataImpl(indexDir);
    ColumnMetadata beforeDerived = before.getColumnMetadataFor(DERIVED);
    ColumnMetadata beforeFinal = before.getColumnMetadataFor(FINAL);
    assertEquals(beforeFinal.getTransformFunction(), FINAL_TRANSFORM);
    assertEquals(beforeFinal.getMinValue(), 121);
    assertNull(before.getColumnMetadataFor(INTERMEDIATE));

    TableConfig updatedTableConfig = tableConfigWithChain(ORIGINAL_TRANSFORM, UPDATED_INTERMEDIATE_TRANSFORM);
    stubTableAndSchema(updatedTableConfig, schemaWithTransformChain());
    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "changedIntermediateWorking"));

    assertNotEquals(result.getFile(), indexDir);
    SegmentMetadataImpl after = new SegmentMetadataImpl(result.getFile());
    ColumnMetadata afterDerived = after.getColumnMetadataFor(DERIVED);
    assertEquals(afterDerived.getMinValue(), 11);
    assertEquals(afterDerived.getMaxValue(), 21);
    assertEquals(afterDerived.getTransformFunctionFingerprint(), beforeDerived.getTransformFunctionFingerprint());
    ColumnMetadata afterFinal = after.getColumnMetadataFor(FINAL);
    assertEquals(afterFinal.getTransformFunction(), FINAL_TRANSFORM);
    assertEquals(afterFinal.getMinValue(), 131);
    assertEquals(afterFinal.getMaxValue(), 141);
    assertNotEquals(afterFinal.getTransformFunctionFingerprint(), beforeFinal.getTransformFunctionFingerprint());
  }

  @Test
  public void testRefreshSegmentRecomputesNormalizedTransformAncestor()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric(SRC, DataType.INT)
        .addMetric(DERIVED, DataType.INT)
        .addMetric(FINAL, DataType.DOUBLE)
        .build();
    TableConfig originalTableConfig = tableConfigWithTransforms(
        new TransformConfig(DERIVED, "Groovy({src + 0.5}, src)"),
        new TransformConfig(FINAL, "plus(derived, 100)"));
    File indexDir = buildSegmentWithTransformChain("normalizedAncestor", originalTableConfig, schema);
    SegmentMetadataImpl before = new SegmentMetadataImpl(indexDir);
    assertEquals(before.getColumnMetadataFor(DERIVED).getMinValue(), 10);
    assertEquals(before.getColumnMetadataFor(FINAL).getMinValue(), 110.5);

    TableConfig updatedTableConfig = tableConfigWithTransforms(
        new TransformConfig(DERIVED, "Groovy({src + 0.5}, src)"),
        new TransformConfig(FINAL, "plus(derived, 200)"));
    stubTableAndSchema(updatedTableConfig, schema);
    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "normalizedAncestorWorking"));

    SegmentMetadataImpl after = new SegmentMetadataImpl(result.getFile());
    assertEquals(after.getColumnMetadataFor(DERIVED).getMinValue(), 10);
    // Fresh ingestion evaluates FINAL against DERIVED's raw 10.5 evaluator result, not its persisted INT value 10.
    assertEquals(after.getColumnMetadataFor(FINAL).getMinValue(), 210.5);
  }

  @Test
  public void testRefreshSegmentPreservesV1NormalizedTransformAncestor()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric(SRC, DataType.INT)
        .addMetric(DERIVED, DataType.INT)
        .addMetric(FINAL, DataType.DOUBLE)
        .build();
    TableConfig originalTableConfig = tableConfigWithTransforms(
        new TransformConfig(DERIVED, "Groovy({src + 0.5}, src)"),
        new TransformConfig(FINAL, "plus(derived, 100)"));
    File indexDir = buildSegmentWithTransformChain("v1NormalizedAncestor", originalTableConfig, schema);
    setTransformProvenanceToV1(indexDir, DERIVED);

    TableConfig updatedTableConfig = tableConfigWithTransforms(
        new TransformConfig(DERIVED, "Groovy({src + 0.5}, src)"),
        new TransformConfig(FINAL, "plus(derived, 200)"));
    stubTableAndSchema(updatedTableConfig, schema);
    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "v1NormalizedAncestorWorking"));

    SegmentMetadataImpl after = new SegmentMetadataImpl(result.getFile());
    assertEquals(after.getColumnMetadataFor(DERIVED).getTransformFunctionProvenanceVersion(), 1);
    // Version 1 does not certify dependency closure, so its persisted value remains authoritative during replay.
    assertEquals(after.getColumnMetadataFor(FINAL).getMinValue(), 210.0);
  }

  @Test
  public void testUnrelatedRefreshPreservesDependencyFingerprint()
      throws Exception {
    TableConfig tableConfig = tableConfigWithChain(ORIGINAL_TRANSFORM, ORIGINAL_INTERMEDIATE_TRANSFORM);
    File indexDir = buildSegmentWithTransformChain("preserveFingerprint", tableConfig);
    ColumnMetadata beforeFinal = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(FINAL);
    String originalFingerprint = beforeFinal.getTransformFunctionFingerprint();
    assertNotNull(originalFingerprint);
    Schema updatedSchema = schemaWithTransformChain();
    updatedSchema.addField(new DimensionFieldSpec(NEW_COL, DataType.STRING, true));
    stubTableAndSchema(tableConfig, updatedSchema);

    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "preserveFingerprintWorking"));

    assertNotEquals(result.getFile(), indexDir);
    ColumnMetadata afterFinal = new SegmentMetadataImpl(result.getFile()).getColumnMetadataFor(FINAL);
    assertEquals(afterFinal.getTransformFunction(), FINAL_TRANSFORM);
    assertEquals(afterFinal.getTransformFunctionProvenanceVersion(), TransformProvenanceUtils.CURRENT_VERSION);
    assertEquals(afterFinal.getTransformFunctionFingerprint(), originalFingerprint);

    SegmentConversionResult noOpResult =
        convert(result.getFile(), new File(TEMP_DIR, "preserveFingerprintNoOpWorking"));
    assertEquals(noOpResult.getFile(), result.getFile());
  }

  @Test
  public void testRefreshSegmentStillRebuildsWhenAddingColumn()
      throws Exception {
    File indexDir = buildSegmentWithAutoGeneratedDerived("addColumn", ORIGINAL_TRANSFORM);
    Schema schema = schemaWithDerived();
    schema.addField(new DimensionFieldSpec(NEW_COL, DataType.STRING, true));
    stubTableAndSchema(tableConfig(ORIGINAL_TRANSFORM), schema);

    File workingDir = new File(TEMP_DIR, "addColumnWorking");
    SegmentConversionResult result = convert(indexDir, workingDir);

    assertNotEquals(result.getFile(), indexDir);
    assertTrue(result.getFile().isDirectory());
    SegmentMetadataImpl refreshedMetadata = new SegmentMetadataImpl(result.getFile());
    assertTrue(refreshedMetadata.getAllColumns().contains(NEW_COL));
    ColumnMetadata derived = refreshedMetadata.getColumnMetadataFor(DERIVED);
    assertTrue(derived.isAutoGenerated());
    assertEquals(derived.getTransformFunction(), ORIGINAL_TRANSFORM);
    assertEquals(derived.getTransformFunctionProvenanceVersion(), 1);
  }

  @Test
  public void testUnrelatedRefreshPreservesLegacyUnknownProvenance()
      throws Exception {
    File indexDir = buildSegmentWithAutoGeneratedDerived("legacyAddColumn", ORIGINAL_TRANSFORM);
    removeTransformFunctionFromMetadata(indexDir, DERIVED);
    Schema schema = schemaWithDerived();
    schema.addField(new DimensionFieldSpec(NEW_COL, DataType.STRING, true));
    stubTableAndSchema(tableConfig(ORIGINAL_TRANSFORM), schema);

    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "legacyAddColumnWorking"));

    assertNotEquals(result.getFile(), indexDir);
    ColumnMetadata derived = new SegmentMetadataImpl(result.getFile()).getColumnMetadataFor(DERIVED);
    assertTrue(derived.isAutoGenerated());
    assertNull(derived.getTransformFunction());
    assertEquals(derived.getTransformFunctionProvenanceVersion(), ColumnMetadata.UNAVAILABLE);
  }

  @Test
  public void testUnrelatedRefreshPreservesLegacyNonAutoGeneratedProvenance()
      throws Exception {
    File indexDir = buildSegmentWithIngestionDerived("legacyIngestionDerived", ORIGINAL_TRANSFORM);
    removeTransformFunctionFromMetadata(indexDir, DERIVED);
    Schema schema = schemaWithDerived();
    schema.addField(new DimensionFieldSpec(NEW_COL, DataType.STRING, true));
    stubTableAndSchema(tableConfig(UPDATED_TRANSFORM), schema);

    SegmentConversionResult result =
        convert(indexDir, new File(TEMP_DIR, "legacyIngestionDerivedWorking"));

    assertNotEquals(result.getFile(), indexDir);
    ColumnMetadata derived = new SegmentMetadataImpl(result.getFile()).getColumnMetadataFor(DERIVED);
    assertFalse(derived.isAutoGenerated());
    assertNull(derived.getTransformFunction());
    assertEquals(derived.getTransformFunctionProvenanceVersion(), ColumnMetadata.UNAVAILABLE);
    assertEquals(derived.getMinValue(), 11);
    assertEquals(derived.getMaxValue(), 21);
  }

  @Test
  public void testUnrelatedRefreshPreservesNullableLegacyDerivedValues()
      throws Exception {
    File indexDir = buildNullableLegacyDerivedSegment("nullableLegacy");
    Schema schema = nullableSchemaWithDerived();
    schema.addField(new DimensionFieldSpec(NEW_COL, DataType.STRING, true));
    TableConfig tableConfig = nullableTableConfig(CURRENT_NULL_TRANSFORM);
    stubTableAndSchema(tableConfig, schema);

    SegmentConversionResult result = convert(indexDir, new File(TEMP_DIR, "nullableLegacyWorking"));

    assertNotEquals(result.getFile(), indexDir);
    ColumnMetadata derivedMetadata = new SegmentMetadataImpl(result.getFile()).getColumnMetadataFor(DERIVED);
    assertTrue(derivedMetadata.isAutoGenerated());
    assertNull(derivedMetadata.getTransformFunction());
    assertEquals(derivedMetadata.getTransformFunctionProvenanceVersion(), ColumnMetadata.UNAVAILABLE);
    ImmutableSegment segment =
        ImmutableSegmentLoader.load(result.getFile(), new IndexLoadingConfig(tableConfig, schema), false);
    try {
      assertNotNull(segment.getDataSource(DERIVED).getNullValueVector());
      assertTrue(segment.getDataSource(DERIVED).getNullValueVector().isNull(0));
      assertTrue(segment.getDataSource(DERIVED).getNullValueVector().isNull(1));
    } finally {
      segment.destroy();
    }
  }

  private static SegmentConversionResult convert(File indexDir, File workingDir)
      throws Exception {
    FileUtils.forceMkdir(workingDir);
    RefreshSegmentTaskExecutor executor = new RefreshSegmentTaskExecutor();
    executor.setMinionEventObserver(MinionTaskTestUtils.getMinionProgressObserver());
    PinotTaskConfig taskConfig = new PinotTaskConfig(MinionConstants.RefreshSegmentTask.TASK_TYPE,
        Map.of(MinionConstants.TABLE_NAME_KEY, TABLE_NAME_WITH_TYPE, MinionConstants.SEGMENT_NAME_KEY, SEGMENT_NAME));
    return executor.convert(taskConfig, indexDir, workingDir);
  }

  @SuppressWarnings("unchecked")
  private static void stubTableAndSchema(TableConfig tableConfig, Schema schema)
      throws Exception {
    ZkHelixPropertyStore<ZNRecord> helixPropertyStore = mock(ZkHelixPropertyStore.class);
    when(helixPropertyStore.get("/CONFIGS/TABLE/" + TABLE_NAME_WITH_TYPE, null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigSerDeUtils.toZNRecord(tableConfig));
    when(helixPropertyStore.get("/SCHEMAS/" + TABLE_NAME, null, AccessOption.PERSISTENT))
        .thenReturn(SchemaSerDeUtils.toZNRecord(schema));
    MinionContext.getInstance().setHelixPropertyStore(helixPropertyStore);
  }

  private static File buildSegmentWithAutoGeneratedDerived(String dirName, String transformFunction)
      throws Exception {
    File outDir = new File(TEMP_DIR, dirName);
    FileUtils.deleteQuietly(outDir);
    Schema baseSchema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric(SRC, DataType.INT)
        .build();
    TableConfig baseTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    List<GenericRow> rows = new ArrayList<>();
    GenericRow row = new GenericRow();
    row.putValue(SRC, 10);
    rows.add(row);
    GenericRow row2 = new GenericRow();
    row2.putValue(SRC, 20);
    rows.add(row2);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(baseTableConfig, baseSchema);
    config.setInstanceType(InstanceType.MINION);
    config.setOutDir(outDir.getPath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    File indexDir = new File(outDir, SEGMENT_NAME);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      DefaultColumnHandlerFactory.getDefaultColumnHandler(indexDir, segmentDirectory.getSegmentMetadata(),
          new IndexLoadingConfig(tableConfig(transformFunction), schemaWithDerived()), writer).updateDefaultColumns();
    }
    assertTrue(new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED).isAutoGenerated());
    return indexDir;
  }

  private static File buildSegmentWithIngestionDerived(String dirName, String transformFunction)
      throws Exception {
    File outDir = new File(TEMP_DIR, dirName);
    FileUtils.deleteQuietly(outDir);
    List<GenericRow> rows = new ArrayList<>();
    GenericRow row = new GenericRow();
    row.putValue(SRC, 10);
    rows.add(row);
    GenericRow row2 = new GenericRow();
    row2.putValue(SRC, 20);
    rows.add(row2);
    SegmentGeneratorConfig config =
        new SegmentGeneratorConfig(tableConfig(transformFunction), schemaWithDerived());
    config.setInstanceType(InstanceType.MINION);
    config.setOutDir(outDir.getPath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    File indexDir = new File(outDir, SEGMENT_NAME);
    assertFalse(new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED).isAutoGenerated());
    return indexDir;
  }

  private static File buildSegmentWithTransformChain(String dirName, TableConfig tableConfig)
      throws Exception {
    return buildSegmentWithTransformChain(dirName, tableConfig, schemaWithTransformChain());
  }

  private static File buildSegmentWithTransformChain(String dirName, TableConfig tableConfig, Schema schema)
      throws Exception {
    File outDir = new File(TEMP_DIR, dirName);
    FileUtils.deleteQuietly(outDir);
    GenericRow row = new GenericRow();
    row.putValue(SRC, 10);
    GenericRow row2 = new GenericRow();
    row2.putValue(SRC, 20);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInstanceType(InstanceType.MINION);
    config.setOutDir(outDir.getPath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(row, row2)));
    driver.build();
    return new File(outDir, SEGMENT_NAME);
  }

  private static File buildSegmentWithDefaultInputChain(String dirName, TableConfig tableConfig, Schema schema)
      throws Exception {
    File outDir = new File(TEMP_DIR, dirName);
    FileUtils.deleteQuietly(outDir);
    GenericRow row = new GenericRow();
    row.putValue(DERIVED, 10);
    GenericRow row2 = new GenericRow();
    row2.putValue(DERIVED, 10);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInstanceType(InstanceType.MINION);
    config.setOutDir(outDir.getPath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(row, row2)));
    driver.build();
    File indexDir = new File(outDir, SEGMENT_NAME);
    PropertiesConfiguration segmentProperties = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    segmentProperties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(DERIVED,
        V1Constants.MetadataKeys.Column.IS_AUTO_GENERATED), true);
    SegmentMetadataUtils.savePropertiesConfiguration(segmentProperties, indexDir);
    return indexDir;
  }

  private static File buildNullableLegacyDerivedSegment(String dirName)
      throws Exception {
    File outDir = new File(TEMP_DIR, dirName);
    FileUtils.deleteQuietly(outDir);
    Schema baseSchema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(SRC_JSON, DataType.STRING)
        .build();
    TableConfig baseTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .build();
    GenericRow row = new GenericRow();
    row.putValue(SRC_JSON, "{}");
    GenericRow row2 = new GenericRow();
    row2.putValue(SRC_JSON, "{\"other\":1}");
    List<GenericRow> rows = List.of(row, row2);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(baseTableConfig, baseSchema);
    config.setInstanceType(InstanceType.MINION);
    config.setOutDir(outDir.getPath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    File indexDir = new File(outDir, SEGMENT_NAME);
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
        SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      DefaultColumnHandlerFactory.getDefaultColumnHandler(indexDir, segmentDirectory.getSegmentMetadata(),
          new IndexLoadingConfig(nullableTableConfig(LEGACY_NULL_TRANSFORM), nullableSchemaWithDerived()), writer)
          .updateDefaultColumns();
    }
    removeTransformFunctionFromMetadata(indexDir, DERIVED);
    return indexDir;
  }

  private static Schema schemaWithDerived() {
    return schemaWithDerived(DataType.INT);
  }

  private static Schema schemaWithDerived(DataType dataType) {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric(SRC, DataType.INT)
        .addMetric(DERIVED, dataType)
        .build();
  }

  private static Schema nullableSchemaWithDerived() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(SRC_JSON, DataType.STRING)
        .addSingleValueDimension(DERIVED, DataType.STRING)
        .build();
  }

  private static Schema schemaWithTransformChain() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMetric(SRC, DataType.INT)
        .addMetric(DERIVED, DataType.INT)
        .addMetric(FINAL, DataType.INT)
        .build();
  }

  private static TableConfig tableConfig(String transformFunction) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(List.of(new TransformConfig(DERIVED, transformFunction)));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(ingestionConfig)
        .build();
  }

  private static TableConfig nullableTableConfig(String transformFunction) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(List.of(new TransformConfig(DERIVED, transformFunction)));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setIngestionConfig(ingestionConfig)
        .build();
  }

  private static TableConfig tableConfigWithChain(String derivedTransform, String intermediateTransform) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(List.of(
        new TransformConfig(DERIVED, derivedTransform),
        new TransformConfig(INTERMEDIATE, intermediateTransform),
        new TransformConfig(FINAL, FINAL_TRANSFORM)));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(ingestionConfig)
        .build();
  }

  private static TableConfig tableConfigWithTransforms(TransformConfig... transformConfigs) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(List.of(transformConfigs));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(ingestionConfig)
        .build();
  }

  private static void removeTransformFunctionFromMetadata(File indexDir, String column)
      throws Exception {
    PropertiesConfiguration segmentProperties = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    segmentProperties.clearProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(column, V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION));
    segmentProperties.clearProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
        V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION_BASE64));
    segmentProperties.clearProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
        V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION_PROVENANCE_VERSION));
    segmentProperties.clearProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
        V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION_FINGERPRINT));
    SegmentMetadataUtils.savePropertiesConfiguration(segmentProperties, indexDir);
  }

  private static void setTransformProvenanceToV1(File indexDir, String column)
      throws Exception {
    PropertiesConfiguration segmentProperties = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    segmentProperties.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
        V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION_PROVENANCE_VERSION), 1);
    segmentProperties.clearProperty(V1Constants.MetadataKeys.Column.getKeyFor(column,
        V1Constants.MetadataKeys.Column.TRANSFORM_FUNCTION_FINGERPRINT));
    SegmentMetadataUtils.savePropertiesConfiguration(segmentProperties, indexDir);
  }
}
