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
package org.apache.pinot.segment.local.segment.index.loader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Reload must rebuild star-trees when an UPDATE_*_TRANSFORM_FUNCTION changes values of a star-tree column, remove
/// stale trees when dynamic creation is disabled, and leave legacy columns without provenance untouched.
public class DerivedColumnStarTreeReloadTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), DerivedColumnStarTreeReloadTest.class.getSimpleName());
  private static final String TABLE = "testTable";
  private static final String SEGMENT = "testSegment";
  private static final String DIM = "dim";
  private static final String SRC = "src";
  private static final String DERIVED = "derived";
  private static final String ORIGINAL_TRANSFORM = "plus(src, 1)";
  private static final String UPDATED_TRANSFORM = "plus(src, 2)";
  private static final String SUM_DERIVED = "sum__derived";

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testStarTreeRebuiltWhenDerivedTransformChanges()
      throws Exception {
    File indexDir = buildBaseSegment("updateValues");
    Schema schema = schemaWithDerived();
    preprocess(indexDir, tableConfig(ORIGINAL_TRANSFORM, true), schema);

    ColumnMetadata derivedAfterAdd = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED);
    assertEquals(derivedAfterAdd.getTransformFunction(), ORIGINAL_TRANSFORM);
    assertEquals(derivedAfterAdd.getMinValue(), 11);
    assertEquals(derivedAfterAdd.getMaxValue(), 31);
    assertEquals(readStarTreeGrandTotal(indexDir), 63L);

    preprocess(indexDir, tableConfig(UPDATED_TRANSFORM, true), schema);

    ColumnMetadata derivedAfterUpdate = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED);
    assertEquals(derivedAfterUpdate.getTransformFunction(), UPDATED_TRANSFORM);
    assertEquals(derivedAfterUpdate.getTransformFunctionProvenanceVersion(), 1);
    assertEquals(derivedAfterUpdate.getMinValue(), 12);
    assertEquals(derivedAfterUpdate.getMaxValue(), 32);
    assertEquals(readStarTreeGrandTotal(indexDir), 66L);
  }

  @Test
  public void testStarTreeNotRebuiltForLegacyColumnWithoutProvenance()
      throws Exception {
    File indexDir = buildBaseSegment("backfillNoRebuild");
    Schema schema = schemaWithDerived();
    preprocess(indexDir, tableConfig(ORIGINAL_TRANSFORM, true), schema);
    long sumBeforeReload = readStarTreeGrandTotal(indexDir);
    byte[] starTreeBefore = FileUtils.readFileToByteArray(findStarTreeIndex(indexDir));

    removeTransformFunctionFromMetadata(indexDir, DERIVED);
    processLegacySegment(indexDir, tableConfig(ORIGINAL_TRANSFORM, true), schema);

    ColumnMetadata legacy = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED);
    assertNull(legacy.getTransformFunction());
    assertEquals(legacy.getTransformFunctionProvenanceVersion(), ColumnMetadata.UNAVAILABLE);
    assertEquals(legacy.getMinValue(), 11);
    assertEquals(legacy.getMaxValue(), 31);
    assertEquals(readStarTreeGrandTotal(indexDir), sumBeforeReload);
    assertEquals(FileUtils.readFileToByteArray(findStarTreeIndex(indexDir)), starTreeBefore);
  }

  @Test
  public void testStaleStarTreeRemovedWhenDynamicCreationIsDisabled()
      throws Exception {
    File indexDir = buildBaseSegment("disabledDynamicCreation");
    Schema schema = schemaWithDerived();
    preprocess(indexDir, tableConfig(ORIGINAL_TRANSFORM, true), schema);
    assertEquals(readStarTreeGrandTotal(indexDir), 63L);

    preprocess(indexDir, tableConfig(UPDATED_TRANSFORM, false), schema);

    ColumnMetadata derived = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED);
    assertEquals(derived.getTransformFunction(), UPDATED_TRANSFORM);
    assertEquals(derived.getMinValue(), 12);
    assertEquals(derived.getMaxValue(), 32);
    File v3 = new File(indexDir, "v3");
    assertFalse(new File(indexDir, "star_tree_index").exists());
    assertFalse(new File(v3, "star_tree_index").exists());
    ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    try {
      assertTrue(segment.getStarTrees() == null || segment.getStarTrees().isEmpty());
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testFailedTransformUpdateKeepsExistingStarTree()
      throws Exception {
    File indexDir = buildBaseSegment("failedValueUpdate");
    Schema schema = schemaWithDerived();
    preprocess(indexDir, tableConfig(ORIGINAL_TRANSFORM, true), schema);
    long sumBeforeReload = readStarTreeGrandTotal(indexDir);
    byte[] starTreeBefore = FileUtils.readFileToByteArray(findStarTreeIndex(indexDir));

    // reverse(dim) produces strings that cannot be stored in the INT derived column. The default failure policy keeps
    // the old column, so its still-valid star-tree must be kept too.
    preprocess(indexDir, tableConfig("reverse(dim)", true), schema);

    ColumnMetadata derived = new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED);
    assertEquals(derived.getTransformFunction(), ORIGINAL_TRANSFORM);
    assertEquals(derived.getMinValue(), 11);
    assertEquals(derived.getMaxValue(), 31);
    assertEquals(readStarTreeGrandTotal(indexDir), sumBeforeReload);
    assertEquals(FileUtils.readFileToByteArray(findStarTreeIndex(indexDir)), starTreeBefore);
  }

  private static File buildBaseSegment(String dirName)
      throws Exception {
    File outDir = new File(TEMP_DIR, dirName);
    FileUtils.deleteQuietly(outDir);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE)
        .addSingleValueDimension(DIM, DataType.STRING)
        .addMetric(SRC, DataType.INT)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).build();
    List<GenericRow> rows = new ArrayList<>();
    rows.add(row("A", 10));
    rows.add(row("A", 20));
    rows.add(row("B", 30));
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(outDir.getAbsolutePath());
    config.setSegmentName(SEGMENT);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(outDir, SEGMENT);
  }

  private static GenericRow row(String dim, int src) {
    GenericRow row = new GenericRow();
    row.putValue(DIM, dim);
    row.putValue(SRC, src);
    return row;
  }

  private static Schema schemaWithDerived() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE)
        .addSingleValueDimension(DIM, DataType.STRING)
        .addMetric(SRC, DataType.INT)
        .addMetric(DERIVED, DataType.INT)
        .build();
  }

  private static TableConfig tableConfig(String transformFunction, boolean withStarTree) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(List.of(new TransformConfig(DERIVED, transformFunction)));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE)
        .setIngestionConfig(ingestionConfig)
        .build();
    if (withStarTree) {
      tableConfig.getIndexingConfig().setEnableDynamicStarTreeCreation(true);
      tableConfig.getIndexingConfig().setStarTreeIndexConfigs(List.of(
          new StarTreeIndexConfig(List.of(DIM), null, List.of("SUM__" + DERIVED), null, 1000)));
    }
    return tableConfig;
  }

  private static void preprocess(File indexDir, TableConfig tableConfig, Schema schema)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema))) {
      assertTrue(processor.needProcess());
      processor.process();
    }
  }

  private static void processLegacySegment(File indexDir, TableConfig tableConfig, Schema schema)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
        SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectory,
            new IndexLoadingConfig(tableConfig, schema))) {
      assertFalse(processor.needProcess());
      processor.process();
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static long readStarTreeGrandTotal(File indexDir)
      throws Exception {
    ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    try {
      List<StarTreeV2> starTrees = segment.getStarTrees();
      assertNotNull(starTrees);
      assertEquals(starTrees.size(), 1);
      StarTreeV2 tree = starTrees.get(0);
      ForwardIndexReader aggReader = tree.getDataSource(SUM_DERIVED).getForwardIndex();
      int aggDocId = tree.getStarTree().getRoot().getAggregatedDocId();
      try (ForwardIndexReaderContext aggCtx = aggReader.createContext()) {
        return Math.round(aggReader.getDouble(aggDocId, aggCtx));
      }
    } finally {
      segment.destroy();
    }
  }

  private static File findStarTreeIndex(File indexDir) {
    File v3 = new File(indexDir, "v3");
    File inV3 = new File(v3, "star_tree_index");
    if (inV3.exists()) {
      return inV3;
    }
    File inRoot = new File(indexDir, "star_tree_index");
    assertTrue(inRoot.exists() || inV3.exists(), "star_tree_index not found under " + indexDir);
    return inRoot.exists() ? inRoot : inV3;
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
    SegmentMetadataUtils.savePropertiesConfiguration(segmentProperties, indexDir);
  }
}
