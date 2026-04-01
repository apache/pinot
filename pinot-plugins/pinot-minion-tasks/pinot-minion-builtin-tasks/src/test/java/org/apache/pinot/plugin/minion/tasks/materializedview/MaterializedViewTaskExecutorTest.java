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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import com.google.common.collect.Lists;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.segment.processing.framework.MergeType;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.plugin.minion.tasks.MinionTaskTestUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;


/**
 * Tests for the {@link MaterializedViewTaskExecutor}
 */
public class MaterializedViewTaskExecutorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "MaterializedViewTaskExecutorTest");
  private static final File ORIGINAL_SEGMENT_DIR = new File(TEMP_DIR, "originalSegment");
  private static final File WORKING_DIR = new File(TEMP_DIR, "workingDir");

  private static final int NUM_SEGMENTS = 2;
  private static final int NUM_ROWS = 5;

  private static final String MV_TABLE_NAME = "mvTable_OFFLINE";
  private static final String MV_SCHEMA_NAME = "mvTable";
  private static final String BASE_TABLE_NAME = "baseTable_OFFLINE";
  private static final String BASE_SCHEMA_NAME = "baseTable";

  private static final String TIME_COL = "T";

  // Base table has 5 dimension columns
  private static final String DIM1 = "Dim1";
  private static final String DIM2 = "Dim2";
  private static final String DIM3 = "Dim3";
  private static final String DIM4 = "Dim4";
  private static final String DIM5 = "Dim5";

  // Metric column in base table
  private static final String METRIC = "Column1";
  // MV schema uses <metric>_<aggType>
  private static final String METRIC_SUM = METRIC + "_sum";

  private List<File> _segmentIndexDirList;

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    // MV table config & schema (executor reads from helix property store)
    TableConfig mvTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(MV_TABLE_NAME).setTimeColumnName(TIME_COL).build();

    // MV schema: only selected dims + aggregated metric output + time column
    Schema mvSchema = new Schema.SchemaBuilder()
        .setSchemaName(MV_SCHEMA_NAME)
        .addSingleValueDimension(DIM1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM2, FieldSpec.DataType.STRING)
        .addMetric(METRIC_SUM, FieldSpec.DataType.LONG)
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    // Base schema: 5 dims + metric + time
    Schema baseSchema = new Schema.SchemaBuilder()
        .setSchemaName(BASE_SCHEMA_NAME)
        .addSingleValueDimension(DIM1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM2, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM3, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM4, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DIM5, FieldSpec.DataType.STRING)
        .addMetric(METRIC, FieldSpec.DataType.INT)
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    TableConfig baseTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(BASE_TABLE_NAME).setTimeColumnName(TIME_COL).build();

    // Build deterministic rows for group-by + aggregation
    // We intentionally create repeated (Dim1, Dim2) pairs so MV rollup can aggregate them.
    List<String> dim1Vals = Lists.newArrayList(
        "foo", "foo", "bar", "bar", "foo");
    List<String> dim2Vals = Lists.newArrayList(
        "a", "a", "a", "b", "b");

    // Other dims exist in base table but are not selected into MV
    List<String> dim3Vals = Lists.newArrayList("x1", "x2", "x3", "x4", "x5");
    List<String> dim4Vals = Lists.newArrayList("y1", "y1", "y2", "y2", "y2");
    List<String> dim5Vals = Lists.newArrayList("z", "z", "z", "z", "z");

    List<List<GenericRow>> rows = new ArrayList<>(NUM_SEGMENTS);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      long startMillis = 1600468000000L;
      List<GenericRow> segmentRows = new ArrayList<>(NUM_ROWS);
      for (int j = 0; j < NUM_ROWS; j++) {
        GenericRow row = new GenericRow();
        row.putValue(DIM1, dim1Vals.get(j));
        row.putValue(DIM2, dim2Vals.get(j));
        row.putValue(DIM3, dim3Vals.get(j));
        row.putValue(DIM4, dim4Vals.get(j));
        row.putValue(DIM5, dim5Vals.get(j));
        row.putValue(METRIC, j);
        row.putValue(TIME_COL, startMillis);
        segmentRows.add(row);
        startMillis += 28800000; // 8h
      }
      rows.add(segmentRows);
    }

    // Create input segments based on base schema/table
    _segmentIndexDirList = new ArrayList<>();
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = "segment_" + i;
      RecordReader recordReader = new GenericRowRecordReader(rows.get(i));
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(baseTableConfig, baseSchema);
      config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
      config.setTableName(BASE_TABLE_NAME);
      config.setSegmentName(segmentName);

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader, InstanceType.MINION);
      driver.build();
      _segmentIndexDirList.add(new File(ORIGINAL_SEGMENT_DIR, segmentName));
    }

    // Setup helix property store for MV table config + MV schema (executor will fetch them)
    MinionContext minionContext = MinionContext.getInstance();
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> helixPropertyStore = Mockito.mock(ZkHelixPropertyStore.class);

    Mockito.when(helixPropertyStore.get("/CONFIGS/TABLE/" + MV_TABLE_NAME, null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigSerDeUtils.toZNRecord(mvTableConfig));

    Mockito.when(helixPropertyStore.get("/SCHEMAS/" + MV_SCHEMA_NAME, null, AccessOption.PERSISTENT))
        .thenReturn(SchemaSerDeUtils.toZNRecord(mvSchema));

    minionContext.setHelixPropertyStore(helixPropertyStore);
  }

  @Test
  public void testMvRollupWithoutFilter() throws Exception {
    FileUtils.deleteQuietly(WORKING_DIR);

    MaterializedViewTaskExecutor executor = new MaterializedViewTaskExecutor(null, null);
    executor.setMinionEventObserver(MinionTaskTestUtils.getMinionProgressObserver());

    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, MV_TABLE_NAME);

    // mvName is required by executor
    configs.put(MinionConstants.MaterializedViewTask.MATERIALIZED_VIEW_NAME, "mvTable");

    // MV rollup configs
    configs.put(MinionConstants.MaterializedViewTask.MERGE_TYPE_KEY, MergeType.MV_ROLLUP.name());
    configs.put(MinionConstants.MaterializedViewTask.SELECTED_DIMENSION_LIST, DIM1 + "," + DIM2);
    configs.put(METRIC + MinionConstants.MaterializedViewTask.AGGREGATION_TYPE_KEY_SUFFIX, "sum");

    PinotTaskConfig pinotTaskConfig =
        new PinotTaskConfig(MinionConstants.MaterializedViewTask.TASK_TYPE, configs);

    List<SegmentConversionResult> conversionResults =
        executor.convert(pinotTaskConfig, _segmentIndexDirList, WORKING_DIR);

    assertEquals(conversionResults.size(), 1);

    File resultingSegment = conversionResults.get(0).getFile();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(resultingSegment);

    // MV schema columns
    assertNotNull(segmentMetadata.getColumnMetadataFor(DIM1));
    assertNotNull(segmentMetadata.getColumnMetadataFor(DIM2));
    assertNotNull(segmentMetadata.getColumnMetadataFor(METRIC_SUM));

    // Non-selected dims should not exist in MV segment
    assertTrue(segmentMetadata.getColumnMetadataFor(DIM3) == null);
    assertTrue(segmentMetadata.getColumnMetadataFor(DIM4) == null);
    assertTrue(segmentMetadata.getColumnMetadataFor(DIM5) == null);

    // No filter -> 4 group-by results
    assertEquals(segmentMetadata.getTotalDocs(), 4);
  }

  @Test
  public void testMvRollup() throws Exception {
    FileUtils.deleteQuietly(WORKING_DIR);

    MaterializedViewTaskExecutor executor = new MaterializedViewTaskExecutor(null, null);
    executor.setMinionEventObserver(MinionTaskTestUtils.getMinionProgressObserver());

    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, MV_TABLE_NAME);

    // MV name is required by executor to derive mv offline table name
    configs.put(
        MinionConstants.MaterializedViewTask.MATERIALIZED_VIEW_NAME,
        "mvTable");

    // Keep window configs for parity (adjust keys if your executor uses different ones)
    configs.put("windowStartMs", "1600468000000");
    configs.put("windowEndMs", "1600617600000");

    // MV rollup configs
    configs.put(MinionConstants.MaterializedViewTask.MERGE_TYPE_KEY, MergeType.MV_ROLLUP.name());
    configs.put(MinionConstants.MaterializedViewTask.SELECTED_DIMENSION_LIST, DIM1 + "," + DIM2);
    configs.put(METRIC + MinionConstants.MaterializedViewTask.AGGREGATION_TYPE_KEY_SUFFIX, "sum");
    configs.put(MinionConstants.MaterializedViewTask.FILTER_FUNCTION, "Groovy({Dim2 != \"X\"}, Dim2)");

    PinotTaskConfig pinotTaskConfig =
        new PinotTaskConfig(MinionConstants.MaterializedViewTask.TASK_TYPE, configs);

    List<SegmentConversionResult> conversionResults =
        executor.convert(pinotTaskConfig, _segmentIndexDirList, WORKING_DIR);

    assertEquals(conversionResults.size(), 1);

    File resultingSegment = conversionResults.get(0).getFile();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(resultingSegment);

    // MV table only has selected dims + aggregated output metric + time column
    assertNotNull(segmentMetadata.getColumnMetadataFor(DIM1));
    assertNotNull(segmentMetadata.getColumnMetadataFor(DIM2));
    assertNotNull(segmentMetadata.getColumnMetadataFor(METRIC_SUM));
    assertNotNull(segmentMetadata.getColumnMetadataFor(TIME_COL));

    // Ensure non-selected base dims are not present in MV segment
    assertTrue(segmentMetadata.getColumnMetadataFor(DIM3) == null);
    assertTrue(segmentMetadata.getColumnMetadataFor(DIM4) == null);
    assertTrue(segmentMetadata.getColumnMetadataFor(DIM5) == null);

    // Expected groups from (Dim1,Dim2): 4 distinct pairs
    assertEquals(segmentMetadata.getTotalDocs(), 4);

    ColumnMetadata dim1Meta = segmentMetadata.getColumnMetadataFor(DIM1);
    ColumnMetadata dim2Meta = segmentMetadata.getColumnMetadataFor(DIM2);
    assertEquals(dim1Meta.getCardinality(), 2);
    assertEquals(dim2Meta.getCardinality(), 2);

    ColumnMetadata metricSumMeta = segmentMetadata.getColumnMetadataFor(METRIC_SUM);
    assertNotNull(metricSumMeta);
    // After SUM aggregation across NUM_SEGMENTS inputs, values should be >= 0 and max >= min
    long min = ((Number) metricSumMeta.getMinValue()).longValue();
    long max = ((Number) metricSumMeta.getMaxValue()).longValue();
    assertTrue(min >= 0);
    assertTrue(max >= min);
  }

  @AfterClass
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
