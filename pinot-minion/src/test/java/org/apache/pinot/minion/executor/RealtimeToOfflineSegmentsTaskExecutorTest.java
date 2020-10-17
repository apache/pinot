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
package org.apache.pinot.minion.executor;

import com.google.common.collect.Lists;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IngestionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for the {@link RealtimeToOfflineSegmentsTaskExecutor}
 */
public class RealtimeToOfflineSegmentsTaskExecutorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "RealtimeToOfflineSegmentTaskExecutorTest");
  private static final File ORIGINAL_SEGMENT_DIR = new File(TEMP_DIR, "originalSegment");
  private static final File WORKING_DIR = new File(TEMP_DIR, "workingDir");
  private static final int NUM_SEGMENTS = 10;
  private static final int NUM_ROWS = 5;
  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final String TABLE_NAME_WITH_PARTITIONING = "testTableWithPartitioning_OFFLINE";
  private static final String TABLE_NAME_WITH_SORTED_COL = "testTableWithSortedCol_OFFLINE";
  private static final String TABLE_NAME_EPOCH_HOURS = "testTableEpochHours_OFFLINE";
  private static final String TABLE_NAME_SDF = "testTableSDF_OFFLINE";
  private static final String D1 = "d1";
  private static final String M1 = "m1";
  private static final String T = "t";
  private static final String T_TRX = "t_trx";

  private List<File> _segmentIndexDirList;
  private List<File> _segmentIndexDirListEpochHours;
  private List<File> _segmentIndexDirListSDF;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(T).build();
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(M1, new ColumnPartitionConfig("Modulo", 2));
    TableConfig tableConfigWithPartitioning =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_WITH_PARTITIONING).setTimeColumnName(T)
            .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap)).build();
    TableConfig tableConfigWithSortedCol =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_WITH_SORTED_COL).setTimeColumnName(T)
            .setSortedColumn(D1).build();
    TableConfig tableConfigEpochHours =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_EPOCH_HOURS).setTimeColumnName(T_TRX)
            .setSortedColumn(D1).setIngestionConfig(
            new IngestionConfig(null, Lists.newArrayList(new TransformConfig(T_TRX, "toEpochHours(t)")))).build();
    TableConfig tableConfigSDF =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_SDF).setTimeColumnName(T_TRX)
            .setSortedColumn(D1).setIngestionConfig(
            new IngestionConfig(null, Lists.newArrayList(new TransformConfig(T_TRX, "toDateTime(t, 'yyyyMMddHH')"))))
            .build();
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension(D1, FieldSpec.DataType.STRING)
            .addMetric(M1, FieldSpec.DataType.INT)
            .addDateTime(T, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    Schema schemaEpochHours =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension(D1, FieldSpec.DataType.STRING)
            .addMetric(M1, FieldSpec.DataType.INT)
            .addDateTime(T_TRX, FieldSpec.DataType.INT, "1:HOURS:EPOCH", "1:HOURS").build();
    Schema schemaSDF =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension(D1, FieldSpec.DataType.STRING)
            .addMetric(M1, FieldSpec.DataType.INT)
            .addDateTime(T_TRX, FieldSpec.DataType.INT, "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH", "1:HOURS").build();

    List<String> d1 = Lists.newArrayList("foo", "bar", "foo", "foo", "bar");
    List<List<GenericRow>> rows = new ArrayList<>(NUM_SEGMENTS);
    // times 1600468000000 1600496800000 1600525600000 1600554400000 1600583200000
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      long startMillis = 1600468000000L;
      List<GenericRow> segmentRows = new ArrayList<>(NUM_ROWS);
      for (int j = 0; j < NUM_ROWS; j++) {
        GenericRow row = new GenericRow();
        row.putValue(D1, d1.get(j));
        row.putValue(M1, j);
        row.putValue(T, startMillis);
        segmentRows.add(row);
        startMillis += 28800000; // create segment spanning across 3 day
      }
      rows.add(segmentRows);
    }

    // create test segments
    _segmentIndexDirList = new ArrayList<>();
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = "segment_" + i;
      RecordReader recordReader = new GenericRowRecordReader(rows.get(i));
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
      config.setTableName(TABLE_NAME);
      config.setSegmentName(segmentName);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();
      _segmentIndexDirList.add(new File(ORIGINAL_SEGMENT_DIR, segmentName));
    }

    // create test segments with time in epoch hours
    _segmentIndexDirListEpochHours = new ArrayList<>();
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = "segmentEpoch_" + i;
      RecordReader recordReader = new GenericRowRecordReader(rows.get(i));
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfigEpochHours, schemaEpochHours);
      config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
      config.setTableName(TABLE_NAME_EPOCH_HOURS);
      config.setSegmentName(segmentName);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();
      _segmentIndexDirListEpochHours.add(new File(ORIGINAL_SEGMENT_DIR, segmentName));
    }

    // create test segments with time in SDF
    _segmentIndexDirListSDF = new ArrayList<>();
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = "segmentSDF_" + i;
      RecordReader recordReader = new GenericRowRecordReader(rows.get(i));
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfigSDF, schemaSDF);
      config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
      config.setTableName(TABLE_NAME_SDF);
      config.setSegmentName(segmentName);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();
      _segmentIndexDirListSDF.add(new File(ORIGINAL_SEGMENT_DIR, segmentName));
    }

    MinionContext minionContext = MinionContext.getInstance();
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> helixPropertyStore = mock(ZkHelixPropertyStore.class);
    when(helixPropertyStore.get("/CONFIGS/TABLE/" + TABLE_NAME, null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigUtils.toZNRecord(tableConfig));
    when(helixPropertyStore.get("/CONFIGS/TABLE/" + TABLE_NAME_WITH_PARTITIONING, null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigUtils.toZNRecord(tableConfigWithPartitioning));
    when(helixPropertyStore.get("/CONFIGS/TABLE/" + TABLE_NAME_WITH_SORTED_COL, null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigUtils.toZNRecord(tableConfigWithSortedCol));
    when(helixPropertyStore.get("/CONFIGS/TABLE/" + TABLE_NAME_EPOCH_HOURS, null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigUtils.toZNRecord(tableConfigEpochHours));
    when(helixPropertyStore.get("/CONFIGS/TABLE/" + TABLE_NAME_SDF, null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigUtils.toZNRecord(tableConfigSDF));
    when(helixPropertyStore.get("/SCHEMAS/testTable", null, AccessOption.PERSISTENT))
        .thenReturn(SchemaUtils.toZNRecord(schema));
    when(helixPropertyStore.get("/SCHEMAS/testTableWithPartitioning", null, AccessOption.PERSISTENT))
        .thenReturn(SchemaUtils.toZNRecord(schema));
    when(helixPropertyStore.get("/SCHEMAS/testTableWithSortedCol", null, AccessOption.PERSISTENT))
        .thenReturn(SchemaUtils.toZNRecord(schema));
    when(helixPropertyStore.get("/SCHEMAS/testTableEpochHours", null, AccessOption.PERSISTENT))
        .thenReturn(SchemaUtils.toZNRecord(schemaEpochHours));
    when(helixPropertyStore.get("/SCHEMAS/testTableSDF", null, AccessOption.PERSISTENT))
        .thenReturn(SchemaUtils.toZNRecord(schemaSDF));
    minionContext.setHelixPropertyStore(helixPropertyStore);
  }

  @Test
  public void testConcat()
      throws Exception {
    FileUtils.deleteQuietly(WORKING_DIR);

    RealtimeToOfflineSegmentsTaskExecutor realtimeToOfflineSegmentsTaskExecutor =
        new RealtimeToOfflineSegmentsTaskExecutor(null);
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, "testTable_OFFLINE");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY, "1600473600000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY, "1600560000000");
    PinotTaskConfig pinotTaskConfig =
        new PinotTaskConfig(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, configs);

    List<SegmentConversionResult> conversionResults =
        realtimeToOfflineSegmentsTaskExecutor.convert(pinotTaskConfig, _segmentIndexDirList, WORKING_DIR);

    assertEquals(conversionResults.size(), 1);
    File resultingSegment = conversionResults.get(0).getFile();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(resultingSegment);
    assertEquals(segmentMetadata.getTotalDocs(), 30);
    ColumnMetadata columnMetadataForT = segmentMetadata.getColumnMetadataFor(T);
    assertEquals(columnMetadataForT.getCardinality(), 3);
    assertTrue((long) columnMetadataForT.getMinValue() >= 1600473600000L);
    assertTrue((long) columnMetadataForT.getMaxValue() < 1600560000000L);
  }

  @Test
  public void testRollupDefault()
      throws Exception {
    FileUtils.deleteQuietly(WORKING_DIR);

    RealtimeToOfflineSegmentsTaskExecutor realtimeToOfflineSegmentsTaskExecutor =
        new RealtimeToOfflineSegmentsTaskExecutor(null);
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME);
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY, "1600473600000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY, "1600560000000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY, "rollup");
    PinotTaskConfig pinotTaskConfig =
        new PinotTaskConfig(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, configs);

    List<SegmentConversionResult> conversionResults =
        realtimeToOfflineSegmentsTaskExecutor.convert(pinotTaskConfig, _segmentIndexDirList, WORKING_DIR);

    assertEquals(conversionResults.size(), 1);
    File resultingSegment = conversionResults.get(0).getFile();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(resultingSegment);
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    ColumnMetadata columnMetadataForT = segmentMetadata.getColumnMetadataFor(T);
    assertEquals(columnMetadataForT.getCardinality(), 3);
    assertTrue((long) columnMetadataForT.getMinValue() >= 1600473600000L);
    assertTrue((long) columnMetadataForT.getMaxValue() < 1600560000000L);
  }

  @Test
  public void testRollupWithTimeTransformation()
      throws Exception {
    FileUtils.deleteQuietly(WORKING_DIR);

    RealtimeToOfflineSegmentsTaskExecutor realtimeToOfflineSegmentsTaskExecutor =
        new RealtimeToOfflineSegmentsTaskExecutor(null);
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME);
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY, "1600473600000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY, "1600560000000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY, "rollup");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.TIME_COLUMN_TRANSFORM_FUNCTION_KEY, "round(t, 86400000)");
    PinotTaskConfig pinotTaskConfig =
        new PinotTaskConfig(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, configs);

    List<SegmentConversionResult> conversionResults =
        realtimeToOfflineSegmentsTaskExecutor.convert(pinotTaskConfig, _segmentIndexDirList, WORKING_DIR);

    assertEquals(conversionResults.size(), 1);
    File resultingSegment = conversionResults.get(0).getFile();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(resultingSegment);
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    ColumnMetadata columnMetadataForT = segmentMetadata.getColumnMetadataFor(T);
    assertEquals(columnMetadataForT.getCardinality(), 1);
    assertEquals((long) columnMetadataForT.getMinValue(), 1600473600000L);
  }

  @Test
  public void testRollupWithMaxAggregation()
      throws Exception {

    FileUtils.deleteQuietly(WORKING_DIR);

    RealtimeToOfflineSegmentsTaskExecutor realtimeToOfflineSegmentsTaskExecutor =
        new RealtimeToOfflineSegmentsTaskExecutor(null);
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME);
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY, "1600473600000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY, "1600560000000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.TIME_COLUMN_TRANSFORM_FUNCTION_KEY, "round(t, 86400000)");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY, "rollup");
    configs.put(M1 + MinionConstants.RealtimeToOfflineSegmentsTask.AGGREGATION_TYPE_KEY_SUFFIX, "max");
    PinotTaskConfig pinotTaskConfig =
        new PinotTaskConfig(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, configs);

    List<SegmentConversionResult> conversionResults =
        realtimeToOfflineSegmentsTaskExecutor.convert(pinotTaskConfig, _segmentIndexDirList, WORKING_DIR);

    assertEquals(conversionResults.size(), 1);
    File resultingSegment = conversionResults.get(0).getFile();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(resultingSegment);
    assertEquals(segmentMetadata.getTotalDocs(), 2);
    ColumnMetadata columnMetadataForT = segmentMetadata.getColumnMetadataFor(T);
    assertEquals(columnMetadataForT.getCardinality(), 1);
    assertEquals((long) columnMetadataForT.getMinValue(), 1600473600000L);
    ColumnMetadata columnMetadataForM1 = segmentMetadata.getColumnMetadataFor(M1);
    assertEquals(columnMetadataForM1.getCardinality(), 2);
    assertEquals((int) columnMetadataForM1.getMinValue(), 1);
    assertEquals((int) columnMetadataForM1.getMaxValue(), 3);
  }

  @Test
  public void testTablePartitioning()
      throws Exception {
    FileUtils.deleteQuietly(WORKING_DIR);

    RealtimeToOfflineSegmentsTaskExecutor realtimeToOfflineSegmentsTaskExecutor =
        new RealtimeToOfflineSegmentsTaskExecutor(null);
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME_WITH_PARTITIONING);
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY, "1600468000000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY, "1600617600000");
    PinotTaskConfig pinotTaskConfig =
        new PinotTaskConfig(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, configs);

    List<SegmentConversionResult> conversionResults =
        realtimeToOfflineSegmentsTaskExecutor.convert(pinotTaskConfig, _segmentIndexDirList, WORKING_DIR);

    assertEquals(conversionResults.size(), 2);
    File resultingSegment = conversionResults.get(0).getFile();
    File otherSegment = conversionResults.get(1).getFile();
    SegmentMetadataImpl segmentMetadata1 = new SegmentMetadataImpl(resultingSegment);
    SegmentMetadataImpl segmentMetadata2 = new SegmentMetadataImpl(otherSegment);
    if (segmentMetadata1.getTotalDocs() == 30) {
      assertEquals(segmentMetadata2.getTotalDocs(), 20);
    } else if (segmentMetadata1.getTotalDocs() == 20) {
      assertEquals(segmentMetadata2.getTotalDocs(), 30);
    } else {
      Assert.fail("Incorrect total docs in segment");
    }
  }

  @Test
  public void testTableSortedColumn()
      throws Exception {

    FileUtils.deleteQuietly(WORKING_DIR);

    RealtimeToOfflineSegmentsTaskExecutor realtimeToOfflineSegmentsTaskExecutor =
        new RealtimeToOfflineSegmentsTaskExecutor(null);
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME_WITH_SORTED_COL);
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY, "1600473600000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY, "1600560000000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY, "rollup");
    PinotTaskConfig pinotTaskConfig =
        new PinotTaskConfig(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, configs);

    List<SegmentConversionResult> conversionResults =
        realtimeToOfflineSegmentsTaskExecutor.convert(pinotTaskConfig, _segmentIndexDirList, WORKING_DIR);

    assertEquals(conversionResults.size(), 1);
    File resultingSegment = conversionResults.get(0).getFile();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(resultingSegment);
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    ColumnMetadata columnMetadataForD1 = segmentMetadata.getColumnMetadataFor(D1);
    assertEquals(columnMetadataForD1.getCardinality(), 2);
    assertTrue(columnMetadataForD1.isSorted());
  }

  @Test
  public void testTimeFormatEpochHours()
      throws Exception {

    FileUtils.deleteQuietly(WORKING_DIR);

    RealtimeToOfflineSegmentsTaskExecutor realtimeToOfflineSegmentsTaskExecutor =
        new RealtimeToOfflineSegmentsTaskExecutor(null);
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME_EPOCH_HOURS);
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY, "1600473600000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY, "1600560000000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY, "rollup");
    PinotTaskConfig pinotTaskConfig =
        new PinotTaskConfig(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, configs);

    List<SegmentConversionResult> conversionResults =
        realtimeToOfflineSegmentsTaskExecutor.convert(pinotTaskConfig, _segmentIndexDirListEpochHours, WORKING_DIR);

    assertEquals(conversionResults.size(), 1);
    File resultingSegment = conversionResults.get(0).getFile();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(resultingSegment);
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    ColumnMetadata columnMetadataForT = segmentMetadata.getColumnMetadataFor(T_TRX);
    assertEquals(columnMetadataForT.getCardinality(), 3);
    assertTrue((int) columnMetadataForT.getMinValue() >= 444576);
    assertTrue((int) columnMetadataForT.getMaxValue() < 444600);
  }

  @Test
  public void testTimeFormatSDF()
      throws Exception {

    FileUtils.deleteQuietly(WORKING_DIR);

    RealtimeToOfflineSegmentsTaskExecutor realtimeToOfflineSegmentsTaskExecutor =
        new RealtimeToOfflineSegmentsTaskExecutor(null);
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME_SDF);
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY, "1600473600000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY, "1600560000000");
    configs.put(MinionConstants.RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY, "rollup");
    PinotTaskConfig pinotTaskConfig =
        new PinotTaskConfig(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, configs);

    List<SegmentConversionResult> conversionResults =
        realtimeToOfflineSegmentsTaskExecutor.convert(pinotTaskConfig, _segmentIndexDirListSDF, WORKING_DIR);

    assertEquals(conversionResults.size(), 1);
    File resultingSegment = conversionResults.get(0).getFile();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(resultingSegment);
    assertEquals(segmentMetadata.getTotalDocs(), 3);
    ColumnMetadata columnMetadataForT = segmentMetadata.getColumnMetadataFor(T_TRX);
    assertEquals(columnMetadataForT.getCardinality(), 3);
    assertTrue((int) columnMetadataForT.getMinValue() >= 2020091900);
    assertTrue((int) columnMetadataForT.getMaxValue() < 2020092000);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
