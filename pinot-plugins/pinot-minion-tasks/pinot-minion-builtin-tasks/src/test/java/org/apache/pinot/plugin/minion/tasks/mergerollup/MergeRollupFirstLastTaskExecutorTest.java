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
package org.apache.pinot.plugin.minion.tasks.mergerollup;

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
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.plugin.minion.tasks.MinionTaskTestUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/// Tests MergeRollup task executor with `firstWithTime`/`lastWithTime` aggregation types, which pick the metric
/// value with the earliest/latest time within each rollup group.
public class MergeRollupFirstLastTaskExecutorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "MergeRollupFirstLastTaskExecutorTest");
  private static final File ORIGINAL_SEGMENT_DIR = new File(TEMP_DIR, "originalSegment");
  private static final File WORKING_DIR = new File(TEMP_DIR, "workingDir");
  private static final String TABLE_NAME = "first_last_table";
  private static final String DIMENSION_COL = "campaign";
  private static final String FIRST_COL = "firstClicks";
  private static final String LAST_COL = "lastClicks";
  private static final String TIME_COL = "ts";
  private static final long DAY_1 = 1597708800000L;

  private List<File> _segmentIndexDirList;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COL).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DIMENSION_COL, FieldSpec.DataType.STRING).addMetric(FIRST_COL, FieldSpec.DataType.LONG)
        .addMetric(LAST_COL, FieldSpec.DataType.LONG)
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

    // Rows within the same day bucket are intentionally out of time order across segments to verify that the
    // first/last values are picked based on the time order instead of the input order:
    // g1: 10@2000, 20@1000, 30@3000 -> first=20, last=30
    // g2: 1@100, 2@50 -> first=2, last=1
    List<List<GenericRow>> segments = new ArrayList<>();
    segments.add(List.of(makeRow("g1", 10, DAY_1 + 2000), makeRow("g2", 1, DAY_1 + 100)));
    segments.add(List.of(makeRow("g1", 20, DAY_1 + 1000)));
    segments.add(List.of(makeRow("g1", 30, DAY_1 + 3000), makeRow("g2", 2, DAY_1 + 50)));

    _segmentIndexDirList = new ArrayList<>();
    for (int i = 0; i < segments.size(); i++) {
      String segmentName = TABLE_NAME + "_seg" + i;
      RecordReader recordReader = new GenericRowRecordReader(segments.get(i));
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
      config.setTableName(TABLE_NAME);
      config.setSegmentName(segmentName);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();
      _segmentIndexDirList.add(new File(ORIGINAL_SEGMENT_DIR, segmentName));
    }

    MinionContext minionContext = MinionContext.getInstance();
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> helixPropertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    Mockito.when(helixPropertyStore.get("/CONFIGS/TABLE/" + TABLE_NAME + "_OFFLINE", null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigSerDeUtils.toZNRecord(tableConfig));
    Mockito.when(helixPropertyStore.get("/SCHEMAS/" + TABLE_NAME, null, AccessOption.PERSISTENT))
        .thenReturn(SchemaSerDeUtils.toZNRecord(schema));
    minionContext.setHelixPropertyStore(helixPropertyStore);
  }

  @Test
  public void testFirstLastAggregation()
      throws Exception {
    MergeRollupTaskExecutor mergeRollupTaskExecutor = new MergeRollupTaskExecutor(new MinionConf());
    mergeRollupTaskExecutor.setMinionEventObserver(MinionTaskTestUtils.getMinionProgressObserver());
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME + "_OFFLINE");
    configs.put(MinionConstants.MergeRollupTask.MERGE_LEVEL_KEY, "daily");
    configs.put(MinionConstants.MergeTask.MERGE_TYPE_KEY, "rollup");
    configs.put(MinionConstants.MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY, "1d");
    configs.put(FIRST_COL + MinionConstants.MergeTask.AGGREGATION_TYPE_KEY_SUFFIX, "firstWithTime");
    configs.put(LAST_COL + MinionConstants.MergeTask.AGGREGATION_TYPE_KEY_SUFFIX, "lastWithTime");

    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig(MinionConstants.MergeRollupTask.TASK_TYPE, configs);
    List<SegmentConversionResult> conversionResults =
        mergeRollupTaskExecutor.convert(pinotTaskConfig, _segmentIndexDirList, WORKING_DIR);

    Assert.assertEquals(conversionResults.size(), 1);
    File mergedSegment = conversionResults.get(0).getFile();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(mergedSegment);
    Assert.assertEquals(segmentMetadata.getTotalDocs(), 2);

    Map<String, GenericRow> rowsByDimension = new HashMap<>();
    try (PinotSegmentRecordReader reader = new PinotSegmentRecordReader()) {
      reader.init(mergedSegment, null, null, true);
      while (reader.hasNext()) {
        GenericRow row = reader.next();
        rowsByDimension.put((String) row.getValue(DIMENSION_COL), row);
      }
    }

    GenericRow g1 = rowsByDimension.get("g1");
    Assert.assertNotNull(g1);
    Assert.assertEquals(g1.getValue(FIRST_COL), 20L);
    Assert.assertEquals(g1.getValue(LAST_COL), 30L);
    Assert.assertEquals(g1.getValue(TIME_COL), DAY_1);

    GenericRow g2 = rowsByDimension.get("g2");
    Assert.assertNotNull(g2);
    Assert.assertEquals(g2.getValue(FIRST_COL), 2L);
    Assert.assertEquals(g2.getValue(LAST_COL), 1L);
    Assert.assertEquals(g2.getValue(TIME_COL), DAY_1);
  }

  /// Verifies the multi-pass (multi-level merge) semantics documented on the aggregators: a second rollup pass over
  /// the first-pass output succeeds, and orders the rows by the already-rounded time of the earlier pass, so the
  /// coarser bucket keeps the first value of the earliest day and the last value of the latest day.
  @Test
  public void testFirstLastAggregationMultiPass()
      throws Exception {
    File originalSegmentDir = new File(TEMP_DIR, "multiPassOriginalSegment");
    File firstPassWorkingDir = new File(TEMP_DIR, "multiPassWorkingDir1");
    File secondPassWorkingDir = new File(TEMP_DIR, "multiPassWorkingDir2");
    long day2 = DAY_1 + 86400000L;

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COL).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DIMENSION_COL, FieldSpec.DataType.STRING).addMetric(FIRST_COL, FieldSpec.DataType.LONG)
        .addMetric(LAST_COL, FieldSpec.DataType.LONG)
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();

    // day 1: first=20 (@1000), last=10 (@2000); day 2: first=5 (@500), last=7 (@800)
    List<GenericRow> rows = List.of(makeRow("g1", 10, DAY_1 + 2000), makeRow("g1", 20, DAY_1 + 1000),
        makeRow("g1", 5, day2 + 500), makeRow("g1", 7, day2 + 800));
    String segmentName = TABLE_NAME + "_multipass_seg";
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(originalSegmentDir.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    // First pass: daily rollup
    File firstPassSegment = runRollup("daily", "1d", List.of(new File(originalSegmentDir, segmentName)),
        firstPassWorkingDir);
    Map<Long, GenericRow> firstPassRowsByTime = new HashMap<>();
    try (PinotSegmentRecordReader reader = new PinotSegmentRecordReader()) {
      reader.init(firstPassSegment, null, null, true);
      while (reader.hasNext()) {
        GenericRow row = reader.next();
        firstPassRowsByTime.put((Long) row.getValue(TIME_COL), row);
      }
    }
    Assert.assertEquals(firstPassRowsByTime.size(), 2);
    Assert.assertEquals(firstPassRowsByTime.get(DAY_1).getValue(FIRST_COL), 20L);
    Assert.assertEquals(firstPassRowsByTime.get(DAY_1).getValue(LAST_COL), 10L);
    Assert.assertEquals(firstPassRowsByTime.get(day2).getValue(FIRST_COL), 5L);
    Assert.assertEquals(firstPassRowsByTime.get(day2).getValue(LAST_COL), 7L);

    // Second pass: 30d rollup over the first-pass output, both days land in the same bucket
    File secondPassSegment = runRollup("monthly", "30d", List.of(firstPassSegment), secondPassWorkingDir);
    List<GenericRow> secondPassRows = new ArrayList<>();
    try (PinotSegmentRecordReader reader = new PinotSegmentRecordReader()) {
      reader.init(secondPassSegment, null, null, true);
      while (reader.hasNext()) {
        secondPassRows.add(reader.next());
      }
    }
    Assert.assertEquals(secondPassRows.size(), 1);
    GenericRow mergedRow = secondPassRows.get(0);
    long bucket30dMs = 30L * 86400000L;
    Assert.assertEquals(mergedRow.getValue(TIME_COL), DAY_1 / bucket30dMs * bucket30dMs);
    Assert.assertEquals(mergedRow.getValue(FIRST_COL), 20L);
    Assert.assertEquals(mergedRow.getValue(LAST_COL), 7L);
  }

  private File runRollup(String mergeLevel, String roundBucketTimePeriod, List<File> inputSegmentDirs, File workingDir)
      throws Exception {
    MergeRollupTaskExecutor mergeRollupTaskExecutor = new MergeRollupTaskExecutor(new MinionConf());
    mergeRollupTaskExecutor.setMinionEventObserver(MinionTaskTestUtils.getMinionProgressObserver());
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME + "_OFFLINE");
    configs.put(MinionConstants.MergeRollupTask.MERGE_LEVEL_KEY, mergeLevel);
    configs.put(MinionConstants.MergeTask.MERGE_TYPE_KEY, "rollup");
    configs.put(MinionConstants.MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY, roundBucketTimePeriod);
    configs.put(FIRST_COL + MinionConstants.MergeTask.AGGREGATION_TYPE_KEY_SUFFIX, "firstWithTime");
    configs.put(LAST_COL + MinionConstants.MergeTask.AGGREGATION_TYPE_KEY_SUFFIX, "lastWithTime");
    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig(MinionConstants.MergeRollupTask.TASK_TYPE, configs);
    List<SegmentConversionResult> conversionResults =
        mergeRollupTaskExecutor.convert(pinotTaskConfig, inputSegmentDirs, workingDir);
    Assert.assertEquals(conversionResults.size(), 1);
    return conversionResults.get(0).getFile();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  private static GenericRow makeRow(String dimensionValue, long clicks, long timeMs) {
    GenericRow row = new GenericRow();
    row.putValue(DIMENSION_COL, dimensionValue);
    row.putValue(FIRST_COL, clicks);
    row.putValue(LAST_COL, clicks);
    row.putValue(TIME_COL, timeMs);
    return row;
  }
}
