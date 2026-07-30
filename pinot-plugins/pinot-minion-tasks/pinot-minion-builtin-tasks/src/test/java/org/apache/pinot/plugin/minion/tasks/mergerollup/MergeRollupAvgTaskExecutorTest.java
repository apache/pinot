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
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.plugin.minion.tasks.MinionTaskTestUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.customobject.AvgPair;
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


/**
 * Tests MergeRollup task executor with AVG aggregation on a {@code BYTES} column holding serialized
 * {@link AvgPair} (sum + count) values.
 * <p>
 * AVG is correct across rollups only because the (sum, count) pair is preserved and merged additively;
 * averaging already-averaged scalars would be wrong whenever groups have unequal counts. The tests below
 * deliberately use unequal counts so an average-of-averages implementation would fail them.
 */
public class MergeRollupAvgTaskExecutorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "MergeRollupAvgTaskExecutorTest");
  private static final File ORIGINAL_SEGMENT_DIR = new File(TEMP_DIR, "originalSegment");
  private static final String TABLE_NAME = "avg_table";
  private static final String DIMENSION_COL = "groupKey";
  private static final String AVG_COL = "avg_metric";

  private static final String GROUP_1 = "group1";
  private static final String GROUP_2 = "group2";
  private static final String GROUP_3 = "group3";

  private TableConfig _tableConfig;
  private Schema _schema;
  private int _workingDirCounter = 0;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    _schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(DIMENSION_COL, FieldSpec.DataType.STRING)
        .addMetric(AVG_COL, FieldSpec.DataType.BYTES)
        .build();

    MinionContext minionContext = MinionContext.getInstance();
    //noinspection unchecked
    ZkHelixPropertyStore<ZNRecord> helixPropertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    Mockito.when(helixPropertyStore.get("/CONFIGS/TABLE/" + TABLE_NAME + "_OFFLINE", null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigSerDeUtils.toZNRecord(_tableConfig));
    Mockito.when(helixPropertyStore.get("/SCHEMAS/" + TABLE_NAME, null, AccessOption.PERSISTENT))
        .thenReturn(SchemaSerDeUtils.toZNRecord(_schema));
    minionContext.setHelixPropertyStore(helixPropertyStore);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  /**
   * Three distinct dimension groups across four segments are merged independently, each preserving the
   * total sum and count (and therefore the correct average).
   */
  @Test
  public void testMultipleDimensionGroupsIndependentMerge()
      throws Exception {
    List<File> segmentDirs = buildSegments(buildStandardSegmentData());
    List<SegmentConversionResult> results = runExecutor(segmentDirs, null);

    Assert.assertEquals(results.size(), 1);
    File mergedSegment = results.get(0).getFile();

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(mergedSegment);
    Assert.assertEquals(metadata.getTotalDocs(), 3);

    Map<String, AvgPair> avgPairs = readMergedAvgPairs(mergedSegment);
    Assert.assertEquals(avgPairs.size(), 3);

    // group1: values [1..300] -> sum 45150, count 300, avg 150.5
    assertAvg(avgPairs.get(GROUP_1), 45150.0, 300L, 150.5);
    // group2: values [500..799] -> sum 194850, count 300, avg 649.5
    assertAvg(avgPairs.get(GROUP_2), 194850.0, 300L, 649.5);
    // group3: values [1..200] -> sum 20100, count 200, avg 100.5
    assertAvg(avgPairs.get(GROUP_3), 20100.0, 200L, 100.5);
  }

  /**
   * The same dimension key appearing in multiple segments has all its AvgPairs merged.
   */
  @Test
  public void testCrossSegmentMerging()
      throws Exception {
    List<File> segmentDirs = buildSegments(buildStandardSegmentData());
    Map<String, AvgPair> avgPairs = readMergedAvgPairs(runExecutor(segmentDirs, null).get(0).getFile());

    // group1 came from 3 segments (100 + 100 + 100 = 300 values)
    Assert.assertEquals(avgPairs.get(GROUP_1).getCount(), 300L);
    // group2 came from 3 segments (100 + 100 + 100 = 300 values)
    Assert.assertEquals(avgPairs.get(GROUP_2).getCount(), 300L);
    // group3 came from 3 segments (50 + 100 + 50 = 200 values)
    Assert.assertEquals(avgPairs.get(GROUP_3).getCount(), 200L);
  }

  /**
   * Groups with unequal counts must merge by adding sums and counts, not by averaging the per-segment
   * averages. With counts 10 and 100, an average-of-averages implementation would yield 77.5 instead of
   * the correct 136.409...
   */
  @Test
  public void testUnequalCountsAvoidsAverageOfAverages()
      throws Exception {
    List<List<GenericRow>> segments = new ArrayList<>();
    // 10 values [1..10] -> sum 55, avg 5.5
    segments.add(List.of(makeRow(GROUP_1, createAvgPair(1, 11))));
    // 100 values [100..199] -> sum 14950, avg 149.5
    segments.add(List.of(makeRow(GROUP_1, createAvgPair(100, 200))));

    Map<String, AvgPair> avgPairs = readMergedAvgPairs(runExecutor(buildSegments(segments), null).get(0).getFile());
    AvgPair merged = avgPairs.get(GROUP_1);
    Assert.assertEquals(merged.getSum(), 15005.0);
    Assert.assertEquals(merged.getCount(), 110L);
    Assert.assertEquals(merged.getSum() / merged.getCount(), 15005.0 / 110.0, 1e-9);
  }

  /**
   * A second rollup pass over already-rolled-up (pre-aggregated) AvgPair segments still yields the correct
   * overall average. Uses unequal counts so average-of-averages would diverge from the true value.
   * <p>
   * Level 1 merges [1..10] (count 10) and [11..30] (count 20) -> sum 465, count 30.
   * Level 2 merges that with [31..100] (count 70) -> sum 5050, count 100, avg 50.5 (the true avg of [1..100]).
   */
  @Test
  public void testTwoLevelRollupPreservesAverage()
      throws Exception {
    List<List<GenericRow>> level1Input = new ArrayList<>();
    level1Input.add(List.of(makeRow(GROUP_1, createAvgPair(1, 11))));    // [1..10]
    level1Input.add(List.of(makeRow(GROUP_1, createAvgPair(11, 31))));   // [11..30]
    File level1Merged = runExecutor(buildSegments(level1Input), null).get(0).getFile();

    // Verify the intermediate (level-1) AvgPair is preserved as (sum, count), not collapsed to an average.
    AvgPair level1 = readMergedAvgPairs(level1Merged).get(GROUP_1);
    Assert.assertEquals(level1.getSum(), 465.0);
    Assert.assertEquals(level1.getCount(), 30L);

    List<File> level2Input = new ArrayList<>();
    level2Input.add(level1Merged);
    level2Input.addAll(buildSegments(List.of(List.of(makeRow(GROUP_1, createAvgPair(31, 101)))))); // [31..100]

    AvgPair finalPair = readMergedAvgPairs(runExecutor(level2Input, null).get(0).getFile()).get(GROUP_1);
    assertAvg(finalPair, 5050.0, 100L, 50.5);
  }

  /**
   * An empty AvgPair byte array (the default null value for BYTES columns) is treated as missing; the merge
   * reflects only the non-empty input.
   */
  @Test
  public void testEmptyAvgPairHandling()
      throws Exception {
    List<List<GenericRow>> segments = new ArrayList<>();
    GenericRow emptyRow = new GenericRow();
    emptyRow.putValue(DIMENSION_COL, GROUP_1);
    emptyRow.putValue(AVG_COL, new byte[0]);
    segments.add(List.of(emptyRow));
    segments.add(List.of(makeRow(GROUP_1, createAvgPair(1, 101)))); // [1..100] -> sum 5050, count 100

    List<File> segmentDirs = buildSegments(segments);
    List<SegmentConversionResult> results = runExecutor(segmentDirs, null);

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(results.get(0).getFile());
    Assert.assertEquals(metadata.getTotalDocs(), 1);

    assertAvg(readMergedAvgPairs(results.get(0).getFile()).get(GROUP_1), 5050.0, 100L, 50.5);
  }

  /**
   * After rollup, the total doc count must equal the number of distinct dimension keys.
   */
  @Test
  public void testSegmentDocCountEqualsDistinctKeys()
      throws Exception {
    List<File> segmentDirs = buildSegments(buildStandardSegmentData());
    List<SegmentConversionResult> results = runExecutor(segmentDirs, null);

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(results.get(0).getFile());
    Assert.assertEquals(metadata.getTotalDocs(), 3,
        "Rollup doc count should equal the number of distinct dimension keys");
  }

  private static void assertAvg(AvgPair avgPair, double expectedSum, long expectedCount, double expectedAvg) {
    Assert.assertNotNull(avgPair);
    Assert.assertEquals(avgPair.getSum(), expectedSum);
    Assert.assertEquals(avgPair.getCount(), expectedCount);
    Assert.assertEquals(avgPair.getSum() / avgPair.getCount(), expectedAvg, 1e-9);
  }

  /**
   * Creates an AvgPair accumulating the integer values in {@code [start, end)}.
   */
  private static AvgPair createAvgPair(int start, int end) {
    AvgPair avgPair = new AvgPair();
    for (int v = start; v < end; v++) {
      avgPair.apply(v);
    }
    return avgPair;
  }

  private static GenericRow makeRow(String dimensionValue, AvgPair avgPair) {
    GenericRow row = new GenericRow();
    row.putValue(DIMENSION_COL, dimensionValue);
    row.putValue(AVG_COL, ObjectSerDeUtils.AVG_PAIR_SER_DE.serialize(avgPair));
    return row;
  }

  private List<File> buildSegments(List<List<GenericRow>> segmentRows)
      throws Exception {
    List<File> segmentDirs = new ArrayList<>();
    for (int i = 0; i < segmentRows.size(); i++) {
      String segmentName = TABLE_NAME + "_seg" + i + "_" + System.nanoTime();
      RecordReader recordReader = new GenericRowRecordReader(segmentRows.get(i));
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _schema);
      config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
      config.setTableName(TABLE_NAME);
      config.setSegmentName(segmentName);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();
      segmentDirs.add(new File(ORIGINAL_SEGMENT_DIR, segmentName));
    }
    return segmentDirs;
  }

  private List<SegmentConversionResult> runExecutor(List<File> segmentDirs, Map<String, String> extraConfigs)
      throws Exception {
    File workingDir = new File(TEMP_DIR, "workingDir_" + (_workingDirCounter++));
    MergeRollupTaskExecutor executor = new MergeRollupTaskExecutor(new MinionConf());
    executor.setMinionEventObserver(MinionTaskTestUtils.getMinionProgressObserver());

    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME + "_OFFLINE");
    configs.put(MinionConstants.MergeRollupTask.MERGE_LEVEL_KEY, "daily");
    configs.put(MinionConstants.MergeTask.MERGE_TYPE_KEY, "rollup");
    configs.put(AVG_COL + MinionConstants.MergeTask.AGGREGATION_TYPE_KEY_SUFFIX, "avg");
    if (extraConfigs != null) {
      configs.putAll(extraConfigs);
    }

    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig(MinionConstants.MergeRollupTask.TASK_TYPE, configs);
    return executor.convert(pinotTaskConfig, segmentDirs, workingDir);
  }

  private Map<String, AvgPair> readMergedAvgPairs(File mergedSegmentDir)
      throws Exception {
    Map<String, AvgPair> result = new HashMap<>();
    PinotSegmentRecordReader reader = new PinotSegmentRecordReader();
    reader.init(mergedSegmentDir, null, null, true);
    while (reader.hasNext()) {
      GenericRow row = reader.next();
      String key = (String) row.getValue(DIMENSION_COL);
      byte[] bytes = (byte[]) row.getValue(AVG_COL);
      result.put(key, ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytes));
    }
    reader.close();
    return result;
  }

  private static List<List<GenericRow>> buildStandardSegmentData() {
    List<List<GenericRow>> segments = new ArrayList<>();

    // Segment 0: group1=[1..100], group2=[500..599]
    segments.add(List.of(makeRow(GROUP_1, createAvgPair(1, 101)), makeRow(GROUP_2, createAvgPair(500, 600))));
    // Segment 1: group1=[101..200], group2=[600..699], group3=[1..50]
    segments.add(List.of(makeRow(GROUP_1, createAvgPair(101, 201)), makeRow(GROUP_2, createAvgPair(600, 700)),
        makeRow(GROUP_3, createAvgPair(1, 51))));
    // Segment 2: group1=[201..300], group3=[51..150]
    segments.add(List.of(makeRow(GROUP_1, createAvgPair(201, 301)), makeRow(GROUP_3, createAvgPair(51, 151))));
    // Segment 3: group2=[700..799], group3=[151..200]
    segments.add(List.of(makeRow(GROUP_2, createAvgPair(700, 800)), makeRow(GROUP_3, createAvgPair(151, 201))));

    return segments;
  }
}
