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

import com.tdunning.math.stats.TDigest;
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
 * Tests MergeRollup task executor with PercentileTDigest aggregation on BYTES columns.
 * <p>
 * Data layout across 4 segments with 3 dimension groups:
 *   Segment 0: group1=[1..100], group2=[500..599]
 *   Segment 1: group1=[101..200], group2=[600..699], group3=[1..50]
 *   Segment 2: group1=[201..300], group3=[51..150]
 *   Segment 3: group2=[700..799], group3=[151..200]
 * <p>
 * After rollup:
 *   group1: 300 values [1..300], P50 ~ 150
 *   group2: 300 values [500..799], P50 ~ 649
 *   group3: 200 values [1..200], P50 ~ 100
 */
public class MergeRollupTDigestTaskExecutorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "MergeRollupTDigestTaskExecutorTest");
  private static final File ORIGINAL_SEGMENT_DIR = new File(TEMP_DIR, "originalSegment");
  private static final String TABLE_NAME = "tdigest_table";
  private static final String DIMENSION_COL = "groupKey";
  private static final String TDIGEST_COL = "tdigest_metric";
  private static final int DEFAULT_COMPRESSION = 100;

  private static final String GROUP_1 = "latency-group1";
  private static final String GROUP_2 = "latency-group2";
  private static final String GROUP_3 = "latency-group3";

  private TableConfig _tableConfig;
  private Schema _schema;
  private int _workingDirCounter = 0;

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    _schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(DIMENSION_COL, FieldSpec.DataType.STRING)
        .addMetric(TDIGEST_COL, FieldSpec.DataType.BYTES)
        .build();

    MinionContext minionContext = MinionContext.getInstance();
    //noinspection unchecked
    ZkHelixPropertyStore<ZNRecord> helixPropertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    Mockito.when(helixPropertyStore
                    .get("/CONFIGS/TABLE/" + TABLE_NAME + "_OFFLINE", null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigSerDeUtils.toZNRecord(_tableConfig));
    Mockito.when(helixPropertyStore
                    .get("/SCHEMAS/" + TABLE_NAME, null, AccessOption.PERSISTENT))
        .thenReturn(SchemaSerDeUtils.toZNRecord(_schema));
    minionContext.setHelixPropertyStore(helixPropertyStore);
  }

  @AfterClass
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  /**
   * Verifies that 3 distinct dimension groups across 4 segments are independently merged,
   * each with correct count and quantile estimates (P0, P50, P99, P100).
   */
  @Test
  public void testMultipleDimensionGroupsIndependentMerge() throws Exception {
    List<File> segmentDirs = buildSegments(buildStandardSegmentData());
    List<SegmentConversionResult> results = runExecutor(segmentDirs, null);

    Assert.assertEquals(results.size(), 1);
    File mergedSegment = results.get(0).getFile();

    // 3 distinct dimension keys => 3 rows after rollup
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(mergedSegment);
    Assert.assertEquals(metadata.getTotalDocs(), 3);

    Map<String, TDigest> digests = readMergedTDigests(mergedSegment);
    Assert.assertEquals(digests.size(), 3);

    // group1: 300 values [1..300]
    TDigest digest1 = digests.get(GROUP_1);
    Assert.assertNotNull(digest1);
    Assert.assertEquals(digest1.size(), 300L);
    Assert.assertEquals(digest1.quantile(0.0), 1, 1);      // P0 ~ 1
    Assert.assertEquals(digest1.quantile(0.5), 150, 5);     // P50 ~ 150
    Assert.assertEquals(digest1.quantile(0.99), 297, 5);    // P99 ~ 297
    Assert.assertEquals(digest1.quantile(1.0), 300, 1);     // P100 ~ 300

    // group2: 300 values [500..799]
    TDigest digest2 = digests.get(GROUP_2);
    Assert.assertNotNull(digest2);
    Assert.assertEquals(digest2.size(), 300L);
    Assert.assertEquals(digest2.quantile(0.0), 500, 1);       // P0 ~ 500
    Assert.assertEquals(digest2.quantile(0.5), 649, 5);       // P50 ~ 649
    Assert.assertEquals(digest2.quantile(0.99), 796, 5);      // P99 ~ 796
    Assert.assertEquals(digest2.quantile(1.0), 799, 1);       // P100 ~ 799

    // group3: 200 values [1..200]
    TDigest digest3 = digests.get(GROUP_3);
    Assert.assertNotNull(digest3);
    Assert.assertEquals(digest3.size(), 200L);
    Assert.assertEquals(digest3.quantile(0.0), 1, 1);           // P0 ~ 1
    Assert.assertEquals(digest3.quantile(0.5), 100, 5);         // P50 ~ 100
    Assert.assertEquals(digest3.quantile(0.99), 198, 5);        // P99 ~ 198
    Assert.assertEquals(digest3.quantile(1.0), 200, 1);         // P100 ~ 200
  }

  /**
   * Verifies cross-segment merging: the same dimension key appears in multiple segments
   * and all values are correctly aggregated.
   *
   * group1 appears in segments 0, 1, 2; group2 in segments 0, 1, 3; group3 in segments 1, 2, 3.
   */
  @Test
  public void testCrossSegmentMerging() throws Exception {
    List<File> segmentDirs = buildSegments(buildStandardSegmentData());
    List<SegmentConversionResult> results = runExecutor(segmentDirs, null);

    Assert.assertEquals(results.size(), 1);
    Map<String, TDigest> digests = readMergedTDigests(results.get(0).getFile());

    // group1 came from 3 segments (100+100+100=300 values)
    TDigest digest1 = digests.get(GROUP_1);
    Assert.assertEquals(digest1.size(), 300L);
    // Verify min and max span the full range
    Assert.assertTrue(digest1.quantile(0.0) <= 2.0, "Min should be close to 1");
    Assert.assertTrue(digest1.quantile(1.0) >= 299.0, "Max should be close to 300");

    // group2 came from 3 segments (100+100+100=300 values)
    TDigest digest2 = digests.get(GROUP_2);
    Assert.assertEquals(digest2.size(), 300L);
    Assert.assertTrue(digest2.quantile(0.0) <= 501.0, "Min should be close to 500");
    Assert.assertTrue(digest2.quantile(1.0) >= 798.0, "Max should be close to 799");

    // group3 came from 3 segments (50+100+50=200 values)
    TDigest digest3 = digests.get(GROUP_3);
    Assert.assertEquals(digest3.size(), 200L);
    Assert.assertTrue(digest3.quantile(0.0) <= 2.0, "Min should be close to 1");
    Assert.assertTrue(digest3.quantile(1.0) >= 199.0, "Max should be close to 200");
  }

  /**
   * Passes a custom compressionFactor via function parameters and verifies merge succeeds
   * with correct results.
   */
  @Test
  public void testCustomCompressionFactor() throws Exception {
    List<File> segmentDirs = buildSegments(buildStandardSegmentData());

    Map<String, String> extraConfigs = new HashMap<>();
    extraConfigs.put("daily." + MinionConstants.MergeTask.AGGREGATION_FUNCTION_PARAMETERS_PREFIX
        + TDIGEST_COL + ".compressionFactor", "200");

    List<SegmentConversionResult> results = runExecutor(segmentDirs, extraConfigs);

    Assert.assertEquals(results.size(), 1);
    File mergedSegment = results.get(0).getFile();
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(mergedSegment);
    Assert.assertEquals(metadata.getTotalDocs(), 3);

    Map<String, TDigest> digests = readMergedTDigests(mergedSegment);

    // With higher compression, quantile estimates should still be accurate
    TDigest digest1 = digests.get(GROUP_1);
    Assert.assertEquals(digest1.size(), 300L);
    Assert.assertEquals(digest1.quantile(0.5), 150, 3);

    TDigest digest2 = digests.get(GROUP_2);
    Assert.assertEquals(digest2.size(), 300L);
    Assert.assertEquals(digest2.quantile(0.5), 649, 3);

    TDigest digest3 = digests.get(GROUP_3);
    Assert.assertEquals(digest3.size(), 200L);
    Assert.assertEquals(digest3.quantile(0.5), 100, 3);
  }

  /**
   * Uses realistic latency value ranges (1ms to 10000ms) with varying distributions per group
   * to verify quantile accuracy on wider data ranges.
   */
  @Test
  public void testLargeValueRange() throws Exception {
    List<List<GenericRow>> segments = new ArrayList<>();

    // Segment 0: group1 [1..500], group2 [5000..10000]
    List<GenericRow> seg0 = new ArrayList<>();
    seg0.add(makeRow(GROUP_1, createTDigest(1, 501)));
    seg0.add(makeRow(GROUP_2, createTDigest(5000, 10001)));
    segments.add(seg0);

    // Segment 1: group1 [501..1000], group3 spread [1..10000] step=10
    List<GenericRow> seg1 = new ArrayList<>();
    seg1.add(makeRow(GROUP_1, createTDigest(501, 1001)));
    TDigest group3Digest = TDigest.createMergingDigest(DEFAULT_COMPRESSION);
    for (int v = 1; v <= 10000; v += 10) {
      group3Digest.add(v);
    }
    seg1.add(makeRow(GROUP_3, group3Digest));
    segments.add(seg1);

    List<File> segmentDirs = buildSegments(segments);
    List<SegmentConversionResult> results = runExecutor(segmentDirs, null);

    Assert.assertEquals(results.size(), 1);
    Map<String, TDigest> digests = readMergedTDigests(results.get(0).getFile());

    // group1: 1000 values [1..1000], P50 ~ 500
    TDigest digest1 = digests.get(GROUP_1);
    Assert.assertEquals(digest1.size(), 1000L);
    Assert.assertEquals(digest1.quantile(0.5), 500, 10);
    Assert.assertEquals(digest1.quantile(0.0), 1, 1);
    Assert.assertEquals(digest1.quantile(1.0), 1000, 1);

    // group2: 5001 values [5000..10000], P50 ~ 7500
    TDigest digest2 = digests.get(GROUP_2);
    Assert.assertEquals(digest2.size(), 5001L);
    Assert.assertEquals(digest2.quantile(0.5), 7500, 50);

    // group3: 1000 values [1,11,21,...,9991], P50 ~ 5000
    TDigest digest3 = digests.get(GROUP_3);
    Assert.assertEquals(digest3.size(), 1000L);
    Assert.assertEquals(digest3.quantile(0.5), 5000, 100);
  }

  /**
   * Verifies that an empty TDigest (no values added) is handled correctly during merge.
   * The merged result for that group should reflect only the non-empty input.
   */
  @Test
  public void testEmptyTDigestHandling() throws Exception {
    List<List<GenericRow>> segments = new ArrayList<>();

    // Segment 0: group with an empty TDigest
    List<GenericRow> seg0 = new ArrayList<>();
    TDigest emptyDigest = TDigest.createMergingDigest(DEFAULT_COMPRESSION);
    seg0.add(makeRow(GROUP_1, emptyDigest));
    segments.add(seg0);

    // Segment 1: group with real values
    List<GenericRow> seg1 = new ArrayList<>();
    seg1.add(makeRow(GROUP_1, createTDigest(1, 101)));
    segments.add(seg1);

    List<File> segmentDirs = buildSegments(segments);
    List<SegmentConversionResult> results = runExecutor(segmentDirs, null);

    Assert.assertEquals(results.size(), 1);
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(results.get(0).getFile());
    Assert.assertEquals(metadata.getTotalDocs(), 1);

    Map<String, TDigest> digests = readMergedTDigests(results.get(0).getFile());
    TDigest digest1 = digests.get(GROUP_1);
    Assert.assertNotNull(digest1);
    // The merged digest should contain the 100 values from the non-empty input
    Assert.assertEquals(digest1.size(), 100L);
    Assert.assertEquals(digest1.quantile(0.5), 50, 2);
  }

  /**
   * Verifies merge works correctly with TDigests that contain exactly 1 value each.
   */
  @Test
  public void testSingleValueTDigest() throws Exception {
    List<List<GenericRow>> segments = new ArrayList<>();

    // Segment 0: single value 42
    List<GenericRow> seg0 = new ArrayList<>();
    TDigest single0 = TDigest.createMergingDigest(DEFAULT_COMPRESSION);
    single0.add(42);
    seg0.add(makeRow(GROUP_1, single0));
    segments.add(seg0);

    // Segment 1: single value 58
    List<GenericRow> seg1 = new ArrayList<>();
    TDigest single1 = TDigest.createMergingDigest(DEFAULT_COMPRESSION);
    single1.add(58);
    seg1.add(makeRow(GROUP_1, single1));
    segments.add(seg1);

    // Segment 2: single value 100 for a different group
    List<GenericRow> seg2 = new ArrayList<>();
    TDigest single2 = TDigest.createMergingDigest(DEFAULT_COMPRESSION);
    single2.add(100);
    seg2.add(makeRow(GROUP_2, single2));
    segments.add(seg2);

    List<File> segmentDirs = buildSegments(segments);
    List<SegmentConversionResult> results = runExecutor(segmentDirs, null);

    Assert.assertEquals(results.size(), 1);
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(results.get(0).getFile());
    Assert.assertEquals(metadata.getTotalDocs(), 2);

    Map<String, TDigest> digests = readMergedTDigests(results.get(0).getFile());

    // group1: 2 values (42, 58)
    TDigest digest1 = digests.get(GROUP_1);
    Assert.assertNotNull(digest1);
    Assert.assertEquals(digest1.size(), 2L);
    Assert.assertEquals(digest1.quantile(0.0), 42, 0.1);
    Assert.assertEquals(digest1.quantile(1.0), 58, 0.1);
    Assert.assertEquals(digest1.quantile(0.5), 50, 8.1);  // median of 42 and 58

    // group2: 1 value (100)
    TDigest digest2 = digests.get(GROUP_2);
    Assert.assertNotNull(digest2);
    Assert.assertEquals(digest2.size(), 1L);
    Assert.assertEquals(digest2.quantile(0.5), 100, 0.1);
  }

  /**
   * Tests skewed distributions: one group has mostly small values, another mostly large.
   * Verifies quantiles are correct after merge.
   */
  @Test
  public void testSkewedDistributions() throws Exception {
    List<List<GenericRow>> segments = new ArrayList<>();

    // Group 1 is heavily skewed low: 900 values in [1..10] and 100 values in [1000..1100]
    // Group 2 is heavily skewed high: 100 values in [1..100] and 900 values in [9000..9900]
    List<GenericRow> seg0 = new ArrayList<>();
    TDigest digest1Low = TDigest.createMergingDigest(DEFAULT_COMPRESSION);
    for (int i = 0; i < 900; i++) {
      digest1Low.add(1 + (i % 10));  // values 1-10, repeated
    }
    seg0.add(makeRow(GROUP_1, digest1Low));

    TDigest digest2Low = TDigest.createMergingDigest(DEFAULT_COMPRESSION);
    for (int i = 1; i <= 100; i++) {
      digest2Low.add(i);
    }
    seg0.add(makeRow(GROUP_2, digest2Low));
    segments.add(seg0);

    List<GenericRow> seg1 = new ArrayList<>();
    TDigest digest1High = TDigest.createMergingDigest(DEFAULT_COMPRESSION);
    for (int i = 1000; i < 1100; i++) {
      digest1High.add(i);
    }
    seg1.add(makeRow(GROUP_1, digest1High));

    TDigest digest2High = TDigest.createMergingDigest(DEFAULT_COMPRESSION);
    for (int i = 0; i < 900; i++) {
      digest2High.add(9000 + (i % 900));
    }
    seg1.add(makeRow(GROUP_2, digest2High));
    segments.add(seg1);

    List<File> segmentDirs = buildSegments(segments);
    List<SegmentConversionResult> results = runExecutor(segmentDirs, null);

    Assert.assertEquals(results.size(), 1);
    Map<String, TDigest> digests = readMergedTDigests(results.get(0).getFile());

    // group1: 1000 total, 90% are in [1..10]
    TDigest digest1 = digests.get(GROUP_1);
    Assert.assertEquals(digest1.size(), 1000L);
    // P50 should be in the low range [1..10] since 90% of values are there
    Assert.assertTrue(digest1.quantile(0.5) <= 10,
        "P50 should be in [1..10] since 90% of values are there, got: " + digest1.quantile(0.5));
    // P80 should still be in the low range (900 of 1000 are low)
    Assert.assertTrue(digest1.quantile(0.80) <= 10,
        "P80 should be in [1..10], got: " + digest1.quantile(0.80));
    // P100 should be near 1099
    Assert.assertEquals(digest1.quantile(1.0), 1099, 1);

    // group2: 1000 total, 90% are in [9000..9899]
    TDigest digest2 = digests.get(GROUP_2);
    Assert.assertEquals(digest2.size(), 1000L);
    // P50 should be in the high range since 90% of values are there
    Assert.assertTrue(digest2.quantile(0.5) >= 9000,
        "P50 should be >= 9000 since 90% of values are there, got: " + digest2.quantile(0.5));
    // P0 should be near 1
    Assert.assertEquals(digest2.quantile(0.0), 1, 1);
  }

  /**
   * After rollup, the total doc count must equal the number of distinct dimension keys.
   */
  @Test
  public void testSegmentDocCountEqualsDistinctKeys() throws Exception {
    List<File> segmentDirs = buildSegments(buildStandardSegmentData());
    List<SegmentConversionResult> results = runExecutor(segmentDirs, null);

    Assert.assertEquals(results.size(), 1);
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(results.get(0).getFile());
    // 3 distinct dimension keys: latency-group1, latency-group2, latency-group3
    Assert.assertEquals(metadata.getTotalDocs(), 3,
        "Rollup doc count should equal the number of distinct dimension keys");
  }

  private static TDigest createTDigest(final int start, final int end) {
    TDigest tDigest = TDigest.createMergingDigest(MergeRollupTDigestTaskExecutorTest.DEFAULT_COMPRESSION);
    for (int v = start; v < end; v++) {
      tDigest.add(v);
    }
    return tDigest;
  }

  private static GenericRow makeRow(String dimensionValue, TDigest tDigest) {
    GenericRow row = new GenericRow();
    row.putValue(DIMENSION_COL, dimensionValue);
    row.putValue(TDIGEST_COL, ObjectSerDeUtils.TDIGEST_SER_DE.serialize(tDigest));
    return row;
  }

  private List<File> buildSegments(List<List<GenericRow>> segmentRows) throws Exception {
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

  private List<SegmentConversionResult> runExecutor(final List<File> segmentDirs,
                                                    final Map<String, String> extraConfigs) throws Exception {
    File workingDir = new File(TEMP_DIR, "workingDir_" + (_workingDirCounter++));
    MergeRollupTaskExecutor executor = new MergeRollupTaskExecutor(new MinionConf());
    executor.setMinionEventObserver(MinionTaskTestUtils.getMinionProgressObserver());

    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME + "_OFFLINE");
    configs.put(MinionConstants.MergeRollupTask.MERGE_LEVEL_KEY, "daily");
    configs.put(MinionConstants.MergeTask.MERGE_TYPE_KEY, "rollup");
    configs.put(TDIGEST_COL + MinionConstants.MergeTask.AGGREGATION_TYPE_KEY_SUFFIX, "percentiletdigest");
    if (extraConfigs != null) {
      configs.putAll(extraConfigs);
    }

    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig(MinionConstants.MergeRollupTask.TASK_TYPE, configs);
    return executor.convert(pinotTaskConfig, segmentDirs, workingDir);
  }

  private Map<String, TDigest> readMergedTDigests(final File mergedSegmentDir) throws Exception {
    Map<String, TDigest> result = new HashMap<>();
    PinotSegmentRecordReader reader = new PinotSegmentRecordReader();
    reader.init(mergedSegmentDir, null, null, true);
    while (reader.hasNext()) {
      GenericRow row = reader.next();
      String key = (String) row.getValue(DIMENSION_COL);
      byte[] bytes = (byte[]) row.getValue(TDIGEST_COL);
      TDigest digest = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytes);
      result.put(key, digest);
    }
    reader.close();
    return result;
  }

  private static List<List<GenericRow>> buildStandardSegmentData() {
    List<List<GenericRow>> segments = new ArrayList<>();

    // Segment 0: group1=[1..100], group2=[500..599]
    List<GenericRow> seg0 = new ArrayList<>();
    seg0.add(makeRow(GROUP_1, createTDigest(1, 101)));
    seg0.add(makeRow(GROUP_2, createTDigest(500, 600)));
    segments.add(seg0);

    // Segment 1: group1=[101..200], group2=[600..699], group3=[1..50]
    List<GenericRow> seg1 = new ArrayList<>();
    seg1.add(makeRow(GROUP_1, createTDigest(101, 201)));
    seg1.add(makeRow(GROUP_2, createTDigest(600, 700)));
    seg1.add(makeRow(GROUP_3, createTDigest(1, 51)));
    segments.add(seg1);

    // Segment 2: group1=[201..300], group3=[51..150]
    List<GenericRow> seg2 = new ArrayList<>();
    seg2.add(makeRow(GROUP_1, createTDigest(201, 301)));
    seg2.add(makeRow(GROUP_3, createTDigest(51, 151)));
    segments.add(seg2);

    // Segment 3: group2=[700..799], group3=[151..200]
    List<GenericRow> seg3 = new ArrayList<>();
    seg3.add(makeRow(GROUP_2, createTDigest(700, 800)));
    seg3.add(makeRow(GROUP_3, createTDigest(151, 201)));
    segments.add(seg3);

    return segments;
  }
}
