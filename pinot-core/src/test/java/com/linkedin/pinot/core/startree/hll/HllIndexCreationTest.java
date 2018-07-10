/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.startree.hll;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.DataBlockCache;
import com.linkedin.pinot.core.common.DataFetcher;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import com.linkedin.pinot.startree.hll.HllConfig;
import com.linkedin.pinot.startree.hll.HllConstants;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Dictionary Index Size for Hll Field is roughly 10 times of the corresponding index for Long field.
 */
public class HllIndexCreationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(HllIndexCreationTest.class);
  private static final String hllDeriveColumnSuffix = HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX;

  // change this to change the columns that need to create hll index on
  private static final Set<String> columnsToDeriveHllFields = new HashSet<>(
      Arrays.asList("column1", "column2", "column3", "count", "weeksSinceEpochSunday", "daysSinceEpoch", "column17",
          "column18"));
  private static final String AVRO_DATA = "data/test_data-sv.avro";
  private static final String timeColumnName = "daysSinceEpoch";
  private static final TimeUnit timeUnit = TimeUnit.DAYS;

  private static final int hllLog2m = HllConstants.DEFAULT_LOG2M;

  private IndexLoadingConfig v3LoadingConfig;

  private HllConfig hllConfig;

  @BeforeClass
  public void setUp() throws Exception {
    hllConfig = new HllConfig(hllLog2m, columnsToDeriveHllFields, hllDeriveColumnSuffix);

    v3LoadingConfig = new IndexLoadingConfig();
    v3LoadingConfig.setReadMode(ReadMode.mmap);
    v3LoadingConfig.setSegmentVersion(SegmentVersion.v3);
  }

  @Test
  public void testColumnStatsWithoutStarTree() {
    SegmentWithHllIndexCreateHelper helper = null;
    boolean hasException = false;
    try {
      LOGGER.debug("================ Without StarTree ================");
      helper = new SegmentWithHllIndexCreateHelper("noStarTree", getClass().getClassLoader().getResource(AVRO_DATA),
          timeColumnName, timeUnit, "starTreeSegment");
      SegmentIndexCreationDriver driver = helper.build(false, null);
      LOGGER.debug("================ Cardinality ================");
      for (String name : helper.getSchema().getColumnNames()) {
        LOGGER.debug("* " + name + ": " + driver.getColumnStatisticsCollector(name).getCardinality());
      }
    } catch (Exception e) {
      hasException = true;
      LOGGER.error(e.getMessage());
    } finally {
      if (helper != null) {
        helper.cleanTempDir();
      }
      Assert.assertEquals(hasException, false);
    }
  }

  @Test
  public void testColumnStatsWithStarTree() {
    SegmentWithHllIndexCreateHelper helper = null;
    boolean hasException = false;
    int maxDocLength = 10000;
    try {
      LOGGER.debug("================ With StarTree ================");
      helper = new SegmentWithHllIndexCreateHelper("withStarTree", getClass().getClassLoader().getResource(AVRO_DATA),
          timeColumnName, timeUnit, "starTreeSegment");
      SegmentIndexCreationDriver driver = helper.build(true, hllConfig);
      LOGGER.debug("================ Cardinality ================");
      for (String name : helper.getSchema().getColumnNames()) {
        LOGGER.debug("* " + name + ": " + driver.getColumnStatisticsCollector(name).getCardinality());
      }
      LOGGER.debug("Loading ...");
      IndexSegment indexSegment = ImmutableSegmentLoader.load(helper.getSegmentDirectory(), ReadMode.mmap);

      int[] docIdSet = new int[maxDocLength];
      for (int i = 0; i < maxDocLength; i++) {
        docIdSet[i] = i;
      }
      Map<String, DataSource> dataSourceMap = new HashMap<>();
      for (String column : indexSegment.getColumnNames()) {
        dataSourceMap.put(column, indexSegment.getDataSource(column));
      }
      DataBlockCache blockCache = new DataBlockCache(new DataFetcher(dataSourceMap));
      blockCache.initNewBlock(docIdSet, maxDocLength);

      String[] strings = blockCache.getStringValuesForSVColumn("column1_hll");
      Assert.assertEquals(strings.length, maxDocLength);

      double[] ints = blockCache.getDoubleValuesForSVColumn("column1");
      Assert.assertEquals(ints.length, maxDocLength);
    } catch (Exception e) {
      hasException = true;
      LOGGER.error(e.getMessage());
    } finally {
      if (helper != null) {
        helper.cleanTempDir();
      }
      Assert.assertEquals(hasException, false);
    }
  }

  @Test
  public void testConvert() throws Exception {
    SegmentWithHllIndexCreateHelper helper = null;
    try {
      helper = new SegmentWithHllIndexCreateHelper("testConvert", getClass().getClassLoader().getResource(AVRO_DATA),
          timeColumnName, timeUnit, "starTreeSegment");

      SegmentIndexCreationDriver driver = helper.build(true, hllConfig);

      File segmentDirectory = new File(helper.getIndexDir(), driver.getSegmentName());
      LOGGER.debug("Segment Directory: " + segmentDirectory.getAbsolutePath());

      SegmentV1V2ToV3FormatConverter converter = new SegmentV1V2ToV3FormatConverter();
      converter.convert(segmentDirectory);
      File v3Location = SegmentDirectoryPaths.segmentDirectoryFor(segmentDirectory, SegmentVersion.v3);
      LOGGER.debug("v3Location: " + v3Location.getAbsolutePath());

      Assert.assertTrue(v3Location.exists());
      Assert.assertTrue(v3Location.isDirectory());
      Assert.assertTrue(new File(v3Location, V1Constants.STAR_TREE_INDEX_FILE).exists());
      Assert.assertTrue(new File(v3Location, V1Constants.SEGMENT_CREATION_META).exists());

      // verify that the segment loads correctly. This is necessary and sufficient
      // full proof way to ensure that segment is correctly translated
      IndexSegment indexSegment = ImmutableSegmentLoader.load(segmentDirectory, v3LoadingConfig);
      Assert.assertNotNull(indexSegment);
      Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3.toString());
    } finally {
      if (helper != null) {
        helper.cleanTempDir();
      }
    }
  }
}
