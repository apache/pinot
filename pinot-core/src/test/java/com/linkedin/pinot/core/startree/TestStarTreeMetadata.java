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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.common.segment.StarTreeMetadata;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestStarTreeMetadata {
  private final String AVRO_DATA = "data/test_sample_data.avro";
  private static final int MAX_LEAF_RECORDS = 99;

  private static final int SKIP_CARDINALITY_THRESHOLD = 99999;
  private static final List<String> DIMENSIONS_SPLIT_ORDER = Arrays.asList(new String[]{"column3", "column4"});

  private static final Set<String> SKIP_STAR_NODE_CREATION_DIMENSTIONS =
      new HashSet<String>(Arrays.asList(new String[]{"column9"}));

  private static final Set<String> SKIP_MATERIALIZATION_DIMENSIONS =
      new HashSet<String>(Arrays.asList(new String[]{"column11"}));

  private static final String TABLE_NAME = "starTreeTable";
  private static final String SEGMENT_NAME = "starTreeSegment";

  private static final String INDEX_DIR_NAME = FileUtils.getTempDirectory() + File.separator + "starTreeMetaData";
  private static File INDEX_DIR = new File(INDEX_DIR_NAME);
  public static IndexSegment _indexSegment;

  /**
   * Build the StarTree segment
   *
   * @throws Exception
   */
  @BeforeTest
  public void setup()
      throws Exception {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    INDEX_DIR.mkdirs();
    setupSegment(INDEX_DIR);
  }

  private void setupSegment(File segmentDir)
      throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));

    if (segmentDir.exists()) {
      FileUtils.deleteQuietly(segmentDir);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), segmentDir, "time_day",
            TimeUnit.DAYS, TABLE_NAME);

    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    StarTreeIndexSpec starTreeIndexSpec = new StarTreeIndexSpec();
    starTreeIndexSpec.setDimensionsSplitOrder(DIMENSIONS_SPLIT_ORDER);
    starTreeIndexSpec.setMaxLeafRecords(MAX_LEAF_RECORDS);
    starTreeIndexSpec.setSkipMaterializationCardinalityThreshold(SKIP_CARDINALITY_THRESHOLD);
    starTreeIndexSpec.setSkipStarNodeCreationForDimensions(SKIP_STAR_NODE_CREATION_DIMENSTIONS);
    starTreeIndexSpec.setSkipMaterializationForDimensions(SKIP_MATERIALIZATION_DIMENSIONS);

    config.enableStarTreeIndex(starTreeIndexSpec);

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

  }

  /**
   * Read the StarTree metadata and assert that the actual values in the metadata are as expected.
   *
   * @throws Exception
   */
  @Test
  public void testStarTreeMetadata()
      throws Exception {
    String segment = INDEX_DIR_NAME + File.separator + SEGMENT_NAME;

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(new File(segment));
    StarTreeMetadata starTreeMetadata = segmentMetadata.getStarTreeMetadata();

    Assert.assertEquals(starTreeMetadata.getDimensionsSplitOrder(), DIMENSIONS_SPLIT_ORDER);
    Assert.assertEquals(starTreeMetadata.getMaxLeafRecords(), MAX_LEAF_RECORDS);

    Assert.assertEquals(starTreeMetadata.getSkipStarNodeCreationForDimensions(), SKIP_STAR_NODE_CREATION_DIMENSTIONS);
    Assert.assertEquals(starTreeMetadata.getSkipMaterializationCardinality(), SKIP_CARDINALITY_THRESHOLD);
    Assert.assertEquals(starTreeMetadata.getSkipMaterializationForDimensions(), SKIP_MATERIALIZATION_DIMENSIONS);
  }
}
