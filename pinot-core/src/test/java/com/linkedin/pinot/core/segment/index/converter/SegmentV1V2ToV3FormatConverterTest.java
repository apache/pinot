/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index.converter;

import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SegmentV1V2ToV3FormatConverterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentV1V2ToV3FormatConverterTest.class);
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private File INDEX_DIR;
  private File segmentDirectory;
  private IndexLoadingConfigMetadata v1LoadingConfig;
  private IndexLoadingConfigMetadata v3LoadingConfig;

  @BeforeMethod
  public void setUp()
      throws Exception {

    INDEX_DIR = Files.createTempDirectory(SegmentV1V2ToV3FormatConverter.class.getName() + "_segmentDir").toFile();

    final String filePath =
        TestUtils
            .getFileFromResourceUrl(SegmentV1V2ToV3FormatConverter.class.getClassLoader().getResource(AVRO_DATA));

    // intentionally changed this to TimeUnit.Hours to make it non-default for testing
    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            TimeUnit.HOURS, "testTable");
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
    File starTreeFile = new File(segmentDirectory, V1Constants.STAR_TREE_INDEX_FILE);
    FileUtils.touch(starTreeFile);
    FileUtils.writeStringToFile(starTreeFile, "This is a star tree index");
    Configuration tableConfig = new PropertiesConfiguration();
    tableConfig.addProperty(IndexLoadingConfigMetadata.KEY_OF_SEGMENT_FORMAT_VERSION, "v1");
    v1LoadingConfig = new IndexLoadingConfigMetadata(tableConfig);

    tableConfig.clear();
    tableConfig.addProperty(IndexLoadingConfigMetadata.KEY_OF_SEGMENT_FORMAT_VERSION,  "v3");
    v3LoadingConfig = new IndexLoadingConfigMetadata(tableConfig);
  }


  @AfterMethod
  public void tearDown()
      throws Exception {
    if (INDEX_DIR != null) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
  }

  @Test
  public void testConvert()
      throws Exception {
    SegmentV1V2ToV3FormatConverter converter = new SegmentV1V2ToV3FormatConverter();
    converter.convert(segmentDirectory);
    File v3Location = SegmentDirectoryPaths.segmentDirectoryFor(segmentDirectory, SegmentVersion.v3);
    Assert.assertTrue(v3Location.exists());
    Assert.assertTrue(v3Location.isDirectory());
    Assert.assertTrue(new File(v3Location, V1Constants.STAR_TREE_INDEX_FILE).exists());

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(v3Location);
    Assert.assertEquals(metadata.getVersion(), SegmentVersion.v3.toString());
    Assert.assertTrue(new File(v3Location, V1Constants.SEGMENT_CREATION_META).exists());
    // Drop the star tree index file because it has invalid data
    new File(v3Location, V1Constants.STAR_TREE_INDEX_FILE).delete();
    new File(segmentDirectory, V1Constants.STAR_TREE_INDEX_FILE).delete();
    FileTime afterConversionTime = Files.getLastModifiedTime(v3Location.toPath());

    // verify that the segment loads correctly. This is necessary and sufficient
    // full proof way to ensure that segment is correctly translated
    IndexSegment indexSegment = Loaders.IndexSegment.load(segmentDirectory, ReadMode.mmap, v3LoadingConfig);
    Assert.assertNotNull(indexSegment);
    Assert.assertEquals(indexSegment.getSegmentName(), metadata.getName());
    Assert.assertEquals(SegmentVersion.v3,
        SegmentVersion.valueOf(indexSegment.getSegmentMetadata().getVersion()));

    FileTime afterLoadTime = Files.getLastModifiedTime(v3Location.toPath());
    Assert.assertEquals(afterConversionTime, afterLoadTime);
  }


}
