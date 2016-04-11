/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.segment.index.loader;

import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SegmentPreProcessorTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(SegmentPreProcessor.class.toString());
  private static File segmentDirectoryFile;
  private static final String column1Name = "column1";
  private static final String column13Name = "column13";
  private static final String column7Name = "column7";
  private static final String noSuchColumnName = "noSuchColumn";

  private static IndexLoadingConfigMetadata indexLoadingConfigMetadata;

  @BeforeClass
  public void setUpClass()
      throws Exception {

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    final String filePath =
        TestUtils.getFileFromResourceUrl(SegmentPreProcessor.class.getClassLoader().getResource(AVRO_DATA));

    // Setup note: we create inverted index for column7 when constructing the segment
    // For IndexLoadingConfig, we specify two new columns, one bad column and one existing column
    // Bad column name is used to verify that non-existing columns (bad configuration/typo) will
    // not bring down the service

    // intentionally changed this to TimeUnit.Hours to make it non-default for testing
    final SegmentGeneratorConfig config = SegmentTestUtils
        .getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch", TimeUnit.HOURS,
            "testTable");
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");
    config.getInvertedIndexCreationColumns().clear();
    config.setInvertedIndexCreationColumns(Arrays.asList(column7Name));
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    segmentDirectoryFile = new File(INDEX_DIR, driver.getSegmentName());
    Configuration indexLoadingProperties = new PropertiesConfiguration();
    indexLoadingConfigMetadata = new IndexLoadingConfigMetadata(indexLoadingProperties);
    String csvColumns = column1Name + "," + column13Name + "," + noSuchColumnName + "," + column7Name;
    indexLoadingConfigMetadata.initLoadingInvertedIndexColumnSet(csvColumns.split(","));
    Set<String> IIColumns = indexLoadingConfigMetadata.getLoadingInvertedIndexColumns();
    Assert.assertEquals(4, IIColumns.size());
    Assert.assertTrue(IIColumns.contains(column1Name));
    Assert.assertTrue(IIColumns.contains(column13Name));
    Assert.assertTrue(IIColumns.contains(column7Name));
    Assert.assertTrue(IIColumns.contains(noSuchColumnName));
  }

  @AfterClass
  public void tearDownClass()
      throws Exception {
    if (INDEX_DIR != null) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
  }

  @Test
  public void testProcess()
      throws Exception {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDirectoryFile);
    boolean exists = true;
    SegmentVersion version = SegmentVersion.valueOf(segmentMetadata.getVersion());
    File col7File = null;
    FileTime preLastModifiedTime = null;
    File singleFileIndex = null;
    long singleFileSize = 0;
    if (version == SegmentVersion.v1 || version == SegmentVersion.v2) {
      String col7IIFilename = segmentMetadata.getBitmapInvertedIndexFileName(column7Name, segmentMetadata.getVersion());
      col7File = new File(segmentDirectoryFile, col7IIFilename);
      Assert.assertTrue(col7File.exists());
      preLastModifiedTime = Files.getLastModifiedTime(col7File.toPath());
    } else {
      singleFileIndex = new File(segmentDirectoryFile, "index.psf");
      preLastModifiedTime = Files.getLastModifiedTime(singleFileIndex.toPath());
      singleFileSize = singleFileIndex.length();
    }

    try (SegmentDirectory segmentDirectory = SegmentDirectory
        .createFromLocalFS(segmentDirectoryFile, segmentMetadata, ReadMode.mmap)) {
      try (SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
        Assert.assertFalse(reader.hasIndexFor(column1Name, ColumnIndexType.INVERTED_INDEX));
        Assert.assertFalse(reader.hasIndexFor(column13Name, ColumnIndexType.INVERTED_INDEX));
        // already created by segment gen specifying config
        Assert.assertTrue(reader.hasIndexFor(column7Name, ColumnIndexType.INVERTED_INDEX));
        Assert.assertFalse(reader.hasIndexFor(noSuchColumnName, ColumnIndexType.INVERTED_INDEX));
      }

      SegmentPreProcessor processor =
          new SegmentPreProcessor(segmentDirectoryFile, segmentMetadata, indexLoadingConfigMetadata);
      processor.process();

      long newLength = 0;
      try (SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
        // new II added; existing not removed
        Assert.assertTrue(reader.hasIndexFor(column1Name, ColumnIndexType.INVERTED_INDEX));
        Assert.assertTrue(reader.hasIndexFor(column13Name, ColumnIndexType.INVERTED_INDEX));
        Assert.assertTrue(reader.hasIndexFor(column7Name, ColumnIndexType.INVERTED_INDEX));
        Assert.assertFalse(reader.hasIndexFor(noSuchColumnName, ColumnIndexType.INVERTED_INDEX));

        try (PinotDataBuffer col1Buffer = reader.getIndexFor(column1Name, ColumnIndexType.INVERTED_INDEX)) {
          newLength += col1Buffer.size();
        }
        try (PinotDataBuffer col13Buffer = reader.getIndexFor(column13Name, ColumnIndexType.INVERTED_INDEX)){
          newLength += col13Buffer.size();
        }
      }
      // check we didn't create column7 index again
      // we violate internals to get more testing here
      if (version == SegmentVersion.v1 || version == SegmentVersion.v2) {
        Assert.assertEquals(Files.getLastModifiedTime(col7File.toPath()), preLastModifiedTime);
      } else {
        Assert.assertTrue(Files.getLastModifiedTime(singleFileIndex.toPath()).compareTo(preLastModifiedTime) > 0);
        long newFileSize = singleFileIndex.length();
        Assert.assertEquals(newFileSize, singleFileSize + newLength + 16);
      }
    }
  }

  @Test
  public void testNullIndexLoadingConfig()
      throws IOException, ConfigurationException {
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDirectoryFile);
    SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectoryFile, metadata, null);
    processor.process();
    // no exception is the validation that we handle null value for IndexLoadingConfig correctly
  }
}
