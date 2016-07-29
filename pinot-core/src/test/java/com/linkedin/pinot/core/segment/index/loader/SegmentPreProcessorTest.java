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
package com.linkedin.pinot.core.segment.index.loader;

import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SegmentPreProcessorTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(SegmentPreProcessor.class.toString());
  private static final String COLUMN1_NAME = "column1";
  private static final String COLUMN7_NAME = "column7";
  private static final String COLUMN13_NAME = "column13";
  private static final String NO_SUCH_COLUMN_NAME = "noSuchColumn";
  private static final String V3_SEGMENT_NAME = "v3";

  private File segmentDirectoryFile;
  private IndexLoadingConfigMetadata indexLoadingConfigMetadata;

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    File avroFile =
        new File(TestUtils.getFileFromResourceUrl(SegmentPreProcessor.class.getClassLoader().getResource(AVRO_DATA)));

    // NOTE:
    // We create inverted index for 'column7' when constructing the segment.
    // For IndexLoadingConfig, we specify two columns without inverted index ('column1', 'column13'), one non-existing
    // column ('noSuchColumn') and one column with existed inverted index ('column7').

    // Intentionally changed this to TimeUnit.Hours to make it non-default for testing.
    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(avroFile, INDEX_DIR, "daysSinceEpoch",
            TimeUnit.HOURS, "testTable");
    config.setSegmentNamePostfix("1");
    config.getInvertedIndexCreationColumns().clear();
    config.setInvertedIndexCreationColumns(Collections.singletonList(COLUMN7_NAME));
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    segmentDirectoryFile = new File(INDEX_DIR, driver.getSegmentName());
    indexLoadingConfigMetadata = new IndexLoadingConfigMetadata(new PropertiesConfiguration());
    indexLoadingConfigMetadata.initLoadingInvertedIndexColumnSet(
        new String[]{COLUMN1_NAME, COLUMN7_NAME, COLUMN13_NAME, NO_SUCH_COLUMN_NAME});
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void testV1Process()
      throws Exception {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDirectoryFile);
    String segmentVersion = segmentMetadata.getVersion();
    Assert.assertEquals(SegmentVersion.valueOf(segmentVersion), SegmentVersion.v1);

    String col1FileName = segmentMetadata.getBitmapInvertedIndexFileName(COLUMN1_NAME, segmentVersion);
    String col7FileName = segmentMetadata.getBitmapInvertedIndexFileName(COLUMN7_NAME, segmentVersion);
    String col13FileName = segmentMetadata.getBitmapInvertedIndexFileName(COLUMN13_NAME, segmentVersion);
    String badColFileName = segmentMetadata.getBitmapInvertedIndexFileName(NO_SUCH_COLUMN_NAME, segmentVersion);
    File col1File = new File(segmentDirectoryFile, col1FileName);
    File col7File = new File(segmentDirectoryFile, col7FileName);
    File col13File = new File(segmentDirectoryFile, col13FileName);
    File badColFile = new File(segmentDirectoryFile, badColFileName);
    Assert.assertFalse(col1File.exists());
    Assert.assertTrue(col7File.exists());
    Assert.assertFalse(col13File.exists());
    Assert.assertFalse(badColFile.exists());
    FileTime col7LastModifiedTime = Files.getLastModifiedTime(col7File.toPath());

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the first time.
    checkInvertedIndexCreation(segmentDirectoryFile, segmentMetadata, false);
    Assert.assertTrue(col1File.exists());
    Assert.assertTrue(col7File.exists());
    Assert.assertTrue(col13File.exists());
    Assert.assertFalse(badColFile.exists());
    Assert.assertEquals(Files.getLastModifiedTime(col7File.toPath()), col7LastModifiedTime);

    // Update inverted index file last modified time.
    FileTime col1LastModifiedTime = Files.getLastModifiedTime(col1File.toPath());
    FileTime col13LastModifiedTime = Files.getLastModifiedTime(col13File.toPath());

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the second time.
    checkInvertedIndexCreation(segmentDirectoryFile, segmentMetadata, true);
    Assert.assertTrue(col1File.exists());
    Assert.assertTrue(col7File.exists());
    Assert.assertTrue(col13File.exists());
    Assert.assertFalse(badColFile.exists());
    Assert.assertEquals(Files.getLastModifiedTime(col1File.toPath()), col1LastModifiedTime);
    Assert.assertEquals(Files.getLastModifiedTime(col7File.toPath()), col7LastModifiedTime);
    Assert.assertEquals(Files.getLastModifiedTime(col13File.toPath()), col13LastModifiedTime);
  }

  @Test
  public void testV3Process()
      throws Exception {
    // Convert segment format to v3.
    SegmentV1V2ToV3FormatConverter converter = new SegmentV1V2ToV3FormatConverter();
    converter.convert(segmentDirectoryFile);
    File v3SegmentDirectoryFile = new File(segmentDirectoryFile, V3_SEGMENT_NAME);

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(v3SegmentDirectoryFile);
    String segmentVersion = segmentMetadata.getVersion();
    Assert.assertEquals(SegmentVersion.valueOf(segmentVersion), SegmentVersion.v3);

    File singleFileIndex = new File(v3SegmentDirectoryFile, "columns.psf");
    FileTime lastModifiedTime = Files.getLastModifiedTime(singleFileIndex.toPath());
    long fileSize = singleFileIndex.length();

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the first time.
    checkInvertedIndexCreation(v3SegmentDirectoryFile, segmentMetadata, false);
    long addedLength = 0L;
    try (
        SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(v3SegmentDirectoryFile, segmentMetadata,
            ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()
    ) {
      // 8 bytes overhead is for checking integrity of the segment.
      try (PinotDataBuffer col1Buffer = reader.getIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX)) {
        addedLength += col1Buffer.size() + 8;
      }
      try (PinotDataBuffer col13Buffer = reader.getIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX)) {
        addedLength += col13Buffer.size() + 8;
      }
    }
    FileTime newLastModifiedTime = Files.getLastModifiedTime(singleFileIndex.toPath());
    Assert.assertTrue(newLastModifiedTime.compareTo(lastModifiedTime) > 0);
    long newFileSize = singleFileIndex.length();
    Assert.assertEquals(fileSize + addedLength, newFileSize);

    // Sleep 2 seconds to prevent the same last modified time when modifying the file.
    Thread.sleep(2000);

    // Create inverted index the second time.
    checkInvertedIndexCreation(v3SegmentDirectoryFile, segmentMetadata, true);
    Assert.assertEquals(Files.getLastModifiedTime(singleFileIndex.toPath()), newLastModifiedTime);
    Assert.assertEquals(singleFileIndex.length(), newFileSize);
  }

  private void checkInvertedIndexCreation(File segmentDirectoryFile, SegmentMetadataImpl segmentMetadata,
      boolean reCreate)
      throws Exception {
    try (
        SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(segmentDirectoryFile, segmentMetadata,
            ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()
    ) {
      if (reCreate) {
        Assert.assertTrue(reader.hasIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX));
        Assert.assertTrue(reader.hasIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX));
        Assert.assertTrue(reader.hasIndexFor(COLUMN7_NAME, ColumnIndexType.INVERTED_INDEX));
        Assert.assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, ColumnIndexType.INVERTED_INDEX));
      } else {
        Assert.assertFalse(reader.hasIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX));
        Assert.assertTrue(reader.hasIndexFor(COLUMN7_NAME, ColumnIndexType.INVERTED_INDEX));
        Assert.assertFalse(reader.hasIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX));
        Assert.assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, ColumnIndexType.INVERTED_INDEX));
      }
    }

    SegmentPreProcessor processor =
        new SegmentPreProcessor(segmentDirectoryFile, segmentMetadata, indexLoadingConfigMetadata);
    processor.process();

    try (
        SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(segmentDirectoryFile, segmentMetadata,
            ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()
    ) {
      Assert.assertTrue(reader.hasIndexFor(COLUMN1_NAME, ColumnIndexType.INVERTED_INDEX));
      Assert.assertTrue(reader.hasIndexFor(COLUMN13_NAME, ColumnIndexType.INVERTED_INDEX));
      Assert.assertTrue(reader.hasIndexFor(COLUMN7_NAME, ColumnIndexType.INVERTED_INDEX));
      Assert.assertFalse(reader.hasIndexFor(NO_SUCH_COLUMN_NAME, ColumnIndexType.INVERTED_INDEX));
    }
  }

  @Test
  public void testNullIndexLoadingConfig()
      throws IOException, ConfigurationException {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDirectoryFile);
    SegmentPreProcessor processor = new SegmentPreProcessor(segmentDirectoryFile, segmentMetadata, null);
    processor.process();
    // No exception is the validation that we handle null value for IndexLoadingConfig correctly.
  }
}
