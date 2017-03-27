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

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LoadersTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final String PADDING_OLD = "data/paddingOld.tar.gz";
  private static final String PADDING_PERCENT = "data/paddingPercent.tar.gz";
  private static final String PADDING_NULL = "data/paddingNull.tar.gz";

  private File _indexDir;
  private File _segmentDirectory;
  private IndexLoadingConfig _v1IndexLoadingConfig;
  private IndexLoadingConfig _v3IndexLoadingConfig;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _indexDir = Files.createTempDirectory(LoadersTest.class.getName() + "_segmentDir").toFile();
    final String filePath = TestUtils.getFileFromResourceUrl(Loaders.class.getClassLoader().getResource(AVRO_DATA));
    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), _indexDir, "daysSinceEpoch",
            TimeUnit.HOURS, "testTable");
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    _segmentDirectory = new File(_indexDir, driver.getSegmentName());

    _v1IndexLoadingConfig = new IndexLoadingConfig();
    _v1IndexLoadingConfig.setReadMode(ReadMode.mmap);
    _v1IndexLoadingConfig.setSegmentVersion(SegmentVersion.v1);

    _v3IndexLoadingConfig = new IndexLoadingConfig();
    _v3IndexLoadingConfig.setReadMode(ReadMode.mmap);
    _v3IndexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
  }

  @AfterMethod
  public void tearDownClass() {
    if (_indexDir != null) {
      FileUtils.deleteQuietly(_indexDir);
    }
  }

  @Test
  public void testLoad()
      throws Exception {
    SegmentMetadataImpl originalMetadata = new SegmentMetadataImpl(_segmentDirectory);
    Assert.assertEquals(originalMetadata.getSegmentVersion(), SegmentVersion.v1);
    // note: ordering of these two test blocks matters
    {
      // Explicitly pass v1 format since we will convert by default to v3
      IndexSegment indexSegment = Loaders.IndexSegment.load(_segmentDirectory, _v1IndexLoadingConfig);
      Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), originalMetadata.getVersion());
      Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_segmentDirectory, SegmentVersion.v3).exists());
    }
    {
      // with this code and converter in place, make sure we still load original version
      // by default. We require specific configuration for v3.
      IndexSegment indexSegment = Loaders.IndexSegment.load(_segmentDirectory, ReadMode.mmap);
      Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3.toString());
      Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_segmentDirectory, SegmentVersion.v3).exists());
    }
  }

  @Test
  public void testLoadWithStaleConversionDir()
      throws Exception {
    // Format converter will leave stale directories around if there was
    // conversion failure. This test case checks loading in that scenario
    SegmentMetadataImpl originalMetadata = new SegmentMetadataImpl(_segmentDirectory);
    Assert.assertEquals(originalMetadata.getSegmentVersion(), SegmentVersion.v1);
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_segmentDirectory, SegmentVersion.v3).exists());
    File v3TempDir = new SegmentV1V2ToV3FormatConverter().v3ConversionTempDirectory(_segmentDirectory);
    FileUtils.touch(v3TempDir);

    {
      IndexSegment indexSegment = Loaders.IndexSegment.load(_segmentDirectory, ReadMode.mmap);
      Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3.toString());
      Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_segmentDirectory, SegmentVersion.v3).exists());
    }
    {
      IndexSegment indexSegment = Loaders.IndexSegment.load(_segmentDirectory, _v3IndexLoadingConfig);
      Assert.assertEquals(SegmentVersion.valueOf(indexSegment.getSegmentMetadata().getVersion()), SegmentVersion.v3);
      Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_segmentDirectory, SegmentVersion.v3).exists());
      Assert.assertFalse(v3TempDir.exists());
    }
  }

  @Test
  public void testPadding()
      throws Exception {
    // Old Format
    TarGzCompressionUtils.unTar(
        new File(TestUtils.getFileFromResourceUrl(Loaders.class.getClassLoader().getResource(PADDING_OLD))), _indexDir);
    File segmentDirectory = new File(_indexDir, "paddingOld");
    SegmentMetadataImpl originalMetadata = new SegmentMetadataImpl(segmentDirectory);
    Assert.assertEquals(originalMetadata.getColumnMetadataFor("name").getPaddingCharacter(),
        V1Constants.Str.LEGACY_STRING_PAD_CHAR);
    SegmentDirectory segmentDir = SegmentDirectory.createFromLocalFS(segmentDirectory, originalMetadata, ReadMode.heap);
    ColumnMetadata columnMetadataFor = originalMetadata.getColumnMetadataFor("name");
    SegmentDirectory.Reader reader = segmentDir.createReader();
    PinotDataBuffer dictionaryBuffer = reader.getIndexFor("name", ColumnIndexType.DICTIONARY);
    StringDictionary dict = new StringDictionary(dictionaryBuffer, columnMetadataFor);
    Assert.assertEquals(dict.getStringValue(0), "lynda 2.0");
    Assert.assertEquals(dict.getStringValue(1), "lynda%%%%");
    Assert.assertEquals(dict.get(0), "lynda 2.0");
    Assert.assertEquals(dict.get(1), "lynda");
    Assert.assertEquals(dict.indexOf("lynda%"), 1);
    Assert.assertEquals(dict.indexOf("lynda%%"), 1);

    // New Format Padding character %
    TarGzCompressionUtils.unTar(
        new File(TestUtils.getFileFromResourceUrl(Loaders.class.getClassLoader().getResource(PADDING_PERCENT))),
        _indexDir);
    segmentDirectory = new File(_indexDir, "paddingPercent");
    originalMetadata = new SegmentMetadataImpl(segmentDirectory);
    Assert.assertEquals(originalMetadata.getColumnMetadataFor("name").getPaddingCharacter(),
        V1Constants.Str.LEGACY_STRING_PAD_CHAR);
    segmentDir = SegmentDirectory.createFromLocalFS(segmentDirectory, originalMetadata, ReadMode.heap);
    columnMetadataFor = originalMetadata.getColumnMetadataFor("name");
    reader = segmentDir.createReader();
    dictionaryBuffer = reader.getIndexFor("name", ColumnIndexType.DICTIONARY);
    dict = new StringDictionary(dictionaryBuffer, columnMetadataFor);
    Assert.assertEquals(dict.getStringValue(0), "lynda 2.0");
    Assert.assertEquals(dict.getStringValue(1), "lynda%%%%");
    Assert.assertEquals(dict.get(0), "lynda 2.0");
    Assert.assertEquals(dict.get(1), "lynda");
    Assert.assertEquals(dict.indexOf("lynda%"), 1);
    Assert.assertEquals(dict.indexOf("lynda%%"), 1);

    // New Format Padding character Null
    TarGzCompressionUtils.unTar(
        new File(TestUtils.getFileFromResourceUrl(Loaders.class.getClassLoader().getResource(PADDING_NULL))),
        _indexDir);
    segmentDirectory = new File(_indexDir, "paddingNull");
    originalMetadata = new SegmentMetadataImpl(segmentDirectory);
    Assert.assertEquals(originalMetadata.getColumnMetadataFor("name").getPaddingCharacter(),
        V1Constants.Str.DEFAULT_STRING_PAD_CHAR);
    segmentDir = SegmentDirectory.createFromLocalFS(segmentDirectory, originalMetadata, ReadMode.heap);
    columnMetadataFor = originalMetadata.getColumnMetadataFor("name");
    reader = segmentDir.createReader();
    dictionaryBuffer = reader.getIndexFor("name", ColumnIndexType.DICTIONARY);
    dict = new StringDictionary(dictionaryBuffer, columnMetadataFor);
    Assert.assertEquals(dict.getStringValue(0), "lynda\0\0\0\0");
    Assert.assertEquals(dict.getStringValue(1), "lynda 2.0");
    Assert.assertEquals(dict.get(0), "lynda");
    Assert.assertEquals(dict.get(1), "lynda 2.0");
    Assert.assertEquals(dict.indexOf("lynda\0"), 0);
    Assert.assertEquals(dict.indexOf("lynda\0\0"), 0);
  }
}
