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
package com.linkedin.pinot.core.segment.index.loader;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
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
import java.net.URL;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class LoaderTest {
  private static final File INDEX_DIR = new File(LoaderTest.class.getName());
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final String PADDING_OLD = "data/paddingOld.tar.gz";
  private static final String PADDING_PERCENT = "data/paddingPercent.tar.gz";
  private static final String PADDING_NULL = "data/paddingNull.tar.gz";

  private File _avroFile;
  private File _indexDir;
  private IndexLoadingConfig _v1IndexLoadingConfig;
  private IndexLoadingConfig _v3IndexLoadingConfig;

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA);
    Assert.assertNotNull(resourceUrl);
    _avroFile = new File(resourceUrl.getFile());

    _v1IndexLoadingConfig = new IndexLoadingConfig();
    _v1IndexLoadingConfig.setReadMode(ReadMode.mmap);
    _v1IndexLoadingConfig.setSegmentVersion(SegmentVersion.v1);

    _v3IndexLoadingConfig = new IndexLoadingConfig();
    _v3IndexLoadingConfig.setReadMode(ReadMode.mmap);
    _v3IndexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
  }

  private Schema constructV1Segment() throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, "testTable");
    segmentGeneratorConfig.setSegmentVersion(SegmentVersion.v1);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(segmentGeneratorConfig);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
    return segmentGeneratorConfig.getSchema();
  }

  @Test
  public void testLoad() throws Exception {
    constructV1Segment();
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getSegmentVersion(), SegmentVersion.v1);
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());

    testConversion();
  }

  /**
   * Format converter will leave stale directory around if there were conversion failures. This test checks loading in
   * that scenario.
   */
  @Test
  public void testLoadWithStaleConversionDir() throws Exception {
    constructV1Segment();

    File v3TempDir = new SegmentV1V2ToV3FormatConverter().v3ConversionTempDirectory(_indexDir);
    Assert.assertTrue(v3TempDir.isDirectory());
    testConversion();
    Assert.assertFalse(v3TempDir.exists());
  }

  private void testConversion() throws Exception {
    // Do not set segment version, should not convert the segment
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, ReadMode.mmap);
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1.toString());
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    indexSegment.destroy();

    // Set segment version to v1, should not convert the segment
    indexSegment = ImmutableSegmentLoader.load(_indexDir, _v1IndexLoadingConfig);
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1.toString());
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    indexSegment.destroy();

    // Set segment version to v3, should convert the segment to v3
    indexSegment = ImmutableSegmentLoader.load(_indexDir, _v3IndexLoadingConfig);
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3.toString());
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    indexSegment.destroy();
  }

  @Test
  public void testPadding() throws Exception {
    // Old Format
    URL resourceUrl = LoaderTest.class.getClassLoader().getResource(PADDING_OLD);
    Assert.assertNotNull(resourceUrl);
    TarGzCompressionUtils.unTar(new File(TestUtils.getFileFromResourceUrl(resourceUrl)), INDEX_DIR);
    File segmentDirectory = new File(INDEX_DIR, "paddingOld");
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDirectory);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor("name");
    Assert.assertEquals(columnMetadata.getPaddingCharacter(), V1Constants.Str.LEGACY_STRING_PAD_CHAR);
    SegmentDirectory segmentDir = SegmentDirectory.createFromLocalFS(segmentDirectory, segmentMetadata, ReadMode.heap);
    SegmentDirectory.Reader reader = segmentDir.createReader();
    PinotDataBuffer dictionaryBuffer = reader.getIndexFor("name", ColumnIndexType.DICTIONARY);
    StringDictionary dict = new StringDictionary(dictionaryBuffer, columnMetadata.getCardinality(),
        columnMetadata.getColumnMaxLength(), (byte) columnMetadata.getPaddingCharacter());
    Assert.assertEquals(dict.getStringValue(0), "lynda 2.0");
    Assert.assertEquals(dict.getStringValue(1), "lynda");
    Assert.assertEquals(dict.get(0), "lynda 2.0");
    Assert.assertEquals(dict.get(1), "lynda");
    Assert.assertEquals(dict.indexOf("lynda%"), 1);
    Assert.assertEquals(dict.indexOf("lynda%%"), 1);

    // New Format Padding character %
    resourceUrl = LoaderTest.class.getClassLoader().getResource(PADDING_PERCENT);
    Assert.assertNotNull(resourceUrl);
    TarGzCompressionUtils.unTar(new File(TestUtils.getFileFromResourceUrl(resourceUrl)), INDEX_DIR);
    segmentDirectory = new File(INDEX_DIR, "paddingPercent");
    segmentMetadata = new SegmentMetadataImpl(segmentDirectory);
    columnMetadata = segmentMetadata.getColumnMetadataFor("name");
    Assert.assertEquals(columnMetadata.getPaddingCharacter(), V1Constants.Str.LEGACY_STRING_PAD_CHAR);
    segmentDir = SegmentDirectory.createFromLocalFS(segmentDirectory, segmentMetadata, ReadMode.heap);
    reader = segmentDir.createReader();
    dictionaryBuffer = reader.getIndexFor("name", ColumnIndexType.DICTIONARY);
    dict = new StringDictionary(dictionaryBuffer, columnMetadata.getCardinality(),
        columnMetadata.getColumnMaxLength(), (byte) columnMetadata.getPaddingCharacter());
    Assert.assertEquals(dict.getStringValue(0), "lynda 2.0");
    Assert.assertEquals(dict.getStringValue(1), "lynda");
    Assert.assertEquals(dict.get(0), "lynda 2.0");
    Assert.assertEquals(dict.get(1), "lynda");
    Assert.assertEquals(dict.indexOf("lynda%"), 1);
    Assert.assertEquals(dict.indexOf("lynda%%"), 1);

    // New Format Padding character Null
    resourceUrl = LoaderTest.class.getClassLoader().getResource(PADDING_NULL);
    Assert.assertNotNull(resourceUrl);
    TarGzCompressionUtils.unTar(new File(TestUtils.getFileFromResourceUrl(resourceUrl)), INDEX_DIR);
    segmentDirectory = new File(INDEX_DIR, "paddingNull");
    segmentMetadata = new SegmentMetadataImpl(segmentDirectory);
    columnMetadata = segmentMetadata.getColumnMetadataFor("name");
    Assert.assertEquals(columnMetadata.getPaddingCharacter(), V1Constants.Str.DEFAULT_STRING_PAD_CHAR);
    segmentDir = SegmentDirectory.createFromLocalFS(segmentDirectory, segmentMetadata, ReadMode.heap);
    reader = segmentDir.createReader();
    dictionaryBuffer = reader.getIndexFor("name", ColumnIndexType.DICTIONARY);
    dict = new StringDictionary(dictionaryBuffer, columnMetadata.getCardinality(),
        columnMetadata.getColumnMaxLength(), (byte) columnMetadata.getPaddingCharacter());
    Assert.assertEquals(dict.getStringValue(0), "lynda");
    Assert.assertEquals(dict.getStringValue(1), "lynda 2.0");
    Assert.assertEquals(dict.get(0), "lynda");
    Assert.assertEquals(dict.get(1), "lynda 2.0");
    Assert.assertEquals(dict.insertionIndexOf("lynda\0"), -2);
    Assert.assertEquals(dict.insertionIndexOf("lynda\0\0"), -2);
  }

  /**
   * Tests loading default string column with empty ("") default null value.
   */
  @Test
  public void testDefaultEmptyValueStringColumn() throws Exception {
    Schema schema = constructV1Segment();
    schema.addField(new DimensionFieldSpec("SVString", FieldSpec.DataType.STRING, true, ""));
    schema.addField(new DimensionFieldSpec("MVString", FieldSpec.DataType.STRING, false, ""));

    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, _v1IndexLoadingConfig, schema);
    Assert.assertEquals(indexSegment.getDataSource("SVString").getDictionary().get(0), "");
    Assert.assertEquals(indexSegment.getDataSource("MVString").getDictionary().get(0), "");
    indexSegment.destroy();

    indexSegment = ImmutableSegmentLoader.load(_indexDir, _v3IndexLoadingConfig, schema);
    Assert.assertEquals(indexSegment.getDataSource("SVString").getDictionary().get(0), "");
    Assert.assertEquals(indexSegment.getDataSource("MVString").getDictionary().get(0), "");
    indexSegment.destroy();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
