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
package org.apache.pinot.segment.local.segment.index.loader;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.local.segment.creator.impl.V1Constants;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
import org.apache.pinot.segment.local.segment.store.ColumnIndexType;
import org.apache.pinot.segment.local.segment.store.SegmentDirectory;
import org.apache.pinot.segment.local.segment.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.apache.pinot.segment.local.segment.creator.impl.V1Constants.Indexes.FST_INDEX_FILE_EXTENSION;


public class LoaderTest {
  private static final File INDEX_DIR = new File(LoaderTest.class.getName());
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final String PADDING_OLD = "data/paddingOld.tar.gz";
  private static final String PADDING_PERCENT = "data/paddingPercent.tar.gz";
  private static final String PADDING_NULL = "data/paddingNull.tar.gz";

  private static final String TEXT_INDEX_COL_NAME = "column5";
  private static final String FST_INDEX_COL_NAME = "column5";

  private File _avroFile;
  private File _indexDir;
  private IndexLoadingConfig _v1IndexLoadingConfig;
  private IndexLoadingConfig _v3IndexLoadingConfig;

  @BeforeClass
  public void setUp()
      throws Exception {
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

  private Schema constructV1Segment()
      throws Exception {
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
  public void testLoad()
      throws Exception {
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
  public void testLoadWithStaleConversionDir()
      throws Exception {
    constructV1Segment();

    File v3TempDir = new SegmentV1V2ToV3FormatConverter().v3ConversionTempDirectory(_indexDir);
    Assert.assertTrue(v3TempDir.isDirectory());
    testConversion();
    Assert.assertFalse(v3TempDir.exists());
  }

  private void testConversion()
      throws Exception {
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
  public void testBuiltInVirtualColumns()
      throws Exception {
    Schema schema = constructV1Segment();

    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, _v1IndexLoadingConfig, schema);
    testBuiltInVirtualColumns(indexSegment);
    indexSegment.destroy();

    indexSegment = ImmutableSegmentLoader.load(_indexDir, _v1IndexLoadingConfig, null);
    testBuiltInVirtualColumns(indexSegment);
    indexSegment.destroy();
  }

  private void testBuiltInVirtualColumns(IndexSegment indexSegment) {
    Assert.assertTrue(indexSegment.getColumnNames().containsAll(
        Arrays.asList(BuiltInVirtualColumn.DOCID, BuiltInVirtualColumn.HOSTNAME, BuiltInVirtualColumn.SEGMENTNAME)));
    Assert.assertNotNull(indexSegment.getDataSource(BuiltInVirtualColumn.DOCID));
    Assert.assertNotNull(indexSegment.getDataSource(BuiltInVirtualColumn.HOSTNAME));
    Assert.assertNotNull(indexSegment.getDataSource(BuiltInVirtualColumn.SEGMENTNAME));
  }

  @Test
  public void testPadding()
      throws Exception {
    // Old Format
    URL resourceUrl = LoaderTest.class.getClassLoader().getResource(PADDING_OLD);
    Assert.assertNotNull(resourceUrl);
    File segmentDirectory =
        TarGzCompressionUtils.untar(new File(TestUtils.getFileFromResourceUrl(resourceUrl)), INDEX_DIR).get(0);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDirectory);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor("name");
    Assert.assertEquals(columnMetadata.getPaddingCharacter(), V1Constants.Str.LEGACY_STRING_PAD_CHAR);
    SegmentDirectory segmentDir = SegmentDirectory.createFromLocalFS(segmentDirectory, segmentMetadata, ReadMode.heap);
    SegmentDirectory.Reader reader = segmentDir.createReader();
    PinotDataBuffer dictionaryBuffer = reader.getIndexFor("name", ColumnIndexType.DICTIONARY);
    StringDictionary dict =
        new StringDictionary(dictionaryBuffer, columnMetadata.getCardinality(), columnMetadata.getColumnMaxLength(),
            (byte) columnMetadata.getPaddingCharacter());
    Assert.assertEquals(dict.getStringValue(0), "lynda 2.0");
    Assert.assertEquals(dict.getStringValue(1), "lynda");
    Assert.assertEquals(dict.get(0), "lynda 2.0");
    Assert.assertEquals(dict.get(1), "lynda");
    Assert.assertEquals(dict.indexOf("lynda%"), 1);
    Assert.assertEquals(dict.indexOf("lynda%%"), 1);

    // New Format Padding character %
    resourceUrl = LoaderTest.class.getClassLoader().getResource(PADDING_PERCENT);
    Assert.assertNotNull(resourceUrl);
    segmentDirectory =
        TarGzCompressionUtils.untar(new File(TestUtils.getFileFromResourceUrl(resourceUrl)), INDEX_DIR).get(0);
    segmentMetadata = new SegmentMetadataImpl(segmentDirectory);
    columnMetadata = segmentMetadata.getColumnMetadataFor("name");
    Assert.assertEquals(columnMetadata.getPaddingCharacter(), V1Constants.Str.LEGACY_STRING_PAD_CHAR);
    segmentDir = SegmentDirectory.createFromLocalFS(segmentDirectory, segmentMetadata, ReadMode.heap);
    reader = segmentDir.createReader();
    dictionaryBuffer = reader.getIndexFor("name", ColumnIndexType.DICTIONARY);
    dict = new StringDictionary(dictionaryBuffer, columnMetadata.getCardinality(), columnMetadata.getColumnMaxLength(),
        (byte) columnMetadata.getPaddingCharacter());
    Assert.assertEquals(dict.getStringValue(0), "lynda 2.0");
    Assert.assertEquals(dict.getStringValue(1), "lynda");
    Assert.assertEquals(dict.get(0), "lynda 2.0");
    Assert.assertEquals(dict.get(1), "lynda");
    Assert.assertEquals(dict.indexOf("lynda%"), 1);
    Assert.assertEquals(dict.indexOf("lynda%%"), 1);

    // New Format Padding character Null
    resourceUrl = LoaderTest.class.getClassLoader().getResource(PADDING_NULL);
    Assert.assertNotNull(resourceUrl);
    segmentDirectory =
        TarGzCompressionUtils.untar(new File(TestUtils.getFileFromResourceUrl(resourceUrl)), INDEX_DIR).get(0);
    segmentMetadata = new SegmentMetadataImpl(segmentDirectory);
    columnMetadata = segmentMetadata.getColumnMetadataFor("name");
    Assert.assertEquals(columnMetadata.getPaddingCharacter(), V1Constants.Str.DEFAULT_STRING_PAD_CHAR);
    segmentDir = SegmentDirectory.createFromLocalFS(segmentDirectory, segmentMetadata, ReadMode.heap);
    reader = segmentDir.createReader();
    dictionaryBuffer = reader.getIndexFor("name", ColumnIndexType.DICTIONARY);
    dict = new StringDictionary(dictionaryBuffer, columnMetadata.getCardinality(), columnMetadata.getColumnMaxLength(),
        (byte) columnMetadata.getPaddingCharacter());
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
  public void testDefaultEmptyValueStringColumn()
      throws Exception {
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

  @Test
  public void testDefaultBytesColumn()
      throws Exception {
    Schema schema = constructV1Segment();
    String newColumnName = "byteMetric";
    String defaultValue = "0000ac0000";

    FieldSpec byteMetric = new MetricFieldSpec(newColumnName, FieldSpec.DataType.BYTES, defaultValue);
    schema.addField(byteMetric);
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, _v3IndexLoadingConfig, schema);
    Assert
        .assertEquals(BytesUtils.toHexString((byte[]) indexSegment.getDataSource(newColumnName).getDictionary().get(0)),
            defaultValue);
    indexSegment.destroy();
  }

  private void constructSegmentWithFSTIndex(SegmentVersion segmentVersion)
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, "testTable");
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    List<String> fstIndexCreationColumns = Lists.newArrayList(FST_INDEX_COL_NAME);
    segmentGeneratorConfig.setFSTIndexCreationColumns(fstIndexCreationColumns);
    segmentGeneratorConfig.setSegmentVersion(segmentVersion);
    driver.init(segmentGeneratorConfig);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
  }

  @Test
  public void testFSTIndexLoad()
      throws Exception {
    constructSegmentWithFSTIndex(SegmentVersion.v3);
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getSegmentVersion(), SegmentVersion.v3);
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    verifyIndexDirIsV3(_indexDir);
    File fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    Assert.assertNull(fstIndexFile);

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setFSTIndexColumns(new HashSet<>(Arrays.asList(FST_INDEX_COL_NAME)));
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3.toString());
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);

    SegmentDirectory segmentDir = SegmentDirectory.createFromLocalFS(_indexDir, ReadMode.heap);
    SegmentDirectory.Reader reader = segmentDir.createReader();
    Assert.assertNotNull(reader);
    Assert.assertTrue(reader.hasIndexFor(FST_INDEX_COL_NAME, ColumnIndexType.FST_INDEX));
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig as V3
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on disk (V3)
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3.toString());
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    segmentDir = SegmentDirectory.createFromLocalFS(_indexDir, ReadMode.heap);
    reader = segmentDir.createReader();
    Assert.assertNotNull(reader);
    Assert.assertTrue(reader.hasIndexFor(FST_INDEX_COL_NAME, ColumnIndexType.FST_INDEX));
    indexSegment.destroy();

    // Test for scenarios by creating on-disk segment in V1 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V1
    // this generates the segment in V1 and does not convert to V3 as part of post-creation processing
    constructSegmentWithFSTIndex(SegmentVersion.v1);

    // check that segment on-disk version is V1 after creation
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getSegmentVersion(), SegmentVersion.v1);
    // check that segment v1 dir exists
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    // check that v3 index sub-dir does not exist
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    Assert.assertNotNull(fstIndexFile);
    Assert.assertFalse(fstIndexFile.isDirectory());
    Assert.assertEquals(fstIndexFile.getName(), FST_INDEX_COL_NAME + FST_INDEX_FILE_EXTENSION);
    Assert.assertEquals(fstIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create fst index reader with on-disk version V1
    indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setFSTIndexColumns(new HashSet<>(Arrays.asList(FST_INDEX_COL_NAME)));
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v1
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1.toString());
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    Assert.assertNotNull(fstIndexFile);
    Assert.assertFalse(fstIndexFile.isDirectory());
    Assert.assertEquals(fstIndexFile.getName(), FST_INDEX_COL_NAME + FST_INDEX_FILE_EXTENSION);
    Assert.assertEquals(fstIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig to V1
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on fisk
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v1);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v1
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1.toString());
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    Assert.assertNotNull(fstIndexFile);
    Assert.assertFalse(fstIndexFile.isDirectory());
    Assert.assertEquals(fstIndexFile.getName(), FST_INDEX_COL_NAME + FST_INDEX_FILE_EXTENSION);
    Assert.assertEquals(fstIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 3: set the segment version to load in IndexLoadingConfig to V3
    // there should be conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is different than the version of segment on disk
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3.toString());
    // the index dir should exist in v3 format due to conversion
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    verifyIndexDirIsV3(_indexDir);
    fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    Assert.assertNull(fstIndexFile);
    segmentDir = SegmentDirectory.createFromLocalFS(_indexDir, ReadMode.heap);
    reader = segmentDir.createReader();
    Assert.assertNotNull(reader);
    Assert.assertTrue(reader.hasIndexFor(FST_INDEX_COL_NAME, ColumnIndexType.FST_INDEX));
    indexSegment.destroy();
  }

  private void constructSegmentWithTextIndex(SegmentVersion segmentVersion)
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, "testTable");
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    List<String> textIndexCreationColumns = Lists.newArrayList(TEXT_INDEX_COL_NAME);
    List<String> rawIndexCreationColumns = Lists.newArrayList(TEXT_INDEX_COL_NAME);
    segmentGeneratorConfig.setTextIndexCreationColumns(textIndexCreationColumns);
    segmentGeneratorConfig.setRawIndexCreationColumns(rawIndexCreationColumns);
    segmentGeneratorConfig.setSegmentVersion(segmentVersion);
    driver.init(segmentGeneratorConfig);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
  }

  @Test
  public void testTextIndexLoad()
      throws Exception {
    // Tests for scenarios by creating on-disk segment in V3 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V3
    // this generates the segment in V1 but converts to V3 as part of post-creation processing
    constructSegmentWithTextIndex(SegmentVersion.v3);

    // check that segment on-disk version is V3 after creation
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getSegmentVersion(), SegmentVersion.v3);
    // check that V3 index sub-dir exists
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // check that text index exists under V3 subdir.
    File textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    Assert.assertNotNull(textIndexFile);
    Assert.assertTrue(textIndexFile.isDirectory());
    Assert.assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + LuceneTextIndexCreator.LUCENE_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create text index reader with on-disk version V3
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setTextIndexColumns(new HashSet<>(Arrays.asList(TEXT_INDEX_COL_NAME)));
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3.toString());
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // no change/conversion should have happened for textIndex dir
    textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    // segment load should have created the docID mapping file in V3 structure
    File textIndexDocIdMappingFile =
        SegmentDirectoryPaths.findTextIndexDocIdMappingFile(_indexDir, TEXT_INDEX_COL_NAME);
    Assert.assertNotNull(textIndexFile);
    Assert.assertNotNull(textIndexDocIdMappingFile);
    Assert.assertTrue(textIndexFile.isDirectory());
    Assert.assertFalse(textIndexDocIdMappingFile.isDirectory());
    Assert.assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + LuceneTextIndexCreator.LUCENE_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    Assert.assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + LuceneTextIndexReader.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    Assert
        .assertEquals(textIndexDocIdMappingFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig as V3
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on disk (V3)
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3.toString());
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // no change/conversion should have happened for textIndex dir
    textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    // segment load should have created the docID mapping file in V3 structure
    textIndexDocIdMappingFile = SegmentDirectoryPaths.findTextIndexDocIdMappingFile(_indexDir, TEXT_INDEX_COL_NAME);
    Assert.assertNotNull(textIndexFile);
    Assert.assertNotNull(textIndexDocIdMappingFile);
    Assert.assertTrue(textIndexFile.isDirectory());
    Assert.assertFalse(textIndexDocIdMappingFile.isDirectory());
    Assert.assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + LuceneTextIndexCreator.LUCENE_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    Assert.assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + LuceneTextIndexReader.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    Assert
        .assertEquals(textIndexDocIdMappingFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();

    // Test for scenarios by creating on-disk segment in V1 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V1
    // this generates the segment in V1 and does not convert to V3 as part of post-creation processing
    constructSegmentWithTextIndex(SegmentVersion.v1);

    // check that segment on-disk version is V1 after creation
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getSegmentVersion(), SegmentVersion.v1);
    // check that segment v1 dir exists
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    // check that v3 index sub-dir does not exist
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that text index exists directly under indexDir (V1). it should exist and should be a subdir
    textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    Assert.assertNotNull(textIndexFile);
    Assert.assertTrue(textIndexFile.isDirectory());
    Assert.assertFalse(textIndexDocIdMappingFile.isDirectory());
    Assert.assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + LuceneTextIndexCreator.LUCENE_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create text index reader with on-disk version V1
    indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setTextIndexColumns(new HashSet<>(Arrays.asList(TEXT_INDEX_COL_NAME)));
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v1
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1.toString());
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // no change/conversion should have happened in text index Dir
    textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    // segment load should have created the docID mapping file in V1 structure
    textIndexDocIdMappingFile = SegmentDirectoryPaths.findTextIndexDocIdMappingFile(_indexDir, TEXT_INDEX_COL_NAME);
    Assert.assertNotNull(textIndexFile);
    Assert.assertNotNull(textIndexDocIdMappingFile);
    Assert.assertTrue(textIndexFile.isDirectory());
    Assert.assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + LuceneTextIndexCreator.LUCENE_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    Assert.assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + LuceneTextIndexReader.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    Assert.assertEquals(textIndexDocIdMappingFile.getParentFile().getName(),
        new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig to V1
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on fisk
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v1);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v1
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1.toString());
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // no change/conversion should have happened in text index Dir
    textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    // segment load should have created the docID mapping file in V1 structure
    textIndexDocIdMappingFile = SegmentDirectoryPaths.findTextIndexDocIdMappingFile(_indexDir, TEXT_INDEX_COL_NAME);
    Assert.assertNotNull(textIndexFile);
    Assert.assertNotNull(textIndexDocIdMappingFile);
    Assert.assertTrue(textIndexFile.isDirectory());
    Assert.assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + LuceneTextIndexCreator.LUCENE_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    Assert.assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + LuceneTextIndexReader.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    Assert.assertEquals(textIndexDocIdMappingFile.getParentFile().getName(),
        new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 3: set the segment version to load in IndexLoadingConfig to V3
    // there should be conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is different than the version of segment on disk
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3.toString());
    // the index dir should exist in v3 format due to conversion
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    verifyIndexDirIsV3(_indexDir);
    // check that text index exists under V3 subdir. It should exist and should be a subdir
    textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    // segment load should have created the docID mapping file in V3 structure
    textIndexDocIdMappingFile = SegmentDirectoryPaths.findTextIndexDocIdMappingFile(_indexDir, TEXT_INDEX_COL_NAME);
    Assert.assertNotNull(textIndexFile);
    Assert.assertNotNull(textIndexDocIdMappingFile);
    Assert.assertTrue(textIndexFile.isDirectory());
    Assert.assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + LuceneTextIndexCreator.LUCENE_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    Assert.assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + LuceneTextIndexReader.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    Assert
        .assertEquals(textIndexDocIdMappingFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();
  }

  private void verifyIndexDirIsV3(File indexDir) {
    File[] files = indexDir.listFiles();
    Assert.assertEquals(files.length, 1);
    Assert.assertEquals(files[0].getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
