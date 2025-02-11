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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.local.utils.SegmentAllIndexPreprocessThrottler;
import org.apache.pinot.segment.local.utils.SegmentPreprocessThrottler;
import org.apache.pinot.segment.local.utils.SegmentStarTreePreprocessThrottler;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.LUCENE_V912_FST_INDEX_FILE_EXTENSION;
import static org.testng.Assert.*;


public class LoaderTest {
  private static final File INDEX_DIR = new File(LoaderTest.class.getName());
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final String VECTOR_AVRO_DATA = "data/test_vector_data.avro";

  private static final String TEXT_INDEX_COL_NAME = "column5";
  private static final String FST_INDEX_COL_NAME = "column5";
  private static final String NO_FORWARD_INDEX_COL_NAME = "column4";

  private static final String VECTOR_INDEX_COL_NAME = "vector1";
  private static final int VECTOR_DIM_SIZE = 512;

  private static final SegmentPreprocessThrottler SEGMENT_PREPROCESS_THROTTLER =
      new SegmentPreprocessThrottler(new SegmentAllIndexPreprocessThrottler(1, 2, true),
          new SegmentStarTreePreprocessThrottler(1, 2, true));

  private File _avroFile;
  private File _vectorAvroFile;
  private IndexLoadingConfig _v1IndexLoadingConfig;
  private IndexLoadingConfig _v3IndexLoadingConfig;
  private File _indexDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resourceUrl);
    _avroFile = new File(resourceUrl.getFile());
    resourceUrl = getClass().getClassLoader().getResource(VECTOR_AVRO_DATA);
    assertNotNull(resourceUrl);
    _vectorAvroFile = new File(resourceUrl.getFile());

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setSegmentVersion("v1").build();
    Schema schema = createSchema();
    _v1IndexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setSegmentVersion("v3").build();
    _v3IndexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
  }

  private Schema createSchema()
      throws Exception {
    return SegmentTestUtils.extractSchemaFromAvroWithoutTime(_avroFile);
  }

  private Schema constructV1Segment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setSegmentVersion("v1").build();
    Schema schema = createSchema();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInputFilePath(_avroFile.getAbsolutePath());
    config.setOutDir(INDEX_DIR.getAbsolutePath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
    return schema;
  }

  @Test
  public void testLoad()
      throws Exception {
    constructV1Segment();
    assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v1);
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());

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
    assertTrue(v3TempDir.isDirectory());
    testConversion();
    assertFalse(v3TempDir.exists());
  }

  @Test
  public void testIfNeedConvertSegmentFormat()
      throws Exception {
    constructV1Segment();

    // The newly generated segment is consistent with table config and schema, thus
    // in follow checks, whether it needs reprocess or not depends on segment format.
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(_indexDir, ReadMode.mmap)) {
      // The segmentVersionToLoad is null, not leading to reprocess.
      assertFalse(ImmutableSegmentLoader.needPreprocess(segmentDirectory, new IndexLoadingConfig(), null));
      // The segmentVersionToLoad is v1, not leading to reprocess.
      assertFalse(ImmutableSegmentLoader.needPreprocess(segmentDirectory, _v1IndexLoadingConfig, null));
      // The segmentVersionToLoad is v3, leading to reprocess.
      assertTrue(ImmutableSegmentLoader.needPreprocess(segmentDirectory, _v3IndexLoadingConfig, null));
    }

    // The segment is in v3 format now, not leading to reprocess.
    ImmutableSegmentLoader.load(_indexDir, _v3IndexLoadingConfig, SEGMENT_PREPROCESS_THROTTLER);

    // Need to reset `segmentDirectory` to point to the correct index directory after the above load since the path
    // changes
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(_indexDir, ReadMode.mmap)) {
      assertFalse(ImmutableSegmentLoader.needPreprocess(segmentDirectory, _v3IndexLoadingConfig, null));
    }
  }

  private void testConversion()
      throws Exception {
    // Do not set segment version, should not convert the segment
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, ReadMode.mmap);
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    indexSegment.destroy();

    // Set segment version to v1, should not convert the segment
    indexSegment = ImmutableSegmentLoader.load(_indexDir, _v1IndexLoadingConfig, SEGMENT_PREPROCESS_THROTTLER);
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    indexSegment.destroy();

    // Set segment version to v3, should convert the segment to v3
    indexSegment = ImmutableSegmentLoader.load(_indexDir, _v3IndexLoadingConfig, SEGMENT_PREPROCESS_THROTTLER);
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    indexSegment.destroy();
  }

  @Test
  public void testBuiltInVirtualColumns()
      throws Exception {
    Schema schema = constructV1Segment();

    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, _v1IndexLoadingConfig, schema,
        SEGMENT_PREPROCESS_THROTTLER);
    testBuiltInVirtualColumns(indexSegment);
    indexSegment.destroy();

    indexSegment = ImmutableSegmentLoader.load(_indexDir, _v1IndexLoadingConfig, SEGMENT_PREPROCESS_THROTTLER);
    testBuiltInVirtualColumns(indexSegment);
    indexSegment.destroy();
  }

  private void testBuiltInVirtualColumns(IndexSegment indexSegment) {
    assertTrue(indexSegment.getColumnNames().containsAll(
        Arrays.asList(BuiltInVirtualColumn.DOCID, BuiltInVirtualColumn.HOSTNAME, BuiltInVirtualColumn.SEGMENTNAME)));
    assertNotNull(indexSegment.getDataSource(BuiltInVirtualColumn.DOCID));
    assertNotNull(indexSegment.getDataSource(BuiltInVirtualColumn.HOSTNAME));
    assertNotNull(indexSegment.getDataSource(BuiltInVirtualColumn.SEGMENTNAME));
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

    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, _v1IndexLoadingConfig, schema,
        SEGMENT_PREPROCESS_THROTTLER);
    assertEquals(indexSegment.getDataSource("SVString").getDictionary().get(0), "");
    assertEquals(indexSegment.getDataSource("MVString").getDictionary().get(0), "");
    indexSegment.destroy();

    indexSegment = ImmutableSegmentLoader.load(_indexDir, _v3IndexLoadingConfig, schema, SEGMENT_PREPROCESS_THROTTLER);
    assertEquals(indexSegment.getDataSource("SVString").getDictionary().get(0), "");
    assertEquals(indexSegment.getDataSource("MVString").getDictionary().get(0), "");
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
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, _v3IndexLoadingConfig, schema,
        SEGMENT_PREPROCESS_THROTTLER);
    assertEquals(BytesUtils.toHexString((byte[]) indexSegment.getDataSource(newColumnName).getDictionary().get(0)),
        defaultValue);
    indexSegment.destroy();
  }

  private void constructSegmentWithFSTIndex(SegmentVersion segmentVersion)
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    TableConfig tableConfig = createTableConfigWithFSTIndex(segmentVersion);
    Schema schema = createSchema();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInputFilePath(_avroFile.getAbsolutePath());
    config.setOutDir(INDEX_DIR.getAbsolutePath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
  }

  private TableConfig createTableConfigWithFSTIndex(@Nullable SegmentVersion segmentVersion) {
    FieldConfig fieldConfig =
        new FieldConfig(FST_INDEX_COL_NAME, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.FST),
            null, null);
    TableConfigBuilder builder =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setFieldConfigList(List.of(fieldConfig));
    if (segmentVersion != null) {
      builder.setSegmentVersion(segmentVersion.toString());
    }
    return builder.build();
  }

  @Test
  public void testFSTIndexLoad()
      throws Exception {
    constructSegmentWithFSTIndex(SegmentVersion.v3);
    assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v3);
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    verifyIndexDirIsV3(_indexDir);
    File fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    assertNull(fstIndexFile);

    TableConfig tableConfig = createTableConfigWithFSTIndex(null);
    Schema schema = createSchema();
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v3
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    indexSegment.destroy();
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(_indexDir, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertNotNull(reader);
      assertTrue(reader.hasIndexFor(FST_INDEX_COL_NAME, StandardIndexes.fst()));
    }

    // CASE 2: set the segment version to load in IndexLoadingConfig as V3
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on disk (V3)
    tableConfig = createTableConfigWithFSTIndex(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v3
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    indexSegment.destroy();
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(_indexDir, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertNotNull(reader);
      assertTrue(reader.hasIndexFor(FST_INDEX_COL_NAME, StandardIndexes.fst()));
    }

    // Test for scenarios by creating on-disk segment in V1 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V1
    // this generates the segment in V1 and does not convert to V3 as part of post-creation processing
    constructSegmentWithFSTIndex(SegmentVersion.v1);

    // check that segment on-disk version is V1 after creation
    assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v1);
    // check that segment v1 dir exists
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    // check that v3 index sub-dir does not exist
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    assertNotNull(fstIndexFile);
    assertFalse(fstIndexFile.isDirectory());
    assertEquals(fstIndexFile.getName(), FST_INDEX_COL_NAME + LUCENE_V912_FST_INDEX_FILE_EXTENSION);
    assertEquals(fstIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create fst index reader with on-disk version V1
    tableConfig = createTableConfigWithFSTIndex(null);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v1
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    assertNotNull(fstIndexFile);
    assertFalse(fstIndexFile.isDirectory());
    assertEquals(fstIndexFile.getName(), FST_INDEX_COL_NAME + LUCENE_V912_FST_INDEX_FILE_EXTENSION);
    assertEquals(fstIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig to V1
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on fisk
    tableConfig = createTableConfigWithFSTIndex(SegmentVersion.v1);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v1
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    assertNotNull(fstIndexFile);
    assertFalse(fstIndexFile.isDirectory());
    assertEquals(fstIndexFile.getName(), FST_INDEX_COL_NAME + LUCENE_V912_FST_INDEX_FILE_EXTENSION);
    assertEquals(fstIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 3: set the segment version to load in IndexLoadingConfig to V3
    // there should be conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is different than the version of segment on disk
    tableConfig = createTableConfigWithFSTIndex(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v3
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    // the index dir should exist in v3 format due to conversion
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    verifyIndexDirIsV3(_indexDir);
    fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    assertNull(fstIndexFile);
    indexSegment.destroy();
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(_indexDir, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertNotNull(reader);
      assertTrue(reader.hasIndexFor(FST_INDEX_COL_NAME, StandardIndexes.fst()));
    }
  }

  private void constructSegmentWithForwardIndexDisabled(SegmentVersion segmentVersion, boolean enableInvertedIndex)
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    TableConfig tableConfig = createTableConfigWithForwardIndexDisabled(segmentVersion, enableInvertedIndex);
    Schema schema = createSchema();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInputFilePath(_avroFile.getAbsolutePath());
    config.setOutDir(INDEX_DIR.getAbsolutePath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
  }

  private TableConfig createTableConfigWithForwardIndexDisabled(@Nullable SegmentVersion segmentVersion,
      boolean enableInvertedIndex) {
    FieldConfig fieldConfig =
        new FieldConfig(NO_FORWARD_INDEX_COL_NAME, FieldConfig.EncodingType.DICTIONARY, List.of(), null,
            Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true"));
    TableConfigBuilder builder =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setFieldConfigList(List.of(fieldConfig));
    if (segmentVersion != null) {
      builder.setSegmentVersion(segmentVersion.toString());
    }
    if (enableInvertedIndex) {
      builder.setInvertedIndexColumns(List.of(NO_FORWARD_INDEX_COL_NAME))
          .setCreateInvertedIndexDuringSegmentGeneration(true);
    }
    return builder.build();
  }

  @Test
  public void testForwardIndexDisabledLoad()
      throws Exception {
    // Tests for scenarios by creating on-disk segment in V3 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V3
    // this generates the segment in V1 but converts to V3 as part of post-creation processing
    constructSegmentWithForwardIndexDisabled(SegmentVersion.v3, true);

    // check that segment on-disk version is V3 after creation
    assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v3);
    // check that V3 index sub-dir exists
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create all index readers with on-disk version V3
    TableConfig tableConfig = createTableConfigWithForwardIndexDisabled(null, false);
    Schema schema = createSchema();
    ImmutableSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v3
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    assertNull(indexSegment.getForwardIndex(NO_FORWARD_INDEX_COL_NAME));
    assertTrue(indexSegment.getSegmentMetadata().getColumnMetadataFor(NO_FORWARD_INDEX_COL_NAME).hasDictionary());
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig as V3
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on disk (V3)
    tableConfig = createTableConfigWithForwardIndexDisabled(SegmentVersion.v3, false);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v3
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    assertNull(indexSegment.getForwardIndex(NO_FORWARD_INDEX_COL_NAME));
    assertTrue(indexSegment.getSegmentMetadata().getColumnMetadataFor(NO_FORWARD_INDEX_COL_NAME).hasDictionary());
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    indexSegment.destroy();

    // Test for scenarios by creating on-disk segment in V1 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V1
    // this generates the segment in V1 and does not convert to V3 as part of post-creation processing
    constructSegmentWithForwardIndexDisabled(SegmentVersion.v1, true);

    // check that segment on-disk version is V1 after creation
    assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v1);
    // check that segment v1 dir exists
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    // check that v3 index sub-dir does not exist
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create all index readers with on-disk version V1
    tableConfig = createTableConfigWithForwardIndexDisabled(null, false);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v1
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    assertNull(indexSegment.getForwardIndex(NO_FORWARD_INDEX_COL_NAME));
    assertTrue(indexSegment.getSegmentMetadata().getColumnMetadataFor(NO_FORWARD_INDEX_COL_NAME).hasDictionary());
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig to V1
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on fisk
    tableConfig = createTableConfigWithForwardIndexDisabled(SegmentVersion.v1, false);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v1
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    assertNull(indexSegment.getForwardIndex(NO_FORWARD_INDEX_COL_NAME));
    assertTrue(indexSegment.getSegmentMetadata().getColumnMetadataFor(NO_FORWARD_INDEX_COL_NAME).hasDictionary());
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    indexSegment.destroy();

    // CASE 3: set the segment version to load in IndexLoadingConfig to V3
    // there should be conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is different than the version of segment on disk
    tableConfig = createTableConfigWithForwardIndexDisabled(SegmentVersion.v3, false);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v3
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    assertNull(indexSegment.getForwardIndex(NO_FORWARD_INDEX_COL_NAME));
    assertTrue(indexSegment.getSegmentMetadata().getColumnMetadataFor(NO_FORWARD_INDEX_COL_NAME).hasDictionary());
    // the index dir should exist in v3 format due to conversion
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    verifyIndexDirIsV3(_indexDir);
    indexSegment.destroy();

    // Test scenarios to create a column with no forward index but without enabling inverted index for it
    // This should still work as the original constraint to enforce that a column has a dictionary + inverted index
    // has been relaxed.
    try {
      constructSegmentWithForwardIndexDisabled(SegmentVersion.v3, false);
    } catch (IllegalStateException e) {
      fail("Disabling forward index without enabling inverted index is allowed now");
    }

    try {
      constructSegmentWithForwardIndexDisabled(SegmentVersion.v1, false);
    } catch (IllegalStateException e) {
      fail("Disabling forward index without enabling inverted index is allowed now");
    }
  }

  private void constructSegmentWithTextIndex(SegmentVersion segmentVersion)
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    TableConfig tableConfig = createTableConfigWithTextIndex(segmentVersion);
    Schema schema = createSchema();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInputFilePath(_avroFile.getAbsolutePath());
    config.setOutDir(INDEX_DIR.getAbsolutePath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
  }

  private TableConfig createTableConfigWithTextIndex(@Nullable SegmentVersion segmentVersion) {
    FieldConfig fieldConfig =
        new FieldConfig(TEXT_INDEX_COL_NAME, FieldConfig.EncodingType.DICTIONARY, List.of(FieldConfig.IndexType.TEXT),
            null, null);
    TableConfigBuilder builder =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setFieldConfigList(List.of(fieldConfig));
    if (segmentVersion != null) {
      builder.setSegmentVersion(segmentVersion.toString());
    }
    return builder.build();
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
    assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v3);
    // check that V3 index sub-dir exists
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // check that text index exists under V3 subdir.
    File textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    assertNotNull(textIndexFile);
    assertTrue(textIndexFile.isDirectory());
    assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    assertEquals(textIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create text index reader with on-disk version V3
    TableConfig tableConfig = createTableConfigWithTextIndex(null);
    Schema schema = createSchema();
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v3
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // no change/conversion should have happened for textIndex dir
    textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    // segment load should have created the docID mapping file in V3 structure
    File textIndexDocIdMappingFile =
        SegmentDirectoryPaths.findTextIndexDocIdMappingFile(_indexDir, TEXT_INDEX_COL_NAME);
    assertNotNull(textIndexFile);
    assertNotNull(textIndexDocIdMappingFile);
    assertTrue(textIndexFile.isDirectory());
    assertFalse(textIndexDocIdMappingFile.isDirectory());
    assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    assertEquals(textIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    assertEquals(textIndexDocIdMappingFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig as V3
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on disk (V3)
    tableConfig = createTableConfigWithTextIndex(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v3
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // no change/conversion should have happened for textIndex dir
    textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    // segment load should have created the docID mapping file in V3 structure
    textIndexDocIdMappingFile = SegmentDirectoryPaths.findTextIndexDocIdMappingFile(_indexDir, TEXT_INDEX_COL_NAME);
    assertNotNull(textIndexFile);
    assertNotNull(textIndexDocIdMappingFile);
    assertTrue(textIndexFile.isDirectory());
    assertFalse(textIndexDocIdMappingFile.isDirectory());
    assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    assertEquals(textIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    assertEquals(textIndexDocIdMappingFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();

    // Test for scenarios by creating on-disk segment in V1 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V1
    // this generates the segment in V1 and does not convert to V3 as part of post-creation processing
    constructSegmentWithTextIndex(SegmentVersion.v1);

    // check that segment on-disk version is V1 after creation
    assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v1);
    // check that segment v1 dir exists
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    // check that v3 index sub-dir does not exist
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that text index exists directly under indexDir (V1). it should exist and should be a subdir
    textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    assertNotNull(textIndexFile);
    assertTrue(textIndexFile.isDirectory());
    assertFalse(textIndexDocIdMappingFile.isDirectory());
    assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    assertEquals(textIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create text index reader with on-disk version V1
    tableConfig = createTableConfigWithTextIndex(null);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v1
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // no change/conversion should have happened in text index Dir
    textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    // segment load should have created the docID mapping file in V1 structure
    textIndexDocIdMappingFile = SegmentDirectoryPaths.findTextIndexDocIdMappingFile(_indexDir, TEXT_INDEX_COL_NAME);
    assertNotNull(textIndexFile);
    assertNotNull(textIndexDocIdMappingFile);
    assertTrue(textIndexFile.isDirectory());
    assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    assertEquals(textIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    assertEquals(textIndexDocIdMappingFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig to V1
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on fisk
    tableConfig = createTableConfigWithTextIndex(SegmentVersion.v1);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v1
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // no change/conversion should have happened in text index Dir
    textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    // segment load should have created the docID mapping file in V1 structure
    textIndexDocIdMappingFile = SegmentDirectoryPaths.findTextIndexDocIdMappingFile(_indexDir, TEXT_INDEX_COL_NAME);
    assertNotNull(textIndexFile);
    assertNotNull(textIndexDocIdMappingFile);
    assertTrue(textIndexFile.isDirectory());
    assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    assertEquals(textIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    assertEquals(textIndexDocIdMappingFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 3: set the segment version to load in IndexLoadingConfig to V3
    // there should be conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is different than the version of segment on disk
    tableConfig = createTableConfigWithTextIndex(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v3
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    // the index dir should exist in v3 format due to conversion
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    verifyIndexDirIsV3(_indexDir);
    // check that text index exists under V3 subdir. It should exist and should be a subdir
    textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    // segment load should have created the docID mapping file in V3 structure
    textIndexDocIdMappingFile = SegmentDirectoryPaths.findTextIndexDocIdMappingFile(_indexDir, TEXT_INDEX_COL_NAME);
    assertNotNull(textIndexFile);
    assertNotNull(textIndexDocIdMappingFile);
    assertTrue(textIndexFile.isDirectory());
    assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    assertEquals(textIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    assertEquals(textIndexDocIdMappingFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();
  }

  private void constructSegmentWithVectorIndex(SegmentVersion segmentVersion)
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    TableConfig tableConfig = createTableConfigWithVectorIndex(segmentVersion);
    Schema schema = createVectorSchema();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInputFilePath(_vectorAvroFile.getAbsolutePath());
    config.setOutDir(INDEX_DIR.getAbsolutePath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
  }

  private TableConfig createTableConfigWithVectorIndex(SegmentVersion segmentVersion) {
    FieldConfig fieldConfig =
        new FieldConfig(VECTOR_INDEX_COL_NAME, FieldConfig.EncodingType.RAW, List.of(FieldConfig.IndexType.VECTOR),
            null, Map.of("vectorDimension", Integer.toString(VECTOR_DIM_SIZE), "vectorIndexType", "HNSW"));
    TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of(VECTOR_INDEX_COL_NAME)).setFieldConfigList(List.of(fieldConfig));
    if (segmentVersion != null) {
      builder.setSegmentVersion(segmentVersion.toString());
    }
    return builder.build();
  }

  private Schema createVectorSchema()
      throws Exception {
    return SegmentTestUtils.extractSchemaFromAvroWithoutTime(_vectorAvroFile);
  }

  @Test
  public void testVectorIndexLoad()
      throws Exception {
    // Tests for scenarios by creating on-disk segment in V3 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V3
    // this generates the segment in V1 but converts to V3 as part of post-creation processing
    constructSegmentWithVectorIndex(SegmentVersion.v3);

    // check that segment on-disk version is V3 after creation
    assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v3);
    // check that V3 index sub-dir exists
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // check that vector index exists under V3 subdir.
    File vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    assertNotNull(vectorIndexFile);
    assertTrue(vectorIndexFile.isDirectory());
    assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    assertEquals(vectorIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create vector index reader with on-disk version V3
    TableConfig tableConfig = createTableConfigWithVectorIndex(null);
    Schema schema = createVectorSchema();
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v3
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // no change/conversion should have happened for vectorIndex dir
    vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    assertNotNull(vectorIndexFile);
    assertTrue(vectorIndexFile.isDirectory());
    assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    assertEquals(vectorIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig as V3
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on disk (V3)
    tableConfig = createTableConfigWithVectorIndex(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v3
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // no change/conversion should have happened for vectorIndex dir
    vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    assertNotNull(vectorIndexFile);
    assertTrue(vectorIndexFile.isDirectory());
    assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    assertEquals(vectorIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();

    // Test for scenarios by creating on-disk segment in V1 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V1
    // this generates the segment in V1 and does not convert to V3 as part of post-creation processing
    constructSegmentWithVectorIndex(SegmentVersion.v1);

    // check that segment on-disk version is V1 after creation
    assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v1);
    // check that segment v1 dir exists
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    // check that v3 index sub-dir does not exist
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that vector index exists directly under indexDir (V1). it should exist and should be a subdir
    vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    assertNotNull(vectorIndexFile);
    assertTrue(vectorIndexFile.isDirectory());
    assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    assertEquals(vectorIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create vector index reader with on-disk version V1
    tableConfig = createTableConfigWithVectorIndex(null);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v1
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // no change/conversion should have happened in vector index Dir
    vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    assertNotNull(vectorIndexFile);
    assertTrue(vectorIndexFile.isDirectory());
    assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    assertEquals(vectorIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig to V1
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on fisk
    tableConfig = createTableConfigWithVectorIndex(SegmentVersion.v1);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v1
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    // no change/conversion should have happened in indexDir
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // no change/conversion should have happened in vector index Dir
    vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    assertNotNull(vectorIndexFile);
    assertTrue(vectorIndexFile.isDirectory());
    assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    assertEquals(vectorIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 3: set the segment version to load in IndexLoadingConfig to V3
    // there should be conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is different than the version of segment on disk
    tableConfig = createTableConfigWithVectorIndex(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, new IndexLoadingConfig(tableConfig, schema),
        SEGMENT_PREPROCESS_THROTTLER);
    // check that loaded segment version is v3
    assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    // the index dir should exist in v3 format due to conversion
    assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    verifyIndexDirIsV3(_indexDir);
    // check that vector index exists under V3 subdir. It should exist and should be a subdir
    vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    assertNotNull(vectorIndexFile);
    assertTrue(vectorIndexFile.isDirectory());
    assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    assertEquals(vectorIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();
  }

  private void verifyIndexDirIsV3(File indexDir) {
    File[] files = indexDir.listFiles();
    assertNotNull(files);
    assertEquals(files.length, 1);
    assertEquals(files[0].getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
