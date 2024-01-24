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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.FstIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.ReadMode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.LUCENE_V9_FST_INDEX_FILE_EXTENSION;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class LoaderTest {
  private static final File INDEX_DIR = new File(LoaderTest.class.getName());
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final String VECTOR_AVRO_DATA = "data/test_vector_data.avro";

  private static final String TEXT_INDEX_COL_NAME = "column5";
  private static final String FST_INDEX_COL_NAME = "column5";
  private static final String NO_FORWARD_INDEX_COL_NAME = "column4";

  private static final String VECTOR_INDEX_COL_NAME = "vector1";
  private static final int VECTOR_DIM_SIZE = 512;

  private File _avroFile;

  private File _vectorAvroFile;
  private File _indexDir;
  private IndexLoadingConfig _v1IndexLoadingConfig;
  private IndexLoadingConfig _v3IndexLoadingConfig;
  private SegmentDirectoryLoader _localSegmentDirectoryLoader;
  private PinotConfiguration _pinotConfiguration;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_DATA);
    Assert.assertNotNull(resourceUrl);
    _avroFile = new File(resourceUrl.getFile());
    _vectorAvroFile = new File(getClass().getClassLoader().getResource(VECTOR_AVRO_DATA).getFile());
    Map<String, Object> props = new HashMap<>();
    props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.heap.toString());
    _pinotConfiguration = new PinotConfiguration(props);

    _v1IndexLoadingConfig = new IndexLoadingConfig();
    _v1IndexLoadingConfig.setReadMode(ReadMode.mmap);
    _v1IndexLoadingConfig.setSegmentVersion(SegmentVersion.v1);
    _v1IndexLoadingConfig.setVectorIndexConfigs(new HashMap<>());

    _v3IndexLoadingConfig = new IndexLoadingConfig();
    _v3IndexLoadingConfig.setReadMode(ReadMode.mmap);
    _v3IndexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    _v3IndexLoadingConfig.setVectorIndexConfigs(new HashMap<>());

    _localSegmentDirectoryLoader = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader();
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
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v1);
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

  @Test
  public void testIfNeedConvertSegmentFormat()
      throws Exception {
    constructV1Segment();

    // The newly generated segment is consistent with table config and schema, thus
    // in follow checks, whether it needs reprocess or not depends on segment format.
    SegmentDirectory segmentDir = _localSegmentDirectoryLoader.load(_indexDir.toURI(),
        new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_pinotConfiguration).build());

    // The segmentVersionToLoad is null, not leading to reprocess.
    assertFalse(ImmutableSegmentLoader.needPreprocess(segmentDir, new IndexLoadingConfig(), null));

    // The segmentVersionToLoad is v1, not leading to reprocess.
    assertFalse(ImmutableSegmentLoader.needPreprocess(segmentDir, _v1IndexLoadingConfig, null));

    // The segmentVersionToLoad is v3, leading to reprocess.
    assertTrue(ImmutableSegmentLoader.needPreprocess(segmentDir, _v3IndexLoadingConfig, null));

    // The segment is in v3 format now, not leading to reprocess.
    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(_indexDir, _v3IndexLoadingConfig);
    // Need to reset `segmentDir` to point to the correct index directory after the above load since the path changes
    segmentDir = _localSegmentDirectoryLoader.load(immutableSegment.getSegmentMetadata().getIndexDir().toURI(),
        new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_pinotConfiguration).build());
    segmentDir.reloadMetadata();
    assertFalse(ImmutableSegmentLoader.needPreprocess(segmentDir, _v3IndexLoadingConfig, null));
  }

  private void testConversion()
      throws Exception {
    // Do not set segment version, should not convert the segment
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, ReadMode.mmap);
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    indexSegment.destroy();

    // Set segment version to v1, should not convert the segment
    indexSegment = ImmutableSegmentLoader.load(_indexDir, _v1IndexLoadingConfig);
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    indexSegment.destroy();

    // Set segment version to v3, should convert the segment to v3
    indexSegment = ImmutableSegmentLoader.load(_indexDir, _v3IndexLoadingConfig);
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
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
    Assert.assertEquals(
        BytesUtils.toHexString((byte[]) indexSegment.getDataSource(newColumnName).getDictionary().get(0)),
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
    FstIndexConfig fstConfig = new FstIndexConfig(segmentGeneratorConfig.getFSTIndexType());
    segmentGeneratorConfig.setIndexOn(StandardIndexes.fst(), fstConfig, fstIndexCreationColumns);
    segmentGeneratorConfig.setSegmentVersion(segmentVersion);
    driver.init(segmentGeneratorConfig);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
  }

  @Test
  public void testFSTIndexLoad()
      throws Exception {
    constructSegmentWithFSTIndex(SegmentVersion.v3);
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v3);
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    verifyIndexDirIsV3(_indexDir);
    File fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    Assert.assertNull(fstIndexFile);

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setFSTIndexColumns(new HashSet<>(Arrays.asList(FST_INDEX_COL_NAME)));
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);

    SegmentDirectory segmentDir = _localSegmentDirectoryLoader.load(_indexDir.toURI(),
        new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_pinotConfiguration).build());
    SegmentDirectory.Reader reader = segmentDir.createReader();
    Assert.assertNotNull(reader);
    Assert.assertTrue(reader.hasIndexFor(FST_INDEX_COL_NAME, StandardIndexes.fst()));
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig as V3
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on disk (V3)
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    segmentDir = _localSegmentDirectoryLoader.load(_indexDir.toURI(),
        new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_pinotConfiguration).build());
    reader = segmentDir.createReader();
    Assert.assertNotNull(reader);
    Assert.assertTrue(reader.hasIndexFor(FST_INDEX_COL_NAME, StandardIndexes.fst()));
    indexSegment.destroy();

    // Test for scenarios by creating on-disk segment in V1 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V1
    // this generates the segment in V1 and does not convert to V3 as part of post-creation processing
    constructSegmentWithFSTIndex(SegmentVersion.v1);

    // check that segment on-disk version is V1 after creation
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v1);
    // check that segment v1 dir exists
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    // check that v3 index sub-dir does not exist
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    Assert.assertNotNull(fstIndexFile);
    Assert.assertFalse(fstIndexFile.isDirectory());
    Assert.assertEquals(fstIndexFile.getName(), FST_INDEX_COL_NAME + LUCENE_V9_FST_INDEX_FILE_EXTENSION);
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
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    Assert.assertNotNull(fstIndexFile);
    Assert.assertFalse(fstIndexFile.isDirectory());
    Assert.assertEquals(fstIndexFile.getName(), FST_INDEX_COL_NAME + LUCENE_V9_FST_INDEX_FILE_EXTENSION);
    Assert.assertEquals(fstIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig to V1
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on fisk
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v1);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v1
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    Assert.assertNotNull(fstIndexFile);
    Assert.assertFalse(fstIndexFile.isDirectory());
    Assert.assertEquals(fstIndexFile.getName(), FST_INDEX_COL_NAME + LUCENE_V9_FST_INDEX_FILE_EXTENSION);
    Assert.assertEquals(fstIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 3: set the segment version to load in IndexLoadingConfig to V3
    // there should be conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is different than the version of segment on disk
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    // the index dir should exist in v3 format due to conversion
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    verifyIndexDirIsV3(_indexDir);
    fstIndexFile = SegmentDirectoryPaths.findFSTIndexIndexFile(_indexDir, FST_INDEX_COL_NAME);
    Assert.assertNull(fstIndexFile);
    segmentDir = _localSegmentDirectoryLoader.load(_indexDir.toURI(),
        new SegmentDirectoryLoaderContext.Builder().setSegmentDirectoryConfigs(_pinotConfiguration).build());
    reader = segmentDir.createReader();
    Assert.assertNotNull(reader);
    Assert.assertTrue(reader.hasIndexFor(FST_INDEX_COL_NAME, StandardIndexes.fst()));
    indexSegment.destroy();
  }

  private void constructSegmentWithForwardIndexDisabled(SegmentVersion segmentVersion, boolean enableInvertedIndex)
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, "testTable");
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    segmentGeneratorConfig.setIndexOn(StandardIndexes.forward(), ForwardIndexConfig.DISABLED,
        NO_FORWARD_INDEX_COL_NAME);
    if (enableInvertedIndex) {
      segmentGeneratorConfig.setIndexOn(StandardIndexes.inverted(), IndexConfig.ENABLED, NO_FORWARD_INDEX_COL_NAME);
    }
    segmentGeneratorConfig.setSegmentVersion(segmentVersion);
    driver.init(segmentGeneratorConfig);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
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
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v3);
    // check that V3 index sub-dir exists
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create all index readers with on-disk version V3
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();

    Set<String> forwardIndexDisabledColumns = new HashSet<>();
    forwardIndexDisabledColumns.add(NO_FORWARD_INDEX_COL_NAME);
    indexLoadingConfig.setForwardIndexDisabledColumns(forwardIndexDisabledColumns);
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    ImmutableSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    Assert.assertNull(indexSegment.getForwardIndex(NO_FORWARD_INDEX_COL_NAME));
    Assert.assertTrue(indexSegment.getSegmentMetadata().getColumnMetadataFor(NO_FORWARD_INDEX_COL_NAME)
        .hasDictionary());
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig as V3
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on disk (V3)
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    Assert.assertNull(indexSegment.getForwardIndex(NO_FORWARD_INDEX_COL_NAME));
    Assert.assertTrue(indexSegment.getSegmentMetadata().getColumnMetadataFor(NO_FORWARD_INDEX_COL_NAME)
        .hasDictionary());
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    indexSegment.destroy();

    // Test for scenarios by creating on-disk segment in V1 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V1
    // this generates the segment in V1 and does not convert to V3 as part of post-creation processing
    constructSegmentWithForwardIndexDisabled(SegmentVersion.v1, true);

    // check that segment on-disk version is V1 after creation
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v1);
    // check that segment v1 dir exists
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    // check that v3 index sub-dir does not exist
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create all index readers with on-disk version V1
    indexLoadingConfig = new IndexLoadingConfig();
    forwardIndexDisabledColumns = new HashSet<>();
    forwardIndexDisabledColumns.add(NO_FORWARD_INDEX_COL_NAME);
    indexLoadingConfig.setForwardIndexDisabledColumns(forwardIndexDisabledColumns);
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v1
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    Assert.assertNull(indexSegment.getForwardIndex(NO_FORWARD_INDEX_COL_NAME));
    Assert.assertTrue(indexSegment.getSegmentMetadata().getColumnMetadataFor(NO_FORWARD_INDEX_COL_NAME)
        .hasDictionary());
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig to V1
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on fisk
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v1);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v1
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    Assert.assertNull(indexSegment.getForwardIndex(NO_FORWARD_INDEX_COL_NAME));
    Assert.assertTrue(indexSegment.getSegmentMetadata().getColumnMetadataFor(NO_FORWARD_INDEX_COL_NAME)
        .hasDictionary());
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    indexSegment.destroy();

    // CASE 3: set the segment version to load in IndexLoadingConfig to V3
    // there should be conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is different than the version of segment on disk
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    Assert.assertNull(indexSegment.getForwardIndex(NO_FORWARD_INDEX_COL_NAME));
    Assert.assertTrue(indexSegment.getSegmentMetadata().getColumnMetadataFor(NO_FORWARD_INDEX_COL_NAME)
        .hasDictionary());
    // the index dir should exist in v3 format due to conversion
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    verifyIndexDirIsV3(_indexDir);
    indexSegment.destroy();

    // Test scenarios to create a column with no forward index but without enabling inverted index for it
    // This should still work as the original constraint to enforce that a column has a dictionary + inverted index
    // has been relaxed.
    try {
      constructSegmentWithForwardIndexDisabled(SegmentVersion.v3, false);
    } catch (IllegalStateException e) {
      Assert.fail("Disabling forward index without enabling inverted index is allowed now");
    }

    try {
      constructSegmentWithForwardIndexDisabled(SegmentVersion.v1, false);
    } catch (IllegalStateException e) {
      Assert.fail("Disabling forward index without enabling inverted index is allowed now");
    }
  }

  private void constructSegmentWithTextIndex(SegmentVersion segmentVersion)
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, "testTable");
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    List<String> rawIndexCreationColumns = Lists.newArrayList(TEXT_INDEX_COL_NAME);
    TextIndexConfig textIndexConfig = new TextIndexConfigBuilder(segmentGeneratorConfig.getFSTIndexType()).build();
    segmentGeneratorConfig.setIndexOn(StandardIndexes.text(), textIndexConfig, TEXT_INDEX_COL_NAME);
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
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v3);
    // check that V3 index sub-dir exists
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // check that text index exists under V3 subdir.
    File textIndexFile = SegmentDirectoryPaths.findTextIndexIndexFile(_indexDir, TEXT_INDEX_COL_NAME);
    Assert.assertNotNull(textIndexFile);
    Assert.assertTrue(textIndexFile.isDirectory());
    Assert.assertEquals(textIndexFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create text index reader with on-disk version V3
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setTextIndexColumns(new HashSet<>(Arrays.asList(TEXT_INDEX_COL_NAME)));
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
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
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    Assert.assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    Assert.assertEquals(textIndexDocIdMappingFile.getParentFile().getName(),
        SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig as V3
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on disk (V3)
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
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
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    Assert.assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    Assert.assertEquals(textIndexDocIdMappingFile.getParentFile().getName(),
        SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();

    // Test for scenarios by creating on-disk segment in V1 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V1
    // this generates the segment in V1 and does not convert to V3 as part of post-creation processing
    constructSegmentWithTextIndex(SegmentVersion.v1);

    // check that segment on-disk version is V1 after creation
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v1);
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
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create text index reader with on-disk version V1
    indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setTextIndexColumns(new HashSet<>(Arrays.asList(TEXT_INDEX_COL_NAME)));
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v1
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
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
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    Assert.assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    Assert.assertEquals(textIndexDocIdMappingFile.getParentFile().getName(),
        new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig to V1
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on fisk
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v1);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v1
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
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
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    Assert.assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    Assert.assertEquals(textIndexDocIdMappingFile.getParentFile().getName(),
        new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 3: set the segment version to load in IndexLoadingConfig to V3
    // there should be conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is different than the version of segment on disk
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
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
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(textIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    Assert.assertEquals(textIndexDocIdMappingFile.getName(),
        TEXT_INDEX_COL_NAME + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    Assert.assertEquals(textIndexDocIdMappingFile.getParentFile().getName(),
        SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();
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
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v3);
    // check that V3 index sub-dir exists
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // check that vector index exists under V3 subdir.
    File vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    Assert.assertNotNull(vectorIndexFile);
    Assert.assertTrue(vectorIndexFile.isDirectory());
    Assert.assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);
    Assert.assertEquals(vectorIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create vector index reader with on-disk version V3
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setVectorIndexConfigs(
        ImmutableMap.of(VECTOR_INDEX_COL_NAME, new VectorIndexConfig(ImmutableMap.of(
            "vectorDimension", String.valueOf(VECTOR_DIM_SIZE),
            "vectorIndexType", "HNSW"
        ))));
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // no change/conversion should have happened for vectorIndex dir
    vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    Assert.assertNotNull(vectorIndexFile);
    Assert.assertTrue(vectorIndexFile.isDirectory());
    Assert.assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);
    Assert.assertEquals(vectorIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig as V3
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on disk (V3)
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that index dir is not in V1 format (the only subdir it should have is V3)
    verifyIndexDirIsV3(_indexDir);
    // no change/conversion should have happened for vectorIndex dir
    vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    Assert.assertNotNull(vectorIndexFile);
    Assert.assertTrue(vectorIndexFile.isDirectory());
    Assert.assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);
    Assert.assertEquals(vectorIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();

    // Test for scenarios by creating on-disk segment in V1 and then loading
    // the segment with and without specifying segmentVersion in IndexLoadingConfig

    // create on-disk segment in V1
    // this generates the segment in V1 and does not convert to V3 as part of post-creation processing
    constructSegmentWithVectorIndex(SegmentVersion.v1);

    // check that segment on-disk version is V1 after creation
    Assert.assertEquals(new SegmentMetadataImpl(_indexDir).getVersion(), SegmentVersion.v1);
    // check that segment v1 dir exists
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    // check that v3 index sub-dir does not exist
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // check that vector index exists directly under indexDir (V1). it should exist and should be a subdir
    vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    Assert.assertNotNull(vectorIndexFile);
    Assert.assertTrue(vectorIndexFile.isDirectory());
    Assert.assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);
    Assert.assertEquals(vectorIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());

    // CASE 1: don't set the segment version to load in IndexLoadingConfig
    // there should be no conversion done by ImmutableSegmentLoader and it should
    // be able to create vector index reader with on-disk version V1
    indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setVectorIndexConfigs(
        ImmutableMap.of(VECTOR_INDEX_COL_NAME, new VectorIndexConfig(ImmutableMap.of(
            "vectorDimension", String.valueOf(VECTOR_DIM_SIZE),
            "vectorIndexType", "HNSW"
        ))));
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v1
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // no change/conversion should have happened in vector index Dir
    vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    Assert.assertNotNull(vectorIndexFile);
    Assert.assertTrue(vectorIndexFile.isDirectory());
    Assert.assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);
    Assert.assertEquals(vectorIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 2: set the segment version to load in IndexLoadingConfig to V1
    // there should be no conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is same as the version of segment on fisk
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v1);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v1
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v1);
    // no change/conversion should have happened in indexDir
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v1).exists());
    Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    // no change/conversion should have happened in vector index Dir
    vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    Assert.assertNotNull(vectorIndexFile);
    Assert.assertTrue(vectorIndexFile.isDirectory());
    Assert.assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);
    Assert.assertEquals(vectorIndexFile.getParentFile().getName(), new SegmentMetadataImpl(_indexDir).getName());
    indexSegment.destroy();

    // CASE 3: set the segment version to load in IndexLoadingConfig to V3
    // there should be conversion done by ImmutableSegmentLoader since the segmentVersionToLoad
    // is different than the version of segment on disk
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v3);
    indexSegment = ImmutableSegmentLoader.load(_indexDir, indexLoadingConfig);
    // check that loaded segment version is v3
    Assert.assertEquals(indexSegment.getSegmentMetadata().getVersion(), SegmentVersion.v3);
    // the index dir should exist in v3 format due to conversion
    Assert.assertTrue(SegmentDirectoryPaths.segmentDirectoryFor(_indexDir, SegmentVersion.v3).exists());
    verifyIndexDirIsV3(_indexDir);
    // check that vector index exists under V3 subdir. It should exist and should be a subdir
    vectorIndexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(_indexDir, VECTOR_INDEX_COL_NAME);
    Assert.assertNotNull(vectorIndexFile);
    Assert.assertTrue(vectorIndexFile.isDirectory());
    Assert.assertEquals(vectorIndexFile.getName(),
        VECTOR_INDEX_COL_NAME + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);
    Assert.assertEquals(vectorIndexFile.getParentFile().getName(), SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    indexSegment.destroy();
  }

  private void constructSegmentWithVectorIndex(SegmentVersion segmentVersion)
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_vectorAvroFile, INDEX_DIR, "testTable");
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    List<String> rawIndexCreationColumns = Lists.newArrayList(VECTOR_INDEX_COL_NAME);
    VectorIndexConfig vectorIndexConfig = new VectorIndexConfig(ImmutableMap.of(
        "vectorDimension", String.valueOf(VECTOR_DIM_SIZE),
        "vectorIndexType", "HNSW"
    ));
    segmentGeneratorConfig.setIndexOn(StandardIndexes.vector(), vectorIndexConfig, VECTOR_INDEX_COL_NAME);
    segmentGeneratorConfig.setRawIndexCreationColumns(rawIndexCreationColumns);
    segmentGeneratorConfig.setSegmentVersion(segmentVersion);
    driver.init(segmentGeneratorConfig);
    driver.build();

    _indexDir = new File(INDEX_DIR, driver.getSegmentName());
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
