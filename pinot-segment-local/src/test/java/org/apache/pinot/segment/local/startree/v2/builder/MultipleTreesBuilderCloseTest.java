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
package org.apache.pinot.segment.local.startree.v2.builder;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexMapUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants.MetadataKey;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.*;

/// Unit test for MultipleTreesBuilder.close() method to verify exception handling
/// when cleanup operations fail.
public class MultipleTreesBuilderCloseTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "MultipleTreesBuilderCloseTest");
  private static final File INDEX_DIR = new File(TEMP_DIR, "testSegment");

  @BeforeMethod
  public void setUp() throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testBuildFailureThenCloseFailure() throws Exception {
    // This test verifies that when build() fails and close() also fails,

    // Build a test segment with star-tree
    buildTestSegment();

    // Build the star-tree index with a good configuration and ensure it passes. This will ensure that the correct
    // close clean-up path is called
    List<StarTreeV2BuilderConfig> builderConfigsValid = createBuilderConfigs();
    MultipleTreesBuilder builder = new MultipleTreesBuilder(builderConfigsValid, INDEX_DIR,
        MultipleTreesBuilder.BuildMode.OFF_HEAP);
    builder.build();
    builder.close();

    // Create a MultipleTreesBuilder with invalid config to force build() to fail
    List<StarTreeV2BuilderConfig> builderConfigsInvalid = createInvalidBuilderConfigs();
    builder = new MultipleTreesBuilder(builderConfigsInvalid, INDEX_DIR, MultipleTreesBuilder.BuildMode.OFF_HEAP);

    // Mock the CommonsConfigurationUtils to emulate failure during close
    try (MockedStatic<CommonsConfigurationUtils> mockedStatic = mockStatic(CommonsConfigurationUtils.class)) {
      assertThrows(Exception.class, builder::build);
      try {
        // This should fail due to invalid config
        assertThrows(Exception.class, builder::build);
      } finally {
        // Mock the static method to always throw RuntimeException on any input to force a close() failure
        mockedStatic.when(() -> CommonsConfigurationUtils.saveToFile(any(PropertiesConfiguration.class),
            any(File.class))).thenThrow(new RuntimeException("Simulated failure"));
        assertThrows(Exception.class, builder::close);
      }
    }
  }

  @Test
  public void testOrdinaryBuildFailureRestoresPreviousTree()
      throws Exception {
    buildTestSegment();
    buildInitialStarTree();

    MultipleTreesBuilder builder = new MultipleTreesBuilder(createInvalidBuilderConfigs(), INDEX_DIR,
        MultipleTreesBuilder.BuildMode.OFF_HEAP);
    assertThrows(Exception.class, builder::build);
    builder.close();

    File segmentDirectory = SegmentDirectoryPaths.findSegmentDirectory(INDEX_DIR);
    assertTrue(new File(segmentDirectory, StarTreeV2Constants.INDEX_FILE_NAME).isFile());
    assertTrue(new File(segmentDirectory, StarTreeV2Constants.INDEX_MAP_FILE_NAME).isFile());
    assertNotNull(new SegmentMetadataImpl(INDEX_DIR).getStarTreeV2MetadataList());
  }

  @Test
  public void testForcedBuildFailureDiscardsPreviousTree()
      throws Exception {
    buildTestSegment();
    buildInitialStarTree();

    MultipleTreesBuilder builder = new MultipleTreesBuilder(createInvalidBuilderConfigs(), INDEX_DIR,
        MultipleTreesBuilder.BuildMode.OFF_HEAP, null, true);
    assertThrows(Exception.class, builder::build);
    builder.close();

    File segmentDirectory = SegmentDirectoryPaths.findSegmentDirectory(INDEX_DIR);
    assertFalse(new File(segmentDirectory, StarTreeV2Constants.INDEX_FILE_NAME).exists());
    assertFalse(new File(segmentDirectory, StarTreeV2Constants.INDEX_MAP_FILE_NAME).exists());
    assertNull(new SegmentMetadataImpl(INDEX_DIR).getStarTreeV2MetadataList());
  }

  @Test
  public void testFinalizationFailureRestoresExactPreviousTree()
      throws Exception {
    buildTestSegment();
    buildInitialStarTree();

    File segmentDirectory = SegmentDirectoryPaths.findSegmentDirectory(INDEX_DIR);
    File indexFile = new File(segmentDirectory, StarTreeV2Constants.INDEX_FILE_NAME);
    File indexMapFile = new File(segmentDirectory, StarTreeV2Constants.INDEX_MAP_FILE_NAME);
    byte[] previousIndex = Files.readAllBytes(indexFile.toPath());
    byte[] previousIndexMap = Files.readAllBytes(indexMapFile.toPath());
    Map<String, Object> previousMetadata = getStarTreeMetadata(segmentDirectory);

    MultipleTreesBuilder builder = new MultipleTreesBuilder(createBuilderConfigs(), INDEX_DIR,
        MultipleTreesBuilder.BuildMode.OFF_HEAP);
    assertBuildFailsDuringIndexMapFinalization(builder);
    assertPartialNewStateRemoved(segmentDirectory);
    assertTrue(new File(segmentDirectory, StarTreeV2Constants.EXISTING_STAR_TREE_TEMP_DIR).isDirectory());

    builder.close();

    assertEquals(Files.readAllBytes(indexFile.toPath()), previousIndex);
    assertEquals(Files.readAllBytes(indexMapFile.toPath()), previousIndexMap);
    assertEquals(getStarTreeMetadata(segmentDirectory), previousMetadata);
    assertNotNull(new SegmentMetadataImpl(INDEX_DIR).getStarTreeV2MetadataList());
    assertFalse(new File(segmentDirectory, StarTreeV2Constants.EXISTING_STAR_TREE_TEMP_DIR).exists());
  }

  @Test
  public void testForcedFinalizationFailureDiscardsPreviousTree()
      throws Exception {
    buildTestSegment();
    buildInitialStarTree();

    File segmentDirectory = SegmentDirectoryPaths.findSegmentDirectory(INDEX_DIR);
    MultipleTreesBuilder builder = new MultipleTreesBuilder(createBuilderConfigs(), INDEX_DIR,
        MultipleTreesBuilder.BuildMode.OFF_HEAP, null, true);
    assertBuildFailsDuringIndexMapFinalization(builder);
    assertPartialNewStateRemoved(segmentDirectory);
    assertTrue(new File(segmentDirectory, StarTreeV2Constants.EXISTING_STAR_TREE_TEMP_DIR).isDirectory());

    builder.close();

    assertPartialNewStateRemoved(segmentDirectory);
    assertFalse(new File(segmentDirectory, StarTreeV2Constants.EXISTING_STAR_TREE_TEMP_DIR).exists());
  }

  private void assertBuildFailsDuringIndexMapFinalization(MultipleTreesBuilder builder)
      throws Exception {
    boolean[] metadataWasSaved = {false};
    try (MockedStatic<StarTreeIndexMapUtils> mockedStatic = mockStatic(StarTreeIndexMapUtils.class)) {
      mockedStatic.when(() -> StarTreeIndexMapUtils.storeToFile(any(), any(File.class))).thenAnswer(invocation -> {
        metadataWasSaved[0] = new SegmentMetadataImpl(INDEX_DIR).getStarTreeV2MetadataList() != null;
        FileUtils.touch(invocation.getArgument(1));
        throw new RuntimeException("Simulated index-map finalization failure");
      });
      assertThrows(RuntimeException.class, builder::build);
    }
    assertTrue(metadataWasSaved[0], "Star-tree metadata should be saved before index-map finalization");
  }

  private void assertPartialNewStateRemoved(File segmentDirectory)
      throws Exception {
    assertFalse(new File(segmentDirectory, StarTreeV2Constants.INDEX_FILE_NAME).exists());
    assertFalse(new File(segmentDirectory, StarTreeV2Constants.INDEX_MAP_FILE_NAME).exists());
    assertFalse(new File(segmentDirectory, StarTreeV2Constants.STAR_TREE_TEMP_DIR).exists());
    assertNull(new SegmentMetadataImpl(INDEX_DIR).getStarTreeV2MetadataList());
  }

  private Map<String, Object> getStarTreeMetadata(File segmentDirectory)
      throws Exception {
    PropertiesConfiguration metadata = CommonsConfigurationUtils.fromFile(
        new File(segmentDirectory, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    return CommonsConfigurationUtils.toMap(metadata.subset(MetadataKey.STAR_TREE_SUBSET));
  }

  private void buildInitialStarTree()
      throws Exception {
    try (MultipleTreesBuilder builder = new MultipleTreesBuilder(createBuilderConfigs(), INDEX_DIR,
        MultipleTreesBuilder.BuildMode.OFF_HEAP)) {
      builder.build();
    }
  }

  private void buildTestSegment() throws Exception {
    // Create a simple test segment
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .addMetric("longCol", FieldSpec.DataType.LONG)
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName("testSegment");

    // Create test data
    List<GenericRow> rows = Arrays.asList(
        createRow("A", 1L),
        createRow("B", 2L),
        createRow("C", 3L)
    );

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
  }

  private GenericRow createRow(String stringValue, Long longValue) {
    GenericRow row = new GenericRow();
    row.putValue("stringCol", stringValue);
    row.putValue("longCol", longValue);
    return row;
  }

  private List<StarTreeV2BuilderConfig> createBuilderConfigs() throws Exception {
    // Create a valid star-tree config
    StarTreeIndexConfig starTreeConfig = new StarTreeIndexConfig(
        Arrays.asList("stringCol"),
        null,
        Arrays.asList("SUM__longCol"),
        null,
        1000
    );

    // Load the segment to get metadata
    File segmentDir = INDEX_DIR.listFiles()[0];
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);
    try {
      return StarTreeBuilderUtils.generateBuilderConfigs(
          Arrays.asList(starTreeConfig),
          false,
          segment.getSegmentMetadata()
      );
    } finally {
      segment.destroy();
    }
  }

  private List<StarTreeV2BuilderConfig> createInvalidBuilderConfigs() throws Exception {
    // Create an invalid star-tree config that will cause build() to fail
    // Using "SUM__*" which should be invalid
    StarTreeIndexConfig invalidStarTreeConfig = new StarTreeIndexConfig(
        Arrays.asList("stringCol"),
        null,
        Arrays.asList("SUM__*"),
        null,
        1000
    );

    // Load the segment to get metadata
    File segmentDir = INDEX_DIR.listFiles()[0];
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);
    try {
      return StarTreeBuilderUtils.generateBuilderConfigs(
          Arrays.asList(invalidStarTreeConfig),
          false,
          segment.getSegmentMetadata()
      );
    } finally {
      segment.destroy();
    }
  }
}
