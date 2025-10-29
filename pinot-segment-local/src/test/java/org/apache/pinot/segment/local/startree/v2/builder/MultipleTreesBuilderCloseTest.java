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
import java.util.Arrays;
import java.util.List;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
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
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

/**
 * Unit test for MultipleTreesBuilder.close() method to verify exception handling
 * when cleanup operations fail.
 */
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
    try (MockedStatic<CommonsConfigurationUtils> mockedStatic = Mockito.mockStatic(CommonsConfigurationUtils.class)) {
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
