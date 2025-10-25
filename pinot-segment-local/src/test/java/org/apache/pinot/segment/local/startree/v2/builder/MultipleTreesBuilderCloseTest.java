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
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
  public void testBuildFailureThenCloseFailureWithSuppressedException() throws Exception {
    // This test verifies that when build() fails and close() also fails,
    // the close() exception is added as a suppressed exception to the build() exception

    // Build a test segment with star-tree
    buildTestSegment();

    // Create a MultipleTreesBuilder with invalid config to force build() to fail
    List<StarTreeV2BuilderConfig> builderConfigs = createInvalidBuilderConfigs();

    MultipleTreesBuilder builder = null;
    Exception finalException = null;

    try {
      builder = new MultipleTreesBuilder(builderConfigs, INDEX_DIR, MultipleTreesBuilder.BuildMode.OFF_HEAP);

      // Manually set up the scenario for close() to fail
      // First, create the separator temp directory and existing metadata
      Field separatorTempDirField = MultipleTreesBuilder.class.getDeclaredField("_separatorTempDir");
      separatorTempDirField.setAccessible(true);

      File segmentDir = INDEX_DIR.listFiles()[0];
      File manualSeparatorTempDir = new File(segmentDir, StarTreeV2Constants.EXISTING_STAR_TREE_TEMP_DIR);
      FileUtils.forceMkdir(manualSeparatorTempDir);
      separatorTempDirField.set(builder, manualSeparatorTempDir);

      // Create temp files to simulate existing star-tree files that need restoration
      File tempIndexFile = new File(manualSeparatorTempDir, StarTreeV2Constants.INDEX_FILE_NAME);
      File tempMapFile = new File(manualSeparatorTempDir, StarTreeV2Constants.INDEX_MAP_FILE_NAME);
      FileUtils.touch(tempIndexFile);
      FileUtils.touch(tempMapFile);

      // Create existing star-tree metadata using reflection
      Field existingStarTreeMetadataField = MultipleTreesBuilder.class.getDeclaredField("_existingStarTreeMetadata");
      existingStarTreeMetadataField.setAccessible(true);
      PropertiesConfiguration existingMetadata = new PropertiesConfiguration();
      existingMetadata.setProperty("test.key", "test.value");
      existingStarTreeMetadataField.set(builder, existingMetadata);

      // Make the metadata file read-only to force close() restoration to fail
      File metadataFile = new File(segmentDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
      if (metadataFile.exists()) {
        metadataFile.setReadOnly();
      }

      // Use try-with-resources just like SegmentPreProcessor does
      // This will automatically call close() if build() throws an exception
      try (MultipleTreesBuilder autoCloseBuilder = builder) {
        // This should fail due to invalid config, setting _starTreeCreationFailed = true
        autoCloseBuilder.build();
        fail("Expected build() to throw an exception due to invalid config");
      } catch (Exception e) {
        finalException = e;
        // Verify that _starTreeCreationFailed was set to true
        Field starTreeCreationFailedField = MultipleTreesBuilder.class.getDeclaredField("_starTreeCreationFailed");
        starTreeCreationFailedField.setAccessible(true);
        assertTrue((Boolean) starTreeCreationFailedField.get(builder),
                   "_starTreeCreationFailed should be true after build() fails");
      } finally {
        // Restore write permissions for cleanup
        if (metadataFile.exists()) {
          metadataFile.setWritable(true);
        }
      }
    } finally {
      // Ensure builder is closed even if test fails
      if (builder != null) {
        try {
          // Restore permissions first
          File segmentDir = INDEX_DIR.listFiles()[0];
          File metadataFile = new File(segmentDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
          if (metadataFile.exists()) {
            metadataFile.setWritable(true);
          }
          builder.close();
        } catch (Exception e) {
          // Ignore cleanup exceptions
        }
      }
    }

    // Verify that the final exception occurred (this includes both build and close exceptions)
    assertNotNull(finalException, "Expected an exception from try-with-resources (build + close)");

    // The key test: verify that finalException has buildException as a suppressed exception
    // OR that finalException is the same as buildException with suppressed exceptions added
    Throwable[] suppressedExceptions = finalException.getSuppressed();

    // The close() exception should have suppressed exceptions
    assertTrue(suppressedExceptions.length > 0,
               "Expected close() exception to have suppressed exceptions, but found none. "
                   + "Final exception: " + finalException.getMessage());

    // Verify the suppressed exception is related to the cleanup failure
    boolean foundCleanupException = false;
    for (Throwable suppressed : suppressedExceptions) {
      if (suppressed.getMessage().contains("Could not reset") || suppressed.getMessage().contains("Permission denied")
          || suppressed.getMessage().contains("Unable to save") || suppressed instanceof IOException) {
        foundCleanupException = true;
        break;
      }
    }

    assertTrue(foundCleanupException,
               "Expected to find cleanup-related suppressed exception, but found: "
                   + Arrays.toString(suppressedExceptions));

    // This scenario should trigger the re-throw condition in SegmentPreProcessor.processStarTrees()
    // because finalException.getSuppressed().length > 0
    System.out.println("SUCCESS: Exception with " + suppressedExceptions.length
        + " suppressed exception(s) - this would be re-thrown by SegmentPreProcessor");
  }

  @Test
  public void testCloseWithCleanupFailure() throws Exception {
    // Build a test segment with star-tree
    buildTestSegment();

    // Create a MultipleTreesBuilder with valid config first
    List<StarTreeV2BuilderConfig> builderConfigs = createBuilderConfigs();

    MultipleTreesBuilder builder = null;
    try {
      builder = new MultipleTreesBuilder(builderConfigs, INDEX_DIR, MultipleTreesBuilder.BuildMode.OFF_HEAP);

      // Use reflection to set _starTreeCreationFailed to true
      // This simulates the scenario where star-tree creation failed during build()
      Field starTreeCreationFailedField = MultipleTreesBuilder.class.getDeclaredField("_starTreeCreationFailed");
      starTreeCreationFailedField.setAccessible(true);
      starTreeCreationFailedField.setBoolean(builder, true);

      // Get the separator temp directory using reflection
      Field separatorTempDirField = MultipleTreesBuilder.class.getDeclaredField("_separatorTempDir");
      separatorTempDirField.setAccessible(true);
      File separatorTempDir = (File) separatorTempDirField.get(builder);

      if (separatorTempDir != null) {
        // Create temp files to simulate existing star-tree files that need restoration
        File tempIndexFile = new File(separatorTempDir, StarTreeV2Constants.INDEX_FILE_NAME);
        File tempMapFile = new File(separatorTempDir, StarTreeV2Constants.INDEX_MAP_FILE_NAME);

        if (!tempIndexFile.exists()) {
          FileUtils.touch(tempIndexFile);
        }
        if (!tempMapFile.exists()) {
          FileUtils.touch(tempMapFile);
        }

        // Make the segment directory read-only to force restoration operations to fail
        File segmentDir = INDEX_DIR.listFiles()[0]; // Get the actual segment directory
        File metadataFile = new File(segmentDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);

        // Make metadata file read-only to force saveToFile operation to fail
        if (metadataFile.exists()) {
          metadataFile.setReadOnly();
        }

        // Make segment directory read-only to force file move operations to fail
        segmentDir.setReadOnly();

        try {
          // This should trigger the exception in close() method
          // When _starTreeCreationFailed is true and restoration operations fail,
          // the exception should be re-thrown from lines 290-293
          builder.close();

          // If we reach here, the test failed because no exception was thrown
          fail("Expected MultipleTreesBuilder.close() to throw an exception when cleanup restoration fails");
        } catch (Exception e) {
          // Verify this is the expected exception from cleanup restoration failure
          assertTrue(e.getMessage().contains("Could not reset") || e.getMessage().contains("Permission denied")
                  || e.getMessage().contains("Unable to save") || e instanceof IOException,
              "Expected exception from cleanup restoration failure, got: " + e.getMessage());

          // Success - the exception was properly thrown from close()
        } finally {
          // Restore write permissions for cleanup
          segmentDir.setWritable(true);
          if (metadataFile.exists()) {
            metadataFile.setWritable(true);
          }
        }
      } else {
        // If separatorTempDir is null, we need to create it manually to test the scenario
        // This happens when there's no existing star-tree, so we need to simulate one

        // Create the separator temp directory manually
        File segmentDir = INDEX_DIR.listFiles()[0];
        File manualSeparatorTempDir = new File(segmentDir, StarTreeV2Constants.EXISTING_STAR_TREE_TEMP_DIR);
        FileUtils.forceMkdir(manualSeparatorTempDir);

        // Set it using reflection
        separatorTempDirField.set(builder, manualSeparatorTempDir);

        // Create temp files to simulate existing star-tree files that need restoration
        File tempIndexFile = new File(manualSeparatorTempDir, StarTreeV2Constants.INDEX_FILE_NAME);
        File tempMapFile = new File(manualSeparatorTempDir, StarTreeV2Constants.INDEX_MAP_FILE_NAME);
        FileUtils.touch(tempIndexFile);
        FileUtils.touch(tempMapFile);

        // Create existing star-tree metadata using reflection
        Field existingStarTreeMetadataField = MultipleTreesBuilder.class.getDeclaredField("_existingStarTreeMetadata");
        existingStarTreeMetadataField.setAccessible(true);
        PropertiesConfiguration existingMetadata = new PropertiesConfiguration();
        existingMetadata.setProperty("test.key", "test.value");
        existingStarTreeMetadataField.set(builder, existingMetadata);

        // Make the metadata file read-only to force saveToFile operation to fail
        File metadataFile = new File(segmentDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
        if (metadataFile.exists()) {
          metadataFile.setReadOnly();
        }

        try {
          // This should trigger the exception in close() method
          builder.close();

          // If we reach here, the test failed because no exception was thrown
          fail("Expected MultipleTreesBuilder.close() to throw an exception when cleanup restoration fails");
        } catch (Exception e) {
          // Verify this is the expected exception from cleanup restoration failure
          assertTrue(e.getMessage().contains("Could not reset") || e.getMessage().contains("Permission denied")
                  || e.getMessage().contains("Unable to save") || e instanceof IOException,
              "Expected exception from cleanup restoration failure, got: " + e.getMessage());

          // Success - the exception was properly thrown from close()
        } finally {
          // Restore write permissions for cleanup
          if (metadataFile.exists()) {
            metadataFile.setWritable(true);
          }
        }
      }
    } finally {
      // Ensure builder is closed even if test fails
      if (builder != null) {
        try {
          builder.close();
        } catch (Exception e) {
          // Ignore cleanup exceptions
        }
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
