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
package org.apache.pinot.segment.local.segment.index.text;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.TextIndexHandler;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for Lucene text index backward compatibility and upgrade scenarios.
 * This test verifies that old format text indexes can be loaded and upgraded
 * to the new format without losing functionality.
 */
public class LuceneTextIndexConfigReloadTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneTextIndexConfigReloadTest.class);

  private static final String TEST_COLUMN = "testColumn";
  private static final String[] TEST_DOCUMENTS = {
      "java programming language", "python programming language", "scala programming language",
      "kotlin programming " + "language", "rust programming language"
  };

  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = Files.createTempDirectory("lucene_text_index_test").toFile();
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    if (_tempDir != null && _tempDir.exists()) {
      FileUtils.deleteDirectory(_tempDir);
    }
  }

  /**
   * Creates test data for the segment
   */
  private List<GenericRow> createTestData() {
    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < TEST_DOCUMENTS.length; i++) {
      GenericRow row = new GenericRow();
      row.putValue(TEST_COLUMN, TEST_DOCUMENTS[i]);
      row.putValue("id", i);
      rows.add(row);
    }
    return rows;
  }

  /**
   * Creates a table config with text index configuration
   */
  private TableConfig createTableConfigWithTextIndex(boolean useNewFormat) {
    // Create field config for text index with custom properties
    // For new format (combined files), set storeInSegmentFile=true
    // For old format (directory structure), set storeInSegmentFile=false
    Map<String, String> properties = Map.of("storeInSegmentFile", String.valueOf(useNewFormat));
    FieldConfig fieldConfig =
        new FieldConfig(TEST_COLUMN, FieldConfig.EncodingType.DICTIONARY, Arrays.asList(FieldConfig.IndexType.TEXT),
            null, properties);

    // Create table config
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setFieldConfigList(Arrays.asList(fieldConfig)).build();

    return tableConfig;
  }

  /**
   * Creates a schema for the test
   */
  private Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addSingleValueDimension(TEST_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension("id", FieldSpec.DataType.INT).build();
  }

  @Test
  public void testFormatUpgradeProcess()
      throws Exception {
    //Create segment with storeInSegmentFile = false and test queries
    TableConfig oldConfig = createTableConfigWithTextIndex(false);
    Schema schema = createSchema();
    File segmentFile = createSegment(oldConfig, schema);

    // Test queries with old config
    testSegmentWithConfig(segmentFile, oldConfig, schema, "old config");

    // Same config - should return false
    boolean needsUpdate = checkNeedUpdateIndices(segmentFile, oldConfig, schema);
    Assert.assertFalse(needsUpdate, "needUpdateIndices should return false when config is not changed");

    //Update table config with storeInSegmentFile = true and create new segment
    TableConfig newConfig = createTableConfigWithTextIndex(true);

    // Different config - should return true
    needsUpdate = checkNeedUpdateIndices(segmentFile, newConfig, schema);
    Assert.assertTrue(needsUpdate, "needUpdateIndices should return true when storeInSegmentFile config is changed");

    File newSegmentFile = createSegment(newConfig, schema);

    // Test queries with new config
    testSegmentWithConfig(newSegmentFile, newConfig, schema, "new config");
    // Same config - should return false
    needsUpdate = checkNeedUpdateIndices(newSegmentFile, newConfig, schema);
    Assert.assertFalse(needsUpdate, "needUpdateIndices should return false when config is not changed");
  }

  /**
   * Helper method to create a segment with given table config and schema
   */
  private File createSegment(TableConfig tableConfig, Schema schema)
      throws Exception {
    File outputDir = new File(_tempDir, "test_segment");
    if (outputDir.exists()) {
      FileUtils.deleteDirectory(outputDir);
    }
    outputDir.mkdirs();

    List<GenericRow> testData = createTestData();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(outputDir.getAbsolutePath());
    config.setTableName("testTable");
    config.setSegmentName("testSegment");
    config.setSegmentVersion(SegmentVersion.v3);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(testData)) {
      driver.init(config, recordReader);
      driver.build();
    }

    String segmentName = driver.getSegmentName();
    File segmentFile = new File(outputDir, segmentName);
    Assert.assertTrue(segmentFile.exists(), "Segment should be created");
    return segmentFile;
  }

  /**
   * Helper method to test a segment with given config
   */
  private void testSegmentWithConfig(File segmentFile, TableConfig tableConfig, Schema schema, String configName)
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentFile, indexLoadingConfig);

    try {
      testTextSearchQueries(segment);
      LOGGER.info("Text search queries work correctly with {}", configName);
    } finally {
      segment.destroy();
    }
  }

  /**
   * Helper method to check if needUpdateIndices returns true for configuration changes
   */
  private boolean checkNeedUpdateIndices(File segmentFile, TableConfig tableConfig, Schema schema)
      throws Exception {
    Map<String, Object> props = new HashMap<>();
    props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap);
    PinotConfiguration segmentDirectoryConfigs = new PinotConfiguration(props);

    SegmentDirectoryLoaderContext segmentLoaderContext =
        new SegmentDirectoryLoaderContext.Builder().setTableConfig(tableConfig).setSchema(schema)
            .setSegmentName(segmentFile.getName()).setSegmentDirectoryConfigs(segmentDirectoryConfigs).build();
    SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(segmentFile.toURI(), segmentLoaderContext);

    // Create TextIndexHandler with the config
    Map<String, FieldIndexConfigs> fieldIndexConfigs =
        new IndexLoadingConfig(tableConfig, schema).getFieldIndexConfigByColName();
    TextIndexHandler textIndexHandler = new TextIndexHandler(segmentDirectory, fieldIndexConfigs, tableConfig, schema);

    // Check if needUpdateIndices detects the configuration change
    SegmentDirectory.Reader reader = segmentDirectory.createReader();
    boolean needsUpdate = textIndexHandler.needUpdateIndices(reader);
    reader.close();
    segmentDirectory.close();

    LOGGER.info("needUpdateIndices returned: {} for config with storeInSegmentFile: {}", needsUpdate,
        tableConfig.getFieldConfigList().get(0).getProperties().get("storeInSegmentFile"));

    return needsUpdate;
  }

  /**
   * Helper method to test text search queries on a segment
   */
  private void testTextSearchQueries(ImmutableSegment segment)
      throws Exception {
    // Get the text index reader
    DataSource dataSource = segment.getDataSource(TEST_COLUMN);
    TextIndexReader textIndexReader = dataSource.getTextIndex();
    Assert.assertNotNull(textIndexReader, "Text index reader should be available");

    // Test various search queries
    testSearchQuery(textIndexReader, "java", "Should find documents containing 'java'");
    testSearchQuery(textIndexReader, "python", "Should find documents containing 'python'");
    testSearchQuery(textIndexReader, "programming", "Should find documents containing 'programming'");
    testSearchQuery(textIndexReader, "language", "Should find documents containing 'language'");
    testSearchQuery(textIndexReader, "nonexistent", "Should return empty results for non-existent terms");

    // Test phrase queries
    testSearchQuery(textIndexReader, "\"java programming\"", "Should find exact phrase");
    testSearchQuery(textIndexReader, "\"python programming\"", "Should find exact phrase");
  }

  /**
   * Helper method to test a single search query
   */
  private void testSearchQuery(TextIndexReader textIndexReader, String query, String description) {
    try {
      LOGGER.debug("Testing query: '{}' - {}", query, description);

      MutableRoaringBitmap results = textIndexReader.getDocIds(query);
      Assert.assertNotNull(results, "Search results should not be null");

      int resultCount = results.getCardinality();
      LOGGER.debug("Found {} matching documents for query: '{}'", resultCount, query);

      // Verify that results are within valid range
      results.forEach((int docId) -> {
        Assert.assertTrue(docId >= 0 && docId < TEST_DOCUMENTS.length,
            "Document ID " + docId + " should be within valid range [0, " + TEST_DOCUMENTS.length + ")");
      });
    } catch (Exception e) {
      Assert.fail("Search query '" + query + "' failed: " + e.getMessage(), e);
    }
  }
}
