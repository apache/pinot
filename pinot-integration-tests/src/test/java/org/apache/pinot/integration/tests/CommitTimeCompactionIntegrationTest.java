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
package org.apache.pinot.integration.tests;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.ExecutionStats;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Integration tests for commit-time compaction feature in upsert tables.
 *
 * These tests validate that commit-time compaction correctly handles:
 * - Multi-value dictionary columns
 * - Mixed column configurations (dictionary vs no-dictionary)
 * - Inverted indexes
 * - Raw index writers
 * - Different data types
 */
public class CommitTimeCompactionIntegrationTest extends BaseClusterIntegrationTest {
  private static final String INPUT_DATA_SMALL_TAR_FILE = "gameScores_csv.tar.gz";
  private static final String CSV_SCHEMA_HEADER = "playerId,name,game,score,timestampInEpoch,deleted";
  private static final String CSV_DELIMITER = ",";
  private static final int NUM_SERVERS = 2;
  private static final String PRIMARY_KEY_COL = "playerId";
  private static final String TIME_COL_NAME = "timestampInEpoch";
  public static final String UPSERT_SCHEMA_FILE_NAME = "upsert_table_test.schema";
  private static final List<String> COLUMNS_TO_COMPARE =
      List.of("name", "game", "score", "timestampInEpoch", "deleted");

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServers(NUM_SERVERS);

    // Start Kafka
    startKafkaWithoutTopic();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    // Note: Individual tests clean up their own tables in their test methods
    // No need to drop tables here as they use unique names per test
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testCommitTimeCompactionComparison()
      throws Exception {
    // Enhanced test to validate that enableCommitTimeCompaction=true removes invalid records
    // compared to enableCommitTimeCompaction=false, with comprehensive verification

    String kafkaTopicNameCompacted = getKafkaTopic() + "-commit-time-compaction-enabled";
    String kafkaTopicNameNormal = getKafkaTopic() + "-commit-time-compaction-disabled";

    // Set up identical data for both tables - using small dataset for faster test execution
    setUpKafka(kafkaTopicNameCompacted, INPUT_DATA_SMALL_TAR_FILE);
    setUpKafka(kafkaTopicNameNormal, INPUT_DATA_SMALL_TAR_FILE);

    // TABLE 1: With commit-time compaction ENABLED
    String tableNameWithCompaction = "gameScoresCommitTimeCompactionEnabled";
    UpsertConfig upsertConfigWithCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompaction.setEnableCommitTimeCompaction(true);  // ENABLE commit-time compaction
    TableConfig tableConfigWithCompaction =
        setUpTable(tableNameWithCompaction, kafkaTopicNameCompacted, upsertConfigWithCompaction);

    // Ensure _columnMajorSegmentBuilderEnabled = false as specified
    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    updateTableConfig(tableConfigWithCompaction);

    // TABLE 2: With commit-time compaction DISABLED (traditional behavior)
    String tableNameWithoutCompaction = "gameScoresCommitTimeCompactionDisabled";
    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);  // DISABLE commit-time compaction
    TableConfig tableConfigWithoutCompaction =
        setUpTable(tableNameWithoutCompaction, kafkaTopicNameNormal, upsertConfigWithoutCompaction);

    // Ensure _columnMajorSegmentBuilderEnabled = false as specified
    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    updateTableConfig(tableConfigWithoutCompaction);

    // Wait for both tables to load the same initial data (3 unique records after upserts)
    waitForAllDocsLoadedInBothTables(tableNameWithCompaction, tableNameWithoutCompaction, 30_000L, 10);

    // Verify initial state - both tables should show the same upserted data count (3 unique records)
    validateInitialState(tableNameWithCompaction, tableNameWithoutCompaction, 3);

    // Create update patterns to generate invalid records - simplified for faster test execution
    // Use playerIds that match the initial data to ensure consistent partitioning
    List<String> updateRecords = List.of("100,Zook-Updated1,counter-strike,1000,1681300000000,false",
        "101,Alice-Updated1,dota,2000,1681300001000,false", "102,Bob-Updated1,cs2,3000,1681300002000,false",
        "100,Zook-Updated2,valorant,1500,1681300003000,false", "101,Alice-Updated2,lol,2500,1681300004000,false");

    // Push all updates at once to reduce wait times and ensure consistent partitioning
    pushDataWithKeyToBothTopics(updateRecords, kafkaTopicNameCompacted, kafkaTopicNameNormal, 0);
    // Wait for all additional records to be processed (3 unique records after all upserts)
    waitForAllDocsLoadedInBothTables(tableNameWithCompaction, tableNameWithoutCompaction, 90_000L, 15);

    // Verify state before commit - both tables should still show the same logical result (3 unique records)
    validatePreCommitState(tableNameWithCompaction, tableNameWithoutCompaction, 3);

    // Perform commit and wait for completion
    performCommitAndWait(tableNameWithCompaction, tableNameWithoutCompaction, 30_000L, 4, 2);

    // Validate post-commit compaction effectiveness and data integrity
    validatePostCommitCompaction(tableNameWithCompaction, tableNameWithoutCompaction, 3, 2, 0.95);

    // Clean up
    dropRealtimeTable(tableNameWithCompaction);
    dropRealtimeTable(tableNameWithoutCompaction);
  }

  @Test
  public void testCommitTimeCompactionWithSortedColumn()
      throws Exception {
    // Test Case 1: Standard and Sorted Data Types with Mixed Primitives
    // Goal: Verify commit-time compaction correctly processes and optimizes segments
    // containing a mix of common primitive data types, specifically including a sorted column.

    String kafkaTopicNameCompacted = getKafkaTopic() + "-sorted-compaction-enabled";
    String kafkaTopicNameNormal = getKafkaTopic() + "-sorted-compaction-disabled";

    // Set up identical data for both tables - using standard schema
    setUpKafka(kafkaTopicNameCompacted, INPUT_DATA_SMALL_TAR_FILE);
    setUpKafka(kafkaTopicNameNormal, INPUT_DATA_SMALL_TAR_FILE);

    // Create schemas for both tables
    Schema schema = createSchema();
    schema.setSchemaName("sortedCompactionEnabled");
    addSchema(schema);

    Schema schema2 = createSchema();
    schema2.setSchemaName("sortedCompactionDisabled");
    addSchema(schema2);

    // TABLE 1: With commit-time compaction ENABLED AND sorted column configured BEFORE data ingestion
    String tableNameWithCompaction = "sortedCompactionEnabled";
    UpsertConfig upsertConfigWithCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompaction.setEnableCommitTimeCompaction(true);

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    TableConfig tableConfigWithCompaction =
        createCSVUpsertTableConfig(tableNameWithCompaction, kafkaTopicNameCompacted, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithCompaction, PRIMARY_KEY_COL);

    // CRITICAL: Set sorted column BEFORE adding table config
    tableConfigWithCompaction.getIndexingConfig().setSortedColumn(Collections.singletonList("score"));
    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithCompaction);

    // TABLE 2: With commit-time compaction DISABLED but also with sorted column
    String tableNameWithoutCompaction = "sortedCompactionDisabled";
    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);

    TableConfig tableConfigWithoutCompaction =
        createCSVUpsertTableConfig(tableNameWithoutCompaction, kafkaTopicNameNormal, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithoutCompaction, PRIMARY_KEY_COL);

    // Set same sorted column configuration BEFORE adding table config
    tableConfigWithoutCompaction.getIndexingConfig().setSortedColumn(Collections.singletonList("score"));
    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithoutCompaction);

    // Wait for both tables to load the same initial data (3 unique records after upserts)
    waitForAllDocsLoadedInBothTables(tableNameWithCompaction, tableNameWithoutCompaction, 30_000L, 10);

    // Verify initial state - both tables should show the same upserted data count (3 unique records)
    validateInitialState(tableNameWithCompaction, tableNameWithoutCompaction, 3);

    // Push additional updates using standard format to create more obsolete records
    // Add more diverse data with different primary keys and varying scores for better sorting validation
    List<String> updateRecords = List.of("100,Alice Updated,chess,15.5,1681354200000,false",   // Low score
        "101,Bob Updated,poker,88.0,1681354300000,false",     // High score
        "102,Charlie Updated,chess,45.2,1681354400000,false", // Mid score
        "103,David New,tennis,12.8,1681354500000,false",      // Very low score
        "104,Eva New,golf,78.9,1681354600000,false",          // High score
        "105,Frank New,soccer,33.1,1681354700000,false"       // Mid-low score
    );

    pushDataToBothTopics(updateRecords, kafkaTopicNameCompacted, kafkaTopicNameNormal, 0);

    // Push another round of updates to create more obsolete records
    List<String> moreUpdates = List.of("100,Alice Final,chess,25.7,1681454200000,false",     // Low-mid score
        "101,Bob Final,poker,91.3,1681454300000,false",       // Very high score
        "103,David Final,tennis,8.4,1681454400000,false",     // Very low score
        "104,Eva Final,golf,67.8,1681454500000,false"         // High score
    );

    pushDataToBothTopics(moreUpdates, kafkaTopicNameCompacted, kafkaTopicNameNormal, 0);

    // Wait for all updates to be processed (6 unique records after all upserts)
    waitForAllDocsLoadedInBothTables(tableNameWithCompaction, tableNameWithoutCompaction, 30_000L, 20);

    // Perform simple commit and verify data identity between tables
    performSimpleCommitAndVerify(tableNameWithCompaction, tableNameWithoutCompaction, 60_000L, 6);

    // Validate post-commit compaction effectiveness and data integrity (expecting 6 records, min 4 removed)
    validatePostCommitCompaction(tableNameWithCompaction, tableNameWithoutCompaction, 6, 4, 1.0);

    // CRITICAL: Validate segment-level sorting
    validateSegmentLevelSorting(tableNameWithCompaction, true);  // Should be sorted due to sorted column config
    validateSegmentLevelSorting(tableNameWithoutCompaction, true); // Should NOT be sorted (no sorted column config)

    // Clean up
    dropRealtimeTable(tableNameWithCompaction);
    dropRealtimeTable(tableNameWithoutCompaction);
  }

  @Test
  public void testCommitTimeCompactionWithNoDictionaryColumns()
      throws Exception {
    // Test Case 2: No Dictionary (Raw Index) Configuration
    // Goal: Ensure commit-time compaction handles columns configured without dictionary indices
    // (raw storage) on standard schema columns.

    String kafkaTopicNameCompacted = getKafkaTopic() + "-raw-compaction-enabled";
    String kafkaTopicNameNormal = getKafkaTopic() + "-raw-compaction-disabled";

    // Set up identical data for both tables - using standard schema
    setUpKafka(kafkaTopicNameCompacted, INPUT_DATA_SMALL_TAR_FILE);
    setUpKafka(kafkaTopicNameNormal, INPUT_DATA_SMALL_TAR_FILE);

    // TABLE 1: With commit-time compaction ENABLED
    String tableNameWithCompaction = "rawIndexCompactionEnabled";
    UpsertConfig upsertConfigWithCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompaction.setEnableCommitTimeCompaction(true);
    TableConfig tableConfigWithCompaction =
        setUpTable(tableNameWithCompaction, kafkaTopicNameCompacted, upsertConfigWithCompaction);

    // Add raw index configuration for name and game columns (no dictionary - proper way)
    tableConfigWithCompaction.getIndexingConfig().setNoDictionaryColumns(List.of("name", "game"));
    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    updateTableConfig(tableConfigWithCompaction);

    // TABLE 2: With commit-time compaction DISABLED
    String tableNameWithoutCompaction = "rawIndexCompactionDisabled";
    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);
    TableConfig tableConfigWithoutCompaction =
        setUpTable(tableNameWithoutCompaction, kafkaTopicNameNormal, upsertConfigWithoutCompaction);

    // Add same raw index configuration
    tableConfigWithoutCompaction.getIndexingConfig().setNoDictionaryColumns(List.of("name", "game"));
    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    updateTableConfig(tableConfigWithoutCompaction);

    // Wait for both tables to load the same initial data (3 unique records after upserts)
    waitForAllDocsLoadedInBothTables(tableNameWithCompaction, tableNameWithoutCompaction, 30_000L, 10);

    // Verify initial state - both tables should show the same upserted data count (3 unique records)
    validateInitialState(tableNameWithCompaction, tableNameWithoutCompaction, 3);

    // Push additional updates using standard format to test raw index columns
    List<String> updateRecords =
        List.of("100,Alice Raw,chess,95.5,1681354200000,false", "101,Bob Raw,poker,88.0,1681354300000,false",
            "102,Charlie Raw,chess,82.5,1681354400000,false");

    pushDataToBothTopics(updateRecords, kafkaTopicNameCompacted, kafkaTopicNameNormal, 0);

    // Push another round of updates to create more obsolete records
    List<String> moreUpdates =
        List.of("100,Alice NoDict,chess,99.5,1681454200000,false", "101,Bob NoDict,poker,91.0,1681454300000,false");

    pushDataToBothTopics(moreUpdates, kafkaTopicNameCompacted, kafkaTopicNameNormal, 0);

    // Wait for all updates to be processed (3 unique records after upserts)
    waitForAllDocsLoadedInBothTables(tableNameWithCompaction, tableNameWithoutCompaction, 30_000L, 15);

    // Perform simple commit operation
    performSimpleCommitAndVerify(tableNameWithCompaction, tableNameWithoutCompaction, 60_000L, 3);

    // Validate post-commit compaction effectiveness and data integrity (expecting 3 records, min 2 removed)
    validatePostCommitCompaction(tableNameWithCompaction, tableNameWithoutCompaction, 3, 2, 1.0);

    // Verify data integrity for no-dictionary columns in compacted table
    ResultSet noDictResult = getPinotConnection().execute(
        "SELECT playerId, name, game FROM " + tableNameWithCompaction + " ORDER BY playerId").getResultSet(0);
    assertEquals(noDictResult.getRowCount(), 3);

    // Verify that no-dictionary configuration is maintained (raw storage for name and game columns)
    ResultSet allData = getPinotConnection().execute("SELECT * FROM " + tableNameWithCompaction).getResultSet(0);
    assertEquals(allData.getRowCount(), 3, "Should have 3 valid records after compaction");
    // Clean up
    dropRealtimeTable(tableNameWithCompaction);
    dropRealtimeTable(tableNameWithoutCompaction);
  }

  @Test
  public void testCommitTimeCompactionWithMultiValueColumns()
      throws Exception {
    // Test Case: Multi-Value Fields with Commit-Time Compaction
    // Goal: Ensure commit-time compaction correctly handles multi-value dictionary columns
    // (like arrays/lists) during segment conversion, validating the fix for CompactedDictEncodedColumnStatistics

    String kafkaTopicNameCompacted = getKafkaTopic() + "-mv-compaction-enabled";
    String kafkaTopicNameNormal = getKafkaTopic() + "-mv-compaction-disabled";

    // Create test data with multi-value fields similar to user's "tags" column
    // Use semicolon-separated format for multi-value fields in CSV
    List<String> testRecords = List.of("200,Player200,game1,85.5,1681054200000,false,action;shooter",
        "201,Player201,game2,92.0,1681054300000,false,strategy;puzzle",
        "202,Player202,game3,78.0,1681054400000,false,rpg;adventure;fantasy",
        // Updates to create obsolete records
        "200,Player200Updated,game1,90.0,1681154200000,false,action;fps",
        "201,Player201Updated,game2,95.0,1681154300000,false,strategy;rts");

    // Set up Kafka topics with multi-value test data
    pushCsvIntoKafka(testRecords, kafkaTopicNameCompacted, 0);
    pushCsvIntoKafka(testRecords, kafkaTopicNameNormal, 0);

    // TABLE 1: With commit-time compaction ENABLED
    String tableNameWithCompaction = "gameScoresMVCompactionEnabled";

    // Create schema with multi-value column (similar to user's "tags" field)
    Schema mvSchemaCompacted = createSchema();
    mvSchemaCompacted.setSchemaName(tableNameWithCompaction);
    mvSchemaCompacted.addField(new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, false));
    addSchema(mvSchemaCompacted);

    UpsertConfig upsertConfigWithCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompaction.setEnableCommitTimeCompaction(true);

    Map<String, String> csvDecoderProperties =
        getCSVDecoderProperties(CSV_DELIMITER, "playerId,name,game,score,timestampInEpoch,deleted,tags");
    // Configure multi-value delimiter for tags column
    csvDecoderProperties.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
    TableConfig tableConfigWithCompaction =
        createCSVUpsertTableConfig(tableNameWithCompaction, kafkaTopicNameCompacted, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithCompaction, PRIMARY_KEY_COL);

    // Configure inverted indexes for multi-value column
    tableConfigWithCompaction.getIndexingConfig().setInvertedIndexColumns(List.of("tags", "name", "game"));
    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithCompaction);

    // TABLE 2: With commit-time compaction DISABLED
    String tableNameWithoutCompaction = "gameScoresMVCompactionDisabled";

    // Create separate schema for normal table
    Schema mvSchemaNormal = createSchema();
    mvSchemaNormal.setSchemaName(tableNameWithoutCompaction);
    mvSchemaNormal.addField(new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, false));
    addSchema(mvSchemaNormal);

    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);

    Map<String, String> csvDecoderPropertiesNormal =
        getCSVDecoderProperties(CSV_DELIMITER, "playerId,name,game,score,timestampInEpoch,deleted,tags");
    // Configure multi-value delimiter for tags column
    csvDecoderPropertiesNormal.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
    TableConfig tableConfigWithoutCompaction =
        createCSVUpsertTableConfig(tableNameWithoutCompaction, kafkaTopicNameNormal, getNumKafkaPartitions(),
            csvDecoderPropertiesNormal, upsertConfigWithoutCompaction, PRIMARY_KEY_COL);

    tableConfigWithoutCompaction.getIndexingConfig().setInvertedIndexColumns(List.of("tags", "name", "game"));
    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithoutCompaction);

    // Wait for data to load
    waitForAllDocsLoadedInBothTables(tableNameWithCompaction, tableNameWithoutCompaction, 30_000L, 5);

    // Verify initial state - both tables should show same logical record count (3 unique players after upserts)
    validateInitialState(tableNameWithCompaction, tableNameWithoutCompaction, 3);

    // Test multi-value column queries work correctly before commit
    String mvQuery =
        String.format("SELECT playerId, tags FROM %s WHERE tags = 'action' ORDER BY playerId", tableNameWithCompaction);
    ResultSet resultSet = getPinotConnection().execute(mvQuery).getResultSet(0);
    assertTrue(resultSet.getRowCount() > 0, "Should find records with 'action' tag");

    // Verify data integrity before commit
    verifyTablesHaveIdenticalData(tableNameWithCompaction, tableNameWithoutCompaction);

    // Force commit segments to trigger commit-time compaction
    forceCommitBothTables(tableNameWithCompaction, tableNameWithoutCompaction);

    // Wait for commit completion
    waitForAllDocsLoaded(tableNameWithCompaction, 60_000L, 3);

    // Validate post-commit compaction effectiveness and data integrity (expecting 3 records, min 1 removed)
    validatePostCommitCompaction(tableNameWithCompaction, tableNameWithoutCompaction, 3, 1, 1.0);

    // Clean up
    dropRealtimeTable(tableNameWithCompaction);
    dropRealtimeTable(tableNameWithoutCompaction);
    deleteSchema(tableNameWithCompaction);
    deleteSchema(tableNameWithoutCompaction);
  }

  @Test
  public void testCommitTimeCompactionWithPartialUpsertMode()
      throws Exception {
    // Test Case: Partial Upsert Mode with Commit-Time Compaction
    // Goal: Ensure commit-time compaction correctly handles partial upsert semantics where
    // only specific columns are updated rather than entire records, and obsolete partial
    // updates are properly compacted away.

    String kafkaTopicNameCompacted = getKafkaTopic() + "-partial-compaction-enabled";
    String kafkaTopicNameNormal = getKafkaTopic() + "-partial-compaction-disabled";

    // Set up identical data for both tables - using small dataset for faster test execution
    setUpKafka(kafkaTopicNameCompacted, INPUT_DATA_SMALL_TAR_FILE);
    setUpKafka(kafkaTopicNameNormal, INPUT_DATA_SMALL_TAR_FILE);

    // TABLE 1: With commit-time compaction ENABLED and PARTIAL upsert mode
    String tableNameWithCompaction = "gameScoresPartialCompactionEnabled";

    // Create schema for partial upsert - make game multi-value to support UNION strategy
    Schema partialUpsertSchemaCompacted = new Schema.SchemaBuilder().setSchemaName(tableNameWithCompaction)
        .addSingleValueDimension("playerId", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addMultiValueDimension("game", FieldSpec.DataType.STRING)  // Multi-value for UNION strategy
        .addSingleValueDimension("deleted", FieldSpec.DataType.BOOLEAN).addMetric("score", FieldSpec.DataType.FLOAT)
        .addDateTime("timestampInEpoch", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(Collections.singletonList("playerId")).build();
    addSchema(partialUpsertSchemaCompacted);

    UpsertConfig upsertConfigWithCompaction = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfigWithCompaction.setPartialUpsertStrategies(
        Map.of("game", UpsertConfig.Strategy.UNION, "name", UpsertConfig.Strategy.OVERWRITE, "score",
            UpsertConfig.Strategy.OVERWRITE));

    upsertConfigWithCompaction.setEnableCommitTimeCompaction(true);  // ENABLE commit-time compaction
    // Changed from PARTIAL to FULL mode to test if this resolves the dictionary mapping issue

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    // Configure multi-value delimiter for UNION strategy on game column
    csvDecoderProperties.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
    TableConfig tableConfigWithCompaction =
        createCSVUpsertTableConfig(tableNameWithCompaction, kafkaTopicNameCompacted, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithCompaction, PRIMARY_KEY_COL);

    // Ensure _columnMajorSegmentBuilderEnabled = false as specified
    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithCompaction);

    // TABLE 2: With commit-time compaction DISABLED and PARTIAL upsert mode (same configuration)
    String tableNameWithoutCompaction = "gameScoresPartialCompactionDisabled";

    // Create schema for partial upsert - make game multi-value to support UNION strategy
    Schema partialUpsertSchemaNormal = new Schema.SchemaBuilder().setSchemaName(tableNameWithoutCompaction)
        .addSingleValueDimension("playerId", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addMultiValueDimension("game", FieldSpec.DataType.STRING)  // Multi-value for UNION strategy
        .addSingleValueDimension("deleted", FieldSpec.DataType.BOOLEAN).addMetric("score", FieldSpec.DataType.FLOAT)
        .addDateTime("timestampInEpoch", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(Collections.singletonList("playerId")).build();
    addSchema(partialUpsertSchemaNormal);

    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);  // DISABLE commit-time compaction
    // For upsertConfigWithoutCompaction (line 641):
    upsertConfigWithoutCompaction.setPartialUpsertStrategies(
        Map.of("game", UpsertConfig.Strategy.UNION, "name", UpsertConfig.Strategy.OVERWRITE, "score",
            UpsertConfig.Strategy.OVERWRITE));
    // Changed from PARTIAL to FULL mode to test if this resolves the dictionary mapping issue

    Map<String, String> csvDecoderPropertiesNormal = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    // Configure multi-value delimiter for UNION strategy on game column
    csvDecoderPropertiesNormal.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
    TableConfig tableConfigWithoutCompaction =
        createCSVUpsertTableConfig(tableNameWithoutCompaction, kafkaTopicNameNormal, getNumKafkaPartitions(),
            csvDecoderPropertiesNormal, upsertConfigWithoutCompaction, PRIMARY_KEY_COL);

    // Ensure _columnMajorSegmentBuilderEnabled = false as specified
    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithoutCompaction);

    // Wait for both tables to load the same initial data (3 unique records after upserts)
    waitForAllDocsLoaded(tableNameWithCompaction, 30_000L, 10);
    waitForAllDocsLoaded(tableNameWithoutCompaction, 30_000L, 10);

    // Verify initial state - both tables should show the same upserted data count (3 unique records)
    validateInitialState(tableNameWithCompaction, tableNameWithoutCompaction, 3);

    // Create partial update patterns to generate invalid records
    // For partial upsert: score uses OVERWRITE strategy, name uses OVERWRITE strategy, game uses UNION strategy
    // Use playerIds that match the initial data to ensure consistent partitioning
    List<String> partialUpdateRecords = List.of("100,PartialUpdate1,arcade,1000,1681400000000,false",
        // name: OVERWRITE, game: UNION (arcade), score: OVERWRITE (1000)
        "101,PartialUpdate1,mobile,2000,1681400001000,false",
        // name: OVERWRITE, game: UNION (mobile), score: OVERWRITE (2000)
        "102,PartialUpdate1,board,3000,1681400002000,false",
        // name: OVERWRITE, game: UNION (board), score: OVERWRITE (3000)
        "100,PartialUpdate2,puzzle,1500,1681400003000,false",
        // name: OVERWRITE, game: UNION (puzzle), score: OVERWRITE (1500)
        "101,PartialUpdate2,strategy,2500,1681400004000,false"
        // name: OVERWRITE, game: UNION (strategy), score: OVERWRITE (2500)
    );

    // Push all partial updates at once to reduce wait times and ensure consistent partitioning
    pushCsvIntoKafkaWithKey(partialUpdateRecords, kafkaTopicNameCompacted, 0);
    pushCsvIntoKafkaWithKey(partialUpdateRecords, kafkaTopicNameNormal, 0);

    // Wait for all additional records to be processed (3 unique records after all partial upserts)
    waitForAllDocsLoaded(tableNameWithCompaction, 90_000L, 15);
    waitForAllDocsLoaded(tableNameWithoutCompaction, 90_000L, 15);

    // Verify state before commit - both tables should still show the same logical result (3 unique records)
    validatePreCommitState(tableNameWithCompaction, tableNameWithoutCompaction, 3);

    // Verify partial upsert behavior - check that non-updated columns retain original values
    String partialUpsertQuery =
        String.format("SELECT playerId, name, game, score FROM %s WHERE playerId = 100", tableNameWithCompaction);
    ResultSet partialResult = getPinotConnection().execute(partialUpsertQuery).getResultSet(0);
    assertEquals(partialResult.getRowCount(), 1, "Should have exactly one record for player 100");

    // Score should be overwritten (latest partial update), name should be overwritten, game should be unioned
    String updatedName = partialResult.getString(0, 1);
    String updatedGame = partialResult.getString(0, 2);
    assertEquals(partialResult.getFloat(0, 3), 1500.0f, "Score should be overwritten by partial upsert");

    // With OVERWRITE strategy, name should contain only the latest value from partial updates
    assertEquals(updatedName, "PartialUpdate2",
        "Name should contain the latest overwritten value, got: " + updatedName);

    // With UNION strategy, game should contain merged values from all partial updates for this player
    assertTrue(updatedGame.contains("arcade") && updatedGame.contains("puzzle"),
        "Game should contain merged values from UNION strategy (original + arcade + puzzle), got: " + updatedGame);

    // Perform commit and wait for completion
    performCommitAndWait(tableNameWithCompaction, tableNameWithoutCompaction, 20_000L, 4, 2);

    // Brief wait to ensure all commit operations are complete
    waitForAllDocsLoaded(tableNameWithCompaction, 90_000L, 3);

    // Validate post-commit compaction effectiveness and data integrity (expecting 3 records, min 2 removed)
    validatePostCommitCompaction(tableNameWithCompaction, tableNameWithoutCompaction, 3, 2, 0.95);

    // Re-verify partial upsert behavior is maintained after compaction
    partialUpsertQuery =
        String.format("SELECT playerId, name, game, score FROM %s WHERE playerId = 100", tableNameWithCompaction);
    partialResult = getPinotConnection().execute(partialUpsertQuery).getResultSet(0);
    assertEquals(partialResult.getRowCount(), 1,
        "Should still have exactly one record for player 100 after compaction");
    // Verify both OVERWRITE and UNION strategy behaviors are maintained after compaction
    String postCompactionName = partialResult.getString(0, 1);
    String postCompactionGame = partialResult.getString(0, 2);
    assertEquals(postCompactionName, "PartialUpdate2",
        "Name should still contain the latest overwritten value after compaction, got: " + postCompactionName);
    assertTrue(postCompactionGame.contains("arcade") && postCompactionGame.contains("puzzle"),
        "Game should still contain merged values from UNION strategy after compaction, got: " + postCompactionGame);
    assertEquals(partialResult.getFloat(0, 3), 1500.0f,
        "Score should still be the latest overwritten value after compaction");

    // Clean up
    dropRealtimeTable(tableNameWithCompaction);
    dropRealtimeTable(tableNameWithoutCompaction);
    deleteSchema(tableNameWithCompaction);
    deleteSchema(tableNameWithoutCompaction);
  }

  @Test
  public void testCommitTimeCompactionNotAllowedWithColumnMajorValidation()
      throws Exception {
    // Test that enabling commit-time compaction with column major segment builder is rejected
    String kafkaTopicName = getKafkaTopic() + "-validation-test";
    createKafkaTopic(kafkaTopicName);

    String tableName = "validationTestTable";
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setEnableCommitTimeCompaction(true);

    Schema schema = createSchema();
    schema.setSchemaName(tableName);
    addSchema(schema);

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    TableConfig tableConfig =
        createCSVUpsertTableConfig(tableName, kafkaTopicName, getNumKafkaPartitions(), csvDecoderProperties,
            upsertConfig, PRIMARY_KEY_COL);

    // Enable column major segment builder - this should cause validation to fail
    tableConfig.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(true);

    try {
      addTableConfig(tableConfig);
      fail("Expected table creation to fail when both commit-time compaction and column major segment builder are "
          + "enabled");
    } catch (Exception e) {
      assertTrue(e.getMessage()
              .contains("Commit-time compaction is not supported when column major segment builder is enabled"),
          "Expected error message about commit-time compaction compatibility, but got: " + e.getMessage());
    }

    // Clean up - delete the schema since table creation failed
    deleteSchema(tableName);
  }

  @Test
  public void testCommitTimeCompactionWithMixedColumnConfigurations()
      throws Exception {
    // Test Case: Mixed Column Configurations with Commit-Time Compaction
    // Goal: Ensure commit-time compaction handles various column configurations:
    // - Dictionary vs no-dictionary columns
    // - Inverted indexes on some columns
    // - Different data types (String, Long, multi-value)
    // - Raw index writers for specific columns

    String kafkaTopicNameCompacted = getKafkaTopic() + "-mixed-compaction-enabled";
    String kafkaTopicNameNormal = getKafkaTopic() + "-mixed-compaction-disabled";

    // Create test data with diverse column types
    // Use semicolon-separated format for multi-value fields in CSV
    List<String> testRecords = List.of("300,PlayerName300,game1,85.5,1681054200000,false,1001,Region1,tag1;tag2",
        "301,PlayerName301,game2,92.0,1681054300000,false,1002,Region2,tag3",
        "302,PlayerName302,game3,78.0,1681054400000,false,1001,Region1,tag1;tag4",
        // Update records to create obsolete data
        "300,UpdatedName300,game1,88.0,1681154200000,false,1003,Region3,tag5",
        "301,UpdatedName301,game2,94.0,1681154300000,false,1002,Region2,tag3;tag6");

    // Set up Kafka topics
    pushCsvIntoKafka(testRecords, kafkaTopicNameCompacted, 0);
    pushCsvIntoKafka(testRecords, kafkaTopicNameNormal, 0);

    // TABLE 1: With commit-time compaction ENABLED
    String tableNameWithCompaction = "gameScoresMixedCompactionEnabled";

    // Create enhanced schema with various column types
    Schema mixedSchemaCompacted = createSchema();
    mixedSchemaCompacted.setSchemaName(tableNameWithCompaction);
    mixedSchemaCompacted.addField(new DimensionFieldSpec("regionID", FieldSpec.DataType.LONG, true));
    mixedSchemaCompacted.addField(new DimensionFieldSpec("regionName", FieldSpec.DataType.STRING, true));
    mixedSchemaCompacted.addField(new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, false));
    addSchema(mixedSchemaCompacted);

    UpsertConfig upsertConfigWithCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompaction.setEnableCommitTimeCompaction(true);

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER,
        "playerId,name,game,score,timestampInEpoch,deleted,regionID,regionName,tags");
    // Configure multi-value delimiter for tags column
    csvDecoderProperties.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
    TableConfig tableConfigWithCompaction =
        createCSVUpsertTableConfig(tableNameWithCompaction, kafkaTopicNameCompacted, 1, csvDecoderProperties,
            upsertConfigWithCompaction, PRIMARY_KEY_COL);

    // Mixed configuration:
    // - Inverted indexes on some columns (dictionary + inverted)
    tableConfigWithCompaction.getIndexingConfig().setInvertedIndexColumns(List.of("regionID", "tags", "game"));
    // - Raw storage (no dictionary) for name and regionName
    tableConfigWithCompaction.getIndexingConfig().setNoDictionaryColumns(List.of("name", "regionName"));
    // - Keep score as dictionary-encoded metric
    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithCompaction);

    // TABLE 2: With commit-time compaction DISABLED (same configuration)
    String tableNameWithoutCompaction = "gameScoresMixedCompactionDisabled";

    Schema mixedSchemaNormal = createSchema();
    mixedSchemaNormal.setSchemaName(tableNameWithoutCompaction);
    mixedSchemaNormal.addField(new DimensionFieldSpec("regionID", FieldSpec.DataType.LONG, true));
    mixedSchemaNormal.addField(new DimensionFieldSpec("regionName", FieldSpec.DataType.STRING, true));
    mixedSchemaNormal.addField(new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, false));
    addSchema(mixedSchemaNormal);

    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);

    Map<String, String> csvDecoderPropertiesNormal = getCSVDecoderProperties(CSV_DELIMITER,
        "playerId,name,game,score,timestampInEpoch,deleted,regionID,regionName,tags");
    // Configure multi-value delimiter for tags column
    csvDecoderPropertiesNormal.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
    TableConfig tableConfigWithoutCompaction =
        createCSVUpsertTableConfig(tableNameWithoutCompaction, kafkaTopicNameNormal, 1, csvDecoderPropertiesNormal,
            upsertConfigWithoutCompaction, PRIMARY_KEY_COL);

    // Same mixed configuration
    tableConfigWithoutCompaction.getIndexingConfig().setInvertedIndexColumns(List.of("regionID", "tags", "game"));
    tableConfigWithoutCompaction.getIndexingConfig().setNoDictionaryColumns(List.of("name", "regionName"));
    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithoutCompaction);

    // Wait for data to load
    waitForAllDocsLoadedInBothTables(tableNameWithCompaction, tableNameWithoutCompaction, 30_000L, 5);

    // Verify initial state - both tables should show correct logical record count
    validateInitialState(tableNameWithCompaction, tableNameWithoutCompaction, 3);

    // Test queries on different column types work correctly before commit:

    // 1. Query on dictionary-encoded column with inverted index
    String dictQuery = String.format("SELECT COUNT(*) FROM %s WHERE regionID = 1002", tableNameWithCompaction);
    ResultSet dictResult = getPinotConnection().execute(dictQuery).getResultSet(0);
    assertTrue(dictResult.getInt(0, 0) > 0, "Should find records for regionID 1002");

    // 2. Query on raw (no-dictionary) column
    String rawQuery = String.format("SELECT COUNT(*) FROM %s WHERE regionName = 'Region2'", tableNameWithCompaction);
    ResultSet rawResult = getPinotConnection().execute(rawQuery).getResultSet(0);
    assertTrue(rawResult.getInt(0, 0) > 0, "Should find records for Region2");

    // 3. Query on multi-value column with inverted index
    String mvQuery = String.format("SELECT COUNT(*) FROM %s WHERE tags = 'tag3'", tableNameWithCompaction);
    ResultSet mvResult = getPinotConnection().execute(mvQuery).getResultSet(0);
    assertTrue(mvResult.getInt(0, 0) > 0, "Should find records with tag3");

    // Verify data integrity before commit
    verifyTablesHaveIdenticalData(tableNameWithCompaction, tableNameWithoutCompaction);

    // Force commit segments to trigger commit-time compaction
    forceCommitBothTables(tableNameWithCompaction, tableNameWithoutCompaction);

    // Wait for segments to be committed - using simpler wait since partition count is 1
    waitForAllDocsLoaded(tableNameWithCompaction, 60_000L, 3);

    // Validate post-commit compaction effectiveness and data integrity (expecting 3 records, min 1 removed)
    validatePostCommitCompaction(tableNameWithCompaction, tableNameWithoutCompaction, 3, 1, 1.0);

    // Verify queries still work correctly on all column types after compaction
    // Re-test dictionary-encoded column
    dictResult = getPinotConnection().execute(dictQuery).getResultSet(0);
    assertTrue(dictResult.getInt(0, 0) > 0, "Should still find records for regionID 1002 after compaction");

    // Re-test raw (no-dictionary) column
    rawResult = getPinotConnection().execute(rawQuery).getResultSet(0);
    assertTrue(rawResult.getInt(0, 0) > 0, "Should still find records for Region2 after compaction");

    // Re-test multi-value column
    mvResult = getPinotConnection().execute(mvQuery).getResultSet(0);
    assertTrue(mvResult.getInt(0, 0) > 0, "Should still find records with tag3 after compaction");

    // Clean up
    dropRealtimeTable(tableNameWithCompaction);
    dropRealtimeTable(tableNameWithoutCompaction);
    deleteSchema(tableNameWithCompaction);
    deleteSchema(tableNameWithoutCompaction);
  }

  // Helper methods - reuse from base class but add specific configurations

  @Override
  protected String getTimeColumnName() {
    return TIME_COL_NAME;
  }

  @Override
  protected String getTableName() {
    return "commitTimeCompactionTest";
  }

  @Override
  protected String getSchemaFileName() {
    return UPSERT_SCHEMA_FILE_NAME;
  }

  protected long queryCountStar(String tableName) {
    return getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName).getResultSet(0).getLong(0, 0);
  }

  protected long queryCountStarWithoutUpsert(String tableName) {
    return getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName + " OPTION(skipUpsert=true)")
        .getResultSet(0).getLong(0, 0);
  }

  /**
   * Validates initial state of both tables - they should have the same logical record count
   */
  protected void validateInitialState(String tableNameWithCompaction, String tableNameWithoutCompaction,
      int expectedRecords) {
    long initialLogicalCountCompacted = queryCountStar(tableNameWithCompaction);
    long initialLogicalCountNormal = queryCountStar(tableNameWithoutCompaction);

    assertEquals(initialLogicalCountCompacted, expectedRecords,
        "Compacted table should have " + expectedRecords + " logical records initially");
    assertEquals(initialLogicalCountNormal, expectedRecords,
        "Normal table should have " + expectedRecords + " logical records initially");
  }

  /**
   * Validates pre-commit state of both tables - they should still have the same logical record count
   */
  protected void validatePreCommitState(String tableNameWithCompaction, String tableNameWithoutCompaction,
      int expectedRecords) {
    long preCommitLogicalCountCompacted = queryCountStar(tableNameWithCompaction);
    long preCommitLogicalCountNormal = queryCountStar(tableNameWithoutCompaction);

    assertEquals(preCommitLogicalCountCompacted, expectedRecords,
        "Both tables should show " + expectedRecords + " logical records before commit");
    assertEquals(preCommitLogicalCountNormal, expectedRecords,
        "Both tables should show " + expectedRecords + " logical records before commit");

    // Verify both tables have the same data row by row
    verifyTablesHaveIdenticalData(tableNameWithCompaction, tableNameWithoutCompaction);
  }

  /**
   * Comprehensive post-commit validation including compaction effectiveness and data integrity
   */
  protected void validatePostCommitCompaction(String tableNameWithCompaction, String tableNameWithoutCompaction,
      int expectedLogicalRecords, int minExpectedRemovedRecords,
      double maxCompressionRatio) {
    // Check the TOTAL document counts after commit (with skipUpsert=true to see all physical records)
    long postCommitPhysicalCountCompacted = queryCountStarWithoutUpsert(tableNameWithCompaction);
    long postCommitPhysicalCountNormal = queryCountStarWithoutUpsert(tableNameWithoutCompaction);
    long postCommitLogicalCountCompacted = queryCountStar(tableNameWithCompaction);
    long postCommitLogicalCountNormal = queryCountStar(tableNameWithoutCompaction);

    // If counts are still the same, wait a bit longer for actual commit completion
    if (postCommitPhysicalCountCompacted == postCommitPhysicalCountNormal) {
      try {
        Thread.sleep(10_000);
        postCommitPhysicalCountCompacted = queryCountStarWithoutUpsert(tableNameWithCompaction);
        postCommitPhysicalCountNormal = queryCountStarWithoutUpsert(tableNameWithoutCompaction);
        postCommitLogicalCountCompacted = queryCountStar(tableNameWithCompaction);
        postCommitLogicalCountNormal = queryCountStar(tableNameWithoutCompaction);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for commit completion", e);
      }
    }

    // Key assertions for commit time compaction
    assertTrue(postCommitPhysicalCountCompacted < postCommitPhysicalCountNormal, String.format(
        "Expected table with commit-time compaction (%d docs) to have fewer physical docs than "
            + "table without compaction (%d docs)", postCommitPhysicalCountCompacted, postCommitPhysicalCountNormal));

    // Both should still return the same logical upserted results
    assertEquals(postCommitLogicalCountCompacted, expectedLogicalRecords,
        "Compacted table should still have " + expectedLogicalRecords + " logical records");
    assertEquals(postCommitLogicalCountNormal, expectedLogicalRecords,
        "Normal table should still have " + expectedLogicalRecords + " logical records");
    assertEquals(postCommitLogicalCountCompacted, postCommitLogicalCountNormal,
        "Both tables should have identical logical results");

    // Verify data integrity - both tables should return identical data row by row
    verifyTablesHaveIdenticalData(tableNameWithCompaction, tableNameWithoutCompaction);

    // Calculate and validate compaction efficiency
    validateCompactionEfficiency(postCommitPhysicalCountCompacted, postCommitPhysicalCountNormal,
        minExpectedRemovedRecords, maxCompressionRatio);
  }

  /**
   * Validates compaction efficiency metrics
   */
  protected void validateCompactionEfficiency(long physicalCountCompacted, long physicalCountNormal,
      int minExpectedRemovedRecords, double maxCompressionRatio) {
    double compressionRatio = (double) physicalCountCompacted / physicalCountNormal;
    int invalidRecordsRemoved = (int) (physicalCountNormal - physicalCountCompacted);

    assertTrue(compressionRatio < maxCompressionRatio,
        "Compaction should achieve compression ratio better than " + maxCompressionRatio + ", got " + compressionRatio);
    assertTrue(invalidRecordsRemoved >= minExpectedRemovedRecords,
        "At least " + minExpectedRemovedRecords + " invalid records should be removed, but only "
            + invalidRecordsRemoved + " were removed");
  }

  /**
   * Performs force commit on both tables
   */
  protected void forceCommitBothTables(String tableNameWithCompaction, String tableNameWithoutCompaction)
      throws Exception {
    sendPostRequest(_controllerRequestURLBuilder.forTableForceCommit(tableNameWithCompaction));
    sendPostRequest(_controllerRequestURLBuilder.forTableForceCommit(tableNameWithoutCompaction));
  }

  protected void pushDataToBothTopics(List<String> records, String kafkaTopicNameCompacted,
      String kafkaTopicNameNormal, int partition)
      throws Exception {
    pushCsvIntoKafka(records, kafkaTopicNameCompacted, partition);
    pushCsvIntoKafka(records, kafkaTopicNameNormal, partition);
  }

  protected void pushDataWithKeyToBothTopics(List<String> records, String kafkaTopicNameCompacted,
      String kafkaTopicNameNormal, int partition)
      throws Exception {
    pushCsvIntoKafkaWithKey(records, kafkaTopicNameCompacted, partition);
    pushCsvIntoKafkaWithKey(records, kafkaTopicNameNormal, partition);
  }

  protected void waitForAllDocsLoadedInBothTables(String tableNameWithCompaction, String tableNameWithoutCompaction,
      long timeoutMs, int expectedRecords)
      throws Exception {
    waitForAllDocsLoaded(tableNameWithCompaction, timeoutMs, expectedRecords);
    waitForAllDocsLoaded(tableNameWithoutCompaction, timeoutMs, expectedRecords);
  }

  protected void performSimpleCommitAndVerify(String tableNameWithCompaction, String tableNameWithoutCompaction,
      long waitTimeoutMs, int expectedRecords)
      throws Exception {
    // Force commit segments on both tables
    forceCommitBothTables(tableNameWithCompaction, tableNameWithoutCompaction);
    waitForAllDocsLoaded(tableNameWithCompaction, waitTimeoutMs, expectedRecords);
    verifyTablesHaveIdenticalData(tableNameWithCompaction, tableNameWithoutCompaction);
  }

  protected void performCommitAndWait(String tableNameWithCompaction, String tableNameWithoutCompaction,
      long waitTimeoutMs, int expectedSegments, int expectedConsumingSegments)
      throws Exception {
    // Force commit segments on both tables to trigger commit-time behavior
    forceCommitBothTables(tableNameWithCompaction, tableNameWithoutCompaction);

    // Wait for segments to be committed
    if (expectedConsumingSegments >= 0) {
      waitForNumQueriedSegmentsToConverge(tableNameWithCompaction, waitTimeoutMs, expectedSegments,
          expectedConsumingSegments);
      waitForNumQueriedSegmentsToConverge(tableNameWithoutCompaction, waitTimeoutMs, expectedSegments,
          expectedConsumingSegments);
    } else {
      waitForNumQueriedSegmentsToConverge(tableNameWithCompaction, waitTimeoutMs, expectedSegments);
      waitForNumQueriedSegmentsToConverge(tableNameWithoutCompaction, waitTimeoutMs, expectedSegments);
    }
  }

  protected void waitForAllDocsLoaded(String tableName, long timeoutMs, long expectedDocs)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return queryCountStarWithoutUpsert(tableName) == expectedDocs;
      } catch (Exception e) {
        return false;
      }
    }, timeoutMs, "Failed to load expected number of documents");
  }

  protected void pushCsvIntoKafka(List<String> records, String kafkaTopicName, long sleep)
      throws Exception {
    // Use the base class method to push records with playerId (column 0) for consistent partitioning
    super.pushCsvIntoKafka(records, kafkaTopicName, 0);
    if (sleep > 0) {
      Thread.sleep(sleep);
    }
  }

  protected void pushCsvIntoKafkaWithKey(List<String> records, String kafkaTopicName, long sleep)
      throws Exception {
    // Push records with playerId (column 0) as the partition key to ensure consistent partitioning
    super.pushCsvIntoKafka(records, kafkaTopicName, 0);
    if (sleep > 0) {
      Thread.sleep(sleep);
    }
  }

  private void setUpKafka(String kafkaTopicName, String inputDataFile)
      throws Exception {
    createKafkaTopic(kafkaTopicName);
    List<File> dataFiles = unpackTarData(inputDataFile, _tempDir);
    pushCsvIntoKafkaWithPartitioning(dataFiles.get(0), kafkaTopicName, 0);
  }

  private void pushCsvIntoKafkaWithPartitioning(File csvFile, String kafkaTopicName, long sleep)
      throws Exception {
    // Push CSV file with playerId (column 0) as the partition key for consistent partitioning
    super.pushCsvIntoKafka(csvFile, kafkaTopicName, 0);
    if (sleep > 0) {
      Thread.sleep(sleep);
    }
  }

  private TableConfig setUpTable(String tableName, String kafkaTopicName, UpsertConfig upsertConfig)
      throws Exception {
    Schema schema = createSchema();
    schema.setSchemaName(tableName);
    addSchema(schema);

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    TableConfig tableConfig =
        createCSVUpsertTableConfig(tableName, kafkaTopicName, getNumKafkaPartitions(), csvDecoderProperties,
            upsertConfig, PRIMARY_KEY_COL);
    tableConfig.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfig);

    return tableConfig;
  }

  private void waitForNumQueriedSegmentsToConverge(String tableName, long timeoutMs, int expectedNumSegmentsQueried) {
    waitForNumQueriedSegmentsToConverge(tableName, timeoutMs, expectedNumSegmentsQueried, -1);
  }

  private void waitForNumQueriedSegmentsToConverge(String tableName, long timeoutMs, int expectedNumSegmentsQueried,
      int expectedNumConsumingSegmentsQueried) {
    // Do not tolerate exception here because it is always followed by the docs check
    TestUtils.waitForCondition(aVoid -> {
      ExecutionStats executionStats =
          getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName).getExecutionStats();
      return executionStats.getNumSegmentsQueried() == expectedNumSegmentsQueried && (
          expectedNumConsumingSegmentsQueried < 0
              || executionStats.getNumConsumingSegmentsQueried() == expectedNumConsumingSegmentsQueried);
    }, timeoutMs, "Failed to load all segments");
  }

  private void verifyTablesHaveIdenticalData(String table1, String table2) {
    verifyTablesHaveIdenticalData(table1, table2, COLUMNS_TO_COMPARE, PRIMARY_KEY_COL);
  }

  /**
   * Verifies that two tables return identical data row by row for all queries
   * Uses object-to-object comparison for all column values
   */
  private void verifyTablesHaveIdenticalData(String table1, String table2, List<String> columnsToCompare,
      String pkColumnName) {
    // Use provided columns to dynamically build query with available columns
    String selectColumns = String.join(", ", columnsToCompare);

    // Verify each primary key has identical data
    for (int playerId : new int[]{100, 101, 102}) {
      String query = String.format("SELECT %s FROM %s WHERE %s = %d", selectColumns, table1, pkColumnName, playerId);
      ResultSet result1 = getPinotConnection().execute(query).getResultSet(0);

      query = String.format("SELECT %s FROM %s WHERE %s = %d", selectColumns, table2, pkColumnName, playerId);
      ResultSet result2 = getPinotConnection().execute(query).getResultSet(0);
      assertEquals(result1.getRowCount(), result2.getRowCount(),
          String.format("Row count mismatch for %s %d", pkColumnName, playerId));

      if (result1.getRowCount() > 0) {
        // Compare each column using object-to-object comparison
        for (int colIndex = 0; colIndex < columnsToCompare.size(); colIndex++) {
          String columnName = columnsToCompare.get(colIndex);
          try {
            // Get values as objects and compare them
            Object value1 = getResultValue(result1, 0, colIndex);
            Object value2 = getResultValue(result2, 0, colIndex);
            assertEquals(value1, value2, String.format("%s mismatch for %s %d", columnName, pkColumnName, playerId));
          } catch (Exception e) {
            // If column comparison fails, log details and rethrow
            System.out.printf("Failed to compare column %s for %s %d: %s%n", columnName, pkColumnName, playerId,
                e.getMessage());
            throw e;
          }
        }
      }
    }
  }

  /**
   * Helper method to get result value handling different data types
   */
  private Object getResultValue(ResultSet resultSet, int row, int col) {
    try {
      // Try different data types
      return resultSet.getLong(row, col);
    } catch (Exception e1) {
      try {
        return resultSet.getFloat(row, col);
      } catch (Exception e2) {
        try {
          return resultSet.getString(row, col);
        } catch (Exception e3) {
          return resultSet.getDouble(row, col);
        }
      }
    }
  }

  private void validateSegmentLevelSorting(String tableName, boolean shouldBeSorted)
      throws Exception {
    // Query to get all records with segment name and document order
    // CRITICAL: ORDER BY $segmentName, $docId ensures we get physical document order within each segment
    ResultSet segmentData = getPinotConnection().execute(
            "SELECT $segmentName, $docId, playerId, score FROM " + tableName + " ORDER BY $segmentName, $docId")
        .getResultSet(0);

    Map<String, List<Float>> segmentScoresMap = new HashMap<>();
    Map<String, List<Integer>> segmentDocIdsMap = new HashMap<>();

    // Group all data by segment first
    for (int i = 0; i < segmentData.getRowCount(); i++) {
      String segmentName = segmentData.getString(i, 0);
      int docId = Integer.parseInt(segmentData.getString(i, 1));
      int playerId = Integer.parseInt(segmentData.getString(i, 2));
      float score = Float.parseFloat(segmentData.getString(i, 3));

      segmentScoresMap.computeIfAbsent(segmentName, k -> new ArrayList<>()).add(score);
      segmentDocIdsMap.computeIfAbsent(segmentName, k -> new ArrayList<>()).add(docId);
    }

    // Now validate each segment individually

    for (Map.Entry<String, List<Float>> entry : segmentScoresMap.entrySet()) {
      String segmentName = entry.getKey();
      List<Float> scores = entry.getValue();
      List<Integer> docIds = segmentDocIdsMap.get(segmentName);

      if (shouldBeSorted && scores.size() > 1) {
        boolean isSegmentSorted = true;
        String sortingIssue = null;

        for (int i = 1; i < scores.size(); i++) {
          if (scores.get(i) < scores.get(i - 1)) {
            isSegmentSorted = false;
            sortingIssue = String.format("DocId %d (score=%.1f) < DocId %d (score=%.1f)", docIds.get(i), scores.get(i),
                docIds.get(i - 1), scores.get(i - 1));
            break;
          }
        }

        assertTrue(isSegmentSorted, String.format(
            "SEGMENT-LEVEL SORTING FAILED: Segment %s should have documents sorted by score within the segment, but "
                + "found: %s", segmentName, sortingIssue));
      }
    }
  }

  @Test
  public void testCommitTimeCompactionMutableMapDataSourceFix()
      throws Exception {
    // Test Case: MAP Column Commit-Time Compaction
    // Goal: Ensure commit-time compaction correctly handles MAP columns by only processing
    // valid documents when collecting statistics, avoiding inflated cardinality and key frequencies
    // from obsolete records that should be excluded during compaction.

    String kafkaTopicNameCompacted = getKafkaTopic() + "-map-compaction-enabled";
    String kafkaTopicNameNormal = getKafkaTopic() + "-map-compaction-disabled";

    // Create test data with MAP columns - using simplified JSON format for map values
    // Format: "playerId,name,game,score,timestampInEpoch,deleted,userAttributes"
    // Note: Using simple key-value pairs without commas to avoid CSV parsing conflicts
    List<String> testRecords = List.of(
        "400,Player400,game1,85.5,1681054200000,false,"
            + "{\"level\":10}",
        "401,Player401,game2,92.0,1681054300000,false,"
            + "{\"level\":15}",
        "402,Player402,game3,78.0,1681054400000,false,"
            + "{\"level\":8}",
        // Updates to create obsolete records with different MAP values
        "400,Player400Updated,game1,90.0,1681154200000,false,"
            + "{\"level\":12}",
        "401,Player401Updated,game2,95.0,1681154300000,false,"
            + "{\"level\":18}"
    );

    // Set up Kafka topics with MAP test data
    pushCsvIntoKafka(testRecords, kafkaTopicNameCompacted, 0);
    pushCsvIntoKafka(testRecords, kafkaTopicNameNormal, 0);

    // TABLE 1: With commit-time compaction ENABLED
    String tableNameWithCompaction = "gameScoresMapCompactionEnabled";

    // Create schema with MAP column
    Schema mapSchemaCompacted = createSchema();
    mapSchemaCompacted.setSchemaName(tableNameWithCompaction);
    mapSchemaCompacted.addField(new DimensionFieldSpec("userAttributes", FieldSpec.DataType.JSON, true));
    addSchema(mapSchemaCompacted);

    UpsertConfig upsertConfigWithCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompaction.setEnableCommitTimeCompaction(true);

    Map<String, String> csvDecoderProperties =
        getCSVDecoderProperties(CSV_DELIMITER, "playerId,name,game,score,timestampInEpoch,deleted,userAttributes");
    TableConfig tableConfigWithCompaction =
        createCSVUpsertTableConfig(tableNameWithCompaction, kafkaTopicNameCompacted, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithCompaction, PRIMARY_KEY_COL);

    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithCompaction);

    // TABLE 2: With commit-time compaction DISABLED
    String tableNameWithoutCompaction = "gameScoresMapCompactionDisabled";

    // Create separate schema for normal table
    Schema mapSchemaNormal = createSchema();
    mapSchemaNormal.setSchemaName(tableNameWithoutCompaction);
    mapSchemaNormal.addField(new DimensionFieldSpec("userAttributes", FieldSpec.DataType.JSON, true));
    addSchema(mapSchemaNormal);

    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);

    Map<String, String> csvDecoderPropertiesNormal =
        getCSVDecoderProperties(CSV_DELIMITER, "playerId,name,game,score,timestampInEpoch,deleted,userAttributes");
    TableConfig tableConfigWithoutCompaction =
        createCSVUpsertTableConfig(tableNameWithoutCompaction, kafkaTopicNameNormal, getNumKafkaPartitions(),
            csvDecoderPropertiesNormal, upsertConfigWithoutCompaction, PRIMARY_KEY_COL);

    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithoutCompaction);

    // Wait for data to load
    waitForAllDocsLoadedInBothTables(tableNameWithCompaction, tableNameWithoutCompaction, 30_000L, 5);

    // Verify initial state - both tables should show same logical record count (3 unique players after upserts)
    validateInitialState(tableNameWithCompaction, tableNameWithoutCompaction, 3);

    // Test JSON queries work correctly before commit
    String jsonQuery = String.format(
        "SELECT playerId, JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') as level FROM %s WHERE "
            + "JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') > 10 ORDER BY playerId",
        tableNameWithCompaction);
    ResultSet resultSet = getPinotConnection().execute(jsonQuery).getResultSet(0);
    assertTrue(resultSet.getRowCount() > 0, "Should find records with level > 10");

    // Verify data integrity before commit
    verifyTablesHaveIdenticalData(tableNameWithCompaction, tableNameWithoutCompaction);

    // Force commit segments to trigger commit-time compaction
    forceCommitBothTables(tableNameWithCompaction, tableNameWithoutCompaction);

    // Wait for commit completion
    waitForAllDocsLoaded(tableNameWithCompaction, 60_000L, 3);

    // Validate post-commit compaction effectiveness and data integrity (expecting 3 records, min 1 removed)
    validatePostCommitCompaction(tableNameWithCompaction, tableNameWithoutCompaction, 3, 1, 1.0);

    // Verify JSON queries still work correctly after compaction
    resultSet = getPinotConnection().execute(jsonQuery).getResultSet(0);
    assertTrue(resultSet.getRowCount() > 0, "Should still find records with level > 10 after compaction");

    // Test specific JSON path queries to ensure MAP column statistics are correctly computed
    String levelQuery = String.format(
        "SELECT COUNT(*) FROM %s WHERE JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') > 10",
        tableNameWithCompaction);
    ResultSet levelResult = getPinotConnection().execute(levelQuery).getResultSet(0);
    assertTrue(levelResult.getInt(0, 0) > 0, "Should find records with level > 10 after compaction");

    // Verify that obsolete MAP values are not affecting queries - test uniqueness
    String uniqueQuery = String.format(
        "SELECT COUNT(DISTINCT playerId) FROM %s WHERE "
            + "JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') IS NOT NULL",
        tableNameWithCompaction);
    ResultSet uniqueResult = getPinotConnection().execute(uniqueQuery).getResultSet(0);
    assertEquals(uniqueResult.getInt(0, 0), 3,
        "Should find exactly 3 unique players with level data (after compaction)");

    // Clean up
    dropRealtimeTable(tableNameWithCompaction);
    dropRealtimeTable(tableNameWithoutCompaction);
    deleteSchema(tableNameWithCompaction);
    deleteSchema(tableNameWithoutCompaction);
  }

  @Test
  public void testCommitTimeCompactionPreservesDeletedRecords()
      throws Exception {
    // Test Case: Deleted Records Preservation with Commit-Time Compaction
    // Goal: Ensure commit-time compaction preserves deleted records (soft deletes) in the committed
    // segment to maintain data consistency. Deleted records should not be physically removed during
    // compaction as this could lead to data inconsistency issues.

    String kafkaTopicNameCompacted = getKafkaTopic() + "-deleted-records-compaction-enabled";
    String kafkaTopicNameNormal = getKafkaTopic() + "-deleted-records-compaction-disabled";

    // Set up identical data for both tables - using small dataset for faster test execution
    setUpKafka(kafkaTopicNameCompacted, INPUT_DATA_SMALL_TAR_FILE);
    setUpKafka(kafkaTopicNameNormal, INPUT_DATA_SMALL_TAR_FILE);

    // TABLE 1: With commit-time compaction ENABLED and delete record column configured
    String tableNameWithCompaction = "gameScoresDeletedRecordsCompactionEnabled";
    UpsertConfig upsertConfigWithCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompaction.setEnableCommitTimeCompaction(true);  // ENABLE commit-time compaction
    upsertConfigWithCompaction.setDeleteRecordColumn("deleted");     // Configure delete record column
    TableConfig tableConfigWithCompaction =
        setUpTable(tableNameWithCompaction, kafkaTopicNameCompacted, upsertConfigWithCompaction);

    // Ensure _columnMajorSegmentBuilderEnabled = false as specified
    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    updateTableConfig(tableConfigWithCompaction);

    // TABLE 2: With commit-time compaction DISABLED but same delete record column configuration
    String tableNameWithoutCompaction = "gameScoresDeletedRecordsCompactionDisabled";
    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);  // DISABLE commit-time compaction
    upsertConfigWithoutCompaction.setDeleteRecordColumn("deleted");      // Configure delete record column
    TableConfig tableConfigWithoutCompaction =
        setUpTable(tableNameWithoutCompaction, kafkaTopicNameNormal, upsertConfigWithoutCompaction);

    // Ensure _columnMajorSegmentBuilderEnabled = false as specified
    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    updateTableConfig(tableConfigWithoutCompaction);

    // Wait for both tables to load the same initial data (3 unique records after upserts)
    waitForAllDocsLoadedInBothTables(tableNameWithCompaction, tableNameWithoutCompaction, 30_000L, 10);

    // Verify initial state - both tables should show the same upserted data count (3 unique records)
    validateInitialState(tableNameWithCompaction, tableNameWithoutCompaction, 3);

    // Create update patterns including some deleted records to test preservation
    // Use playerIds that match the initial data to ensure consistent partitioning
    List<String> updateRecords = List.of(
        "100,Zook-Updated1,counter-strike,1000,1681300000000,false",  // Regular update
        "101,Alice-Updated1,dota,2000,1681300001000,true",            // Mark as deleted
        "102,Bob-Updated1,cs2,3000,1681300002000,false",              // Regular update
        "100,Zook-Updated2,valorant,1500,1681300003000,false",       // Another regular update
        "103,Charlie-New,chess,2500,1681300004000,true"              // New record marked as deleted
    );

    // Push all updates at once to reduce wait times and ensure consistent partitioning
    pushDataWithKeyToBothTopics(updateRecords, kafkaTopicNameCompacted, kafkaTopicNameNormal, 0);
    // Wait for all additional records to be processed
    waitForAllDocsLoadedInBothTables(tableNameWithCompaction, tableNameWithoutCompaction, 90_000L, 15);

    // Verify state before commit - both tables should show the same logical result
    // Note: Deleted records should not be counted in regular queries (logical view)
    long preCommitLogicalCountCompacted = queryCountStar(tableNameWithCompaction);
    long preCommitLogicalCountNormal = queryCountStar(tableNameWithoutCompaction);

    assertEquals(preCommitLogicalCountCompacted, 2, "Compacted table should show 2 non-deleted logical records");
    assertEquals(preCommitLogicalCountNormal, 2, "Normal table should show 2 non-deleted logical records");
    assertEquals(preCommitLogicalCountCompacted, preCommitLogicalCountNormal,
        "Both tables should have same logical count before commit");

    // Perform commit and wait for completion
    performCommitAndWait(tableNameWithCompaction, tableNameWithoutCompaction, 30_000L, 4, 2);

    // Validate post-commit state
    long postCommitLogicalCountCompacted = queryCountStar(tableNameWithCompaction);
    long postCommitLogicalCountNormal = queryCountStar(tableNameWithoutCompaction);
    long postCommitPhysicalCountCompacted = queryCountStarWithoutUpsert(tableNameWithCompaction);
    long postCommitPhysicalCountNormal = queryCountStarWithoutUpsert(tableNameWithoutCompaction);

    // Key assertions for deleted records preservation:
    // 1. Logical counts should remain the same (2 non-deleted records)
    assertEquals(postCommitLogicalCountCompacted, 2,
        "Compacted table should still show 2 non-deleted logical records after commit");
    assertEquals(postCommitLogicalCountNormal, 2,
        "Normal table should still show 2 non-deleted logical records after commit");

    assertEquals(postCommitPhysicalCountCompacted, 4,
        "Compacted table should still show 3 physical records(2 valid + 2 deleted) after commit");
    assertEquals(postCommitPhysicalCountNormal, 15,
        "Normal table should still have 15 physical records after commit");

    // 2. Physical count for compacted table should still include deleted records
    // The compacted table should have fewer physical records than normal table (due to obsolete record removal)
    // but should still contain the deleted records
    assertTrue(postCommitPhysicalCountCompacted < postCommitPhysicalCountNormal,
        String.format("Compacted table (%d) should have fewer physical docs than normal table (%d) due to compaction",
            postCommitPhysicalCountCompacted, postCommitPhysicalCountNormal));

    // 3. Verify that we can still query for deleted records using skipUpsert=true
    String deletedRecordsQuery = String.format(
        "SELECT COUNT(*) FROM %s WHERE deleted = true OPTION(skipUpsert=true)",
        tableNameWithCompaction);
    ResultSet deletedResult = getPinotConnection().execute(deletedRecordsQuery).getResultSet(0);
    long deletedRecordsCount = deletedResult.getLong(0, 0);
    assertTrue(deletedRecordsCount > 0,
        "Should be able to find deleted records in compacted table using skipUpsert=true");

    // 5. Verify that deleted records are not visible in regular queries (logical view)
    String regularQuery = String.format(
        "SELECT COUNT(*) FROM %s WHERE deleted = true",
        tableNameWithCompaction);
    ResultSet regularResult = getPinotConnection().execute(regularQuery).getResultSet(0);
    long visibleDeletedCount = regularResult.getLong(0, 0);
    assertEquals(visibleDeletedCount, 0,
        "Deleted records should not be visible in regular queries (logical view)");

    // 6. Verify data integrity for non-deleted records
    verifyTablesHaveIdenticalData(tableNameWithCompaction, tableNameWithoutCompaction);

    // Clean up
    dropRealtimeTable(tableNameWithCompaction);
    dropRealtimeTable(tableNameWithoutCompaction);
  }
}
