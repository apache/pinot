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
  private String _kafkaTopicName;

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
    // Now includes testing commit-time compaction with column-major segment builder
    String kafkaTopicName = getKafkaTopic() + "-compaction-comparison";
    setUpKafka(kafkaTopicName, INPUT_DATA_SMALL_TAR_FILE);

    // TABLE 1: With commit-time compaction DISABLED (baseline)
    String tableNameWithoutCompaction = "gameScoresCommitTimeCompactionDisabled";
    createUpsertTable(tableNameWithoutCompaction, kafkaTopicName,
        UpsertConfig.Mode.FULL, false, false);

    // TABLE 2: With commit-time compaction ENABLED + row-major build
    String tableNameWithCompaction = "gameScoresCommitTimeCompactionEnabled";
    createUpsertTable(tableNameWithCompaction, kafkaTopicName,
        UpsertConfig.Mode.FULL, true, false);

    // TABLE 3: With commit-time compaction ENABLED + column-major build
    String tableNameWithCompactionColumnMajor = "gameScoresCommitTimeCompactionColumnMajor";
    createUpsertTable(tableNameWithCompactionColumnMajor, kafkaTopicName,
        UpsertConfig.Mode.FULL, true, true);

    // Wait for all three tables to load the same initial data (3 unique records after upserts)
    waitForAllDocsLoaded(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 30_000L, 10);

    // Verify initial state - all three tables should show the same upserted data count (3 unique records)
    validateInitialState(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3);
    List<String> updateRecords = List.of("100,Zook-Updated1,counter-strike,1000,1681300000000,false",
        "101,Alice-Updated1,dota,2000,1681300001000,false", "102,Bob-Updated1,cs2,3000,1681300002000,false",
        "100,Zook-Updated2,valorant,1500,1681300003000,false", "101,Alice-Updated2,lol,2500,1681300004000,false");

    pushCsvIntoKafkaWithKey(updateRecords, kafkaTopicName, 0);

    // Wait for all additional records to be processed (3 unique records after all upserts)
    waitForAllDocsLoaded(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 90_000L, 15);

    // Verify state before commit - all three tables should still show the same logical result (3 unique records)
    validatePreCommitState(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3);

    // Perform commit and wait for completion
    performCommitAndWait(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 30_000L, 4, 2);

    // Validate post-commit compaction effectiveness and data integrity
    validatePostCommitCompaction(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3, 2, 0.95);

    // Clean up
    cleanupTablesAndSchemas(
        List.of(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor),
        List.of(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor));
  }

  @Test
  public void testCommitTimeCompactionWithSortedColumn()
      throws Exception {
    // Test Case 1: Standard and Sorted Data Types with Mixed Primitives
    // Goal: Verify commit-time compaction correctly processes and optimizes segments
    // containing a mix of common primitive data types, specifically including a sorted column.

    // Create schemas for both tables
    String kafkaTopicName = getKafkaTopic() + "-sorted-column";
    setUpKafka(kafkaTopicName, INPUT_DATA_SMALL_TAR_FILE);

    Schema schema = createSchema();
    schema.setSchemaName("sortedCompactionEnabled");
    addSchema(schema);

    Schema schema2 = createSchema();
    schema2.setSchemaName("sortedCompactionDisabled");
    addSchema(schema2);

    Schema schema3 = createSchema();
    schema3.setSchemaName("sortedCompactionEnabledAndColumnMajor");
    addSchema(schema3);

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);

    // TABLE 1: With commit-time compaction ENABLED AND sorted column configured BEFORE data ingestion
    String tableNameWithCompaction = "sortedCompactionEnabled";
    UpsertConfig upsertConfigWithCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompaction.setEnableCommitTimeCompaction(true);

    TableConfig tableConfigWithCompaction =
        createCSVUpsertTableConfig(tableNameWithCompaction, kafkaTopicName, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithCompaction, PRIMARY_KEY_COL);
    tableConfigWithCompaction.getIndexingConfig().setSortedColumn(Collections.singletonList("score"));
    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithCompaction);

    // TABLE 2: With commit-time compaction DISABLED but also with sorted column
    String tableNameWithoutCompaction = "sortedCompactionDisabled";
    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);

    TableConfig tableConfigWithoutCompaction =
        createCSVUpsertTableConfig(tableNameWithoutCompaction, kafkaTopicName, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithoutCompaction, PRIMARY_KEY_COL);
    tableConfigWithoutCompaction.getIndexingConfig().setSortedColumn(Collections.singletonList("score"));
    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithoutCompaction);

    // TABLE 3: With commit-time compaction ENABLED, with Column Major Build AND sorted column configured BEFORE data
    // ingestion
    String tableNameWithCompactionAndColumnMajor = "sortedCompactionEnabledAndColumnMajor";
    TableConfig tableConfigWithCompactionAndColumnMajor =
        createCSVUpsertTableConfig(tableNameWithCompactionAndColumnMajor, kafkaTopicName, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithCompaction, PRIMARY_KEY_COL);
    tableConfigWithCompactionAndColumnMajor.getIndexingConfig().setSortedColumn(Collections.singletonList("score"));
    tableConfigWithCompactionAndColumnMajor.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(true);
    addTableConfig(tableConfigWithCompactionAndColumnMajor);

    // Wait for both tables to load the same initial data (3 unique records after upserts)
    waitForAllDocsLoaded(tableNameWithCompaction, tableNameWithoutCompaction,
        tableNameWithCompactionAndColumnMajor, 30_000L, 10);

    validatePreCommitState(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionAndColumnMajor, 3);

    // Push additional updates using standard format to create more obsolete records
    // Add more diverse data with different primary keys and varying scores for better sorting validation
    List<String> updateRecords = List.of("100,Alice Updated,chess,15.5,1681354200000,false",   // Low score
        "101,Bob Updated,poker,88.0,1681354300000,false",     // High score
        "102,Charlie Updated,chess,45.2,1681354400000,false", // Mid score
        "103,David New,tennis,12.8,1681354500000,false",      // Very low score
        "104,Eva New,golf,78.9,1681354600000,false",          // High score
        "105,Frank New,soccer,33.1,1681354700000,false",       // Mid-low score
        "100,Alice Final,chess,25.7,1681454200000,false",     // Low-mid score
        "101,Bob Final,poker,91.3,1681454300000,false",       // Very high score
        "103,David Final,tennis,8.4,1681454400000,false",     // Very low score
        "104,Eva Final,golf,67.8,1681454500000,false"         // High score
    );

    pushCsvIntoKafka(updateRecords, kafkaTopicName, 0);

    // Wait for all updates to be processed (6 unique records after all upserts)
    waitForAllDocsLoaded(tableNameWithCompaction, tableNameWithoutCompaction,
        tableNameWithCompactionAndColumnMajor, 30_000L, 20);

    // Perform simple commit and verify data identity between tables
    performCommitAndWait(tableNameWithCompaction, tableNameWithoutCompaction,
        tableNameWithCompactionAndColumnMajor, 30_000L, 4, 2);

    // Validate post-commit compaction effectiveness and data integrity (expecting 6 records, min 4 removed)
    validatePostCommitCompaction(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionAndColumnMajor, 6, 4, 1.0);

    // CRITICAL: Validate segment-level sorting
    validateSegmentLevelSorting(tableNameWithCompaction);  // Should be sorted due to sorted column config
    validateSegmentLevelSorting(tableNameWithoutCompaction); // Should NOT be sorted (no sorted column config)
    validateSegmentLevelSorting(
        tableNameWithCompactionAndColumnMajor); // Should NOT be sorted (no sorted column config)

    // Clean up
    cleanupTablesAndSchemas(
        List.of(tableNameWithCompaction, tableNameWithoutCompaction, tableNameWithCompactionAndColumnMajor),
        List.of(tableNameWithCompaction, tableNameWithoutCompaction, tableNameWithCompactionAndColumnMajor));
  }

  @Test
  public void testCommitTimeCompactionWithNoDictionaryColumns()
      throws Exception {
    // Test Case 2: No Dictionary (Raw Index) Configuration
    // Goal: Ensure commit-time compaction handles columns configured without dictionary indices
    // (raw storage) on standard schema columns.

    // TABLE 1: With commit-time compaction ENABLED
    String kafkaTopicName = getKafkaTopic() + "-no-dictonary";
    setUpKafka(kafkaTopicName, INPUT_DATA_SMALL_TAR_FILE);

    String tableNameWithCompaction = "rawIndexCompactionEnabled";
    TableConfig tableConfigWithCompaction = createUpsertTable(tableNameWithCompaction, kafkaTopicName,
        UpsertConfig.Mode.FULL, true, false);
    tableConfigWithCompaction.getIndexingConfig().setNoDictionaryColumns(List.of("name", "game"));
    updateTableConfig(tableConfigWithCompaction);

    // TABLE 2: With commit-time compaction DISABLED
    String tableNameWithoutCompaction = "rawIndexCompactionDisabled";
    TableConfig tableConfigWithoutCompaction = createUpsertTable(tableNameWithoutCompaction, kafkaTopicName,
        UpsertConfig.Mode.FULL, false, false);
    tableConfigWithCompaction.getIndexingConfig().setNoDictionaryColumns(List.of("name", "game"));
    updateTableConfig(tableConfigWithoutCompaction);

    // TABLE 3: With commit-time compaction ENABLED + column-major build
    String tableNameWithCompactionAndColumnMajor = "rawIndexCompactionAndColumnMajor";
    TableConfig tableConfigWithCompactionAndColumnMajor =
        createUpsertTable(tableNameWithCompactionAndColumnMajor, kafkaTopicName,
            UpsertConfig.Mode.FULL, true, true);
    tableConfigWithCompactionAndColumnMajor.getIndexingConfig().setNoDictionaryColumns(List.of("name", "game"));
    updateTableConfig(tableConfigWithCompactionAndColumnMajor);

    // Wait for both tables to load the same initial data (3 unique records after upserts)
    waitForAllDocsLoaded(tableNameWithCompaction, tableNameWithoutCompaction,
        tableNameWithCompactionAndColumnMajor, 60_000L, 10);

    // Verify initial state - both tables should show the same upserted data count (3 unique records)
    validateInitialState(tableNameWithCompaction, tableNameWithoutCompaction,
        tableNameWithCompactionAndColumnMajor, 3);

    // Push additional updates using standard format to test raw index columns
    List<String> updateRecords =
        List.of("100,Alice Raw,chess,95.5,1681354200000,false",
            "101,Bob Raw,poker,88.0,1681354300000,false",
            "102,Charlie Raw,chess,82.5,1681354400000,false",
            "100,Alice NoDict,chess,99.5,1681454200000,false",
            "101,Bob NoDict,poker,91.0,1681454300000,false");

    pushCsvIntoKafka(updateRecords, kafkaTopicName, 0);

    // Wait for all updates to be processed (3 unique records after upserts)
    waitForAllDocsLoaded(tableNameWithCompaction, tableNameWithoutCompaction,
        tableNameWithCompactionAndColumnMajor, 30_000L, 15);

    // Perform simple commit operation
    performCommitAndWait(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionAndColumnMajor, 30_000L, 4, 2);

    // Validate post-commit compaction effectiveness and data integrity (expecting 3 records, min 2 removed)
    validatePostCommitCompaction(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionAndColumnMajor, 3, 2, 1.0);

    // Clean up
    cleanupTablesAndSchemas(
        List.of(tableNameWithCompaction, tableNameWithoutCompaction, tableNameWithCompactionAndColumnMajor),
        List.of(tableNameWithCompaction, tableNameWithoutCompaction, tableNameWithCompactionAndColumnMajor));
  }

  @Test
  public void testCommitTimeCompactionWithMultiValueColumns()
      throws Exception {
    // Test Case: Multi-Value Fields with Commit-Time Compaction
    // Goal: Ensure commit-time compaction correctly handles multi-value dictionary columns
    // (like arrays/lists) during segment conversion, validating the fix for CompactedDictEncodedColumnStatistics

    // Create test data with multi-value fields similar to user's "tags" column
    List<String> testRecords = List.of("200,Player200,game1,85.5,1681054200000,false,action;shooter",
        "201,Player201,game2,92.0,1681054300000,false,strategy;puzzle",
        "202,Player202,game3,78.0,1681054400000,false,rpg;adventure;fantasy",
        // Updates to create obsolete records
        "200,Player200Updated,game1,90.0,1681154200000,false,action;fps",
        "201,Player201Updated,game2,95.0,1681154300000,false,strategy;rts");

    String kafkaTopicName = getKafkaTopic() + "-multivalue";

    // TABLE 1: With commit-time compaction DISABLED (baseline)
    String tableNameWithoutCompaction = "gameScoresMVCompactionDisabled";
    Schema mvSchemaBaseline = createSchema();
    mvSchemaBaseline.setSchemaName(tableNameWithoutCompaction);
    mvSchemaBaseline.addField(new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, false));
    addSchema(mvSchemaBaseline);

    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);

    Map<String, String> csvDecoderProperties =
        getCSVDecoderProperties(CSV_DELIMITER, "playerId,name,game,score,timestampInEpoch,deleted,tags");
    csvDecoderProperties.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
    TableConfig tableConfigWithoutCompaction =
        createCSVUpsertTableConfig(tableNameWithoutCompaction, kafkaTopicName, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithoutCompaction, PRIMARY_KEY_COL);

    tableConfigWithoutCompaction.getIndexingConfig().setInvertedIndexColumns(List.of("tags", "name", "game"));
    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithoutCompaction);

    // TABLE 2: With commit-time compaction ENABLED + row-major build
    String tableNameWithCompaction = "gameScoresMVCompactionEnabled";
    Schema mvSchemaCompacted = createSchema();
    mvSchemaCompacted.setSchemaName(tableNameWithCompaction);
    mvSchemaCompacted.addField(new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, false));
    addSchema(mvSchemaCompacted);

    UpsertConfig upsertConfigWithCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompaction.setEnableCommitTimeCompaction(true);

    Map<String, String> csvDecoderPropertiesCompacted =
        getCSVDecoderProperties(CSV_DELIMITER, "playerId,name,game,score,timestampInEpoch,deleted,tags");
    csvDecoderPropertiesCompacted.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
    TableConfig tableConfigWithCompaction =
        createCSVUpsertTableConfig(tableNameWithCompaction, kafkaTopicName, getNumKafkaPartitions(),
            csvDecoderPropertiesCompacted, upsertConfigWithCompaction, PRIMARY_KEY_COL);
    tableConfigWithCompaction.getIndexingConfig().setInvertedIndexColumns(List.of("tags", "name", "game"));
    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithCompaction);

    // TABLE 3: With commit-time compaction ENABLED + column-major build
    String tableNameWithCompactionColumnMajor = "gameScoresMVCompactionColumnMajor";
    Schema mvSchemaColumnMajor = createSchema();
    mvSchemaColumnMajor.setSchemaName(tableNameWithCompactionColumnMajor);
    mvSchemaColumnMajor.addField(new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, false));
    addSchema(mvSchemaColumnMajor);
    UpsertConfig upsertConfigWithCompactionColumnMajor = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompactionColumnMajor.setEnableCommitTimeCompaction(true);
    Map<String, String> csvDecoderPropertiesColumnMajor =
        getCSVDecoderProperties(CSV_DELIMITER, "playerId,name,game,score,timestampInEpoch,deleted,tags");
    csvDecoderPropertiesColumnMajor.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
    TableConfig tableConfigWithCompactionColumnMajor =
        createCSVUpsertTableConfig(tableNameWithCompactionColumnMajor, kafkaTopicName, getNumKafkaPartitions(),
            csvDecoderPropertiesColumnMajor, upsertConfigWithCompactionColumnMajor, PRIMARY_KEY_COL);
    tableConfigWithCompactionColumnMajor.getIndexingConfig().setInvertedIndexColumns(List.of("tags", "name", "game"));
    tableConfigWithCompactionColumnMajor.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(true);
    addTableConfig(tableConfigWithCompactionColumnMajor);

    pushCsvIntoKafka(testRecords, kafkaTopicName, 0);
    waitForAllDocsLoaded(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 30_000L, 5);

    // Verify initial state - all three tables should show same logical record count (3 unique players after upserts)
    validateInitialState(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3);

    // Force commit segments to trigger commit-time compaction
    forceCommit(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor);

    // Wait for commit completion
    waitForAllDocsLoaded(tableNameWithCompaction, 60_000L, 3);
    waitForAllDocsLoaded(tableNameWithCompactionColumnMajor, 60_000L, 3);

    // Validate post-commit compaction effectiveness and data integrity (expecting 3 records, min 1 removed)
    validatePostCommitCompaction(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3, 1, 1.0);

    // Clean up
    cleanupTablesAndSchemas(
        List.of(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor),
        List.of(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor));
  }

  @Test
  public void testCommitTimeCompactionWithPartialUpsertMode()
      throws Exception {
    // Test Case: Partial Upsert Mode with Commit-Time Compaction
    // Goal: Ensure commit-time compaction correctly handles partial upsert semantics where
    // only specific columns are updated rather than entire records, and obsolete partial
    // updates are properly compacted away.

    // TABLE 1: With commit-time compaction DISABLED and PARTIAL upsert mode (baseline)

    String kafkaTopicName = getKafkaTopic() + "-partial-upsert";
    setUpKafka(kafkaTopicName, INPUT_DATA_SMALL_TAR_FILE);

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    csvDecoderProperties.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");

    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder()
        .addSingleValueDimension("playerId", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addMultiValueDimension("game", FieldSpec.DataType.STRING)  // Multi-value for UNION strategy
        .addSingleValueDimension("deleted", FieldSpec.DataType.BOOLEAN).addMetric("score", FieldSpec.DataType.FLOAT)
        .addDateTime("timestampInEpoch", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(Collections.singletonList("playerId"));

    // Create schema for partial upsert - make game multi-value to support UNION strategy
    String tableNameWithoutCompaction = "gameScoresPartialCompactionDisabled";
    Schema partialUpsertSchemaBaseline = schemaBuilder.setSchemaName(tableNameWithoutCompaction).build();
    addSchema(partialUpsertSchemaBaseline);

    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);  // DISABLE commit-time compaction
    upsertConfigWithoutCompaction.setPartialUpsertStrategies(
        Map.of("game", UpsertConfig.Strategy.UNION, "name", UpsertConfig.Strategy.OVERWRITE, "score",
            UpsertConfig.Strategy.OVERWRITE));
    TableConfig tableConfigWithoutCompaction =
        createCSVUpsertTableConfig(tableNameWithoutCompaction, kafkaTopicName, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithoutCompaction, PRIMARY_KEY_COL);
    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithoutCompaction);

    // TABLE 2: With commit-time compaction ENABLED + row-major build and PARTIAL upsert mode
    String tableNameWithCompaction = "gameScoresPartialCompactionEnabled";
    Schema partialUpsertSchemaCompacted = schemaBuilder.setSchemaName(tableNameWithCompaction).build();
    addSchema(partialUpsertSchemaCompacted);
    UpsertConfig upsertConfigWithCompaction = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfigWithCompaction.setPartialUpsertStrategies(
        Map.of("game", UpsertConfig.Strategy.UNION, "name", UpsertConfig.Strategy.OVERWRITE, "score",
            UpsertConfig.Strategy.OVERWRITE));
    upsertConfigWithCompaction.setEnableCommitTimeCompaction(true);  // ENABLE commit-time compaction
    TableConfig tableConfigWithCompaction =
        createCSVUpsertTableConfig(tableNameWithCompaction, kafkaTopicName, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithCompaction, PRIMARY_KEY_COL);
    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithCompaction);

    // TABLE 3: With commit-time compaction ENABLED + column-major build and PARTIAL upsert mode
    String tableNameWithCompactionColumnMajor = "gameScoresPartialCompactionColumnMajor";
    Schema partialUpsertSchemaColumnMajor = schemaBuilder.setSchemaName(tableNameWithCompactionColumnMajor).build();
    addSchema(partialUpsertSchemaColumnMajor);
    UpsertConfig upsertConfigWithCompactionColumnMajor = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfigWithCompactionColumnMajor.setPartialUpsertStrategies(
        Map.of("game", UpsertConfig.Strategy.UNION, "name", UpsertConfig.Strategy.OVERWRITE, "score",
            UpsertConfig.Strategy.OVERWRITE));
    upsertConfigWithCompactionColumnMajor.setEnableCommitTimeCompaction(true);  // ENABLE commit-time compaction
    TableConfig tableConfigWithCompactionColumnMajor =
        createCSVUpsertTableConfig(tableNameWithCompactionColumnMajor, kafkaTopicName, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithCompactionColumnMajor, PRIMARY_KEY_COL);

    // Enable _columnMajorSegmentBuilderEnabled = true for this table
    tableConfigWithCompactionColumnMajor.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(true);
    addTableConfig(tableConfigWithCompactionColumnMajor);

    // Wait for all three tables to load the same initial data (3 unique records after upserts)
    waitForAllDocsLoaded(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 30_000L, 10);

    // Verify initial state - all three tables should show the same upserted data count (3 unique records)
    validateInitialState(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3);

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

    // Push all partial updates to the shared Kafka topic
    pushCsvIntoKafkaWithKey(partialUpdateRecords, kafkaTopicName, 0);

    // Wait for all additional records to be processed (3 unique records after all partial upserts)
    waitForAllDocsLoaded(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 90_000L, 15);

    // Verify state before commit - all three tables should still show the same logical result (3 unique records)
    validatePreCommitState(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3);

    // Perform commit and wait for completion
    performCommitAndWait(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 20_000L, 4, 2);

    // Brief wait to ensure all commit operations are complete
    waitForAllDocsLoaded(tableNameWithCompaction, 60_000L, 3);
    waitForAllDocsLoaded(tableNameWithCompactionColumnMajor, 60_000L, 3);

    // Validate post-commit compaction effectiveness and data integrity (expecting 3 records, min 2 removed)
    validatePostCommitCompaction(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3, 2, 0.95);

    // Clean up
    cleanupTablesAndSchemas(
        List.of(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor),
        List.of(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor));
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

    String kafkaTopicName = getKafkaTopic() + "-mixed-compaction-comparison";

    // Create test data with diverse column types
    // Use semicolon-separated format for multi-value fields in CSV
    List<String> testRecords = List.of("300,PlayerName300,game1,85.5,1681054200000,false,1001,Region1,tag1;tag2",
        "301,PlayerName301,game2,92.0,1681054300000,false,1002,Region2,tag3",
        "302,PlayerName302,game3,78.0,1681054400000,false,1001,Region1,tag1;tag4",
        // Update records to create obsolete data
        "300,UpdatedName300,game1,88.0,1681154200000,false,1003,Region3,tag5",
        "301,UpdatedName301,game2,94.0,1681154300000,false,1002,Region2,tag3;tag6");

    // Set up Kafka topic
    pushCsvIntoKafka(testRecords, kafkaTopicName, 0);

    // TABLE 1: With commit-time compaction DISABLED (baseline)
    String tableNameWithoutCompaction = "gameScoresMixedCompactionDisabled";

    Schema mixedSchemaBaseline = createSchema();
    mixedSchemaBaseline.setSchemaName(tableNameWithoutCompaction);
    mixedSchemaBaseline.addField(new DimensionFieldSpec("regionID", FieldSpec.DataType.LONG, true));
    mixedSchemaBaseline.addField(new DimensionFieldSpec("regionName", FieldSpec.DataType.STRING, true));
    mixedSchemaBaseline.addField(new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, false));
    addSchema(mixedSchemaBaseline);

    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER,
        "playerId,name,game,score,timestampInEpoch,deleted,regionID,regionName,tags");
    // Configure multi-value delimiter for tags column
    csvDecoderProperties.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
    TableConfig tableConfigWithoutCompaction =
        createCSVUpsertTableConfig(tableNameWithoutCompaction, kafkaTopicName, 1, csvDecoderProperties,
            upsertConfigWithoutCompaction, PRIMARY_KEY_COL);

    // Mixed configuration:
    // - Inverted indexes on some columns (dictionary + inverted)
    tableConfigWithoutCompaction.getIndexingConfig().setInvertedIndexColumns(List.of("regionID", "tags", "game"));
    // - Raw storage (no dictionary) for name and regionName
    tableConfigWithoutCompaction.getIndexingConfig().setNoDictionaryColumns(List.of("name", "regionName"));
    // - Keep score as dictionary-encoded metric
    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithoutCompaction);

    // TABLE 2: With commit-time compaction ENABLED + row-major build
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

    Map<String, String> csvDecoderPropertiesCompacted = getCSVDecoderProperties(CSV_DELIMITER,
        "playerId,name,game,score,timestampInEpoch,deleted,regionID,regionName,tags");
    // Configure multi-value delimiter for tags column
    csvDecoderPropertiesCompacted.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
    TableConfig tableConfigWithCompaction =
        createCSVUpsertTableConfig(tableNameWithCompaction, kafkaTopicName, 1, csvDecoderPropertiesCompacted,
            upsertConfigWithCompaction, PRIMARY_KEY_COL);

    // Same mixed configuration
    tableConfigWithCompaction.getIndexingConfig().setInvertedIndexColumns(List.of("regionID", "tags", "game"));
    tableConfigWithCompaction.getIndexingConfig().setNoDictionaryColumns(List.of("name", "regionName"));
    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithCompaction);

    // TABLE 3: With commit-time compaction ENABLED + column-major build
    String tableNameWithCompactionColumnMajor = "gameScoresMixedCompactionColumnMajor";

    Schema mixedSchemaColumnMajor = createSchema();
    mixedSchemaColumnMajor.setSchemaName(tableNameWithCompactionColumnMajor);
    mixedSchemaColumnMajor.addField(new DimensionFieldSpec("regionID", FieldSpec.DataType.LONG, true));
    mixedSchemaColumnMajor.addField(new DimensionFieldSpec("regionName", FieldSpec.DataType.STRING, true));
    mixedSchemaColumnMajor.addField(new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, false));
    addSchema(mixedSchemaColumnMajor);

    UpsertConfig upsertConfigWithCompactionColumnMajor = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompactionColumnMajor.setEnableCommitTimeCompaction(true);

    Map<String, String> csvDecoderPropertiesColumnMajor = getCSVDecoderProperties(CSV_DELIMITER,
        "playerId,name,game,score,timestampInEpoch,deleted,regionID,regionName,tags");
    // Configure multi-value delimiter for tags column
    csvDecoderPropertiesColumnMajor.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
    TableConfig tableConfigWithCompactionColumnMajor =
        createCSVUpsertTableConfig(tableNameWithCompactionColumnMajor, kafkaTopicName, 1,
            csvDecoderPropertiesColumnMajor, upsertConfigWithCompactionColumnMajor, PRIMARY_KEY_COL);

    // Same mixed configuration AND enable column-major segment builder
    tableConfigWithCompactionColumnMajor.getIndexingConfig().setInvertedIndexColumns(
        List.of("regionID", "tags", "game"));
    tableConfigWithCompactionColumnMajor.getIndexingConfig().setNoDictionaryColumns(List.of("name", "regionName"));
    tableConfigWithCompactionColumnMajor.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(true);
    addTableConfig(tableConfigWithCompactionColumnMajor);

    // Wait for data to load
    waitForAllDocsLoaded(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 30_000L, 5);

    // Verify initial state - all three tables should show correct logical record count
    validateInitialState(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3);

    // Force commit segments to trigger commit-time compaction
    forceCommit(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor);

    // Wait for segments to be committed - using simpler wait since partition count is 1
    waitForAllDocsLoaded(tableNameWithCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 60_000L, 3);

    // Validate post-commit compaction effectiveness and data integrity (expecting 3 records, min 1 removed)
    validatePostCommitCompaction(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3, 1, 1.0);

    // Clean up
    cleanupTablesAndSchemas(
        List.of(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor),
        List.of(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor));
  }

  @Test
  public void testCommitTimeCompactionMutableMapDataSourceFix()
      throws Exception {
    // Test Case: MAP Column Commit-Time Compaction
    // Goal: Ensure commit-time compaction correctly handles MAP columns by only processing
    // valid documents when collecting statistics, avoiding inflated cardinality and key frequencies
    // from obsolete records that should be excluded during compaction.
    // Now includes testing commit-time compaction with column-major segment builder

    String kafkaTopicName = getKafkaTopic() + "-map-compaction-comparison";

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

    // Set up Kafka topic with MAP test data
    pushCsvIntoKafka(testRecords, kafkaTopicName, 0);

    // TABLE 1: With commit-time compaction DISABLED (baseline)
    String tableNameWithoutCompaction = "gameScoresMapCompactionDisabled";

    // Create schema for baseline table
    Schema mapSchemaBaseline = createSchema();
    mapSchemaBaseline.setSchemaName(tableNameWithoutCompaction);
    mapSchemaBaseline.addField(new DimensionFieldSpec("userAttributes", FieldSpec.DataType.JSON, true));
    addSchema(mapSchemaBaseline);

    UpsertConfig upsertConfigWithoutCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithoutCompaction.setEnableCommitTimeCompaction(false);

    Map<String, String> csvDecoderProperties =
        getCSVDecoderProperties(CSV_DELIMITER, "playerId,name,game,score,timestampInEpoch,deleted,userAttributes");
    TableConfig tableConfigWithoutCompaction =
        createCSVUpsertTableConfig(tableNameWithoutCompaction, kafkaTopicName, getNumKafkaPartitions(),
            csvDecoderProperties, upsertConfigWithoutCompaction, PRIMARY_KEY_COL);

    tableConfigWithoutCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithoutCompaction);

    // TABLE 2: With commit-time compaction ENABLED + row-major build
    String tableNameWithCompaction = "gameScoresMapCompactionEnabled";

    // Create schema with MAP column
    Schema mapSchemaCompacted = createSchema();
    mapSchemaCompacted.setSchemaName(tableNameWithCompaction);
    mapSchemaCompacted.addField(new DimensionFieldSpec("userAttributes", FieldSpec.DataType.JSON, true));
    addSchema(mapSchemaCompacted);

    UpsertConfig upsertConfigWithCompaction = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompaction.setEnableCommitTimeCompaction(true);

    Map<String, String> csvDecoderPropertiesCompacted =
        getCSVDecoderProperties(CSV_DELIMITER, "playerId,name,game,score,timestampInEpoch,deleted,userAttributes");
    TableConfig tableConfigWithCompaction =
        createCSVUpsertTableConfig(tableNameWithCompaction, kafkaTopicName, getNumKafkaPartitions(),
            csvDecoderPropertiesCompacted, upsertConfigWithCompaction, PRIMARY_KEY_COL);

    tableConfigWithCompaction.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(false);
    addTableConfig(tableConfigWithCompaction);

    // TABLE 3: With commit-time compaction ENABLED + column-major build
    String tableNameWithCompactionColumnMajor = "gameScoresMapCompactionColumnMajor";

    // Create schema for column-major table
    Schema mapSchemaColumnMajor = createSchema();
    mapSchemaColumnMajor.setSchemaName(tableNameWithCompactionColumnMajor);
    mapSchemaColumnMajor.addField(new DimensionFieldSpec("userAttributes", FieldSpec.DataType.JSON, true));
    addSchema(mapSchemaColumnMajor);

    UpsertConfig upsertConfigWithCompactionColumnMajor = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithCompactionColumnMajor.setEnableCommitTimeCompaction(true);

    Map<String, String> csvDecoderPropertiesColumnMajor =
        getCSVDecoderProperties(CSV_DELIMITER, "playerId,name,game,score,timestampInEpoch,deleted,userAttributes");
    TableConfig tableConfigWithCompactionColumnMajor =
        createCSVUpsertTableConfig(tableNameWithCompactionColumnMajor, kafkaTopicName, getNumKafkaPartitions(),
            csvDecoderPropertiesColumnMajor, upsertConfigWithCompactionColumnMajor, PRIMARY_KEY_COL);

    tableConfigWithCompactionColumnMajor.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(true);
    addTableConfig(tableConfigWithCompactionColumnMajor);

    // Wait for data to load
    waitForAllDocsLoaded(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 30_000L, 5);

    // Verify initial state - all three tables should show same logical record count (3 unique players after upserts)
    validateInitialState(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3);

    // Test JSON queries work correctly before commit
    String jsonQuery = String.format(
        "SELECT playerId, JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') as level FROM %s WHERE "
            + "JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') > 10 ORDER BY playerId",
        tableNameWithCompaction);
    ResultSet resultSet = getPinotConnection().execute(jsonQuery).getResultSet(0);
    assertTrue(resultSet.getRowCount() > 0, "Should find records with level > 10");

    // Verify data integrity before commit
    verifyTablesHaveIdenticalData(tableNameWithoutCompaction, tableNameWithCompaction);
    verifyTablesHaveIdenticalData(tableNameWithoutCompaction, tableNameWithCompactionColumnMajor);

    // Force commit segments to trigger commit-time compaction
    forceCommit(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor);

    // Wait for commit completion
    waitForAllDocsLoaded(tableNameWithCompaction, 60_000L, 3);
    waitForAllDocsLoaded(tableNameWithCompactionColumnMajor, 60_000L, 3);

    // Validate post-commit compaction effectiveness and data integrity (expecting 3 records, min 1 removed)
    validatePostCommitCompaction(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3, 1, 1.0);

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

    // Test same queries on column-major table
    String jsonQueryColumnMajor = String.format(
        "SELECT playerId, JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') as level FROM %s WHERE "
            + "JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') > 10 ORDER BY playerId",
        tableNameWithCompactionColumnMajor);
    ResultSet resultSetColumnMajor = getPinotConnection().execute(jsonQueryColumnMajor).getResultSet(0);
    assertTrue(resultSetColumnMajor.getRowCount() > 0,
        "Should still find records with level > 10 after compaction (column-major)");

    String levelQueryColumnMajor = String.format(
        "SELECT COUNT(*) FROM %s WHERE JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') > 10",
        tableNameWithCompactionColumnMajor);
    ResultSet levelResultColumnMajor = getPinotConnection().execute(levelQueryColumnMajor).getResultSet(0);
    assertTrue(levelResultColumnMajor.getInt(0, 0) > 0,
        "Should find records with level > 10 after compaction (column-major)");

    String uniqueQueryColumnMajor = String.format(
        "SELECT COUNT(DISTINCT playerId) FROM %s WHERE "
            + "JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') IS NOT NULL",
        tableNameWithCompactionColumnMajor);
    ResultSet uniqueResultColumnMajor = getPinotConnection().execute(uniqueQueryColumnMajor).getResultSet(0);
    assertEquals(uniqueResultColumnMajor.getInt(0, 0), 3,
        "Should find exactly 3 unique players with level data (after compaction, column-major)");

    // Clean up
    cleanupTablesAndSchemas(
        List.of(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor),
        List.of(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor));
  }

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

  private TableConfig createUpsertTable(String tableName, String kafkaTopicName, UpsertConfig.Mode upsertMode,
      boolean enableCommitTimeCompaction, boolean enableColumnMajorSegmentBuilder)
      throws Exception {
    // Create schema
    Schema schema = createSchema();
    schema.setSchemaName(tableName);
    addSchema(schema);

    // Create upsert config
    UpsertConfig upsertConfig = new UpsertConfig(upsertMode);
    upsertConfig.setEnableCommitTimeCompaction(enableCommitTimeCompaction);

    // Create table config
    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    TableConfig tableConfig =
        createCSVUpsertTableConfig(tableName, kafkaTopicName, getNumKafkaPartitions(), csvDecoderProperties,
            upsertConfig, PRIMARY_KEY_COL);

    // Set column-major segment builder setting
    tableConfig.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(enableColumnMajorSegmentBuilder);
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

  private void validateSegmentLevelSorting(String tableName) {
    ResultSet segmentData = getPinotConnection().execute(
            "SELECT $segmentName, $docId, playerId, score FROM " + tableName + " ORDER BY $segmentName, $docId")
        .getResultSet(0);

    Map<String, List<Float>> segmentScoresMap = new HashMap<>();
    Map<String, List<Integer>> segmentDocIdsMap = new HashMap<>();

    // Group all data by segment first
    for (int i = 0; i < segmentData.getRowCount(); i++) {
      String segmentName = segmentData.getString(i, 0);
      int docId = Integer.parseInt(segmentData.getString(i, 1));
      float score = Float.parseFloat(segmentData.getString(i, 3));

      segmentScoresMap.computeIfAbsent(segmentName, k -> new ArrayList<>()).add(score);
      segmentDocIdsMap.computeIfAbsent(segmentName, k -> new ArrayList<>()).add(docId);
    }

    // Now validate each segment individually
    for (Map.Entry<String, List<Float>> entry : segmentScoresMap.entrySet()) {
      String segmentName = entry.getKey();
      List<Float> scores = entry.getValue();
      List<Integer> docIds = segmentDocIdsMap.get(segmentName);

      if (scores.size() > 1) {
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

  protected void waitForAllDocsLoaded(String tableName1, String tableName2, String tableName3,
      long timeoutMs, int expectedRecords)
      throws Exception {
    waitForAllDocsLoaded(tableName1, timeoutMs, expectedRecords);
    waitForAllDocsLoaded(tableName2, timeoutMs, expectedRecords);
    waitForAllDocsLoaded(tableName3, timeoutMs, expectedRecords);
  }

  protected void validateInitialState(String tableName1, String tableName2, String tableName3,
      int expectedRecords) {
    long initialLogicalCount1 = queryCountStar(tableName1);
    long initialLogicalCount2 = queryCountStar(tableName2);
    long initialLogicalCount3 = queryCountStar(tableName3);

    assertEquals(initialLogicalCount1, expectedRecords,
        "Table 1 should have " + expectedRecords + " logical records initially");
    assertEquals(initialLogicalCount2, expectedRecords,
        "Table 2 should have " + expectedRecords + " logical records initially");
    assertEquals(initialLogicalCount3, expectedRecords,
        "Table 3 should have " + expectedRecords + " logical records initially");
  }

  protected void validatePreCommitState(String tableName1, String tableName2, String tableName3,
      int expectedRecords) {
    long preCommitLogicalCount1 = queryCountStar(tableName1);
    long preCommitLogicalCount2 = queryCountStar(tableName2);
    long preCommitLogicalCount3 = queryCountStar(tableName3);

    assertEquals(preCommitLogicalCount1, expectedRecords,
        "All tables should show " + expectedRecords + " logical records before commit");
    assertEquals(preCommitLogicalCount2, expectedRecords,
        "All tables should show " + expectedRecords + " logical records before commit");
    assertEquals(preCommitLogicalCount3, expectedRecords,
        "All tables should show " + expectedRecords + " logical records before commit");

    // Verify all tables have the same data row by row
    verifyTablesHaveIdenticalData(tableName1, tableName2);
    verifyTablesHaveIdenticalData(tableName1, tableName3);
  }

  protected void performCommitAndWait(String tableName1, String tableName2, String tableName3,
      long waitTimeoutMs, int expectedSegments, int expectedConsumingSegments)
      throws Exception {
    forceCommit(tableName1, tableName2, tableName3);

    // Wait for segments to be committed
    if (expectedConsumingSegments >= 0) {
      waitForNumQueriedSegmentsToConverge(tableName1, waitTimeoutMs, expectedSegments, expectedConsumingSegments);
      waitForNumQueriedSegmentsToConverge(tableName2, waitTimeoutMs, expectedSegments, expectedConsumingSegments);
      waitForNumQueriedSegmentsToConverge(tableName3, waitTimeoutMs, expectedSegments, expectedConsumingSegments);
    } else {
      waitForNumQueriedSegmentsToConverge(tableName1, waitTimeoutMs, expectedSegments);
      waitForNumQueriedSegmentsToConverge(tableName2, waitTimeoutMs, expectedSegments);
      waitForNumQueriedSegmentsToConverge(tableName3, waitTimeoutMs, expectedSegments);
    }
  }

  protected void forceCommit(String tableName1, String tableName2, String tableName3)
      throws Exception {
    sendPostRequest(_controllerRequestURLBuilder.forTableForceCommit(tableName1));
    sendPostRequest(_controllerRequestURLBuilder.forTableForceCommit(tableName2));
    sendPostRequest(_controllerRequestURLBuilder.forTableForceCommit(tableName3));
  }

  protected void validatePostCommitCompaction(String tableNameBaseline, String tableNameCompacted1,
      String tableNameCompacted2, int expectedLogicalRecords, int minExpectedRemovedRecords,
      double maxCompressionRatio) {
    // Check the TOTAL document counts after commit (with skipUpsert=true to see all physical records)
    long postCommitPhysicalCountBaseline = queryCountStarWithoutUpsert(tableNameBaseline);
    long postCommitPhysicalCountCompacted1 = queryCountStarWithoutUpsert(tableNameCompacted1);
    long postCommitPhysicalCountCompacted2 = queryCountStarWithoutUpsert(tableNameCompacted2);
    long postCommitLogicalCountBaseline = queryCountStar(tableNameBaseline);
    long postCommitLogicalCountCompacted1 = queryCountStar(tableNameCompacted1);
    long postCommitLogicalCountCompacted2 = queryCountStar(tableNameCompacted2);

    // If counts are still the same, wait a bit longer for actual commit completion
    if (postCommitPhysicalCountCompacted1 == postCommitPhysicalCountBaseline
        || postCommitPhysicalCountCompacted2 == postCommitPhysicalCountBaseline) {
      try {
        Thread.sleep(10_000);
        postCommitPhysicalCountBaseline = queryCountStarWithoutUpsert(tableNameBaseline);
        postCommitPhysicalCountCompacted1 = queryCountStarWithoutUpsert(tableNameCompacted1);
        postCommitPhysicalCountCompacted2 = queryCountStarWithoutUpsert(tableNameCompacted2);
        postCommitLogicalCountBaseline = queryCountStar(tableNameBaseline);
        postCommitLogicalCountCompacted1 = queryCountStar(tableNameCompacted1);
        postCommitLogicalCountCompacted2 = queryCountStar(tableNameCompacted2);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for commit completion", e);
      }
    }

    // Key assertions for commit time compaction - both compacted tables should have fewer physical docs
    assertTrue(postCommitPhysicalCountCompacted1 < postCommitPhysicalCountBaseline, String.format(
        "Expected table with commit-time compaction + row-major (%d docs) to have fewer physical docs than "
            + "baseline table (%d docs)", postCommitPhysicalCountCompacted1, postCommitPhysicalCountBaseline));

    assertTrue(postCommitPhysicalCountCompacted2 < postCommitPhysicalCountBaseline, String.format(
        "Expected table with commit-time compaction + column-major (%d docs) to have fewer physical docs than "
            + "baseline table (%d docs)", postCommitPhysicalCountCompacted2, postCommitPhysicalCountBaseline));

    // All tables should still return the same logical upserted results
    assertEquals(postCommitLogicalCountBaseline, expectedLogicalRecords,
        "Baseline table should have " + expectedLogicalRecords + " logical records");
    assertEquals(postCommitLogicalCountCompacted1, expectedLogicalRecords,
        "Compacted table (row-major) should have " + expectedLogicalRecords + " logical records");
    assertEquals(postCommitLogicalCountCompacted2, expectedLogicalRecords,
        "Compacted table (column-major) should have " + expectedLogicalRecords + " logical records");

    // Verify data integrity - all tables should return identical data row by row
    verifyTablesHaveIdenticalData(tableNameBaseline, tableNameCompacted1);
    verifyTablesHaveIdenticalData(tableNameBaseline, tableNameCompacted2);

    // Calculate and validate compaction efficiency for both compacted tables
    validateCompactionEfficiency(postCommitPhysicalCountCompacted1, postCommitPhysicalCountBaseline,
        minExpectedRemovedRecords, maxCompressionRatio);
    validateCompactionEfficiency(postCommitPhysicalCountCompacted2, postCommitPhysicalCountBaseline,
        minExpectedRemovedRecords, maxCompressionRatio);
  }

  protected void cleanupTablesAndSchemas(List<String> tableNames, List<String> schemaNames) {
    tableNames.forEach(name -> {
      try {
        dropRealtimeTable(name);
      } catch (IOException ignored) {
      }
    });

    schemaNames.forEach(name -> {
      try {
        deleteSchema(name);
      } catch (IOException ignored) {
      }
    });
  }

  @Test
  public void testCommitTimeCompactionPreservesDeletedRecords()
      throws Exception {
    // Test Case: Deleted Records Preservation with Commit-Time Compaction
    // Goal: Ensure commit-time compaction preserves deleted records (soft deletes) in the committed
    // segment to maintain data consistency. Deleted records should not be physically removed during
    // compaction as this could lead to data inconsistency issues.
    // Now includes testing commit-time compaction with column-major segment builder

    String kafkaTopicName = getKafkaTopic() + "-deleted-records-preservation";
    setUpKafka(kafkaTopicName, INPUT_DATA_SMALL_TAR_FILE);

    // TABLE 1: With commit-time compaction DISABLED (baseline) and delete record column configured
    String tableNameWithoutCompaction = "gameScoresDeletedRecordsCompactionDisabled";
    TableConfig tableConfigWithoutCompaction = createUpsertTable(tableNameWithoutCompaction, kafkaTopicName,
        UpsertConfig.Mode.FULL, false, false);
    // Configure delete record column for soft deletes
    tableConfigWithoutCompaction.getUpsertConfig().setDeleteRecordColumn("deleted");
    updateTableConfig(tableConfigWithoutCompaction);

    // TABLE 2: With commit-time compaction ENABLED + row-major build and delete record column configured
    String tableNameWithCompaction = "gameScoresDeletedRecordsCompactionEnabled";
    TableConfig tableConfigWithCompaction = createUpsertTable(tableNameWithCompaction, kafkaTopicName,
        UpsertConfig.Mode.FULL, true, false);
    // Configure delete record column for soft deletes
    tableConfigWithCompaction.getUpsertConfig().setDeleteRecordColumn("deleted");
    updateTableConfig(tableConfigWithCompaction);

    // TABLE 3: With commit-time compaction ENABLED + column-major build and delete record column configured
    String tableNameWithCompactionColumnMajor = "gameScoresDeletedRecordsCompactionColumnMajor";
    TableConfig tableConfigWithCompactionColumnMajor = createUpsertTable(tableNameWithCompactionColumnMajor,
        kafkaTopicName, UpsertConfig.Mode.FULL, true, true);
    // Configure delete record column for soft deletes
    tableConfigWithCompactionColumnMajor.getUpsertConfig().setDeleteRecordColumn("deleted");
    updateTableConfig(tableConfigWithCompactionColumnMajor);

    // Wait for all three tables to load the same initial data (3 unique records after upserts)
    waitForAllDocsLoaded(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 30_000L, 10);

    // Verify initial state - all three tables should show the same upserted data count (3 unique records)
    validateInitialState(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 3);

    // Create update patterns including some deleted records to test preservation
    // Use playerIds that match the initial data to ensure consistent partitioning
    List<String> updateRecords = List.of(
        "100,Zook-Updated1,counter-strike,1000,1681300000000,false",  // Regular update
        "101,Alice-Updated1,dota,2000,1681300001000,true",            // Mark as deleted
        "102,Bob-Updated1,cs2,3000,1681300002000,false",              // Regular update
        "100,Zook-Updated2,valorant,1500,1681300003000,false",       // Another regular update
        "103,Charlie-New,chess,2500,1681300004000,true"              // New record marked as deleted
    );

    pushCsvIntoKafkaWithKey(updateRecords, kafkaTopicName, 0);

    // Wait for all additional records to be processed
    waitForAllDocsLoaded(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 90_000L, 15);

    // Verify state before commit - all three tables should show the same logical result
    // Note: Deleted records should not be counted in regular queries (logical view)
    validatePreCommitState(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 2);

    // Perform commit and wait for completion
    performCommitAndWait(tableNameWithoutCompaction, tableNameWithCompaction,
        tableNameWithCompactionColumnMajor, 30_000L, 4, 2);

    // Validate post-commit state with custom validation for deleted records
    long postCommitLogicalCountBaseline = queryCountStar(tableNameWithoutCompaction);
    long postCommitLogicalCountCompacted1 = queryCountStar(tableNameWithCompaction);
    long postCommitLogicalCountCompacted2 = queryCountStar(tableNameWithCompactionColumnMajor);
    long postCommitPhysicalCountBaseline = queryCountStarWithoutUpsert(tableNameWithoutCompaction);
    long postCommitPhysicalCountCompacted1 = queryCountStarWithoutUpsert(tableNameWithCompaction);
    long postCommitPhysicalCountCompacted2 = queryCountStarWithoutUpsert(tableNameWithCompactionColumnMajor);

    // Key assertions for deleted records preservation:
    // 1. Logical counts should remain the same (2 non-deleted records)
    assertEquals(postCommitLogicalCountBaseline, 2,
        "Baseline table should show 2 non-deleted logical records after commit");
    assertEquals(postCommitLogicalCountCompacted1, 2,
        "Compacted table (row-major) should show 2 non-deleted logical records after commit");
    assertEquals(postCommitLogicalCountCompacted2, 2,
        "Compacted table (column-major) should show 2 non-deleted logical records after commit");

    // 2. Physical count for compacted tables should still include deleted records
    // The compacted tables should have fewer physical records than baseline table (due to obsolete record removal)
    // but should still contain the deleted records
    assertTrue(postCommitPhysicalCountCompacted1 < postCommitPhysicalCountBaseline,
        String.format("Compacted table (row-major) (%d) should have fewer physical docs than baseline table (%d) "
            + "due to compaction", postCommitPhysicalCountCompacted1, postCommitPhysicalCountBaseline));

    assertTrue(postCommitPhysicalCountCompacted2 < postCommitPhysicalCountBaseline,
        String.format("Compacted table (column-major) (%d) should have fewer physical docs than baseline table (%d) "
            + "due to compaction", postCommitPhysicalCountCompacted2, postCommitPhysicalCountBaseline));

    // 3. Verify that we can still query for deleted records using skipUpsert=true
    String deletedRecordsQuery = String.format(
        "SELECT COUNT(*) FROM %s WHERE deleted = true OPTION(skipUpsert=true)",
        tableNameWithCompaction);
    ResultSet deletedResult = getPinotConnection().execute(deletedRecordsQuery).getResultSet(0);
    long deletedRecordsCount = deletedResult.getLong(0, 0);
    assertTrue(deletedRecordsCount > 0,
        "Should be able to find deleted records in compacted table using skipUpsert=true");

    // Test same query on column-major table
    String deletedRecordsQueryColumnMajor = String.format(
        "SELECT COUNT(*) FROM %s WHERE deleted = true OPTION(skipUpsert=true)",
        tableNameWithCompactionColumnMajor);
    ResultSet deletedResultColumnMajor = getPinotConnection().execute(deletedRecordsQueryColumnMajor).getResultSet(0);
    long deletedRecordsCountColumnMajor = deletedResultColumnMajor.getLong(0, 0);
    assertTrue(deletedRecordsCountColumnMajor > 0,
        "Should be able to find deleted records in compacted table (column-major) using skipUpsert=true");

    // 4. Verify that deleted records are not visible in regular queries (logical view)
    String regularQuery = String.format(
        "SELECT COUNT(*) FROM %s WHERE deleted = true",
        tableNameWithCompaction);
    ResultSet regularResult = getPinotConnection().execute(regularQuery).getResultSet(0);
    long visibleDeletedCount = regularResult.getLong(0, 0);
    assertEquals(visibleDeletedCount, 0,
        "Deleted records should not be visible in regular queries (logical view)");

    String regularQueryColumnMajor = String.format(
        "SELECT COUNT(*) FROM %s WHERE deleted = true",
        tableNameWithCompactionColumnMajor);
    ResultSet regularResultColumnMajor = getPinotConnection().execute(regularQueryColumnMajor).getResultSet(0);
    long visibleDeletedCountColumnMajor = regularResultColumnMajor.getLong(0, 0);
    assertEquals(visibleDeletedCountColumnMajor, 0,
        "Deleted records should not be visible in regular queries (logical view, column-major)");

    // 5. Verify data integrity for non-deleted records
    verifyTablesHaveIdenticalData(tableNameWithoutCompaction, tableNameWithCompaction);
    verifyTablesHaveIdenticalData(tableNameWithoutCompaction, tableNameWithCompactionColumnMajor);

    // Clean up
    cleanupTablesAndSchemas(
        List.of(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor),
        List.of(tableNameWithoutCompaction, tableNameWithCompaction, tableNameWithCompactionColumnMajor));
  }
}
