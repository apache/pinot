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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.ExecutionStats;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.ConfigChangeListenerConstants;
import org.apache.pinot.spi.utils.ConsumingSegmentConsistencyModeListener;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.UpsertDedupTestFixtures.GAME_SCORES_COLUMNS;
import static org.apache.pinot.integration.tests.UpsertDedupTestFixtures.GAME_SCORES_WINNERS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Integration tests for commit-time compaction feature in upsert tables.
///
/// These tests validate that commit-time compaction correctly handles:
/// - Multi-value dictionary columns
/// - Mixed column configurations (dictionary vs no-dictionary)
/// - Inverted indexes
/// - Raw index writers
/// - Different data types
public class CommitTimeCompactionIntegrationTest extends BaseClusterIntegrationTest {
  private static final String INPUT_DATA_SMALL_TAR_FILE = "gameScores_csv.tar.gz";
  private static final String CSV_SCHEMA_HEADER = "playerId,name,game,score,timestampInEpoch,deleted";
  private static final String CSV_DELIMITER = ",";
  private static final int NUM_SERVERS = 2;
  private static final String PRIMARY_KEY_COL = "playerId";
  private static final String TIME_COL_NAME = "timestampInEpoch";
  public static final String UPSERT_SCHEMA_FILE_NAME = "upsert_table_test.schema";

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
    // Validate that enableCommitTimeCompaction=true removes invalid records compared to
    // enableCommitTimeCompaction=false, with both the row-major and the column-major segment builder
    String kafkaTopicName = getKafkaTopic() + "-compaction-comparison";
    setUpKafka(kafkaTopicName, INPUT_DATA_SMALL_TAR_FILE);
    String[] tables = createTables("gameScoresCommitTime", kafkaTopicName, getNumKafkaPartitions(), CSV_SCHEMA_HEADER,
        this::createSchema, () -> new UpsertConfig(UpsertConfig.Mode.FULL), indexingConfig -> {
        });

    // All three tables serve the latest fixture row per player
    waitForAllDocsLoaded(tables, 60_000L, 10);
    validateRows(GAME_SCORES_COLUMNS, GAME_SCORES_WINNERS, tables);

    updateCommitAndValidate(tables, kafkaTopicName,
        List.of("100,Zook-Updated1,counter-strike,1000,1681300000000,false",
            "101,Alice-Updated1,dota,2000,1681300001000,false", "102,Bob-Updated1,cs2,3000,1681300002000,false",
            "100,Zook-Updated2,valorant,1500,1681300003000,false", "101,Alice-Updated2,lol,2500,1681300004000,false"),
        15, GAME_SCORES_COLUMNS, List.of(
            List.of(100, "Zook-Updated2", "valorant", 1500.0, 1681300003000L, false),
            List.of(101, "Alice-Updated2", "lol", 2500.0, 1681300004000L, false),
            List.of(102, "Bob-Updated1", "cs2", 3000.0, 1681300002000L, false)));

    cleanupTablesAndSchemas(tables);
  }

  @Test
  public void testCommitTimeCompactionWithSortedColumn()
      throws Exception {
    // Verify commit-time compaction correctly processes and optimizes segments with a sorted column
    String kafkaTopicName = getKafkaTopic() + "-sorted-column";
    setUpKafka(kafkaTopicName, INPUT_DATA_SMALL_TAR_FILE);
    String[] tables = createTables("sorted", kafkaTopicName, getNumKafkaPartitions(), CSV_SCHEMA_HEADER,
        this::createSchema, () -> new UpsertConfig(UpsertConfig.Mode.FULL),
        indexingConfig -> indexingConfig.setSortedColumn(List.of("score")));

    waitForAllDocsLoaded(tables, 60_000L, 10);
    validateRows(GAME_SCORES_COLUMNS, GAME_SCORES_WINNERS, tables);

    // Add more diverse data with different primary keys and varying scores for better sorting validation
    updateCommitAndValidate(tables, kafkaTopicName, List.of("100,Alice Updated,chess,15.5,1681354200000,false",
        "101,Bob Updated,poker,88.0,1681354300000,false", "102,Charlie Updated,chess,45.2,1681354400000,false",
        "103,David New,tennis,12.8,1681354500000,false", "104,Eva New,golf,78.9,1681354600000,false",
        "105,Frank New,soccer,33.1,1681354700000,false", "100,Alice Final,chess,25.7,1681454200000,false",
        "101,Bob Final,poker,91.3,1681454300000,false", "103,David Final,tennis,8.4,1681454400000,false",
        "104,Eva Final,golf,67.8,1681454500000,false"), 20, GAME_SCORES_COLUMNS, List.of(
        List.of(100, "Alice Final", "chess", 25.7, 1681454200000L, false),
        List.of(101, "Bob Final", "poker", 91.3, 1681454300000L, false),
        List.of(102, "Charlie Updated", "chess", 45.2, 1681354400000L, false),
        List.of(103, "David Final", "tennis", 8.4, 1681454400000L, false),
        List.of(104, "Eva Final", "golf", 67.8, 1681454500000L, false),
        List.of(105, "Frank New", "soccer", 33.1, 1681354700000L, false)));

    // Every committed segment is sorted by score
    for (String table : tables) {
      validateSegmentLevelSorting(table);
    }

    cleanupTablesAndSchemas(tables);
  }

  @Test
  public void testCommitTimeCompactionWithNoDictionaryColumns()
      throws Exception {
    // Ensure commit-time compaction handles columns configured without dictionary (raw index)
    String kafkaTopicName = getKafkaTopic() + "-no-dictonary";
    setUpKafka(kafkaTopicName, INPUT_DATA_SMALL_TAR_FILE);
    String[] tables = createTables("rawIndex", kafkaTopicName, getNumKafkaPartitions(), CSV_SCHEMA_HEADER,
        this::createSchema, () -> new UpsertConfig(UpsertConfig.Mode.FULL),
        indexingConfig -> indexingConfig.setNoDictionaryColumns(List.of("name", "game")));

    waitForAllDocsLoaded(tables, 60_000L, 10);
    validateRows(GAME_SCORES_COLUMNS, GAME_SCORES_WINNERS, tables);

    updateCommitAndValidate(tables, kafkaTopicName, List.of("100,Alice Raw,chess,95.5,1681354200000,false",
        "101,Bob Raw,poker,88.0,1681354300000,false", "102,Charlie Raw,chess,82.5,1681354400000,false",
        "100,Alice NoDict,chess,99.5,1681454200000,false", "101,Bob NoDict,poker,91.0,1681454300000,false"), 15,
        GAME_SCORES_COLUMNS, List.of(
            List.of(100, "Alice NoDict", "chess", 99.5, 1681454200000L, false),
            List.of(101, "Bob NoDict", "poker", 91.0, 1681454300000L, false),
            List.of(102, "Charlie Raw", "chess", 82.5, 1681354400000L, false)));

    cleanupTablesAndSchemas(tables);
  }

  @Test
  public void testCommitTimeCompactionWithMultiValueColumns()
      throws Exception {
    // Ensure commit-time compaction correctly handles multi-value dictionary columns during segment conversion,
    // validating the fix for CompactedColumnStatistics
    String kafkaTopicName = getKafkaTopic() + "-multivalue";
    createKafkaTopic(kafkaTopicName);
    String[] tables = createTables("gameScoresMV", kafkaTopicName, getNumKafkaPartitions(),
        CSV_SCHEMA_HEADER + ",tags", () -> {
          Schema schema = createSchema();
          schema.addField(new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, false));
          return schema;
        }, () -> new UpsertConfig(UpsertConfig.Mode.FULL),
        indexingConfig -> indexingConfig.setInvertedIndexColumns(List.of("tags", "name", "game")));

    pushCsvIntoKafka(List.of("200,Player200,game1,85.5,1681054200000,false,action;shooter",
        "201,Player201,game2,92.0,1681054300000,false,strategy;puzzle",
        "202,Player202,game3,78.0,1681054400000,false,rpg;adventure;fantasy",
        // Updates to create obsolete records
        "200,Player200Updated,game1,90.0,1681154200000,false,action;fps",
        "201,Player201Updated,game2,95.0,1681154300000,false,strategy;rts"), kafkaTopicName, 0);
    waitForAllDocsLoaded(tables, 60_000L, 5);

    commitAndValidate(tables, getNumKafkaPartitions(), 5, GAME_SCORES_COLUMNS + ", tags", List.of(
        List.of(200, "Player200Updated", "game1", 90.0, 1681154200000L, false, List.of("action", "fps")),
        List.of(201, "Player201Updated", "game2", 95.0, 1681154300000L, false, List.of("strategy", "rts")),
        List.of(202, "Player202", "game3", 78.0, 1681054400000L, false,
            List.of("rpg", "adventure", "fantasy"))));

    cleanupTablesAndSchemas(tables);
  }

  @Test
  public void testCommitTimeCompactionWithPartialUpsertMode()
      throws Exception {
    // Ensure commit-time compaction correctly handles partial upsert semantics where only specific columns are
    // updated rather than entire records, and obsolete partial updates are properly compacted away
    String kafkaTopicName = getKafkaTopic() + "-partial-upsert";
    setUpKafka(kafkaTopicName, INPUT_DATA_SMALL_TAR_FILE);
    // Multi-value game column for the UNION strategy
    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder()
        .addSingleValueDimension("playerId", FieldSpec.DataType.INT)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addMultiValueDimension("game", FieldSpec.DataType.STRING)
        .addSingleValueDimension("deleted", FieldSpec.DataType.BOOLEAN).addMetric("score", FieldSpec.DataType.FLOAT)
        .addDateTime("timestampInEpoch", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(List.of("playerId"));
    String[] tables = createTables("gameScoresPartial", kafkaTopicName, getNumKafkaPartitions(), CSV_SCHEMA_HEADER,
        schemaBuilder::build, () -> {
          UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
          upsertConfig.setPartialUpsertStrategies(
              Map.of("game", UpsertConfig.Strategy.UNION, "name", UpsertConfig.Strategy.OVERWRITE, "score",
                  UpsertConfig.Strategy.OVERWRITE));
          return upsertConfig;
        }, indexingConfig -> {
        });

    // All three tables serve the merged row per player. Out-of-order fixture rows are not merged, and UNION keeps the
    // values sorted.
    waitForAllDocsLoaded(tables, 60_000L, 10);
    validateRows(GAME_SCORES_COLUMNS, List.of(
        List.of(100, "Zook", List.of("counter-strike"), 2050.0, 1681256400000L, false),
        List.of(101, "Suess", List.of("counter-strike"), 12500.2, 1681258290000L, false),
        List.of(102, "Clifford", List.of("counter-strike"), 102.0, 1681036400000L, false)), tables);

    try {
      setConsumingSegmentConsistencyMode(ConsumingSegmentConsistencyModeListener.Mode.PROTECTED);
      // name and score are overwritten, game is the sorted union
      updateCommitAndValidate(tables, kafkaTopicName, List.of("100,PartialUpdate1,arcade,1000,1681400000000,false",
          "101,PartialUpdate1,mobile,2000,1681400001000,false", "102,PartialUpdate1,board,3000,1681400002000,false",
          "100,PartialUpdate2,puzzle,1500,1681400003000,false",
          "101,PartialUpdate2,strategy,2500,1681400004000,false"), 15, GAME_SCORES_COLUMNS, List.of(
          List.of(100, "PartialUpdate2", List.of("arcade", "counter-strike", "puzzle"), 1500.0, 1681400003000L,
              false),
          List.of(101, "PartialUpdate2", List.of("counter-strike", "mobile", "strategy"), 2500.0, 1681400004000L,
              false),
          List.of(102, "PartialUpdate1", List.of("board", "counter-strike"), 3000.0, 1681400002000L, false)));
    } finally {
      try {
        resetConsumingSegmentConsistencyMode();
      } finally {
        cleanupTablesAndSchemas(tables);
      }
    }
  }

  @Test
  public void testCommitTimeCompactionWithMixedColumnConfigurations()
      throws Exception {
    // Ensure commit-time compaction handles various column configurations: dictionary vs no-dictionary columns,
    // inverted indexes, different data types (String, Long, multi-value) and raw index writers
    String kafkaTopicName = getKafkaTopic() + "-mixed-compaction-comparison";
    pushCsvIntoKafka(List.of("300,PlayerName300,game1,85.5,1681054200000,false,1001,Region1,tag1;tag2",
        "301,PlayerName301,game2,92.0,1681054300000,false,1002,Region2,tag3",
        "302,PlayerName302,game3,78.0,1681054400000,false,1001,Region1,tag1;tag4",
        // Update records to create obsolete data
        "300,UpdatedName300,game1,88.0,1681154200000,false,1003,Region3,tag5",
        "301,UpdatedName301,game2,94.0,1681154300000,false,1002,Region2,tag3;tag6"), kafkaTopicName, 0);
    String[] tables = createTables("gameScoresMixed", kafkaTopicName, 1,
        CSV_SCHEMA_HEADER + ",regionID,regionName,tags", () -> {
          Schema schema = createSchema();
          schema.addField(new DimensionFieldSpec("regionID", FieldSpec.DataType.LONG, true));
          schema.addField(new DimensionFieldSpec("regionName", FieldSpec.DataType.STRING, true));
          schema.addField(new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, false));
          return schema;
        }, () -> new UpsertConfig(UpsertConfig.Mode.FULL), indexingConfig -> {
          // Dictionary + inverted index on some columns, raw storage for name and regionName
          indexingConfig.setInvertedIndexColumns(List.of("regionID", "tags", "game"));
          indexingConfig.setNoDictionaryColumns(List.of("name", "regionName"));
        });
    waitForAllDocsLoaded(tables, 60_000L, 5);

    // The push above auto-created the topic with one partition
    commitAndValidate(tables, 1, 5, GAME_SCORES_COLUMNS + ", regionID, regionName, tags", List.of(
        List.of(300, "UpdatedName300", "game1", 88.0, 1681154200000L, false, 1003, "Region3", List.of("tag5")),
        List.of(301, "UpdatedName301", "game2", 94.0, 1681154300000L, false, 1002, "Region2",
            List.of("tag3", "tag6")),
        List.of(302, "PlayerName302", "game3", 78.0, 1681054400000L, false, 1001, "Region1",
            List.of("tag1", "tag4"))));

    cleanupTablesAndSchemas(tables);
  }

  @Test
  public void testCommitTimeCompactionMutableMapDataSourceFix()
      throws Exception {
    // Ensure commit-time compaction correctly handles MAP columns by only processing valid documents when collecting
    // statistics, avoiding inflated cardinality and key frequencies from obsolete records
    String kafkaTopicName = getKafkaTopic() + "-map-compaction-comparison";
    // Simple JSON values without commas to avoid CSV parsing conflicts
    pushCsvIntoKafka(List.of("400,Player400,game1,85.5,1681054200000,false,{\"level\":10}",
        "401,Player401,game2,92.0,1681054300000,false,{\"level\":15}",
        "402,Player402,game3,78.0,1681054400000,false,{\"level\":8}",
        // Updates to create obsolete records with different MAP values
        "400,Player400Updated,game1,90.0,1681154200000,false,{\"level\":12}",
        "401,Player401Updated,game2,95.0,1681154300000,false,{\"level\":18}"), kafkaTopicName, 0);
    String[] tables = createTables("gameScoresMap", kafkaTopicName, getNumKafkaPartitions(),
        CSV_SCHEMA_HEADER + ",userAttributes", () -> {
          Schema schema = createSchema();
          schema.addField(new DimensionFieldSpec("userAttributes", FieldSpec.DataType.JSON, true));
          return schema;
        }, () -> new UpsertConfig(UpsertConfig.Mode.FULL), indexingConfig -> {
        });
    waitForAllDocsLoaded(tables, 60_000L, 5);

    // JSON queries see only the latest level of each player, before and after the commit
    String levelColumns = "playerId, JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') AS level";
    String levelAboveTen = "JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') > 10";
    List<List<Object>> levelsAboveTen = List.of(List.of(400, 12), List.of(401, 18));
    validateRows(levelColumns, levelAboveTen, levelsAboveTen, tables);

    // The push above auto-created the topic with one partition
    commitAndValidate(tables, 1, 5, GAME_SCORES_COLUMNS + ", userAttributes", List.of(
        List.of(400, "Player400Updated", "game1", 90.0, 1681154200000L, false, "{\"level\":12}"),
        List.of(401, "Player401Updated", "game2", 95.0, 1681154300000L, false, "{\"level\":18}"),
        List.of(402, "Player402", "game3", 78.0, 1681054400000L, false, "{\"level\":8}")));

    validateRows(levelColumns, levelAboveTen, levelsAboveTen, tables);
    validateRows(levelColumns, "JSON_EXTRACT_SCALAR(userAttributes, '$.level', 'INT') IS NOT NULL",
        List.of(List.of(400, 12), List.of(401, 18), List.of(402, 8)), tables);

    cleanupTablesAndSchemas(tables);
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

  protected void waitForAllDocsLoaded(String tableName, long timeoutMs, long expectedDocs)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getCurrentCountStarResultWithoutUpsert(tableName) == expectedDocs;
      } catch (Exception e) {
        return false;
      }
    }, timeoutMs, "Failed to load expected number of documents");
  }

  private void setUpKafka(String kafkaTopicName, String inputDataFile)
      throws Exception {
    createKafkaTopic(kafkaTopicName);
    List<File> dataFiles = unpackTarData(inputDataFile, _tempDir);
    // playerId (column 0) is the partition key
    pushCsvIntoKafka(dataFiles.get(0), kafkaTopicName, 0);
  }

  /// Index of the baseline table without commit-time compaction in the array [#createTables] returns
  private static final int BASELINE = 0;
  /// Index of the table with commit-time compaction and the row-major segment builder
  private static final int ROW_MAJOR = 1;
  /// Index of the table with commit-time compaction and the column-major segment builder
  private static final int COLUMN_MAJOR = 2;

  /// Creates the three tables every test compares, all consuming the same topic: a baseline table without
  /// commit-time compaction, and two tables with it, built with the row-major and the column-major segment builder.
  /// Only the compaction and the segment builder differ between the three.
  ///
  /// @return the table names, indexed by [#BASELINE], [#ROW_MAJOR] and [#COLUMN_MAJOR]
  private String[] createTables(String namePrefix, String kafkaTopicName, int numPartitions, String csvHeader,
      Callable<Schema> schemaSupplier, Supplier<UpsertConfig> upsertConfigSupplier,
      Consumer<IndexingConfig> indexingConfigCustomizer)
      throws Exception {
    String[] tableNames = new String[3];
    tableNames[BASELINE] = namePrefix + "CompactionDisabled";
    tableNames[ROW_MAJOR] = namePrefix + "CompactionEnabled";
    tableNames[COLUMN_MAJOR] = namePrefix + "CompactionColumnMajor";
    for (int i = 0; i < tableNames.length; i++) {
      Schema schema = schemaSupplier.call();
      schema.setSchemaName(tableNames[i]);
      addSchema(schema);

      UpsertConfig upsertConfig = upsertConfigSupplier.get();
      upsertConfig.setEnableCommitTimeCompaction(i != BASELINE);
      Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, csvHeader);
      csvDecoderProperties.put("stream.kafka.decoder.prop.multiValueDelimiter", ";");
      TableConfig tableConfig = createCSVUpsertTableConfig(tableNames[i], kafkaTopicName, numPartitions,
          csvDecoderProperties, upsertConfig, PRIMARY_KEY_COL);
      indexingConfigCustomizer.accept(tableConfig.getIndexingConfig());
      tableConfig.getIndexingConfig().setColumnMajorSegmentBuilderEnabled(i == COLUMN_MAJOR);
      addTableConfig(tableConfig);
    }
    return tableNames;
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

  protected void waitForAllDocsLoaded(String[] tableNames, long timeoutMs, long expectedDocs)
      throws Exception {
    for (String tableName : tableNames) {
      waitForAllDocsLoaded(tableName, timeoutMs, expectedDocs);
    }
  }

  private static String rowsQuery(String columns, @Nullable String filter, String tableName) {
    return "SELECT " + columns + " FROM " + tableName + (filter != null ? " WHERE " + filter : "")
        + " ORDER BY playerId LIMIT 100";
  }

  /// Every physical row, including the ones upsert replaced or deleted.
  private static String rawRowsQuery(String columns, String tableName) {
    return "SELECT " + columns + " FROM " + tableName
        + " ORDER BY playerId, timestampInEpoch LIMIT 100 OPTION(skipUpsert=true)";
  }

  /// Asserts that every table serves exactly the expected rows, ordered by playerId.
  protected void validateRows(String columns, List<List<Object>> expectedRows, String... tableNames)
      throws Exception {
    validateRows(columns, null, expectedRows, tableNames);
  }

  /// Asserts that every table serves exactly the expected rows that match the filter, ordered by playerId.
  protected void validateRows(String columns, @Nullable String filter, List<List<Object>> expectedRows,
      String... tableNames)
      throws Exception {
    for (String tableName : tableNames) {
      assertRows(rowsQuery(columns, filter, tableName), expectedRows);
    }
  }

  /// Pushes the update records, checks that every table serves the expected rows, commits the consuming segments
  /// (one per partition) and validates the compaction.
  private void updateCommitAndValidate(String[] tableNames, String kafkaTopicName, List<String> updateRecords,
      long expectedRawDocs, String columns, List<List<Object>> expectedRows)
      throws Exception {
    pushCsvIntoKafka(updateRecords, kafkaTopicName, 0);
    waitForAllDocsLoaded(tableNames, 60_000L, expectedRawDocs);
    commitAndValidate(tableNames, getNumKafkaPartitions(), expectedRawDocs, columns, expectedRows);
  }

  /// Checks that every table serves the expected rows, commits the consuming segment of each partition, waits for the
  /// committed segments to be served, and validates the compaction.
  private void commitAndValidate(String[] tableNames, int numStreamPartitions, long expectedRawDocs, String columns,
      List<List<Object>> expectedRows)
      throws Exception {
    validateRows(columns, expectedRows, tableNames);
    // One committed and one new consuming segment per partition
    performCommitAndWait(tableNames, 30_000L, 2 * numStreamPartitions, numStreamPartitions);
    validatePostCommitCompaction(tableNames, expectedRawDocs, columns, expectedRows);
  }

  protected void performCommitAndWait(String[] tableNames, long waitTimeoutMs, int expectedSegments,
      int expectedConsumingSegments)
      throws Exception {
    forceCommit(tableNames);
    // Wait for segments to be committed
    for (String tableName : tableNames) {
      waitForNumQueriedSegmentsToConverge(tableName, waitTimeoutMs, expectedSegments, expectedConsumingSegments);
      waitForClusterStateSettled(TableNameBuilder.REALTIME.tableNameWithType(tableName), expectedSegments, 5,
          waitTimeoutMs);
    }
  }

  protected void forceCommit(String[] tableNames)
      throws Exception {
    for (String tableName : tableNames) {
      getOrCreateAdminClient().getTableClient().forceCommit(tableName);
    }
  }

  /// Validates the tables after the commit: each compacted table holds exactly the expected rows physically, i.e.
  /// commit-time compaction dropped every replaced row and kept every valid one; the baseline table keeps every
  /// ingested row; and all three tables still serve the expected rows.
  protected void validatePostCommitCompaction(String[] tableNames, long expectedBaselineRawDocs, String columns,
      List<List<Object>> expectedRows)
      throws Exception {
    waitForRows(rawRowsQuery(columns, tableNames[ROW_MAJOR]), expectedRows, 30_000L);
    waitForRows(rawRowsQuery(columns, tableNames[COLUMN_MAJOR]), expectedRows, 30_000L);
    assertEquals(getCurrentCountStarResultWithoutUpsert(tableNames[BASELINE]), expectedBaselineRawDocs,
        "Baseline table without commit-time compaction should keep every ingested row");
    validateRows(columns, expectedRows, tableNames);
  }

  protected void cleanupTablesAndSchemas(String[] tableNames) {
    for (String tableName : tableNames) {
      try {
        dropRealtimeTable(tableName);
      } catch (IOException ignored) {
      }
      try {
        deleteSchema(tableName);
      } catch (IOException ignored) {
      }
    }
  }

  private void setConsumingSegmentConsistencyMode(ConsumingSegmentConsistencyModeListener.Mode mode)
      throws Exception {
    updateClusterConfig(Map.of(ConfigChangeListenerConstants.CONSUMING_SEGMENT_CONSISTENCY_MODE, mode.name()));
    waitForConsumingSegmentConsistencyMode(mode);
  }

  private void resetConsumingSegmentConsistencyMode()
      throws Exception {
    deleteClusterConfig(ConfigChangeListenerConstants.CONSUMING_SEGMENT_CONSISTENCY_MODE);
    waitForConsumingSegmentConsistencyMode(
        ConsumingSegmentConsistencyModeListener.Mode.DEFAULT_CONSUMING_SEGMENT_CONSISTENCY_MODE);
  }

  private void waitForConsumingSegmentConsistencyMode(ConsumingSegmentConsistencyModeListener.Mode expectedMode) {
    TestUtils.waitForCondition(
        aVoid -> ConsumingSegmentConsistencyModeListener.getInstance().getConsistencyMode() == expectedMode, 10_000L,
        "Timed out waiting for consuming segment consistency mode to become: " + expectedMode);
  }

  @Test
  public void testCommitTimeCompactionPreservesDeletedRecords()
      throws Exception {
    // Ensure commit-time compaction keeps the latest delete record of each key in the committed segment, so that a
    // deleted key does not come back from an older segment
    String kafkaTopicName = getKafkaTopic() + "-deleted-records-preservation";
    setUpKafka(kafkaTopicName, INPUT_DATA_SMALL_TAR_FILE);
    String[] tables = createTables("gameScoresDeletedRecords", kafkaTopicName, getNumKafkaPartitions(),
        CSV_SCHEMA_HEADER, this::createSchema, () -> {
          UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
          upsertConfig.setDeleteRecordColumn("deleted");
          return upsertConfig;
        }, indexingConfig -> {
        });

    waitForAllDocsLoaded(tables, 60_000L, 10);
    validateRows(GAME_SCORES_COLUMNS, GAME_SCORES_WINNERS, tables);

    // Updates including deletes of an existing player (101) and of a new player (103)
    pushCsvIntoKafka(List.of("100,Zook-Updated1,counter-strike,1000,1681300000000,false",
        "101,Alice-Updated1,dota,2000,1681300001000,true", "102,Bob-Updated1,cs2,3000,1681300002000,false",
        "100,Zook-Updated2,valorant,1500,1681300003000,false", "103,Charlie-New,chess,2500,1681300004000,true"),
        kafkaTopicName, 0);
    waitForAllDocsLoaded(tables, 60_000L, 15);

    // Deleted players 101 and 103 are not served, before and after the commit
    List<List<Object>> expectedRows = List.of(
        List.of(100, "Zook-Updated2", "valorant", 1500.0, 1681300003000L, false),
        List.of(102, "Bob-Updated1", "cs2", 3000.0, 1681300002000L, false));
    validateRows(GAME_SCORES_COLUMNS, expectedRows, tables);
    performCommitAndWait(tables, 30_000L, 4, 2);
    validateRows(GAME_SCORES_COLUMNS, expectedRows, tables);

    // Compaction keeps the latest row of each player, including the delete rows, and drops the rest
    List<List<Object>> expectedRawRows = List.of(expectedRows.get(0),
        List.of(101, "Alice-Updated1", "dota", 2000.0, 1681300001000L, true), expectedRows.get(1),
        List.of(103, "Charlie-New", "chess", 2500.0, 1681300004000L, true));
    waitForRows(rawRowsQuery(GAME_SCORES_COLUMNS, tables[ROW_MAJOR]), expectedRawRows, 30_000L);
    waitForRows(rawRowsQuery(GAME_SCORES_COLUMNS, tables[COLUMN_MAJOR]), expectedRawRows, 30_000L);
    assertEquals(getCurrentCountStarResultWithoutUpsert(tables[BASELINE]), 15L,
        "Baseline table without commit-time compaction should keep every ingested row");

    cleanupTablesAndSchemas(tables);
  }
}
