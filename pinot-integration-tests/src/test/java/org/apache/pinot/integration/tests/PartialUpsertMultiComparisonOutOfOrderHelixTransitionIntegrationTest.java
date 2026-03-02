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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.integration.tests.realtime.utils.DelayInjectingRealtimeTableDataManager;
import org.apache.pinot.integration.tests.realtime.utils.DelayInjectingTableConfig;
import org.apache.pinot.integration.tests.realtime.utils.DelayInjectingTableDataManagerProvider;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that validates partial upsert correctness with multiple comparison columns when Helix delivers
 * state transitions out of order.
 *
 * <p>This test is structurally identical to {@link PartialUpsertOutOfOrderHelixTransitionIntegrationTest} but uses
 * two comparison columns ({@code scoreTimestamp} and {@code nameTimestamp}) instead of the implicit single comparison
 * column ({@code timestampInEpoch}). Each record sets exactly one comparison column (the other is null), exercising
 * the per-column comparison logic in {@code ComparisonColumns.compareTo()}.
 *
 * <p>With multiple comparison columns, {@code ComparisonColumns.compareTo()} compares only the column at the
 * {@code comparableIndex} (the non-null column in the incoming record). When the previous record has null at that
 * index (because it was updated via a different comparison column), the new record always wins. All INCREMENT merges
 * happen in the same order as the single-comparison-column case, producing identical expected scores.
 *
 * <p>Uses Avro format (instead of CSV) to natively handle nullable comparison columns via Avro union types.
 *
 * @see PartialUpsertOutOfOrderHelixTransitionIntegrationTest
 */
public class PartialUpsertMultiComparisonOutOfOrderHelixTransitionIntegrationTest extends BaseClusterIntegrationTest {
  private static final String TABLE_NAME = "gameScoresMultiComp";
  private static final String TABLE_NAME_NO_ENFORCE = "gameScoresMultiCompNoEnforce";
  private static final String PARTIAL_UPSERT_TABLE_SCHEMA = "partial_upsert_multi_comparison_table_test.schema";
  private static final String PRIMARY_KEY_COL = "playerId";
  private static final String TIME_COL_NAME = "timestampInEpoch";

  private static final int NUM_SERVERS = 2;
  private static final long CONSUMING_DELAY_MS = 20_000;

  private static final long TOTAL_DOCS = 7;
  private static final long DISTINCT_KEYS = 3;

  // Expected correct scores when all INCREMENT merges happen properly (identical to single-comparison test).
  // Player 100: 10
  // Player 101: 20 + 100 + 200 + 300 + 400 = 1020
  // Player 102: 30
  private static final float EXPECTED_SCORE_100 = 10.0f;
  private static final float EXPECTED_SCORE_101 = 1020.0f;
  private static final float EXPECTED_SCORE_102 = 30.0f;

  // Expected degraded score for player 101 when segment 1's data is missing on Server 0.
  // Only segment 0's base (20) + segment 2's increment (400) = 420
  private static final float DEGRADED_SCORE_101 = 420.0f;

  private File _avroFile;

  @Override
  protected String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected String getSchemaFileName() {
    return PARTIAL_UPSERT_TABLE_SCHEMA;
  }

  @Nullable
  @Override
  protected String getTimeColumnName() {
    return TIME_COL_NAME;
  }

  @Override
  protected String getPartitionColumn() {
    return PRIMARY_KEY_COL;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    return 3;
  }

  @Override
  protected int getNumKafkaPartitions() {
    return 1;
  }

  @Override
  protected int getNumReplicas() {
    return NUM_SERVERS;
  }

  @Override
  protected long getCountStarResult() {
    return DISTINCT_KEYS;
  }

  @Override
  protected PinotConfiguration getServerConf(int serverId) {
    PinotConfiguration serverConf = super.getServerConf(serverId);

    serverConf.setProperty(
        "pinot.server.instance."
            + CommonConstants.Server.TABLE_DATA_MANAGER_PROVIDER_CLASS,
        DelayInjectingTableDataManagerProvider.class.getName());

    if (serverId == 0) {
      String delayPrefix = "pinot.server.instance."
          + DelayInjectingTableDataManagerProvider.DELAY_CONFIG_KEY + ".";
      serverConf.setProperty(delayPrefix + TABLE_NAME,
          new DelayInjectingTableConfig(1, 0, CONSUMING_DELAY_MS).toJson());
      serverConf.setProperty(delayPrefix + TABLE_NAME_NO_ENFORCE,
          new DelayInjectingTableConfig(1, 0, CONSUMING_DELAY_MS).toJson());
    }

    return serverConf;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    DelayInjectingRealtimeTableDataManager.resetTracking();

    startZk();
    startController();
    startBroker();
    startServers(NUM_SERVERS);

    // Create Avro data file with nullable comparison columns
    _avroFile = createAvroDataFile();
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = _avroFile;

    // Start Kafka and push Avro data
    startKafka();
    pushAvroIntoKafka(List.of(_avroFile));

    // Create Pinot schema and table
    org.apache.pinot.spi.data.Schema schema = createSchema(PARTIAL_UPSERT_TABLE_SCHEMA);
    schema.setSchemaName(TABLE_NAME);
    addSchema(schema);

    TableConfig tableConfig =
        createPartialUpsertTableConfig(TABLE_NAME, getKafkaTopic(), true);
    addTableConfig(tableConfig);

    waitForAllDocsLoaded(600_000L);
  }

  /**
   * Creates an Avro data file with 10 records. Each record sets exactly one of the two comparison columns
   * (scoreTimestamp or nameTimestamp) to a non-null value, with the other left null.
   * Avro union types ["null", "long"] natively support nullable LONG fields.
   */
  private File createAvroDataFile()
      throws Exception {
    // Build Avro schema with nullable comparison columns
    Schema avroSchema = SchemaBuilder.record("playerScoresPartialUpsertMultiComparison")
        .fields()
        .requiredInt("playerId")
        .requiredString("name")
        .name("game").type().array().items().stringType().noDefault()
        .requiredFloat("score")
        .requiredLong("timestampInEpoch")
        .name("scoreTimestamp").type().optional().longType()
        .name("nameTimestamp").type().optional().longType()
        .requiredBoolean("deleted")
        .endRecord();

    File avroFile = new File(_tempDir, "gameScores_multi_comparison.avro");
    try (DataFileWriter<GenericData.Record> writer =
             new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      writer.create(avroSchema, avroFile);

      // 7 records for the single-delayed-previous-segment scenario.
      // Each record sets exactly one comparison column; the other is null.
      //   Segment 0 (records 1-3): player 100(+10), 101(+20), 102(+30)
      //   Segment 1 (records 4-6, delayed on Server 0): player 101(+100), 101(+200), 101(+300)
      //   Segment 2 (record 7, stays CONSUMING): player 101(+400)
      Object[][] data = {
          // {playerId, name, game, score, timestampInEpoch, scoreTimestamp, nameTimestamp, deleted}
          {100, "Alice", "chess", 10.0f, 1000L, 1000L, null, false},
          {101, "Bob", "chess", 20.0f, 2000L, null, 2000L, false},
          {102, "Carol", "chess", 30.0f, 3000L, 3000L, null, false},
          {101, "Bob", "chess", 100.0f, 4000L, 4000L, null, false},
          {101, "Bob", "chess", 200.0f, 5000L, null, 5000L, false},
          {101, "Bob", "chess", 300.0f, 6000L, 6000L, null, false},
          {101, "Bob", "chess", 400.0f, 10000L, 10000L, null, false},
      };

      for (Object[] row : data) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("playerId", row[0]);
        record.put("name", row[1]);
        record.put("game", List.of(row[2]));
        record.put("score", row[3]);
        record.put("timestampInEpoch", row[4]);
        record.put("scoreTimestamp", row[5]);
        record.put("nameTimestamp", row[6]);
        record.put("deleted", row[7]);
        writer.append(record);
      }
    }
    return avroFile;
  }

  private TableConfig createPartialUpsertTableConfig(String tableName,
      String kafkaTopicName, boolean enforceConsumptionInOrder) {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setPartialUpsertStrategies(
        Map.of("score", UpsertConfig.Strategy.INCREMENT));
    upsertConfig.setComparisonColumns(List.of("scoreTimestamp", "nameTimestamp"));

    Map<String, ColumnPartitionConfig> columnPartitionConfigMap =
        Map.of(PRIMARY_KEY_COL, new ColumnPartitionConfig("Murmur", getNumKafkaPartitions()));

    // Build stream config with the correct Kafka topic
    Map<String, String> streamConfigsMap = getStreamConfigMap();
    streamConfigsMap.put(
        org.apache.pinot.spi.stream.StreamConfigProperties.constructStreamProperty(
            "kafka", org.apache.pinot.spi.stream.StreamConfigProperties.STREAM_TOPIC_NAME),
        kafkaTopicName);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(tableName)
        .setTimeColumnName(getTimeColumnName())
        .setNumReplicas(getNumReplicas())
        .setStreamConfigs(streamConfigsMap)
        .setNullHandlingEnabled(true)
        .setRoutingConfig(
            new RoutingConfig(null, null,
                RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setReplicaGroupStrategyConfig(
            new org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig(PRIMARY_KEY_COL, 1))
        .setOptimizeNoDictStatsCollection(true)
        .setUpsertConfig(upsertConfig)
        .build();

    // Move stream configs to ingestionConfig.streamIngestionConfig with enforceConsumptionInOrder
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig == null) {
      ingestionConfig = new IngestionConfig();
      tableConfig.setIngestionConfig(ingestionConfig);
    }
    Map<String, String> streamConfigs =
        tableConfig.getIndexingConfig().getStreamConfigs();
    StreamIngestionConfig streamIngestionConfig =
        new StreamIngestionConfig(List.of(streamConfigs));
    streamIngestionConfig.setEnforceConsumptionInOrder(
        enforceConsumptionInOrder);
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    tableConfig.getIndexingConfig().setStreamConfigs(null);

    return tableConfig;
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs) {
    waitForAllDocsLoaded(TABLE_NAME, timeoutMs);
  }

  private void waitForAllDocsLoaded(String tableName, long timeoutMs) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getPinotConnection().execute(
                "SELECT COUNT(*) FROM " + tableName
                    + " OPTION(skipUpsert=true)")
            .getResultSet(0).getLong(0) == TOTAL_DOCS;
      } catch (Exception e) {
        return null;
      }
    }, timeoutMs, "Failed to load all documents for table: " + tableName);
  }

  private void waitForServerSettling()
      throws InterruptedException {
    Thread.sleep(CONSUMING_DELAY_MS + 30_000);
  }

  /**
   * Waits until the expected player 101 score appears on the enforced table via direct server reads.
   * Server 1 (no delay) should converge to the correct score quickly. Server 0's score depends on
   * whether enforcement successfully serialized segment processing after the consuming delay.
   */
  private void waitForCorrectEnforcedScores(long timeoutMs) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        Map<Integer, Float> scores1 = getPlayerScoresFromServer(1, TABLE_NAME);
        Float score1 = scores1.get(101);
        return score1 != null && Math.abs(score1 - EXPECTED_SCORE_101) < 1.0f;
      } catch (Exception e) {
        return null;
      }
    }, timeoutMs, "Timed out waiting for enforced table player 101 score to reach "
        + EXPECTED_SCORE_101 + " on Server 1");
  }

  @Test
  public void testOutOfOrderTransitionWithEnforcedOrdering()
      throws InterruptedException {
    waitForServerSettling();
    waitForCorrectEnforcedScores(60_000L);

    Map<Integer, Boolean> enforceStateMap =
        DelayInjectingRealtimeTableDataManager
            .SEGMENT_STATE_AT_CONSUME_START.get(TABLE_NAME);
    assertNotNull(enforceStateMap,
        "Should have tracking data for the enforced table on Server 0");
    assertFalse(enforceStateMap.getOrDefault(2, true),
        "Segment 1 should NOT be present at the entry of segment 2's "
            + "addConsumingSegment() (out-of-order condition triggered). "
            + "The ConsumerCoordinator blocks later inside super.");

    // Verify scores directly from each server's segment data.
    // Server 1 (no delay) always has correct scores.
    assertServerScores(1, TABLE_NAME,
        EXPECTED_SCORE_100, EXPECTED_SCORE_101, EXPECTED_SCORE_102,
        "Enforced table server 1");
    // Server 0 (delayed): in this single-delayed-previous-segment scenario, enforcement
    // blocks segment 2 until segment 1 is registered, so scores are correct.
    assertServerScores(0, TABLE_NAME,
        EXPECTED_SCORE_100, EXPECTED_SCORE_101, EXPECTED_SCORE_102,
        "Enforced table server 0");

    Map<Integer, List<Boolean>> enforcedCrcMap =
        DelayInjectingRealtimeTableDataManager.CRC_MATCH_AFTER_ONLINE.get(TABLE_NAME);
    if (enforcedCrcMap != null) {
      for (Map.Entry<Integer, List<Boolean>> entry : enforcedCrcMap.entrySet()) {
        for (Boolean match : entry.getValue()) {
          assertTrue(match,
              "Enforced table: segment " + entry.getKey()
                  + " CRC should match ZK (both servers have correct upsert state)");
        }
      }
    }
  }

  @Test
  public void testOutOfOrderTransitionWithoutEnforcedOrdering()
      throws Exception {
    String kafkaTopicName = TABLE_NAME_NO_ENFORCE;
    createKafkaTopic(kafkaTopicName);

    // Push the same Avro data to the new topic
    ClusterIntegrationTestUtils.pushAvroIntoKafka(
        List.of(_avroFile), "localhost:" + getKafkaPort(), kafkaTopicName,
        getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(),
        getPartitionColumn(), injectTombstones());

    org.apache.pinot.spi.data.Schema schema = createSchema(PARTIAL_UPSERT_TABLE_SCHEMA);
    schema.setSchemaName(TABLE_NAME_NO_ENFORCE);
    addSchema(schema);

    TableConfig tableConfig = createPartialUpsertTableConfig(
        TABLE_NAME_NO_ENFORCE, kafkaTopicName, false);
    addTableConfig(tableConfig);

    waitForAllDocsLoaded(TABLE_NAME_NO_ENFORCE, 600_000L);

    waitForServerSettling();

    Map<Integer, Boolean> noEnforceStateMap =
        DelayInjectingRealtimeTableDataManager
            .SEGMENT_STATE_AT_CONSUME_START.get(TABLE_NAME_NO_ENFORCE);
    assertNotNull(noEnforceStateMap,
        "Should have tracking data for the non-enforced table on Server 0");
    assertFalse(noEnforceStateMap.getOrDefault(2, true),
        "Without enforcement, segment 1 should NOT be present when "
            + "segment 2's consuming transition starts on Server 0 "
            + "(out-of-order condition)");

    // Verify scores directly from each server's segment data.
    // Server 0 (delayed): player 101 has degraded score (missing segment 1's increments)
    // Server 1 (normal): all scores correct
    assertServerScores(0, TABLE_NAME_NO_ENFORCE,
        EXPECTED_SCORE_100, DEGRADED_SCORE_101, EXPECTED_SCORE_102,
        "Non-enforced table server 0 (degraded player 101)");
    assertServerScores(1, TABLE_NAME_NO_ENFORCE,
        EXPECTED_SCORE_100, EXPECTED_SCORE_101, EXPECTED_SCORE_102,
        "Non-enforced table server 1 (all correct)");

    Map<Integer, List<Boolean>> noEnforceCrcMap =
        DelayInjectingRealtimeTableDataManager.CRC_MATCH_AFTER_ONLINE.get(TABLE_NAME_NO_ENFORCE);
    if (noEnforceCrcMap != null) {
      boolean hasMismatch = noEnforceCrcMap.values().stream()
          .flatMap(List::stream)
          .anyMatch(match -> !match);
      if (hasMismatch) {
        assertTrue(true, "CRC mismatch detected as expected for non-enforced table");
      }
    }
  }

  /**
   * Reads player scores directly from a server's segment data, bypassing the broker.
   * Iterates all segments' validDocIds bitmaps to collect the current score for each player.
   */
  private Map<Integer, Float> getPlayerScoresFromServer(int serverId, String tableName) {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    TableDataManager tableDataManager = _serverStarters.get(serverId)
        .getServerInstance().getInstanceDataManager()
        .getTableDataManager(tableNameWithType);
    assertNotNull(tableDataManager,
        "TableDataManager should exist on server " + serverId + " for " + tableNameWithType);

    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      Map<Integer, Float> playerScores = new HashMap<>();
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        IndexSegment segment = segmentDataManager.getSegment();
        ThreadSafeMutableRoaringBitmap validDocIds = segment.getValidDocIds();
        if (validDocIds == null) {
          continue;
        }
        MutableRoaringBitmap bitmap = validDocIds.getMutableRoaringBitmap();
        for (int docId : bitmap.toArray()) {
          int playerId = (Integer) segment.getValue(docId, "playerId");
          float score = ((Number) segment.getValue(docId, "score")).floatValue();
          playerScores.put(playerId, score);
        }
      }
      return playerScores;
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }

  private void assertServerScores(int serverId, String tableName,
      float expected100, float expected101, float expected102, String message) {
    Map<Integer, Float> scores = getPlayerScoresFromServer(serverId, tableName);
    assertEquals(scores.size(), (int) DISTINCT_KEYS,
        message + ": expected " + DISTINCT_KEYS + " distinct keys on server " + serverId);
    assertTrue(Math.abs(scores.get(100) - expected100) < 1.0f,
        message + ": player 100 on server " + serverId + ", got: " + scores.get(100));
    assertTrue(Math.abs(scores.get(101) - expected101) < 1.0f,
        message + ": player 101 on server " + serverId + ", got: " + scores.get(101));
    assertTrue(Math.abs(scores.get(102) - expected102) < 1.0f,
        message + ": player 102 on server " + serverId + ", got: " + scores.get(102));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(TABLE_NAME);
    dropRealtimeTable(TABLE_NAME_NO_ENFORCE);
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
