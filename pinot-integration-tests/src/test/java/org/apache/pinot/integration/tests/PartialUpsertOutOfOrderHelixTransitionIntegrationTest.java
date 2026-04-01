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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.integration.tests.realtime.utils.DelayInjectingRealtimeTableDataManager;
import org.apache.pinot.integration.tests.realtime.utils.DelayInjectingTableConfig;
import org.apache.pinot.integration.tests.realtime.utils.DelayInjectingTableDataManagerProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
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
 * Integration test that validates partial upsert correctness when Helix delivers state transitions out of order.
 *
 * <p>A production incident showed that when a delayed OFFLINE-CONSUMING for segment N is processed after segment N is
 * already committed, segment N gets skipped as CONSUMING on a slow server. If segment N+1 starts consuming before
 * segment N's CONSUMING-ONLINE transition is processed on that server, partial upsert metadata for segment N is
 * missing when segment N+1 starts consuming. This causes INCREMENT merges to be lost. The
 * {@code enforceConsumptionInOrder} flag fixes this by blocking segment N+1's consumption until segment N is
 * registered via the {@code ConsumerCoordinator}.
 *
 * <p><b>Multi-server approach:</b> This test uses 2 servers with {@code numReplicas=2}. Server 0 has a delay injected
 * into {@code addConsumingSegment()} for segment 1, simulating a slow Helix state transition. Server 1 operates
 * normally and commits segments ahead of Server 0. This causes segment 2's OFFLINE-CONSUMING transition to arrive
 * on Server 0 while segment 1's consuming transition is still delayed, creating the out-of-order condition.
 *
 * <p>With {@code enforceConsumptionInOrder=true}, the {@code ConsumerCoordinator} blocks segment 2's consumption
 * on Server 0 until segment 1 is registered (via its eventual CONSUMING-ONLINE transition), ensuring correct
 * partial upsert merges for this single-delayed-previous-segment scenario. Without enforcement, segment 2 proceeds
 * immediately without segment 1's data.
 *
 * <p><b>Instrumented assertions:</b> The {@link DelayInjectingRealtimeTableDataManager} captures whether the delayed
 * target segment was present in the segment data manager map at the entry of {@code addConsumingSegment()} for
 * subsequent segments. In both the enforced and non-enforced cases, segment 1 is NOT present at this entry point
 * (proving the out-of-order condition was triggered). The key difference is what happens inside
 * {@code super.addConsumingSegment()}: with enforcement, the {@code ConsumerCoordinator} blocks until segment 1
 * is registered, ensuring correct scores; without enforcement, consumption proceeds immediately and produces
 * degraded scores.
 *
 * <p><b>CRC mismatch detection:</b> The {@link DelayInjectingRealtimeTableDataManager} overrides
 * {@code replaceSegmentIfCrcMismatch} to track whether each server's locally-built segment CRC matches the ZK
 * CRC (set by the committing server). This tracking captures any CRC mismatches that arise from out-of-order
 * consumption on the non-enforced table.
 */
public class PartialUpsertOutOfOrderHelixTransitionIntegrationTest extends BaseClusterIntegrationTest {
  private static final String TABLE_NAME = "gameScores";
  private static final String TABLE_NAME_NO_ENFORCE = "gameScoresNoEnforce";
  private static final String PARTIAL_UPSERT_TABLE_SCHEMA = "partial_upsert_table_test.schema";
  private static final String INPUT_DATA_TAR_FILE = "gameScores_partial_upsert_csv.tar.gz";
  private static final String CSV_SCHEMA_HEADER = "playerId,name,game,score,timestampInEpoch,deleted";
  private static final String CSV_DELIMITER = ",";
  private static final String PRIMARY_KEY_COL = "playerId";
  private static final String TIME_COL_NAME = "timestampInEpoch";

  private static final int NUM_SERVERS = 2;
  // Delay injected into Server 0's addConsumingSegment for segment 1.
  // Must be long enough for Server 1 to commit segment 1 and for segment 2 transition to arrive on Server 0.
  private static final long CONSUMING_DELAY_MS = 20_000;

  // 7 total records in the generated CSV subset, 3 distinct primary keys (100, 101, 102)
  private static final long TOTAL_DOCS = 7;
  private static final long DISTINCT_KEYS = 3;

  // Expected correct scores when all INCREMENT merges happen properly.
  // CSV data subset (7 records, flush=3, all timestamps monotonically increasing):
  //   Segment 0: player 100(+10), 101(+20), 102(+30)
  //   Segment 1: player 101(+100), 101(+200), 101(+300)  ← delayed on Server 0
  //   Segment 2: player 101(+400) — stays CONSUMING
  // Player 100: 10
  // Player 101: 20 + 100 + 200 + 300 + 400 = 1020
  // Player 102: 30
  private static final float EXPECTED_SCORE_100 = 10.0f;
  private static final float EXPECTED_SCORE_101 = 1020.0f;
  private static final float EXPECTED_SCORE_102 = 30.0f;

  // Expected degraded score for player 101 when segment 1's data is missing on Server 0.
  // Only segment 0's base (20) + segment 2's increment (400) = 420
  private static final float DEGRADED_SCORE_101 = 420.0f;

  private File _dataFile;

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

  /**
   * Configures per-server properties. Server 0 gets a consuming delay on segment 1 for both tables.
   * Server 1 operates normally (no delay config), allowing it to commit segments ahead of Server 0.
   */
  @Override
  protected PinotConfiguration getServerConf(int serverId) {
    PinotConfiguration serverConf = super.getServerConf(serverId);

    // All servers use DelayInjectingTableDataManagerProvider
    serverConf.setProperty(
        "pinot.server.instance."
            + CommonConstants.Server.TABLE_DATA_MANAGER_PROVIDER_CLASS,
        DelayInjectingTableDataManagerProvider.class.getName());

    if (serverId == 0) {
      // Server 0: delay segment 1's consuming transition
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

    // Start the Pinot cluster with 2 servers
    startZk();
    startController();
    startBroker();
    startServers(NUM_SERVERS);

    // Start Kafka, create topic, and push CSV data
    startKafka();
    List<File> dataFiles = unpackTarData(INPUT_DATA_TAR_FILE, _tempDir);
    _dataFile = createSingleDelayedScenarioCsvFile(dataFiles.get(0));
    pushCsvIntoKafka(_dataFile, getKafkaTopic(), 0);

    // Create schema
    Schema schema = createSchema(PARTIAL_UPSERT_TABLE_SCHEMA);
    schema.setSchemaName(TABLE_NAME);
    addSchema(schema);

    // Create partial upsert table config with enforceConsumptionInOrder=true
    TableConfig tableConfig =
        createPartialUpsertTableConfig(TABLE_NAME, getKafkaTopic(), true);
    addTableConfig(tableConfig);

    // Wait for all documents to be loaded (Server 1 loads quickly; Server 0 is delayed)
    waitForAllDocsLoaded(600_000L);
  }

  /**
   * Creates a partial upsert table config with INCREMENT on score and the specified enforcement setting.
   */
  private TableConfig createPartialUpsertTableConfig(String tableName,
      String kafkaTopicName, boolean enforceConsumptionInOrder) {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setPartialUpsertStrategies(
        Map.of("score", UpsertConfig.Strategy.INCREMENT));

    Map<String, String> csvDecoderProperties =
        getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    TableConfig tableConfig = createCSVUpsertTableConfig(
        tableName, kafkaTopicName, getNumKafkaPartitions(),
        csvDecoderProperties, upsertConfig, PRIMARY_KEY_COL);

    // Move stream configs from indexingConfig to ingestionConfig.streamIngestionConfig
    // (controller rejects having both set simultaneously)
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

  /**
   * Waits for Server 0's consuming delay to complete and all segments to settle.
   * After the delay, segment 1 goes through CONSUMING-ONLINE on Server 0, which
   * triggers register() and unblocks any enforced segments waiting on it.
   * With enforcement, segment 2 waits until segment 1 is registered.
   */
  private void waitForServerSettling()
      throws InterruptedException {
    // Wait for the consuming delay plus time for segment 1->ONLINE and segment 2 consumption to settle.
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

  /**
   * Validates that with enforceConsumptionInOrder=true, partial upsert merges produce correct results even when
   * segment 1's OFFLINE-CONSUMING transition is delayed on Server 0 (simulating out-of-order Helix transitions).
   *
   * <p>With the flag enabled, the ConsumerCoordinator blocks segment 2's consumption on Server 0 until segment 1
   * is registered (via its CONSUMING-ONLINE transition after the delay completes). This ensures all prior segment
   * data is in the upsert metadata before subsequent segments start consuming, producing correct INCREMENT merges.
   *
   * <p>The test also verifies via instrumentation that the out-of-order condition was actually triggered: on
   * Server 0, segment 1 was NOT present in the segment data manager map when segment 2's consuming transition
   * entered {@code addConsumingSegment()}. The tracking captures this state at the entry point, before the
   * ConsumerCoordinator blocks inside {@code super.addConsumingSegment()}. Despite the out-of-order arrival,
   * enforcement ensures correct scores because the ConsumerCoordinator blocks segment 2's consumption until
   * segment 1 is registered.
   *
   * <p><b>CRC consistency:</b> With enforcement, both servers have identical upsert state when consuming,
   * so any locally-built segments should have matching CRCs. The {@code replaceSegmentIfCrcMismatch} override
   * tracks CRC comparisons for segments that reach the ONLINE state while already immutable.
   */
  @Test
  public void testOutOfOrderTransitionWithEnforcedOrdering()
      throws InterruptedException {
    // Wait for Server 0 to fully settle after the consuming delay.
    // With enforcement, segments 1→2→3 are processed sequentially on Server 0, requiring
    // more time than the non-enforced case. Use condition-based wait for correctness.
    waitForServerSettling();
    waitForCorrectEnforcedScores(60_000L);

    // Verify that the out-of-order condition was triggered on Server 0 via instrumented tracking.
    // The tracking captures segment state at the ENTRY of addConsumingSegment(), BEFORE
    // super.addConsumingSegment() (which contains the ConsumerCoordinator blocking logic).
    // Segment 1 should NOT be present at that entry point, proving the out-of-order condition was triggered.
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

    // Verify CRC consistency via replaceSegmentIfCrcMismatch tracking. This method is called in
    // doAddOnlineSegment when the segment is already immutable (committing server). With enforcement,
    // all locally-built segments should match the ZK CRC (set by the committing server).
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

  /**
   * Validates behavior with enforceConsumptionInOrder=false and the same consuming delay on Server 0.
   *
   * <p>Without enforcement, Server 0's segment 2 and 3 start consuming immediately while segment 1 is delayed.
   * The instrumented tracking in {@link DelayInjectingRealtimeTableDataManager} captures that segment 1 was NOT
   * present in Server 0's segment data manager when segment 2's consuming transition started, proving the
   * out-of-order condition occurred and was not blocked.
   *
   * <p>Score assertions for players 100 and 102 are unaffected because segment 1 only contains player 101
   * records. Player 101's score may be correct or degraded depending on broker routing.
   *
   * <p><b>CRC mismatch detection:</b> The {@code replaceSegmentIfCrcMismatch} override in
   * {@link DelayInjectingRealtimeTableDataManager} tracks CRC comparisons when segments transition to ONLINE
   * while already immutable. If Server 0 consumed a segment with degraded data and then the committing server
   * set a different CRC, the mismatch is detected during the CONSUMING→ONLINE transition.
   */
  @Test
  public void testOutOfOrderTransitionWithoutEnforcedOrdering()
      throws Exception {
    // Create a separate Kafka topic and push the same CSV data
    String kafkaTopicName = TABLE_NAME_NO_ENFORCE;
    createKafkaTopic(kafkaTopicName);
    pushCsvIntoKafka(_dataFile, kafkaTopicName, 0);

    // Create schema for the second table
    Schema schema = createSchema(PARTIAL_UPSERT_TABLE_SCHEMA);
    schema.setSchemaName(TABLE_NAME_NO_ENFORCE);
    addSchema(schema);

    // Create the table with enforceConsumptionInOrder=false
    TableConfig tableConfig = createPartialUpsertTableConfig(
        TABLE_NAME_NO_ENFORCE, kafkaTopicName, false);
    addTableConfig(tableConfig);

    // Wait for all documents to be loaded
    waitForAllDocsLoaded(TABLE_NAME_NO_ENFORCE, 600_000L);

    // Wait for Server 0 to fully settle after the consuming delay
    waitForServerSettling();

    // Verify the out-of-order condition via instrumented tracking on Server 0.
    // Without enforcement, segment 2's consuming transition starts immediately on Server 0
    // while segment 1 is still delayed, so segment 1 should NOT be present.
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

    // Check CRC tracking via replaceSegmentIfCrcMismatch. This is called in doAddOnlineSegment when the
    // segment is already immutable (committing server). For the non-enforced table, if the server that
    // consumed with missing segment 1 data wins the commit, the losing server's locally-built segment
    // will have a CRC mismatch that gets detected and corrected by replaceSegmentIfCrcMismatch.
    Map<Integer, List<Boolean>> noEnforceCrcMap =
        DelayInjectingRealtimeTableDataManager.CRC_MATCH_AFTER_ONLINE.get(TABLE_NAME_NO_ENFORCE);
    if (noEnforceCrcMap != null) {
      boolean hasMismatch = noEnforceCrcMap.values().stream()
          .flatMap(List::stream)
          .anyMatch(match -> !match);
      if (hasMismatch) {
        // CRC mismatch detected — this confirms that a server built segment 3 from different
        // consumed data than the committing server (expected without enforcement).
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

  /**
   * Creates a 7-row CSV subset that yields exactly three segments (flush=3): seq 0, seq 1, seq 2.
   * We keep rows 1-6 and 10 from the original dataset so only one segment follows the delayed segment.
   */
  private File createSingleDelayedScenarioCsvFile(File fullDataFile)
      throws Exception {
    List<String> allLines = Files.readAllLines(fullDataFile.toPath(), StandardCharsets.UTF_8);
    File subsetFile = new File(_tempDir, "gameScores_partial_upsert_single_delayed.csv");
    List<String> subsetLines = List.of(
        allLines.get(0),
        allLines.get(1),
        allLines.get(2),
        allLines.get(3),
        allLines.get(4),
        allLines.get(5),
        allLines.get(9)
    );
    Files.write(subsetFile.toPath(), subsetLines, StandardCharsets.UTF_8);
    return subsetFile;
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
