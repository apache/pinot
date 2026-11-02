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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.restlet.resources.PauseStatusDetails;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.QuotaEnforcementMode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for the per-partition primary key quota on upsert tables.
 *
 * <p>The gameScores data set has only 3 distinct primary keys (playerId 100/101/102). With 2 Kafka partitions and a cap
 * of 1 primary key per partition, at least one partition is guaranteed (pigeonhole) to hold 2 keys, so the quota is
 * always breached regardless of the hash distribution. This makes the breach deterministic.
 *
 * <p>SHADOW: the breach meter fires but consumption is not paused. ENFORCED: the table is paused with reason
 * {@link org.apache.pinot.spi.config.table.PauseState.ReasonCode#PRIMARY_KEY_QUOTA_EXCEEDED}.
 */
public class UpsertPrimaryKeyQuotaIntegrationTest extends BaseClusterIntegrationTest {
  private static final String INPUT_DATA_LARGE_TAR_FILE = "gameScores_large_csv.tar.gz";
  private static final String CSV_SCHEMA_HEADER = "playerId,name,game,score,timestampInEpoch,deleted";
  private static final String CSV_DELIMITER = ",";
  private static final String PRIMARY_KEY_COL = "playerId";
  private static final String TIME_COL_NAME = "timestampInEpoch";
  private static final String UPSERT_SCHEMA_FILE_NAME = "upsert_table_test.schema";
  private static final int NUM_SERVERS = 2;
  // 1 primary key per partition; 3 distinct keys over 2 partitions always breaches this
  private static final long PK_CAP = 1L;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startController();
    startBroker();
    startServers(NUM_SERVERS);
    startKafkaWithoutTopic();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Override
  protected String getSchemaFileName() {
    return UPSERT_SCHEMA_FILE_NAME;
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

  private TableConfig setUpTableWithPkQuota(String tableName, String kafkaTopicName, QuotaEnforcementMode mode)
      throws Exception {
    createKafkaTopic(kafkaTopicName);
    List<File> dataFiles = unpackTarData(INPUT_DATA_LARGE_TAR_FILE, _tempDir);
    pushCsvIntoKafka(dataFiles.get(0), kafkaTopicName, 0);

    Schema schema = createSchema();
    schema.setSchemaName(tableName);
    addSchema(schema);

    Map<String, String> csvDecoderProperties = getCSVDecoderProperties(CSV_DELIMITER, CSV_SCHEMA_HEADER);
    TableConfig tableConfig =
        createCSVUpsertTableConfig(tableName, kafkaTopicName, getNumKafkaPartitions(), csvDecoderProperties,
            new UpsertConfig(UpsertConfig.Mode.FULL), PRIMARY_KEY_COL);
    tableConfig.setQuotaConfig(new QuotaConfig(null, null, null, null, PK_CAP, mode));
    addTableConfig(tableConfig);
    return tableConfig;
  }

  private long breachMeterCount(String realtimeTableName) {
    return _controllerStarter.getControllerMetrics()
        .getMeteredTableValue(realtimeTableName, ControllerMeter.PRIMARY_KEY_QUOTA_EXCEEDED).count();
  }

  private PauseStatusDetails getPauseStatus(String realtimeTableName)
      throws Exception {
    return getOrCreateAdminClient().getTableClient().getPauseStatusDetails(realtimeTableName);
  }

  // The large gameScores data set has 1000 rows mapping to 3 distinct primary keys.
  private void waitForThreeDistinctKeys(String realtimeTableName) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getCurrentCountStarResult(realtimeTableName) == 3;
      } catch (Exception e) {
        return false;
      }
    }, 1000L, 600_000L, "Failed to load all documents");
  }

  @Test
  public void testPrimaryKeyQuotaShadowModeMetersButDoesNotPause()
      throws Exception {
    String tableName = "gameScoresPkQuotaShadow";
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    setUpTableWithPkQuota(tableName, getKafkaTopic() + "-pk-quota-shadow", QuotaEnforcementMode.SHADOW);
    waitForThreeDistinctKeys(realtimeTableName);

    long before = breachMeterCount(realtimeTableName);
    _controllerStarter.getRealtimeSegmentValidationManager().run();

    // The breach is metered...
    assertTrue(breachMeterCount(realtimeTableName) > before,
        "PRIMARY_KEY_QUOTA_EXCEEDED meter should have fired in SHADOW mode");
    // ...but consumption is NOT paused.
    assertFalse(getPauseStatus(realtimeTableName).getPauseFlag(), "Table must not be paused in SHADOW mode");

    dropRealtimeTable(realtimeTableName);
  }

  @Test
  public void testPrimaryKeyQuotaEnforcedModePausesConsumption()
      throws Exception {
    String tableName = "gameScoresPkQuotaEnforced";
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    setUpTableWithPkQuota(tableName, getKafkaTopic() + "-pk-quota-enforced", QuotaEnforcementMode.ENFORCED);
    waitForThreeDistinctKeys(realtimeTableName);

    _controllerStarter.getRealtimeSegmentValidationManager().run();

    TestUtils.waitForCondition(aVoid -> {
      try {
        PauseStatusDetails pauseStatus = getPauseStatus(realtimeTableName);
        return pauseStatus.getPauseFlag()
            && pauseStatus.getReasonCode() == PauseState.ReasonCode.PRIMARY_KEY_QUOTA_EXCEEDED;
      } catch (Exception e) {
        return false;
      }
    }, 1000L, 60_000L, "Table should be paused with PRIMARY_KEY_QUOTA_EXCEEDED in ENFORCED mode");

    assertTrue(breachMeterCount(realtimeTableName) > 0);

    dropRealtimeTable(realtimeTableName);
  }

  @Override
  protected long getCountStarResult() {
    return 3;
  }
}
