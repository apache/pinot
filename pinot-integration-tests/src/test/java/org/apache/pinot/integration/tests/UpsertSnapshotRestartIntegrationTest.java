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

import com.google.common.base.Joiner;
import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Restarts the server of a full upsert table with metadata TTL, snapshot and preload enabled, consuming from Kafka.
/// Holding the segmentLock of the first segment while the next segments are committed makes the snapshot passes skip
/// it, like a segment reload holding the lock does, so its snapshot on disk is stale or missing while it is out of TTL.
/// The restarted server must still return exactly one row per primary key, i.e. the segment must not be added as out
/// of TTL with the stale snapshot, or with all docs valid when there is no snapshot.
public class UpsertSnapshotRestartIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String CSV_HEADER = "playerId,name,game,score,timestampInEpoch,deleted";
  private static final int NUM_KEYS = 20;
  /// Must be smaller than the comparison-column gap between phase 1 (~1k) and phase 2 (~200k) so seg0 is out of TTL
  /// after phase 2.
  private static final double TTL_MS = 100_000d;
  private static final long PHASE1_BASE_TS = 1_000L;
  private static final long PHASE2_BASE_TS = 200_000L;
  private static final long WAIT_MS = 300_000L;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startController();
    startBroker();
    startServers(1);
    startKafkaWithoutTopic();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Override
  protected String getSchemaFileName() {
    return "upsert_table_test.schema";
  }

  @Nullable
  @Override
  protected String getTimeColumnName() {
    return "timestampInEpoch";
  }

  @Override
  protected String getPartitionColumn() {
    return "playerId";
  }

  @Override
  protected int getNumKafkaPartitions() {
    return 1;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    return NUM_KEYS;
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Joiner.on(".")
        .join(Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX, Server.Upsert.CONFIG_PREFIX,
            Server.Upsert.DEFAULT_ENABLE_SNAPSHOT), "true");
    serverConf.setProperty(Joiner.on(".")
        .join(Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX, Server.Upsert.CONFIG_PREFIX,
            Server.Upsert.DEFAULT_ENABLE_PRELOAD), "true");
  }

  @Test
  public void testRestartWithoutLockContention()
      throws Exception {
    runScenario("noContention", false, false, false);
  }

  @Test
  public void testRestartWithStaleSnapshot()
      throws Exception {
    runScenario("staleSnapshot", true, false, false);
  }

  @Test
  public void testRestartWithMissingSnapshot()
      throws Exception {
    runScenario("missingSnapshot", true, true, false);
  }

  @Test
  public void testReloadAfterRestart()
      throws Exception {
    runScenario("reload", true, false, true);
  }

  private List<String> rows(long baseTs) {
    List<String> rows = new ArrayList<>();
    for (int pk = 1; pk <= NUM_KEYS; pk++) {
      rows.add(pk + ",name" + pk + ",game," + pk + "," + (baseTs + pk) + ",false");
    }
    return rows;
  }

  private RealtimeTableDataManager tdm(String realtimeTable) {
    return (RealtimeTableDataManager) _serverStarters.get(0).getServerInstance().getInstanceDataManager()
        .getTableDataManager(realtimeTable);
  }

  private List<String> sortedLlcSegmentNames(String realtimeTable) {
    IdealState idealState = _helixResourceManager.getTableIdealState(realtimeTable);
    if (idealState == null) {
      return List.of();
    }
    List<String> names = new ArrayList<>();
    for (String segmentName : idealState.getRecord().getMapFields().keySet()) {
      if (LLCSegmentName.isLLCSegment(segmentName)) {
        names.add(segmentName);
      }
    }
    names.sort(Comparator.comparingInt(name -> LLCSegmentName.of(name).getSequenceNumber()));
    return names;
  }

  private File segmentDir(String realtimeTable, TableConfig tableConfig, String segmentName) {
    return tdm(realtimeTable).getSegmentDataDir(segmentName, null, tableConfig);
  }

  private static File findSnapshot(File segmentDir) {
    File v3 = new File(segmentDir, "v3");
    File snap = new File(v3, V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME);
    if (snap.exists()) {
      return snap;
    }
    File[] found = new File[1];
    FileUtils.listFiles(segmentDir, null, true).forEach(f -> {
      if (f.getName().equals(V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME)) {
        found[0] = f;
      }
    });
    return found[0];
  }

  private long countStar(String table)
      throws Exception {
    return getPinotConnection().execute("SELECT COUNT(*) FROM " + table).getResultSet(0).getLong(0, 0);
  }

  private int duplicatePkCount(String table)
      throws Exception {
    ResultSet dups = getPinotConnection().execute(
        "SELECT playerId, COUNT(*) FROM " + table + " GROUP BY playerId HAVING COUNT(*) > 1 LIMIT 100")
        .getResultSet(0);
    return dups.getRowCount();
  }

  private String summary(String table)
      throws Exception {
    return "count(*)=" + countStar(table) + ", pks with >1 rows=" + duplicatePkCount(table);
  }

  private void waitForRowCount(String table, long expected)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return countStar(table) == expected;
      } catch (Exception e) {
        return false;
      }
    }, 500L, WAIT_MS, "expected " + expected + " rows in " + table);
  }

  private void waitForLlcSegments(String realtimeTable, int minSegments)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> sortedLlcSegmentNames(realtimeTable).size() >= minSegments, 500L, WAIT_MS,
        "expected >=" + minSegments + " LLC segments in ideal state, have "
            + sortedLlcSegmentNames(realtimeTable).size());
  }

  private void runScenario(String name, boolean holdLock, boolean deleteSnapshotFile, boolean reloadAfterRestart)
      throws Exception {
    String table = "upsertSnapshotRestart_" + name;
    String rtTable = TableNameBuilder.REALTIME.tableNameWithType(table);
    String topic = table + "_topic";
    createKafkaTopic(topic);
    Schema schema = createSchema();
    schema.setSchemaName(table);
    addSchema(schema);
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setSnapshot(Enablement.ENABLE);
    upsertConfig.setEnablePreload(true);
    upsertConfig.setMetadataTTL(TTL_MS);
    TableConfig tableConfig = createCSVUpsertTableConfig(table, topic, getNumKafkaPartitions(),
        getCSVDecoderProperties(",", CSV_HEADER), upsertConfig, "playerId");
    addTableConfig(tableConfig);

    pushCsvIntoKafka(rows(PHASE1_BASE_TS), topic, 0);
    waitForRowCount(table, NUM_KEYS);
    waitForLlcSegments(rtTable, 2);
    String seg0Name = sortedLlcSegmentNames(rtTable).get(0);

    Lock lock = holdLock ? tdm(rtTable).getSegmentLock(seg0Name) : null;
    if (lock != null) {
      lock.lock();
    }
    try {
      pushCsvIntoKafka(rows(PHASE2_BASE_TS), topic, 0);
      waitForRowCount(table, NUM_KEYS);
      waitForLlcSegments(rtTable, 3);
      Thread.sleep(10_000L);
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
    if (deleteSnapshotFile) {
      File snapshot = findSnapshot(segmentDir(rtTable, tableConfig, seg0Name));
      if (snapshot != null) {
        FileUtils.forceDelete(snapshot);
      }
    }
    assertEquals(countStar(table), (long) NUM_KEYS, "pre-restart count");

    _serverStarters.set(0, restartServer(_serverStarters.get(0)));
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getPinotConnection().execute("SELECT COUNT(*) FROM " + table).getExecutionStats()
            .getNumSegmentsQueried() >= 3;
      } catch (Exception e) {
        return false;
      }
    }, 1000L, WAIT_MS, "segments back online after restart");
    Thread.sleep(5_000L);
    String afterRestart = summary(table);

    assertEquals(countStar(table), (long) NUM_KEYS,
        name + ": expected one row per primary key after restart, got " + afterRestart);
    assertEquals(duplicatePkCount(table), 0,
        name + ": found duplicate primary keys after restart, got " + afterRestart);

    if (reloadAfterRestart) {
      String jobId = reloadTableAndValidateResponse(table, TableType.REALTIME, false);
      TestUtils.waitForCondition(aVoid -> {
        try {
          return isReloadJobCompleted(jobId);
        } catch (Exception e) {
          return false;
        }
      }, 1000L, WAIT_MS, "reload job finished");
      Thread.sleep(3_000L);
      String afterReload = summary(table);
      assertEquals(countStar(table), (long) NUM_KEYS, "reload should keep one row per PK, got " + afterReload);
      assertEquals(duplicatePkCount(table), 0, "duplicate PKs after reload, got " + afterReload);
    }
    dropRealtimeTable(table);
  }
}
