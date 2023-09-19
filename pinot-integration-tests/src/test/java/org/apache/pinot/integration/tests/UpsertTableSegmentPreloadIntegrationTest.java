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
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;


public class UpsertTableSegmentPreloadIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_SERVERS = 1;
  private static final String PRIMARY_KEY_COL = "clientId";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(DEFAULT_TABLE_NAME);

  // Segment 1 contains records of pk value 100000 (partition 0)
  private static final String UPLOADED_SEGMENT_1 = "mytable_10027_19736_0 %";
  // Segment 2 contains records of pk value 100001 (partition 1)
  private static final String UPLOADED_SEGMENT_2 = "mytable_10072_19919_1 %";
  // Segment 3 contains records of pk value 100002 (partition 1)
  private static final String UPLOADED_SEGMENT_3 = "mytable_10158_19938_2 %";

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBroker();
    startServers(NUM_SERVERS);

    // Start Kafka and push data into Kafka
    startKafka();

    populateTables();
  }

  protected void populateTables()
      throws Exception {
    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createUpsertTableConfig(avroFiles.get(0), PRIMARY_KEY_COL, null, getNumKafkaPartitions());
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    assertNotNull(upsertConfig);
    upsertConfig.setEnableSnapshot(true);
    upsertConfig.setEnablePreload(true);
    addTableConfig(tableConfig);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), TableType.REALTIME, _tarDir);

    pushAvroIntoKafka(avroFiles);
    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(CommonConstants.Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX + ".max.segment.preload.threads",
        "1");
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());

    // Test dropping all segments one by one
    List<String> segments = listSegments(realtimeTableName);
    assertFalse(segments.isEmpty());
    for (String segment : segments) {
      dropSegment(realtimeTableName, segment);
    }
    // NOTE: There is a delay to remove the segment from property store
    TestUtils.waitForCondition((aVoid) -> {
      try {
        return listSegments(realtimeTableName).isEmpty();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to drop the segments");

    dropRealtimeTable(realtimeTableName);
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Override
  protected String getSchemaFileName() {
    return "upsert_upload_segment_test.schema";
  }

  @Override
  protected String getSchemaName() {
    return "upsertSchema";
  }

  @Override
  protected String getAvroTarFileName() {
    return "upsert_upload_segment_test.tar.gz";
  }

  @Override
  protected String getPartitionColumn() {
    return PRIMARY_KEY_COL;
  }

  @Override
  protected long getCountStarResult() {
    // Three distinct records are expected with pk values of 100000, 100001, 100002
    return 3;
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getCurrentCountStarResultWithoutUpsert() == getCountStarResultWithoutUpsert();
      } catch (Exception e) {
        return null;
      }
    }, 100L, timeoutMs, "Failed to load all documents");
    assertEquals(getCurrentCountStarResult(), getCountStarResult());
  }

  private long getCurrentCountStarResultWithoutUpsert() {
    return getPinotConnection().execute("SELECT COUNT(*) FROM " + getTableName() + " OPTION(skipUpsert=true)")
        .getResultSet(0).getLong(0);
  }

  private long getCountStarResultWithoutUpsert() {
    // 3 Avro files, each with 100 documents, one copy from streaming source, one copy from batch source
    return 600;
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    verifyIdealState(5);

    // Run the real-time segment validation and check again
    _controllerStarter.getRealtimeSegmentValidationManager().run();
    verifyIdealState(5);
    assertEquals(getCurrentCountStarResult(), getCountStarResult());
    assertEquals(getCurrentCountStarResultWithoutUpsert(), getCountStarResultWithoutUpsert());

    waitForSnapshotCreation();

    // Restart the servers and check again
    restartServers();
    verifyIdealState(7);
    waitForAllDocsLoaded(600_000L);
  }

  protected void waitForSnapshotCreation()
      throws Exception {
    // Trigger force commit so that snapshots are created
    String rawTableName = getTableName();
    sendPostRequest(_controllerRequestURLBuilder.forTableForceCommit(rawTableName), null);

    // All the uploaded segments should have snapshots generated. There is no guarantee that the just committed segments
    // have snapshots generated because when the snapshot is taken, the committed segment might not be replaced with the
    // immutable segment yet.
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    TestUtils.waitForCondition(aVoid -> {
      for (BaseServerStarter serverStarter : _serverStarters) {
        String segmentDir = serverStarter.getConfig().getProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_DIR);
        File[] files = new File(segmentDir, realtimeTableName).listFiles(
            (dir, name) -> name.startsWith(rawTableName) && !LLCSegmentName.isLLCSegment(name));
        for (File file : files) {
          if (!new File(new File(file, "v3"), V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME).exists()) {
            return false;
          }
        }
      }
      return true;
    }, 600_000L, "Failed to verify snapshots");
  }

  protected void verifyIdealState(int numSegmentsExpected) {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> segmentAssignment = idealState.getRecord().getMapFields();
    assertEquals(segmentAssignment.size(), numSegmentsExpected);

    String serverForPartition0 = null;
    String serverForPartition1 = null;

    int maxSequenceNumber = 0;
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName != null) {
        maxSequenceNumber = Math.max(maxSequenceNumber, llcSegmentName.getSequenceNumber());
      }
    }

    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();

      // Verify that all segments have the correct state
      assertEquals(instanceStateMap.size(), 1);
      Map.Entry<String, String> instanceIdAndState = instanceStateMap.entrySet().iterator().next();
      String state = instanceIdAndState.getValue();
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName != null) {
        if (llcSegmentName.getSequenceNumber() < maxSequenceNumber) {
          assertEquals(state, SegmentStateModel.ONLINE);
        } else {
          assertEquals(state, SegmentStateModel.CONSUMING);
        }
      } else {
        assertEquals(state, SegmentStateModel.ONLINE);
      }

      // Verify that all segments of the same partition are mapped to the same server
      String instanceId = instanceIdAndState.getKey();
      int partitionId = getSegmentPartitionId(segmentName);
      if (partitionId == 0) {
        if (serverForPartition0 == null) {
          serverForPartition0 = instanceId;
        } else {
          assertEquals(instanceId, serverForPartition0);
        }
      } else {
        assertEquals(partitionId, 1);
        if (serverForPartition1 == null) {
          serverForPartition1 = instanceId;
        } else {
          assertEquals(instanceId, serverForPartition1);
        }
      }
    }
  }

  private static int getSegmentPartitionId(String segmentName) {
    switch (segmentName) {
      case UPLOADED_SEGMENT_1:
        return 0;
      case UPLOADED_SEGMENT_2:
      case UPLOADED_SEGMENT_3:
        return 1;
      default:
        return new LLCSegmentName(segmentName).getPartitionGroupId();
    }
  }
}
