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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


/// Uploads segments to a full-upsert real-time table that also consumes the same records from the stream, and checks
/// that each uploaded segment is assigned with its partition and that the table serves the latest row per key, before
/// and after a server restart. [UpsertTableSegmentPreloadIntegrationTest] runs the same flow with snapshots and
/// preload.
public class UpsertTableSegmentUploadIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String PRIMARY_KEY_COL = "clientId";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(DEFAULT_TABLE_NAME);
  /// 3 uploaded segments and 2 consuming segments
  protected static final int NUM_SEGMENTS = 5;

  // Segment 1 contains records of pk value 100000 (partition 0)
  private static final String UPLOADED_SEGMENT_1 = "mytable_10027_19736_0 %";
  // Segment 2 contains records of pk value 100001 (partition 1)
  private static final String UPLOADED_SEGMENT_2 = "mytable_10072_19919_1 %";
  // Segment 3 contains records of pk value 100002 (partition 1)
  private static final String UPLOADED_SEGMENT_3 = "mytable_10158_19938_2 %";

  private List<File> _avroFiles;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBroker();
    startServers(getNumServers());
    startKafka();

    // Unpack the Avro files
    _avroFiles = unpackAvroData(_tempDir);
    if (pushBeforeUpload()) {
      pushAvroIntoKafka(_avroFiles);
    }

    // Create and upload schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig =
        createUpsertTableConfig(_avroFiles.get(0), PRIMARY_KEY_COL, null, getNumKafkaPartitions());
    addTableConfig(tableConfig);

    // Create and upload segments with the same records as the stream
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(_avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), TableType.REALTIME, _tarDir);
    if (!pushBeforeUpload()) {
      pushAvroIntoKafka(_avroFiles);
    }

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
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

  protected int getNumServers() {
    return 2;
  }

  /// Whether the records are pushed into the stream before the table is created, so that the uploaded segments take
  /// over keys the consuming segments already hold. Otherwise they are pushed after the segments are uploaded.
  protected boolean pushBeforeUpload() {
    return true;
  }

  @Override
  protected String getSchemaFileName() {
    return "upsert_upload_segment_test.schema";
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
        return getCurrentCountStarResultWithoutUpsert(getTableName()) == getCountStarResultWithoutUpsert();
      } catch (Exception e) {
        return null;
      }
    }, 100L, timeoutMs, "Failed to load all documents");
    assertLatestRowPerKey();
  }

  /// The table serves, per clientId, the Avro record with the largest DaysSinceEpoch. The streaming and the uploaded
  /// copies of a record are identical, so it does not matter which copy wins.
  private void assertLatestRowPerKey()
      throws Exception {
    assertLatestRowPerKey(getTableName(), _avroFiles, PRIMARY_KEY_COL, getTimeColumnName(),
        List.of(PRIMARY_KEY_COL, "city", "description", "salary", getTimeColumnName()));
  }

  private long getCountStarResultWithoutUpsert() {
    // 3 Avro files, each with 100 documents, one copy from streaming source, one copy from batch source
    return 600;
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    verifyIdealState(NUM_SEGMENTS);

    // Run the real-time segment validation and check again
    _controllerStarter.getRealtimeSegmentValidationManager().run();
    verifyIdealState(NUM_SEGMENTS);
    assertLatestRowPerKey();
    assertEquals(getCurrentCountStarResultWithoutUpsert(getTableName()), getCountStarResultWithoutUpsert());

    beforeRestart();

    // Restart the servers and check again
    restartServers();
    verifyIdealState(getNumSegmentsAfterRestart());
    // The row check runs once, so first wait for every segment to be served again
    waitForClusterStateSettled(TableNameBuilder.REALTIME.tableNameWithType(getTableName()),
        getNumSegmentsAfterRestart(), 5, 60_000L);
    waitForAllDocsLoaded(600_000L);
  }

  /// Runs before the servers restart in [#testSegmentAssignment()].
  protected void beforeRestart()
      throws Exception {
  }

  protected int getNumSegmentsAfterRestart() {
    return NUM_SEGMENTS;
  }

  /// Verifies the segment states (uploaded and committed segments ONLINE, the latest LLC segment of each partition
  /// CONSUMING), and that all segments of a partition are on the same server.
  private void verifyIdealState(int numSegmentsExpected) {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> segmentAssignment = idealState.getRecord().getMapFields();
    assertEquals(segmentAssignment.size(), numSegmentsExpected);

    String serverForPartition0 = null;
    String serverForPartition1 = null;

    // Latest LLC sequence number of each partition: that segment is CONSUMING, the earlier ones ONLINE
    Map<Integer, Integer> partitionToMaxSequenceNumber = new HashMap<>();
    for (String segmentName : segmentAssignment.keySet()) {
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName != null) {
        partitionToMaxSequenceNumber.merge(llcSegmentName.getPartitionGroupId(), llcSegmentName.getSequenceNumber(),
            Math::max);
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
        if (llcSegmentName.getSequenceNumber() < partitionToMaxSequenceNumber.get(
            llcSegmentName.getPartitionGroupId())) {
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
