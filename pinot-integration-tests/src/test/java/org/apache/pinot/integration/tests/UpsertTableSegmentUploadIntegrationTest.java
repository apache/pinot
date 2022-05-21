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


public class UpsertTableSegmentUploadIntegrationTest extends ClusterTest {
  private static final int NUM_SERVERS = 2;
  private static final String PRIMARY_KEY_COL = "clientId";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(
      DefaultIntegrationTestDataSet.DEFAULT_TABLE_NAME);

  // Segment 1 contains records of pk value 100000 (partition 0)
  private static final String UPLOADED_SEGMENT_1 = "mytable_10027_19736_0 %";
  // Segment 2 contains records of pk value 100001 (partition 1)
  private static final String UPLOADED_SEGMENT_2 = "mytable_10072_19919_1 %";
  // Segment 3 contains records of pk value 100002 (partition 1)
  private static final String UPLOADED_SEGMENT_3 = "mytable_10158_19938_2 %";

  private UpsertTableSegmentUploadIntegrationTestDataSet _testDataSet;

  @Override
  public boolean useLlc() {
    return true;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    setUpTestDirectories(this.getClass().getSimpleName());
    TestUtils.ensureDirectoriesExistAndEmpty(getTempDir(), getSegmentDir(), getTarDir());
    _testDataSet = new UpsertTableSegmentUploadIntegrationTestDataSet(this);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBroker();
    startServers(NUM_SERVERS);

    // Unpack the Avro files
    List<File> avroFiles = _testDataSet.unpackAvroData(getTempDir());

    // Start Kafka and push data into Kafka
    startKafka();
    _testDataSet.pushAvroIntoKafka(avroFiles);

    // Create and upload schema and table config
    Schema schema = _testDataSet.createSchema();
    addSchema(schema);
    TableConfig tableConfig = _testDataSet.createUpsertTableConfig(
        avroFiles.get(0), PRIMARY_KEY_COL, getNumKafkaPartitions());
    addTableConfig(tableConfig);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, getSegmentDir(), getTarDir());
    uploadSegments(_testDataSet.getTableName(), TableType.REALTIME, getTarDir());

    // Wait for all documents loaded
    _testDataSet.waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    dropRealtimeTable(_testDataSet.getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(getTempDir());
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    verifyIdealState();

    // Run the real-time segment validation and check again
    _controllerStarter.getRealtimeSegmentValidationManager().run();
    verifyIdealState();
    assertEquals(_testDataSet.getCurrentCountStarResult(), _testDataSet.getCountStarResult());
    assertEquals(_testDataSet.getCurrentCountStarResultWithoutUpsert(), _testDataSet.getCountStarResultWithoutUpsert());

    // Restart the servers and check again
    restartServers();
    verifyIdealState();
    _testDataSet.waitForAllDocsLoaded(600_000L);
  }

  private void verifyIdealState() {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> segmentAssignment = idealState.getRecord().getMapFields();
    assertEquals(segmentAssignment.size(), 5);

    String serverForPartition0 = null;
    String serverForPartition1 = null;
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();

      // Verify that all segments have the correct state
      assertEquals(instanceStateMap.size(), 1);
      Map.Entry<String, String> instanceIdAndState = instanceStateMap.entrySet().iterator().next();
      String state = instanceIdAndState.getValue();
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        assertEquals(state, SegmentStateModel.CONSUMING);
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

  private static class UpsertTableSegmentUploadIntegrationTestDataSet extends DefaultIntegrationTestDataSet {

    public UpsertTableSegmentUploadIntegrationTestDataSet(ClusterTest clusterTest) {
      super(clusterTest);
    }

    @Override
    public String getSchemaFileName() {
      return "upsert_table_test.schema";
    }

    @Override
    public String getSchemaName() {
      return "upsertSchema";
    }

    @Override
    public String getAvroTarFileName() {
      return "upsert_test.tar.gz";
    }

    @Override
    public String getPartitionColumn() {
      return PRIMARY_KEY_COL;
    }

    @Override
    public long getCountStarResult() {
      // Three distinct records are expected with pk values of 100000, 100001, 100002
      return 3;
    }

    @Override
    public void waitForAllDocsLoaded(long timeoutMs)
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
  }
}
