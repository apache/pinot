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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class UpsertTableRebalanceIntegrationTest extends BaseClusterIntegrationTest {
  private static final int NUM_SERVERS = 1;
  private static final String PRIMARY_KEY_COL = "clientId";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(DEFAULT_TABLE_NAME);

  // Segment 1 contains records of pk value 100000 (partition 0)
  private static final String UPLOADED_SEGMENT_1 = "mytable_10027_19736_0 %";
  // Segment 2 contains records of pk value 100001 (partition 1)
  private static final String UPLOADED_SEGMENT_2 = "mytable_10072_19919_1 %";
  // Segment 3 contains records of pk value 100002 (partition 1)
  private static final String UPLOADED_SEGMENT_3 = "mytable_10158_19938_2 %";

  private PinotHelixResourceManager _resourceManager;
  private TableRebalancer _tableRebalancer;

  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServers(NUM_SERVERS);

    // Start Kafka and push data into Kafka
    startKafka();

    populateTables();

    _resourceManager = getControllerStarter().getHelixResourceManager();
    _tableRebalancer = new TableRebalancer(_resourceManager.getHelixZkManager(), null);

    waitForAllDocsLoaded(600_000L);
  }

  @Test
  public void testRebalance() throws Exception {
    verifyIdealState(5, NUM_SERVERS);

    // setup the rebalance config
    Configuration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.addProperty(RebalanceConfigConstants.DRY_RUN, false);
    rebalanceConfig.addProperty(RebalanceConfigConstants.MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME, 0);
    rebalanceConfig.addProperty(RebalanceConfigConstants.INCLUDE_CONSUMING, true);

    // Add a new server
    startOneServer(1234);

    // Now we trigger a rebalance operation
    TableConfig tableConfig = _resourceManager.getTableConfig(REALTIME_TABLE_NAME);
    _tableRebalancer.rebalance(tableConfig, rebalanceConfig);

    // Check the number of replicas after rebalancing
    int finalReplicas = _resourceManager.getServerInstancesForTable(getTableName(), TableType.REALTIME).size();

    // Check that a replica has been added
    assertEquals(finalReplicas, NUM_SERVERS + 1, "Rebalancing didn't correctly add the new server");

    verifyIdealState(5, finalReplicas);

    // Add a new server
    startOneServer(4567);
    _tableRebalancer.rebalance(tableConfig, rebalanceConfig);

    // Check the number of replicas after rebalancing
    finalReplicas = _resourceManager.getServerInstancesForTable(getTableName(), TableType.REALTIME).size();

    // Check that a replica has been added
    assertEquals(finalReplicas, NUM_SERVERS + 2, "Rebalancing didn't correctly add the new server");

    // number of instances assigned can't be more than number of partitions for rf = 1
    verifyIdealState(5, getNumKafkaPartitions());
  }


  @Test
  public void testReload() throws Exception {
    reloadRealtimeTable(getTableName());
    waitForAllDocsLoaded(600_000L);
    verifyIdealState(5, NUM_SERVERS);
  }

  protected void verifyIdealState(int numSegmentsExpected, int numInstancesExpected) {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> segmentAssignment = idealState.getRecord().getMapFields();
    assertEquals(segmentAssignment.size(), numSegmentsExpected);

    int maxSequenceNumber = 0;
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        maxSequenceNumber = Math.max(maxSequenceNumber, llcSegmentName.getSequenceNumber());
      }
    }

    Map<Integer, String> serverForPartition = new HashMap<>();
    Set<String> uniqueServers = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();

      // Verify that all segments have the correct state
      assertEquals(instanceStateMap.size(), 1);
      Map.Entry<String, String> instanceIdAndState = instanceStateMap.entrySet().iterator().next();
      String state = instanceIdAndState.getValue();
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        if (llcSegmentName.getSequenceNumber() < maxSequenceNumber) {
          assertEquals(state, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE);
        } else {
          assertEquals(state, CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING);
        }
      } else {
        assertEquals(state, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE);
      }

      // Verify that all segments of the same partition are mapped to the same server
      String instanceId = instanceIdAndState.getKey();
      int partitionId = getSegmentPartitionId(segmentName);
      uniqueServers.add(instanceId);
      serverForPartition.compute(partitionId, (key, value) -> {
        if (value == null) {
          return instanceId;
        } else {
          assertEquals(instanceId, value);
          return value;
        }
      });
    }

    assertEquals(uniqueServers.size(), numInstancesExpected);
  }

  protected void populateTables()
      throws Exception {
    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig =
        createUpsertTableConfig(avroFiles.get(0), PRIMARY_KEY_COL, null, getNumKafkaPartitions());
    addTableConfig(tableConfig);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), TableType.REALTIME, _tarDir);

    pushAvroIntoKafka(avroFiles);
    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }


  @AfterClass
  public void tearDown() {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
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
