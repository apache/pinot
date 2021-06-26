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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.http.HttpStatus;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class UpsertTableSegmentUploadIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 2;
  // Segment 1 contains records of pk value 100000
  private static final String UPLOADED_SEGMENT_1 = "mytable_10027_19736_0 %";
  // Segment 2 contains records of pk value 100001
  private static final String UPLOADED_SEGMENT_2 = "mytable_10072_19919_1 %";
  // Segment 3 contains records of pk value 100000
  private static final String UPLOADED_SEGMENT_3 = "mytable_10158_19938_2 %";
  private static final String PRIMARY_KEY_COL = "clientId";
  private static final String TABLE_NAME_WITH_TYPE = "mytable_REALTIME";

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBrokers(getNumBrokers());
    startServers(NUM_SERVERS);

    // Start Kafka
    startKafka();

    // Create and upload the schema.
    Schema schema = createSchema();
    addSchema(schema);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Push data to Kafka
    pushAvroIntoKafka(avroFiles);
    // Create and upload the table config
    TableConfig upsertTableConfig = createUpsertTableConfig(avroFiles.get(0), PRIMARY_KEY_COL, getNumKafkaPartitions());
    addTableConfig(upsertTableConfig);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, upsertTableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), TableType.REALTIME, _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }


  @AfterClass
  public void tearDown()
      throws IOException {
    dropRealtimeTable(getTableName());
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

  @Override
  protected String getSchemaName() {
    return "upsertSchema";
  }

  @Override
  protected String getAvroTarFileName() {
    return "upsert_test.tar.gz";
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  @Override
  protected long getCountStarResult() {
    // Three distinct records are expected with pk values of 100000, 100001, 100002
    return 3;
  }

  @Override
  protected String getPartitionColumn() {
    return PRIMARY_KEY_COL;
  }

  @Override
  protected void startController()
      throws Exception {
    Map<String, Object> controllerConfig = getDefaultControllerConfiguration();
    // Perform realtime segment validation every second with 1 second initial delay.
    controllerConfig
        .put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS, 1);
    controllerConfig.put(ControllerConf.ControllerPeriodicTasksConf.SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS, 1);
    controllerConfig
        .put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS, 1);
    startController(controllerConfig);
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(getCurrentCountStarResult(), getCountStarResult());
    verifyTableIdealStates(idealState);
    // Wait 3 seconds to let the realtime validation thread to run.
    Thread.sleep(3000);
    // Verify the result again.
    Assert.assertEquals(getCurrentCountStarResult(), getCountStarResult());
    verifyTableIdealStates(idealState);
  }

  private void verifyTableIdealStates(IdealState idealState) {
    // Verify various ideal state properties
    Set<String> segments = idealState.getPartitionSet();
    Assert.assertEquals(segments.size(), 5);
    Map<String, Integer> segment2PartitionId = new HashMap<>();
    segment2PartitionId.put(UPLOADED_SEGMENT_1, 0);
    segment2PartitionId.put(UPLOADED_SEGMENT_2, 1);
    segment2PartitionId.put(UPLOADED_SEGMENT_3, 1);

    // Verify that all segments of the same partition are mapped to the same single server.
    Map<Integer, Set<String>> segmentAssignment = new HashMap<>();
    for (String segment : segments) {
      Integer partitionId;
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segment)) {
        partitionId = new LLCSegmentName(segment).getPartitionGroupId();
      } else {
        partitionId = segment2PartitionId.get(segment);
      }
      Assert.assertNotNull(partitionId);
      Set<String> instances = idealState.getInstanceSet(segment);
      Assert.assertEquals(1, instances.size());
      if (segmentAssignment.containsKey(partitionId)) {
        Assert.assertEquals(instances, segmentAssignment.get(partitionId));
      } else {
        segmentAssignment.put(partitionId, instances);
      }
    }
  }

  private void uploadSegments(String tableName, TableType tableType, File tarDir)
      throws Exception {
    File[] segmentTarFiles = tarDir.listFiles();
    assertNotNull(segmentTarFiles);
    int numSegments = segmentTarFiles.length;
    assertTrue(numSegments > 0);

    URI uploadSegmentHttpURI = FileUploadDownloadClient.getUploadSegmentHttpURI(LOCAL_HOST, _controllerPort);
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      if (numSegments == 1) {
        File segmentTarFile = segmentTarFiles[0];
        assertEquals(fileUploadDownloadClient
            .uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile, tableName, tableType)
            .getStatusCode(), HttpStatus.SC_OK);
      } else {
        // Upload all segments in parallel
        ExecutorService executorService = Executors.newFixedThreadPool(numSegments);
        List<Future<Integer>> futures = new ArrayList<>(numSegments);
        for (File segmentTarFile : segmentTarFiles) {
          futures.add(executorService.submit(() -> {
            return fileUploadDownloadClient
                .uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile, tableName, tableType)
                .getStatusCode();
          }));
        }
        executorService.shutdown();
        for (Future<Integer> future : futures) {
          assertEquals((int) future.get(), HttpStatus.SC_OK);
        }
      }
    }
  }
}
