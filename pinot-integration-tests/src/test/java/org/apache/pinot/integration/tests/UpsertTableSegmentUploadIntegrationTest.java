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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.helix.model.IdealState;
import org.apache.http.HttpStatus;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class UpsertTableSegmentUploadIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 2;
  public static final String UPLOADED_SEGMENT_NAME = "mytable_10027_19736_0 %";
  public static final String PRIMARY_KEY_COL = "clientId";
  public static final String TABLE_NAME_WITH_TYPE = "mytable_REALTIME";

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
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
    // Create and upload the table config
    TableConfig upsertTableConfig = createUpsertTableConfig(avroFiles.get(0), PRIMARY_KEY_COL);
    addTableConfig(upsertTableConfig);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, upsertTableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), TableType.REALTIME, _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
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
    // Only 1 result is expected as the table is an upsert table and all records of the same primary key are versions of
    // the same record.
    return 1;
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    Assert.assertEquals(getCurrentCountStarResult(), 1);
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, TABLE_NAME_WITH_TYPE);

    // Verify various ideal state properties
    Set<String> segments = idealState.getPartitionSet();
    Assert.assertEquals(segments.size(), 3);
    Map<String, Integer> segment2PartitionId = new HashMap<>();
    segment2PartitionId.put(UPLOADED_SEGMENT_NAME, 0);

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
