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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.plugin.ingestion.batch.common.BaseSegmentPushJobRunner;
import org.apache.pinot.plugin.ingestion.batch.standalone.SegmentMetadataPushJobRunner;
import org.apache.pinot.plugin.ingestion.batch.standalone.SegmentTarPushJobRunner;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test for advanced push types.
 * Currently only tests METADATA push type.
 * todo: add test for URI push
 */
public class SegmentUploadIntegrationTest extends BaseClusterIntegrationTest {
  private static final String TABLE_NAME_WITH_TYPE = DEFAULT_TABLE_NAME + "_" + "OFFLINE";

  @Override
  protected Map<String, String> getStreamConfigs() {
    return null;
  }

  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return null;
  }

  @Override
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Override
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  @BeforeMethod
  public void setUpTest()
      throws IOException {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    // Start Zk and Kafka
    startZk();

    // Start the Pinot cluster
    startController();
    startBroker();
    startServer();
  }

  @Test
  public void testUploadAndQuery()
      throws Exception {
    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    waitForEVToDisappear(offlineTableConfig.getTableName());
    addTableConfig(offlineTableConfig);

    List<File> avroFiles = getAllAvroFiles();

    // Create 1 segment, for METADATA push WITH move to final location
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(0), offlineTableConfig, schema, "_with_move",
        _segmentDir, _tarDir);

    SegmentMetadataPushJobRunner runner = new SegmentMetadataPushJobRunner();
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    PushJobSpec pushJobSpec = new PushJobSpec();
    // set moveToDeepStoreForMetadataPush to true
    pushJobSpec.setCopyToDeepStoreForMetadataPush(true);
    jobSpec.setPushJobSpec(pushJobSpec);
    PinotFSSpec fsSpec = new PinotFSSpec();
    fsSpec.setScheme("file");
    fsSpec.setClassName("org.apache.pinot.spi.filesystem.LocalPinotFS");
    jobSpec.setPinotFSSpecs(Lists.newArrayList(fsSpec));
    jobSpec.setOutputDirURI(_tarDir.getAbsolutePath());
    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(TABLE_NAME_WITH_TYPE);
    tableSpec.setTableConfigURI(_controllerRequestURLBuilder.forUpdateTableConfig(TABLE_NAME_WITH_TYPE));
    jobSpec.setTableSpec(tableSpec);
    PinotClusterSpec clusterSpec = new PinotClusterSpec();
    clusterSpec.setControllerURI(getControllerBaseApiUrl());
    jobSpec.setPinotClusterSpecs(new PinotClusterSpec[]{clusterSpec});

    File dataDir = new File(_controllerConfig.getDataDir());
    File dataDirSegments = new File(dataDir, DEFAULT_TABLE_NAME);

    // Not present in dataDir, only present in sourceDir
    Assert.assertFalse(dataDirSegments.exists());
    Assert.assertEquals(_tarDir.listFiles().length, 1);

    runner.init(jobSpec);
    runner.run();

    // Segment should be seen in dataDir
    Assert.assertTrue(dataDirSegments.exists());
    Assert.assertEquals(dataDirSegments.listFiles().length, 1);
    Assert.assertEquals(_tarDir.listFiles().length, 1);

    // test segment loaded
    JsonNode segmentsList = getSegmentsList();
    Assert.assertEquals(segmentsList.size(), 1);
    String segmentNameWithMove = segmentsList.get(0).asText();
    Assert.assertTrue(segmentNameWithMove.endsWith("_with_move"));
    long numDocs = getNumDocs(segmentNameWithMove);
    testCountStar(numDocs);

    // Clear segment and tar dir
    for (File segment : _segmentDir.listFiles()) {
      FileUtils.deleteQuietly(segment);
    }
    for (File tar : _tarDir.listFiles()) {
      FileUtils.deleteQuietly(tar);
    }

    // Create 1 segment, for METADATA push WITHOUT move to final location
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(1), offlineTableConfig, schema, "_without_move",
        _segmentDir, _tarDir);
    jobSpec.setPushJobSpec(new PushJobSpec());
    runner = new SegmentMetadataPushJobRunner();

    Assert.assertEquals(dataDirSegments.listFiles().length, 1);
    Assert.assertEquals(_tarDir.listFiles().length, 1);

    runner.init(jobSpec);
    runner.run();

    // should not see new segments in dataDir
    Assert.assertEquals(dataDirSegments.listFiles().length, 1);
    Assert.assertEquals(_tarDir.listFiles().length, 1);

    // test segment loaded
    segmentsList = getSegmentsList();
    Assert.assertEquals(segmentsList.size(), 2);
    String segmentNameWithoutMove = null;
    for (JsonNode segment : segmentsList) {
      if (segment.asText().endsWith("_without_move")) {
        segmentNameWithoutMove = segment.asText();
      }
    }
    Assert.assertNotNull(segmentNameWithoutMove);
    numDocs += getNumDocs(segmentNameWithoutMove);
    testCountStar(numDocs);
  }

  /**
   * Runs both SegmentMetadataPushJobRunner and SegmentTarPushJobRunner while enabling consistent data push.
   * Checks that segments are properly loaded and segment lineage entry were also in expected states.
   */
  @Test
  public void testUploadAndQueryWithConsistentPush()
      throws Exception {
    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfigWithConsistentPush();
    waitForEVToDisappear(offlineTableConfig.getTableName());
    addTableConfig(offlineTableConfig);

    List<File> avroFiles = getAllAvroFiles();

    String firstTimeStamp = Long.toString(System.currentTimeMillis());

    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(0), offlineTableConfig, schema, firstTimeStamp,
        _segmentDir, _tarDir);

    // First test standalone metadata push job runner
    BaseSegmentPushJobRunner runner = new SegmentMetadataPushJobRunner();
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setCopyToDeepStoreForMetadataPush(true);
    jobSpec.setPushJobSpec(pushJobSpec);
    PinotFSSpec fsSpec = new PinotFSSpec();
    fsSpec.setScheme("file");
    fsSpec.setClassName("org.apache.pinot.spi.filesystem.LocalPinotFS");
    jobSpec.setPinotFSSpecs(Lists.newArrayList(fsSpec));
    jobSpec.setOutputDirURI(_tarDir.getAbsolutePath());
    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(TABLE_NAME_WITH_TYPE);
    tableSpec.setTableConfigURI(_controllerRequestURLBuilder.forUpdateTableConfig(TABLE_NAME_WITH_TYPE));
    jobSpec.setTableSpec(tableSpec);
    PinotClusterSpec clusterSpec = new PinotClusterSpec();
    clusterSpec.setControllerURI(getControllerBaseApiUrl());
    jobSpec.setPinotClusterSpecs(new PinotClusterSpec[]{clusterSpec});

    File dataDir = new File(_controllerConfig.getDataDir());
    File dataDirSegments = new File(dataDir, DEFAULT_TABLE_NAME);

    Assert.assertEquals(_tarDir.listFiles().length, 1);

    runner.init(jobSpec);
    runner.run();

    // Segment should be seen in dataDir
    Assert.assertTrue(dataDirSegments.exists());
    Assert.assertEquals(dataDirSegments.listFiles().length, 1);
    Assert.assertEquals(_tarDir.listFiles().length, 1);

    // test segment loaded
    JsonNode segmentsList = getSegmentsList();
    Assert.assertEquals(segmentsList.size(), 1);
    String firstSegmentName = segmentsList.get(0).asText();
    Assert.assertTrue(firstSegmentName.endsWith(firstTimeStamp));
    long numDocs = getNumDocs(firstSegmentName);
    testCountStar(numDocs);

    // Fetch segment lineage entry after running segment metadata push with consistent push enabled.
    String segmentLineageResponse = ControllerTest.sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(getControllerBaseApiUrl())
            .forListAllSegmentLineages(DEFAULT_TABLE_NAME, TableType.OFFLINE.toString()));
    // Segment lineage should be in completed state.
    Assert.assertTrue(segmentLineageResponse.contains("\"state\":\"COMPLETED\""));
    // SegmentsFrom should be empty as we started with a blank table.
    Assert.assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[]"));
    // SegmentsTo should contain uploaded segment.
    Assert.assertTrue(segmentLineageResponse.contains("\"segmentsTo\":[\"" + firstSegmentName + "\"]"));

    // Clear segment and tar dir
    for (File segment : _segmentDir.listFiles()) {
      FileUtils.deleteQuietly(segment);
    }
    for (File tar : _tarDir.listFiles()) {
      FileUtils.deleteQuietly(tar);
    }

    String secondTimeStamp = Long.toString(System.currentTimeMillis());

    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(1), offlineTableConfig, schema, secondTimeStamp,
        _segmentDir, _tarDir);
    jobSpec.setPushJobSpec(new PushJobSpec());

    // Now test standalone tar push job runner
    runner = new SegmentTarPushJobRunner();

    Assert.assertEquals(dataDirSegments.listFiles().length, 1);
    Assert.assertEquals(_tarDir.listFiles().length, 1);

    runner.init(jobSpec);
    runner.run();

    Assert.assertEquals(_tarDir.listFiles().length, 1);

    // test segment loaded
    segmentsList = getSegmentsList();
    Assert.assertEquals(segmentsList.size(), 2);
    String secondSegmentName = null;
    for (JsonNode segment : segmentsList) {
      if (segment.asText().endsWith(secondTimeStamp)) {
        secondSegmentName = segment.asText();
      }
    }
    Assert.assertNotNull(secondSegmentName);
    numDocs = getNumDocs(secondSegmentName);
    testCountStar(numDocs);

    // Fetch segment lineage entry after running segment tar push with consistent push enabled.
    segmentLineageResponse = ControllerTest.sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(getControllerBaseApiUrl())
            .forListAllSegmentLineages(DEFAULT_TABLE_NAME, TableType.OFFLINE.toString()));
    // Segment lineage should be in completed state.
    Assert.assertTrue(segmentLineageResponse.contains("\"state\":\"COMPLETED\""));
    // SegmentsFrom should contain the previous segment
    Assert.assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[\"" + firstSegmentName + "\"]"));
    // SegmentsTo should contain uploaded segment.
    Assert.assertTrue(segmentLineageResponse.contains("\"segmentsTo\":[\"" + secondSegmentName + "\"]"));
  }

  protected TableConfig createOfflineTableConfigWithConsistentPush() {
    TableConfig offlineTableConfig = createOfflineTableConfig();
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", "DAILY", true));
    offlineTableConfig.setIngestionConfig(ingestionConfig);
    return offlineTableConfig;
  }

  private long getNumDocs(String segmentName)
      throws IOException {
    return JsonUtils.stringToJsonNode(
            sendGetRequest(_controllerRequestURLBuilder.forSegmentMetadata(DEFAULT_TABLE_NAME, segmentName)))
        .get("segment.total.docs").asLong();
  }

  private JsonNode getSegmentsList()
      throws IOException {
    return JsonUtils.stringToJsonNode(sendGetRequest(
            _controllerRequestURLBuilder.forSegmentListAPI(DEFAULT_TABLE_NAME, TableType.OFFLINE.toString())))
        .get(0).get("OFFLINE");
  }

  protected void testCountStar(final long countStarResult) {
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          return getCurrentCountStarResult() == countStarResult;
        } catch (Exception e) {
          return null;
        }
      }
    }, 100L, 300_000, "Failed to load " + countStarResult + " documents", true);
  }

  @AfterMethod
  public void tearDownTest()
      throws IOException {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    dropOfflineTable(offlineTableName);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }
}
