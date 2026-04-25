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
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
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
public class SegmentUploadIntegrationTest extends SharedRichClusterIntegrationTest {
  private static final String SHARED_TABLE_NAME_PREFIX = "segment_upload";

  private String _tableName;
  private File _testTempDir;
  private File _testSegmentDir;
  private File _testTarDir;

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
    String tableNameSuffix = RandomStringUtils.randomAlphabetic(12);
    _tableName = isSharedRichClusterEnabled() ? SHARED_TABLE_NAME_PREFIX + "_" + tableNameSuffix
        : DEFAULT_TABLE_NAME + tableNameSuffix;
    _testTempDir = getTestTempDir(tableNameSuffix);
    _testSegmentDir = isSharedRichClusterEnabled() ? new File(_testTempDir, "segmentDir") : _segmentDir;
    _testTarDir = isSharedRichClusterEnabled() ? new File(_testTempDir, "tarDir") : _tarDir;
    TestUtils.ensureDirectoriesExistAndEmpty(_testTempDir, _testSegmentDir, _testTarDir);
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
        _testSegmentDir, _testTarDir);

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
    jobSpec.setOutputDirURI(_testTarDir.getAbsolutePath());
    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(getTableName());
    tableSpec.setTableConfigURI(getControllerBaseApiUrl() + "/tables/" + getTableName());
    jobSpec.setTableSpec(tableSpec);
    PinotClusterSpec clusterSpec = new PinotClusterSpec();
    clusterSpec.setControllerURI(getControllerBaseApiUrl());
    jobSpec.setPinotClusterSpecs(new PinotClusterSpec[]{clusterSpec});

    File dataDir = new File(_controllerConfig.getDataDir());
    File dataDirSegments = new File(dataDir, getTableName());

    // Not present in dataDir, only present in sourceDir
    Assert.assertFalse(dataDirSegments.exists());
    Assert.assertEquals(_testTarDir.listFiles().length, 1);

    runner.init(jobSpec);
    runner.run();

    // Segment should be seen in dataDir
    Assert.assertTrue(dataDirSegments.exists());
    Assert.assertEquals(dataDirSegments.listFiles().length, 1);
    Assert.assertEquals(_testTarDir.listFiles().length, 1);

    // test segment loaded
    JsonNode segmentsList = getSegmentsList();
    Assert.assertEquals(segmentsList.size(), 1);
    String segmentNameWithMove = segmentsList.get(0).asText();
    Assert.assertTrue(segmentNameWithMove.endsWith("_with_move"));
    long numDocs = getNumDocs(segmentNameWithMove);
    testCountStar(numDocs);

    // Clear segment and tar dir
    for (File segment : _testSegmentDir.listFiles()) {
      FileUtils.deleteQuietly(segment);
    }
    for (File tar : _testTarDir.listFiles()) {
      FileUtils.deleteQuietly(tar);
    }

    // Create 1 segment, for METADATA push WITHOUT move to final location
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(1), offlineTableConfig, schema, "_without_move",
        _testSegmentDir, _testTarDir);
    jobSpec.setPushJobSpec(new PushJobSpec());
    runner = new SegmentMetadataPushJobRunner();

    Assert.assertEquals(dataDirSegments.listFiles().length, 1);
    Assert.assertEquals(_testTarDir.listFiles().length, 1);

    runner.init(jobSpec);
    runner.run();

    // should not see new segments in dataDir
    Assert.assertEquals(dataDirSegments.listFiles().length, 1);
    Assert.assertEquals(_testTarDir.listFiles().length, 1);

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

  @Test
  public void testUploadMultipleSegmentsInBatchModeAndQuery()
      throws Exception {
    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    waitForEVToDisappear(offlineTableConfig.getTableName());
    addTableConfig(offlineTableConfig);

    List<File> avroFiles = getAllAvroFiles();

    // Create the list of segments
    for (int segNum = 0; segNum < 12; segNum++) {
      ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(segNum), offlineTableConfig, schema,
          "_seg" + segNum, _testSegmentDir, _testTarDir);
    }

    SegmentMetadataPushJobRunner runner = new SegmentMetadataPushJobRunner();
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setCopyToDeepStoreForMetadataPush(true);
    // enable batch mode
    pushJobSpec.setBatchSegmentUpload(true);
    jobSpec.setPushJobSpec(pushJobSpec);
    PinotFSSpec fsSpec = new PinotFSSpec();
    fsSpec.setScheme("file");
    fsSpec.setClassName("org.apache.pinot.spi.filesystem.LocalPinotFS");
    jobSpec.setPinotFSSpecs(Lists.newArrayList(fsSpec));
    jobSpec.setOutputDirURI(_testTarDir.getAbsolutePath());
    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(getTableName() + "_OFFLINE");
    tableSpec.setTableConfigURI(getControllerBaseApiUrl() + "/tables/" + getTableName());
    jobSpec.setTableSpec(tableSpec);
    PinotClusterSpec clusterSpec = new PinotClusterSpec();
    clusterSpec.setControllerURI(getControllerBaseApiUrl());
    jobSpec.setPinotClusterSpecs(new PinotClusterSpec[]{clusterSpec});

    File dataDir = new File(_controllerConfig.getDataDir());
    File dataDirSegments = new File(dataDir, getTableName());

    // Not present in dataDir, only present in sourceDir
    Assert.assertFalse(dataDirSegments.exists());
    Assert.assertEquals(_testTarDir.listFiles().length, 12);

    runner.init(jobSpec);
    runner.run();

    // Segment should be seen in dataDir
    Assert.assertTrue(dataDirSegments.exists());
    Assert.assertEquals(dataDirSegments.listFiles().length, 12);
    Assert.assertEquals(_testTarDir.listFiles().length, 12);

    // test segment loaded
    JsonNode segmentsList = getSegmentsList();
    Assert.assertEquals(segmentsList.size(), 12);
    long numDocs = 0;
    for (JsonNode segmentName : segmentsList) {
      numDocs += getNumDocs(segmentName.asText());
    }
    testCountStar(numDocs);

    // Clear segment and tar dir
    for (File segment : _testSegmentDir.listFiles()) {
      FileUtils.deleteQuietly(segment);
    }
    for (File tar : _testTarDir.listFiles()) {
      FileUtils.deleteQuietly(tar);
    }
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
        _testSegmentDir, _testTarDir);

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
    jobSpec.setOutputDirURI(_testTarDir.getAbsolutePath());
    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(getTableName());
    tableSpec.setTableConfigURI(getControllerBaseApiUrl() + "/tables/" + getTableName());
    jobSpec.setTableSpec(tableSpec);
    PinotClusterSpec clusterSpec = new PinotClusterSpec();
    clusterSpec.setControllerURI(getControllerBaseApiUrl());
    jobSpec.setPinotClusterSpecs(new PinotClusterSpec[]{clusterSpec});

    File dataDir = new File(_controllerConfig.getDataDir());
    File dataDirSegments = new File(dataDir, getTableName());

    Assert.assertEquals(_testTarDir.listFiles().length, 1);

    runner.init(jobSpec);
    runner.run();

    // Segment should be seen in dataDir
    Assert.assertTrue(dataDirSegments.exists());
    Assert.assertEquals(dataDirSegments.listFiles().length, 1);
    Assert.assertEquals(_testTarDir.listFiles().length, 1);

    // test segment loaded
    JsonNode segmentsList = getSegmentsList();
    Assert.assertEquals(segmentsList.size(), 1);
    String firstSegmentName = segmentsList.get(0).asText();
    Assert.assertTrue(firstSegmentName.endsWith(firstTimeStamp));
    long numDocs = getNumDocs(firstSegmentName);
    testCountStar(numDocs);

    // Fetch segment lineage entry after running segment metadata push with consistent push enabled.
    String segmentLineageResponse =
        getOrCreateAdminClient().getSegmentClient().listSegmentLineage(getTableName(), TableType.OFFLINE.toString());
    // Segment lineage should be in completed state.
    Assert.assertTrue(segmentLineageResponse.contains("\"state\":\"COMPLETED\""));
    // SegmentsFrom should be empty as we started with a blank table.
    Assert.assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[]"));
    // SegmentsTo should contain uploaded segment.
    Assert.assertTrue(segmentLineageResponse.contains("\"segmentsTo\":[\"" + firstSegmentName + "\"]"));

    // Clear segment and tar dir
    for (File segment : _testSegmentDir.listFiles()) {
      FileUtils.deleteQuietly(segment);
    }
    for (File tar : _testTarDir.listFiles()) {
      FileUtils.deleteQuietly(tar);
    }

    String secondTimeStamp = Long.toString(System.currentTimeMillis());

    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(1), offlineTableConfig, schema, secondTimeStamp,
        _testSegmentDir, _testTarDir);
    jobSpec.setPushJobSpec(new PushJobSpec());

    // Now test standalone tar push job runner
    runner = new SegmentTarPushJobRunner();

    Assert.assertEquals(dataDirSegments.listFiles().length, 1);
    Assert.assertEquals(_testTarDir.listFiles().length, 1);

    runner.init(jobSpec);
    runner.run();

    Assert.assertEquals(_testTarDir.listFiles().length, 1);

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
    segmentLineageResponse =
        getOrCreateAdminClient().getSegmentClient().listSegmentLineage(getTableName(), TableType.OFFLINE.toString());
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
      throws Exception {
    Map<String, Object> metadata =
        getOrCreateAdminClient().getSegmentClient().getSegmentMetadata(getTableName(), segmentName, null);
    Object totalDocs = metadata.get("segment.total.docs");
    return totalDocs == null ? 0L : Long.parseLong(totalDocs.toString());
  }

  private JsonNode getSegmentsList()
      throws Exception {
    List<String> segments =
        getOrCreateAdminClient().getSegmentClient().listSegments(getTableName(), TableType.OFFLINE.toString(), false);
    return JsonUtils.objectToJsonNode(segments);
  }

  protected void testCountStar(long countStarResult) {
    TestUtils.waitForCondition(() -> getCurrentCountStarResult() == countStarResult, 100L, 300_000L,
        "Failed to load " + countStarResult + " documents", null);
  }

  @Override
  public String getTableName() {
    return _tableName != null ? _tableName : isSharedRichClusterEnabled() ? SHARED_TABLE_NAME_PREFIX
        : DEFAULT_TABLE_NAME;
  }

  @Override
  protected List<File> getAllAvroFiles()
      throws Exception {
    int numSegments = unpackAvroData(_testTempDir).size();

    List<File> avroFiles = Lists.newArrayListWithCapacity(numSegments);
    for (int i = 1; i <= numSegments; i++) {
      avroFiles.add(new File(_testTempDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }
    return avroFiles;
  }

  private File getTestTempDir(String tableNameSuffix) {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-" + tableNameSuffix)
        : _tempDir;
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownTest()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::cleanTestDirectory);
    _tableName = null;
    _testTempDir = null;
    _testSegmentDir = null;
    _testTarDir = null;
    if (exception != null) {
      throw exception;
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopController);
    exception = runCleanup(exception, this::stopZk);
    if (exception != null) {
      throw exception;
    }
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null || _tableName == null) {
      return;
    }

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    if (_helixResourceManager.getAllTables().contains(offlineTableName)
        || _helixResourceManager.hasOfflineTable(getTableName())) {
      dropOfflineTable(getTableName());
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }
    if (_helixResourceManager.getSchema(getTableName()) != null) {
      deleteSchema(getTableName());
    }
  }

  private void cleanTestDirectory()
      throws IOException {
    if (isSharedRichClusterEnabled() && _testTempDir != null) {
      FileUtils.deleteDirectory(_testTempDir);
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
