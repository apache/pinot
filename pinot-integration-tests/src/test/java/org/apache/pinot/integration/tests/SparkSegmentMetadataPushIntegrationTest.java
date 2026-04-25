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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.plugin.ingestion.batch.spark3.SparkSegmentMetadataPushJobRunner;
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
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.apache.spark.SparkContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SparkSegmentMetadataPushIntegrationTest extends SharedRichClusterIntegrationTest {
  private static final String SHARED_TABLE_NAME_PREFIX = "spark_segment_metadata_push";

  private SparkContext _sparkContext;
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

  @BeforeMethod(alwaysRun = true)
  public void setUpTest()
      throws Exception {
    String tableNameSuffix = RandomStringUtils.randomAlphabetic(12);
    _tableName = isSharedRichClusterEnabled() ? SHARED_TABLE_NAME_PREFIX + "_" + tableNameSuffix
        : DEFAULT_TABLE_NAME;
    _testTempDir = getTestTempDir(tableNameSuffix);
    _testSegmentDir = isSharedRichClusterEnabled() ? new File(_testTempDir, "segmentDir") : _segmentDir;
    _testTarDir = isSharedRichClusterEnabled() ? new File(_testTempDir, "tarDir") : _tarDir;
    TestUtils.ensureDirectoriesExistAndEmpty(_testTempDir, _testSegmentDir, _testTarDir);
    cleanTableAndSchema();
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    // Start Zk and the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Setup Spark context
    _sparkContext = new SparkContext("local", SparkSegmentMetadataPushIntegrationTest.class.getName());
  }

  @Test
  public void testSparkSegmentMetadataPushWithoutConsistentPush()
      throws Exception {
    runMetadataPushWithoutConsistentPushTest(false);
  }

  @Test
  public void testSparkSegmentMetadataPushWithoutConsistentPushWithBatchSegmentUpload()
      throws Exception {
    runMetadataPushWithoutConsistentPushTest(true);
  }

  private void runMetadataPushWithoutConsistentPushTest(boolean batchSegmentUpload)
      throws Exception {
    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    waitForEVToDisappear(offlineTableConfig.getTableName());
    addTableConfig(offlineTableConfig);

    List<File> avroFiles = getAllAvroFiles();

    // Create and push the segment using SparkSegmentMetadataPushJobRunner
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(0), offlineTableConfig, schema,
        "_no_consistent_push", _testSegmentDir, _testTarDir);

    SparkSegmentMetadataPushJobRunner runner = new SparkSegmentMetadataPushJobRunner();
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    jobSpec.setJobType("SegmentMetadataPush");
    jobSpec.setInputDirURI(avroFiles.get(0).getParent());

    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setPushParallelism(5);
    pushJobSpec.setPushAttempts(1);
    pushJobSpec.setCopyToDeepStoreForMetadataPush(true);
    pushJobSpec.setBatchSegmentUpload(batchSegmentUpload);
    jobSpec.setPushJobSpec(pushJobSpec);

    PinotFSSpec fsSpec = new PinotFSSpec();
    fsSpec.setScheme("file");
    fsSpec.setClassName("org.apache.pinot.spi.filesystem.LocalPinotFS");
    jobSpec.setPinotFSSpecs(Lists.newArrayList(fsSpec));
    jobSpec.setOutputDirURI(_testTarDir.getAbsolutePath());

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(getTableNameWithType());
    tableSpec.setTableConfigURI(getControllerBaseApiUrl() + "/tables/" + getTableNameWithType());
    jobSpec.setTableSpec(tableSpec);

    PinotClusterSpec clusterSpec = new PinotClusterSpec();
    clusterSpec.setControllerURI(getControllerBaseApiUrl());
    jobSpec.setPinotClusterSpecs(new PinotClusterSpec[]{clusterSpec});

    runner.init(jobSpec);
    runner.run();

    // Check that the segment is pushed and loaded
    List<String> segmentsList = getSegmentsList();
    Assert.assertEquals(segmentsList.size(), 1);
    String segmentName = segmentsList.get(0);
    Assert.assertTrue(segmentName.endsWith("_no_consistent_push"));
    long numDocs = getNumDocs(segmentName);
    testCountStar(numDocs);
  }

  @Test
  public void testSparkSegmentMetadataPushWithConsistentPushParallelism1()
      throws Exception {
    runMetadataPushWithConsistentDataPushTest(5, 1, false);
  }

  @Test
  public void testSparkSegmentMetadataPushWithConsistentPushParallelism1WithBatchSegmentUpload()
      throws Exception {
    runMetadataPushWithConsistentDataPushTest(5, 1, true);
  }

  @Test
  public void testSparkSegmentMetadataPushWithConsistentPushParallelism5()
      throws Exception {
    runMetadataPushWithConsistentDataPushTest(5, 5, false);
  }

  @Test
  public void testSparkSegmentMetadataPushWithConsistentPushParallelism5WithBatchSegmentUpload()
      throws Exception {
    runMetadataPushWithConsistentDataPushTest(5, 5, true);
  }

  @Test
  public void testSparkSegmentMetadataPushWithConsistentPushHigherParallelismThenSegments()
      throws Exception {
    runMetadataPushWithConsistentDataPushTest(1, 5, false);
  }

  // In an empty table with consistent push enabled, we:
  // 1. Push numSegment segments and verify:
  //    a. Lineage is created.
  //    b. Segments are loaded successfully.
  //    c. The total record count is correct by running a COUNT(*) query, with the result equal to the sum of the
  //    total docs of each segment pushed.
  // 2. Push a additional segment and verify:
  //    a. The new segment is loaded successfully.
  //    b. Only the record count of the additional segment is present by running a COUNT(*) query, confirming previous
  //    segments are replaced and no longer queryable.
  private void runMetadataPushWithConsistentDataPushTest(int numSegments, int parallelism, boolean batchSegmentUpload)
      throws Exception {
    int pushAttempts = 1;

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfigWithConsistentPush();
    waitForEVToDisappear(offlineTableConfig.getTableName());
    addTableConfig(offlineTableConfig);

    List<File> avroFiles = getAllAvroFiles();

    String firstTimeStamp = Long.toString(System.currentTimeMillis());

    // Create and push the segment using SparkSegmentMetadataPushJobRunner
    for (int i = 0; i < numSegments; i++) {
      ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(i), offlineTableConfig, schema, firstTimeStamp,
          _testSegmentDir, _testTarDir);
    }

    SparkSegmentMetadataPushJobRunner runner = new SparkSegmentMetadataPushJobRunner();
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    jobSpec.setJobType("SegmentMetadataPush");

    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setPushParallelism(parallelism);
    pushJobSpec.setPushAttempts(pushAttempts);
    pushJobSpec.setCopyToDeepStoreForMetadataPush(true);
    pushJobSpec.setBatchSegmentUpload(batchSegmentUpload);
    jobSpec.setPushJobSpec(pushJobSpec);

    PinotFSSpec fsSpec = new PinotFSSpec();
    fsSpec.setScheme("file");
    fsSpec.setClassName("org.apache.pinot.spi.filesystem.LocalPinotFS");
    jobSpec.setPinotFSSpecs(Lists.newArrayList(fsSpec));
    jobSpec.setOutputDirURI(_testTarDir.getAbsolutePath());

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(getTableNameWithType());
    tableSpec.setTableConfigURI(getControllerBaseApiUrl() + "/tables/" + getTableNameWithType());
    jobSpec.setTableSpec(tableSpec);

    PinotClusterSpec clusterSpec = new PinotClusterSpec();
    clusterSpec.setControllerURI(getControllerBaseApiUrl());
    jobSpec.setPinotClusterSpecs(new PinotClusterSpec[]{clusterSpec});

    runner.init(jobSpec);
    runner.run();

    // Check that the segment is pushed and loaded
    List<String> segmentsList = getSegmentsList();
    Assert.assertEquals(segmentsList.size(), numSegments);
    long numDocs = 0;
    for (int i = 0; i < numSegments; i++) {
      String segmentName = segmentsList.get(i);
      Assert.assertTrue(segmentName.endsWith(firstTimeStamp));
      numDocs += getNumDocs(segmentName);
    }
    testCountStar(numDocs);

    // Fetch segment lineage entry after running segment metadata push with consistent push enabled
    String segmentLineageResponse =
        getOrCreateAdminClient().getSegmentClient().listSegmentLineage(getTableName(), TableType.OFFLINE.toString());
    // Segment lineage should be in completed state
    Assert.assertTrue(segmentLineageResponse.contains("\"state\":\"COMPLETED\""));
    // SegmentsFrom should be empty as we started with a blank table
    Assert.assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[]"));
    // SegmentsTo should contain uploaded segments
    String segmentsTo = extractSegmentsFromLineageKey("segmentsTo", segmentLineageResponse);
    for (int i = 0; i < numSegments; i++) {
      String segmentName = segmentsList.get(i);
      Assert.assertTrue(segmentsTo.contains(segmentName));
    }

    // Keep track of the segment names so we can check that they are no longer queryable after
    // the additional segment is pushed as part of a new push job
    List<String> previousSegmentNames = Lists.newArrayList();
    for (int i = 0; i < numSegments; i++) {
      previousSegmentNames.add(segmentsList.get(i));
    }

    // Create and push the additional segment using SparkSegmentMetadataPushJobRunner
    File[] tarFiles = _testTarDir.listFiles();
    Assert.assertNotNull(tarFiles);
    for (File tarFile : tarFiles) {
      FileUtils.deleteQuietly(tarFile);
    }
    String secondTimeStamp = Long.toString(System.currentTimeMillis());
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(numSegments), offlineTableConfig, schema,
        secondTimeStamp, _testSegmentDir, _testTarDir);
    Assert.assertEquals(_testTarDir.listFiles().length, 1);

    runner.init(jobSpec);
    runner.run();

    // Check that the segment is pushed and loaded. We expect the initial push of segments + 1 additional
    // but we expect only the 1 additional pushed to be queryable only.
    segmentsList = getSegmentsList();
    Assert.assertEquals(segmentsList.size(), numSegments + 1);
    String additionalSegmentName = segmentsList.get(numSegments);
    Assert.assertTrue(additionalSegmentName.endsWith(secondTimeStamp));

    // Check that the count is now only the count of the additional segment
    testCountStar(getNumDocs(additionalSegmentName));
  }

  /**
   * Extracts a list of segments from a given lineage response based on a provided key.
   *
   * This method searches for the specified key within the lineage response and extracts the
   * segment list enclosed in square brackets following the key. The list is returned as a substring.
   *
   * Example keys are "segmentsTo" and "segmentsFrom".
   *
   * @param key The key to search for within the lineage response. It is expected to be a JSON
   *            key that maps to an array of segments, formatted as "key":[...].
   * @param lineageResponse The JSON-formatted lineage response containing the key and segments.
   * @return A substring containing the list of segments associated with the provided key. If the
   *         key is not found, or if there are no segments, an empty string is returned.
   */
  private static String extractSegmentsFromLineageKey(String key, String lineageResponse) {
    String segmentKey = "\"" + key + "\":[";
    int startIndex = lineageResponse.indexOf(segmentKey);
    if (startIndex == -1) {
      return "";
    }
    startIndex += segmentKey.length();
    int endIndex = lineageResponse.indexOf(']', startIndex);

    if (endIndex != -1 && startIndex < endIndex) {
      return lineageResponse.substring(startIndex, endIndex);
    }

    return "";
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
    Map<String, Object> segmentMetadata = getOrCreateAdminClient().getSegmentClient()
        .getSegmentMetadata(getTableNameWithType(), segmentName, null);
    Object totalDocs = segmentMetadata.get("segment.total.docs");
    return totalDocs == null ? 0L : Long.parseLong(totalDocs.toString());
  }

  private List<String> getSegmentsList()
      throws Exception {
    return getOrCreateAdminClient().getSegmentClient().listSegments(getTableName(), TableType.OFFLINE.toString(),
        false);
  }

  protected void testCountStar(long countStarResult) {
    TestUtils.waitForCondition(() -> getCurrentCountStarResult() == countStarResult, 100L, 300_000L,
        "Failed to load " + countStarResult + " documents", null);
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
    exception = runCleanup(exception, this::stopSparkContext);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopControllerIfStarted);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::deleteClassTempDir);
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  public String getTableName() {
    return _tableName != null ? _tableName : isSharedRichClusterEnabled() ? SHARED_TABLE_NAME_PREFIX
        : DEFAULT_TABLE_NAME;
  }

  @Override
  protected boolean shouldStartSharedKafka() {
    return false;
  }

  @Override
  protected int getSharedNumBrokers() {
    return 1;
  }

  @Override
  protected int getSharedNumServers() {
    return 1;
  }

  @Override
  protected boolean shouldStartSharedMinion() {
    return false;
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

  private String getTableNameWithType() {
    return TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
  }

  private File getTestTempDir(String tableNameSuffix) {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-" + tableNameSuffix)
        : _tempDir;
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null || _tableName == null) {
      return;
    }

    String tableName = getTableName();
    String offlineTableName = getTableNameWithType();
    if (_helixResourceManager.getAllTables().contains(offlineTableName)
        || _helixResourceManager.hasOfflineTable(tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }
    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void cleanTestDirectory()
      throws IOException {
    if (_testTempDir == null) {
      return;
    }
    if (isSharedRichClusterEnabled()) {
      FileUtils.deleteDirectory(_testTempDir);
    } else {
      TestUtils.ensureDirectoriesExistAndEmpty(_testTempDir, _testSegmentDir, _testTarDir);
    }
  }

  private void stopSparkContext() {
    if (_sparkContext != null) {
      _sparkContext.stop();
      _sparkContext = null;
    }
  }

  private void closePinotConnections() {
    if (_pinotConnection != null) {
      _pinotConnection.close();
      _pinotConnection = null;
    }
    if (_pinotConnectionV2 != null) {
      _pinotConnectionV2.close();
      _pinotConnectionV2 = null;
    }
  }

  private void stopControllerIfStarted() {
    if (_controllerStarter != null) {
      stopController();
    }
  }

  private void deleteClassTempDir()
      throws IOException {
    if (!isSharedRichClusterEnabled()) {
      FileUtils.deleteDirectory(_tempDir);
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
