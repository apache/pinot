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
import org.apache.pinot.plugin.ingestion.batch.spark4.SparkSegmentMetadataPushJobRunner;
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
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Spark 4 equivalent of {@link SparkSegmentMetadataPushIntegrationTest}. Lives in the
 * {@code src/test/java-spark4} source root so it is only compiled when the pinot-spark-4
 * profile (JDK 21+) is active, which is the only configuration in which
 * {@code pinot-batch-ingestion-spark-4} is on the classpath. See this module's pom.xml.
 */
public class SparkSegmentMetadataPushIntegrationTest4 extends BaseClusterIntegrationTest {

  private SparkContext _sparkContext;
  private final String _testTable = DEFAULT_TABLE_NAME;
  private final String _testTableWithType = _testTable + "_OFFLINE";

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
    startZk();
    startController();
    startBroker();
    startServer();

    // Disable the Spark UI: Spark 4's UI pulls Jetty 12 + Jakarta Servlet 5, which conflicts
    // with Pinot's javax-namespace Jersey/Jetty stack on the integration-test classpath. See
    // SparkSegmentGenerationJobRunnerTest in pinot-batch-ingestion-spark-4 for the same fix.
    SparkConf sparkConf = new SparkConf().setAppName(SparkSegmentMetadataPushIntegrationTest4.class.getName())
        .setMaster("local").set("spark.ui.enabled", "false");
    _sparkContext = new SparkContext(sparkConf);
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
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    waitForEVToDisappear(offlineTableConfig.getTableName());
    addTableConfig(offlineTableConfig);

    List<File> avroFiles = getAllAvroFiles();

    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(0), offlineTableConfig, schema,
        "_no_consistent_push", _segmentDir, _tarDir);

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
    jobSpec.setOutputDirURI(_tarDir.getAbsolutePath());

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(_testTableWithType);
    tableSpec.setTableConfigURI(getControllerBaseApiUrl() + "/tables/" + _testTableWithType);
    jobSpec.setTableSpec(tableSpec);

    PinotClusterSpec clusterSpec = new PinotClusterSpec();
    clusterSpec.setControllerURI(getControllerBaseApiUrl());
    jobSpec.setPinotClusterSpecs(new PinotClusterSpec[]{clusterSpec});

    runner.init(jobSpec);
    runner.run();

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

  private void runMetadataPushWithConsistentDataPushTest(int numSegments, int parallelism, boolean batchSegmentUpload)
      throws Exception {
    int pushAttempts = 1;

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfigWithConsistentPush();
    waitForEVToDisappear(offlineTableConfig.getTableName());
    addTableConfig(offlineTableConfig);

    List<File> avroFiles = getAllAvroFiles();

    String firstTimeStamp = Long.toString(System.currentTimeMillis());

    for (int i = 0; i < numSegments; i++) {
      ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(i), offlineTableConfig, schema, firstTimeStamp,
          _segmentDir, _tarDir);
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
    jobSpec.setOutputDirURI(_tarDir.getAbsolutePath());

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(_testTableWithType);
    tableSpec.setTableConfigURI(getControllerBaseApiUrl() + "/tables/" + _testTableWithType);
    jobSpec.setTableSpec(tableSpec);

    PinotClusterSpec clusterSpec = new PinotClusterSpec();
    clusterSpec.setControllerURI(getControllerBaseApiUrl());
    jobSpec.setPinotClusterSpecs(new PinotClusterSpec[]{clusterSpec});

    runner.init(jobSpec);
    runner.run();

    List<String> segmentsList = getSegmentsList();
    Assert.assertEquals(segmentsList.size(), numSegments);
    long numDocs = 0;
    for (int i = 0; i < numSegments; i++) {
      String segmentName = segmentsList.get(i);
      Assert.assertTrue(segmentName.endsWith(firstTimeStamp));
      numDocs += getNumDocs(segmentName);
    }
    testCountStar(numDocs);

    String segmentLineageResponse =
        getOrCreateAdminClient().getSegmentClient().listSegmentLineage(_testTable, TableType.OFFLINE.toString());
    Assert.assertTrue(segmentLineageResponse.contains("\"state\":\"COMPLETED\""));
    Assert.assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[]"));
    String segmentsTo = extractSegmentsFromLineageKey("segmentsTo", segmentLineageResponse);
    for (int i = 0; i < numSegments; i++) {
      String segmentName = segmentsList.get(i);
      Assert.assertTrue(segmentsTo.contains(segmentName));
    }

    List<String> previousSegmentNames = Lists.newArrayList();
    for (int i = 0; i < numSegments; i++) {
      previousSegmentNames.add(segmentsList.get(i));
    }

    for (File tarFile : _tarDir.listFiles()) {
      FileUtils.deleteQuietly(tarFile);
    }
    String secondTimeStamp = Long.toString(System.currentTimeMillis());
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(numSegments), offlineTableConfig, schema,
        secondTimeStamp, _segmentDir, _tarDir);
    Assert.assertEquals(_tarDir.listFiles().length, 1);

    runner.init(jobSpec);
    runner.run();

    segmentsList = getSegmentsList();
    Assert.assertEquals(segmentsList.size(), numSegments + 1);
    String additionalSegmentName = segmentsList.get(numSegments);
    Assert.assertTrue(additionalSegmentName.endsWith(secondTimeStamp));

    testCountStar(getNumDocs(additionalSegmentName));
  }

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
        .getSegmentMetadata(TableNameBuilder.OFFLINE.tableNameWithType(_testTable), segmentName, null);
    Object totalDocs = segmentMetadata.get("segment.total.docs");
    return totalDocs == null ? 0L : Long.parseLong(totalDocs.toString());
  }

  private List<String> getSegmentsList()
      throws Exception {
    return getOrCreateAdminClient().getSegmentClient().listSegments(_testTable, TableType.OFFLINE.toString(), false);
  }

  protected void testCountStar(long countStarResult) {
    TestUtils.waitForCondition(() -> getCurrentCountStarResult() == countStarResult, 100L, 300_000L,
        "Failed to load " + countStarResult + " documents", null);
  }

  @AfterMethod
  public void tearDownTest()
      throws IOException {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    dropOfflineTable(offlineTableName);

    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _sparkContext.stop();

    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }
}
