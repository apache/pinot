/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerTestUtils;
import com.linkedin.pinot.tools.query.comparison.QueryComparison;
import com.linkedin.pinot.tools.query.comparison.SegmentInfoProvider;
import com.linkedin.pinot.tools.query.comparison.StarTreeQueryGenerator;
import com.linkedin.pinot.util.TestUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterStateVerifier;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test for Star Tree based indexes: - Sets up the Pinot cluster and creates two tables,
 * one with default indexes, and another with star tree indexes. - Sends queries to both the tables
 * and asserts that results match. - Query to reference table is sent with TOP 10000, and the
 * comparator ensures that response from star tree is contained within the reference response. This
 * is to avoid false failures when groups with same value are truncated due to LIMIT or TOP N.
 */
public class StarTreeClusterIntegrationTest extends ClusterTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(StarTreeClusterIntegrationTest.class);
  private static final int TOTAL_EXPECTED_DOCS = 115545;
  private final String DEFAULT_TABLE_NAME = "myTable";

  private final String STAR_TREE_TABLE_NAME = "myStarTable";
  private final String TIME_COLUMN_NAME = "DaysSinceEpoch";

  private final String TIME_UNIT = "daysSinceEpoch";
  private final String RETENTION_TIME_UNIT = "DAYS";

  private final int RETENTION_TIME = 3000;
  private static final int SEGMENT_COUNT = 12;
  private static final long TIMEOUT_IN_MILLISECONDS = 30 * 1000;
  private static final long TIMEOUT_IN_SECONDS = 3600;

  private final File _tmpDir = new File("/tmp/StarTreeClusterIntegrationTest");
  private final File _segmentsDir = new File("/tmp/StarTreeClusterIntegrationTest/segmentDir");
  private final File _tarredSegmentsDir = new File("/tmp/StarTreeClusterIntegrationTest/tarDir");
  private StarTreeQueryGenerator _queryGenerator;
  private File _queryFile;

  /**
   * Start the Pinot Cluster: - Zookeeper - One Controller - One Broker - Two Servers
   * @throws Exception
   */
  private void startCluster() throws Exception {

    startZk();
    startController();
    startBroker();
    startServers(2);
  }

  /**
   * Add the reference and star tree tables to the cluster.
   * @throws Exception
   */
  private void addOfflineTables() throws Exception {
    addOfflineTable(DEFAULT_TABLE_NAME, TIME_COLUMN_NAME, TIME_UNIT, RETENTION_TIME,
        RETENTION_TIME_UNIT, null, null);
    addOfflineTable(STAR_TREE_TABLE_NAME, TIME_COLUMN_NAME, TIME_UNIT, RETENTION_TIME,
        RETENTION_TIME_UNIT, null, null);
  }

  /**
   * Generate the reference and star tree indexes and upload to corresponding tables.
   * @param avroFiles
   * @param tableName
   * @param starTree
   * @throws IOException
   * @throws ArchiveException
   * @throws InterruptedException
   */
  private void generateAndUploadSegments(List<File> avroFiles, String tableName, boolean starTree)
      throws IOException, ArchiveException, InterruptedException {
    BaseClusterIntegrationTest.ensureDirectoryExistsAndIsEmpty(_segmentsDir);
    BaseClusterIntegrationTest.ensureDirectoryExistsAndIsEmpty(_tarredSegmentsDir);

    ExecutorService executor = Executors.newCachedThreadPool();
    BaseClusterIntegrationTest.buildSegmentsFromAvro(avroFiles, executor, 0, _segmentsDir,
        _tarredSegmentsDir, tableName, starTree);

    executor.shutdown();
    executor.awaitTermination(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);

    for (String segmentName : _tarredSegmentsDir.list()) {
      LOGGER.info("Uploading segment {}", segmentName);
      File file = new File(_tarredSegmentsDir, segmentName);
      FileUploadUtils.sendSegmentFile(ControllerTestUtils.DEFAULT_CONTROLLER_HOST,
          ControllerTestUtils.DEFAULT_CONTROLLER_API_PORT, segmentName, new FileInputStream(file),
          file.length());
    }
  }

  /**
   * Waits for total docs to match the expected value in the given table. There may be delay between
   * @param expectedRecordCount
   * @param deadline
   * @throws Exception
   */
  private void waitForTotalDocsToMatch(String tableName, int expectedRecordCount, long deadline)
      throws Exception {
    int actualRecordCount;

    do {
      String query = "select count(*) from " + tableName;
      JSONObject response = postQuery(query);
      actualRecordCount = response.getInt("totalDocs");

      String msg =
          "Actual record count: " + actualRecordCount + "\tExpected count: " + expectedRecordCount;
      LOGGER.info(msg);
      Assert.assertTrue(System.currentTimeMillis() < deadline,
          "Failed to read all records within the deadline.  " + msg);
      Thread.sleep(2000L);
    } while (expectedRecordCount != actualRecordCount);
  }

  /**
   * Wait for External View to be in sync with Ideal State.
   * @return
   */
  private boolean waitForExternalViewUpdate() {
    final ZKHelixAdmin helixAdmin = new ZKHelixAdmin(ZkStarter.DEFAULT_ZK_STR);
    ClusterStateVerifier.Verifier customVerifier = new ClusterStateVerifier.Verifier() {

      @Override
      public boolean verify() {
        String clusterName = getHelixClusterName();

        List<String> resourcesInCluster = helixAdmin.getResourcesInCluster(clusterName);
        LOGGER.info("Waiting for external view to update for resources: {} startTime: {}",
            resourcesInCluster, new Timestamp(System.currentTimeMillis()));

        for (String resourceName : resourcesInCluster) {
          IdealState idealState = helixAdmin.getResourceIdealState(clusterName, resourceName);
          ExternalView externalView = helixAdmin.getResourceExternalView(clusterName, resourceName);
          LOGGER.info("HERE for {},\n IS:{} \n EV:{}", resourceName, idealState, externalView);

          if (idealState == null || externalView == null) {
            return false;
          }

          Set<String> partitionSet = idealState.getPartitionSet();
          for (String partition : partitionSet) {
            Map<String, String> instanceStateMapIS = idealState.getInstanceStateMap(partition);
            Map<String, String> instanceStateMapEV = externalView.getStateMap(partition);

            if (instanceStateMapIS == null || instanceStateMapEV == null) {
              return false;
            }
            if (!instanceStateMapIS.equals(instanceStateMapEV)) {
              return false;
            }
          }
          LOGGER.info("External View updated successfully for {},\n IS:{} \n EV:{}", resourceName,
              idealState, externalView);
        }

        LOGGER.info("External View updated successfully for {}", resourcesInCluster);
        return true;
      }
    };

    return ClusterStateVerifier.verifyByPolling(customVerifier, TIMEOUT_IN_MILLISECONDS);
  }

  /**
   * Replace the star tree table name with reference table name, and add TOP 10000. The TOP 10000 is
   * added to make the reference result a super-set of star tree result. This will ensure any groups
   * with equal values that are truncated still appear in the reference result.
   * @param starQuery
   */
  private String convertToRefQuery(String starQuery) {
    String refQuery = StringUtils.replace(starQuery, STAR_TREE_TABLE_NAME, DEFAULT_TABLE_NAME);
    return (refQuery + " TOP 10000");
  }

  @BeforeClass
  public void setUp() throws Exception {
    startCluster();
    addOfflineTables();

    BaseClusterIntegrationTest.ensureDirectoryExistsAndIsEmpty(_tmpDir);
    List<File> avroFiles = BaseClusterIntegrationTest.unpackAvroData(_tmpDir, SEGMENT_COUNT);
    _queryFile = new File(TestUtils.getFileFromResourceUrl(BaseClusterIntegrationTest.class
        .getClassLoader().getResource("OnTimeStarTreeQueries.txt")));

    generateAndUploadSegments(avroFiles, DEFAULT_TABLE_NAME, false);
    generateAndUploadSegments(avroFiles, STAR_TREE_TABLE_NAME, true);

    Thread.sleep(15000);
    // Ensure that External View is in sync with Ideal State.
    if (!waitForExternalViewUpdate()) {
      Assert.fail("Cluster did not reach stable state");
    }

    // Wait until all docs are available, this is required because the broker routing tables may not
    // be updated yet.
    waitForTotalDocsToMatch(DEFAULT_TABLE_NAME, TOTAL_EXPECTED_DOCS,
        System.currentTimeMillis() + 1500000L);
    waitForTotalDocsToMatch(STAR_TREE_TABLE_NAME, TOTAL_EXPECTED_DOCS,
        System.currentTimeMillis() + 1500000L);

    // Initialize the query generator
    SegmentInfoProvider dictionaryReader =
        new SegmentInfoProvider(_tarredSegmentsDir.getAbsolutePath());

    List<String> dimensionColumns = dictionaryReader.getDimensionColumns();
    List<String> metricColumns = dictionaryReader.getMetricColumns();
    Map<String, List<String>> columnValuesMap = dictionaryReader.getColumnValuesMap();

    _queryGenerator = new StarTreeQueryGenerator(STAR_TREE_TABLE_NAME, dimensionColumns,
        metricColumns, columnValuesMap);
  }

  /**
   * Given a query string for star tree: - Get the result from star tree cluster - Convert the query
   * to reference query (change table name, add TOP 10000) - Get the result from reference cluster -
   * Compare the results and assert that result of star tree is contained in reference result. NOTE:
   * This method of testing is limited in that it cannot detect cases where a valid entry is missing
   * from star tree result (to be addressed in future).
   * @param starQuery
   * @param expectNonZeroDocsScanned
   */
  public void testOneQuery(String starQuery, boolean expectNonZeroDocsScanned) {
    try {
      JSONObject starResponse = postQuery(starQuery);
      if (expectNonZeroDocsScanned) {
        int numDocsScanned = starResponse.getInt("numDocsScanned");
        String message = "Zero Docs Scanned for query: " + starQuery;
        Assert.assertTrue((numDocsScanned > 0), message);
      }

      String refQuery = convertToRefQuery(starQuery);
      JSONObject refResponse = postQuery(refQuery);

      boolean result = QueryComparison.compare(starResponse, refResponse, false);
      String message = "Result mis-match for Query: " + starQuery + "\nStar: "
          + starResponse.toString() + "\nRef: " + refResponse.toString();
      Assert.assertTrue(result, message);
    } catch (Exception e) {
      LOGGER.error("Exception caught when executing query {}", starQuery, e);
    }
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopBroker();
    stopController();
    stopServer();
    stopZk();

    FileUtils.deleteDirectory(_tmpDir);
  }

  @Test(enabled = false)
  public void testGeneratedQueries() {
    for (int i = 0; i < 1000; ++i) {
      String starQuery = _queryGenerator.nextQuery();
      testOneQuery(starQuery, false);
    }
  }

  @Test
  public void testHardCodedQueries() {
    BufferedReader queryReader = null;
    try {
      queryReader = new BufferedReader(new FileReader(_queryFile));
      String starQuery;
      while ((starQuery = queryReader.readLine()) != null) {
        testOneQuery(starQuery, true);
      }
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      IOUtils.closeQuietly(queryReader);
    }
  }
}
