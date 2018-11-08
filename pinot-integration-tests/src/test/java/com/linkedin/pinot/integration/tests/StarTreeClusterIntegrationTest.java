/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.tools.query.comparison.QueryComparison;
import com.linkedin.pinot.tools.query.comparison.SegmentInfoProvider;
import com.linkedin.pinot.tools.query.comparison.StarTreeQueryGenerator;
import com.linkedin.pinot.util.TestUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test for Star-Tree based indexes.
 * <ul>
 *   <li>
 *     Set up the Pinot cluster and create two tables, one with default indexes, one with star tree indexes
 *   </li>
 *   <li>
 *     Send queries to both the tables and assert that results match
 *   </li>
 *   <li>
 *     Query to reference table is sent with TOP 10000, and the comparator ensures that response from star tree is
 *     contained within the reference response. This is to avoid false failures when groups with same value are
 *     truncated due to TOP N
 *   </li>
 * </ul>
 */
public class StarTreeClusterIntegrationTest extends BaseClusterIntegrationTest {
  protected static final String DEFAULT_TABLE_NAME = "myTable";
  protected static final String STAR_TREE_TABLE_NAME = "myStarTable";
  private static final String SCHEMA_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_single_value_columns.schema";
  private static final String QUERY_FILE_NAME = "OnTimeStarTreeQueries.txt";
  private static final int NUM_QUERIES_TO_GENERATE = 100;

  protected Schema _schema;
  private StarTreeQueryGenerator _queryGenerator;
  private String _currentTable;

  @Nonnull
  @Override
  protected String getTableName() {
    return _currentTable;
  }

  @Nonnull
  @Override
  protected String getSchemaFileName() {
    return SCHEMA_FILE_NAME;
  }

  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServers(2);

    // Create the tables
    addOfflineTable(DEFAULT_TABLE_NAME);
    addOfflineTable(STAR_TREE_TABLE_NAME);

    // Set up segments and query generator
    _schema = Schema.fromFile(getSchemaFile());
    setUpSegmentsAndQueryGenerator();

    // Wait for all documents loaded
    _currentTable = DEFAULT_TABLE_NAME;
    waitForAllDocsLoaded(600_000L);
    _currentTable = STAR_TREE_TABLE_NAME;
    waitForAllDocsLoaded(600_000L);
  }

  protected void setUpSegmentsAndQueryGenerator() throws Exception {
    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments without star tree indexes from Avro data
    createAndUploadSegments(avroFiles, DEFAULT_TABLE_NAME, false);

    // Initialize the query generator using segments without star tree indexes
    SegmentInfoProvider segmentInfoProvider = new SegmentInfoProvider(_tarDir.getAbsolutePath());
    _queryGenerator =
        new StarTreeQueryGenerator(STAR_TREE_TABLE_NAME, segmentInfoProvider.getSingleValueDimensionColumns(),
            segmentInfoProvider.getMetricColumns(), segmentInfoProvider.getSingleValueDimensionValuesMap());

    // Create and upload segments with star tree indexes from Avro data
    createAndUploadSegments(avroFiles, STAR_TREE_TABLE_NAME, true);
  }

  private void createAndUploadSegments(List<File> avroFiles, String tableName, boolean createStarTreeIndex)
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);

    ExecutorService executor = Executors.newCachedThreadPool();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, tableName,
        createStarTreeIndex, null, null, _schema, executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    uploadSegments(_tarDir);
  }

  @Test
  public void testQueriesFromQueryFile() throws Exception {
    URL resourceUrl = BaseClusterIntegrationTestSet.class.getClassLoader().getResource(QUERY_FILE_NAME);
    Assert.assertNotNull(resourceUrl);
    File queryFile = new File(resourceUrl.getFile());

    try (BufferedReader reader = new BufferedReader(new FileReader(queryFile))) {
      String starQuery;
      while ((starQuery = reader.readLine()) != null) {
        testStarQuery(starQuery);
      }
    }
  }

  @Test
  public void testGeneratedQueries() throws Exception {
    for (int i = 0; i < NUM_QUERIES_TO_GENERATE; i++) {
      testStarQuery(generateQuery());
    }
  }

  protected String generateQuery() {
    return _queryGenerator.nextQuery();
  }

  @Test
  public void testPredicateOnMetrics() throws Exception {
    String starQuery;

    // Query containing predicate on one metric only
    starQuery = "SELECT SUM(DepDelayMinutes) FROM myStarTable WHERE DepDelay > 0";
    testStarQuery(starQuery);
    starQuery = "SELECT SUM(DepDelayMinutes) FROM myStarTable WHERE DepDelay BETWEEN 0 and 10000";
    testStarQuery(starQuery);

    // Query containing predicate on multiple metrics
    starQuery = "SELECT SUM(DepDelayMinutes) FROM myStarTable WHERE DepDelay > 0 AND ArrDelay > 0";
    testStarQuery(starQuery);

    // Query containing predicate on multiple metrics and dimensions
    starQuery =
        "SELECT SUM(DepDelayMinutes) FROM myStarTable WHERE DepDelay > 0 AND ArrDelay > 0 AND OriginStateName = 'Massachusetts'";
    testStarQuery(starQuery);
  }

  private void testStarQuery(String starQuery) throws Exception {
    String referenceQuery = starQuery.replace(STAR_TREE_TABLE_NAME, DEFAULT_TABLE_NAME) + " TOP 10000";
    JSONObject starResponse = postQuery(starQuery);
    JSONObject referenceResponse = postQuery(referenceQuery);

    // Skip comparison if not all results returned in reference response
    if (referenceResponse.has("aggregationResults")) {
      JSONObject aggregationResults = referenceResponse.getJSONArray("aggregationResults").getJSONObject(0);
      if (aggregationResults.has("groupByResult")
          && aggregationResults.getJSONArray("groupByResult").length() == 10000) {
        return;
      }
    }

    Assert.assertTrue(QueryComparison.compare(starResponse, referenceResponse, false),
        "Query comparison failed for: \nStar Query: " + starQuery + "\nStar Response: " + starResponse
            + "\nReference Query: " + referenceQuery + "\nReference Response: " + referenceResponse);
  }

  @AfterClass
  public void tearDown() throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);
    dropOfflineTable(STAR_TREE_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
