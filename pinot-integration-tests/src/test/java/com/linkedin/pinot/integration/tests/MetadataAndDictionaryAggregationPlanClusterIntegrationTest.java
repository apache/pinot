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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ServiceStatus;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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
 * /**
 * Integration test to check aggregation functions which use DictionaryBasedAggregationPlan and MetadataBasedAggregationPlan
 *
 * <ul>
 *   <li>
 *     Set up the Pinot cluster and create two tables, one with default indexes, one with star tree indexes
 *   </li>
 *   <li>
 *     Send queries to both the tables and check results
 *   </li>
 * </ul>
 */
public class MetadataAndDictionaryAggregationPlanClusterIntegrationTest extends BaseClusterIntegrationTest {

  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;

  private final List<ServiceStatus.ServiceStatusCallback> _serviceStatusCallbacks =
      new ArrayList<>(getNumBrokers() + getNumServers());

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  protected int getNumServers() {
    return NUM_SERVERS;
  }

  private static final String SCHEMA_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_single_value_columns.schema";

  @Nonnull
  @Override
  protected String getSchemaFileName() {
    return SCHEMA_FILE_NAME;
  }

  private static final String DEFAULT_TABLE_NAME = "myTable";
  private static final String STAR_TREE_TABLE_NAME = "myStarTable";

  private String _currentTable;

  @Nonnull
  @Override
  protected String getTableName() {
    return _currentTable;
  }


  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(getNumBrokers());
    startServers(getNumServers());

    // Set up service status callbacks
    List<String> instances = _helixAdmin.getInstancesInCluster(_clusterName);
    for (String instance : instances) {
      if (instance.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
        _serviceStatusCallbacks.add(
            new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_helixManager, _clusterName, instance,
                Collections.singletonList(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)));
      }
      if (instance.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)) {
        _serviceStatusCallbacks.add(new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList.of(
            new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_helixManager, _clusterName,
                instance),
            new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_helixManager, _clusterName,
                instance))));
      }
    }
    // Create the tables
    addOfflineTable(DEFAULT_TABLE_NAME);
    addOfflineTable(STAR_TREE_TABLE_NAME);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments without star tree indexes from Avro data
    createAndUploadSegments(avroFiles, DEFAULT_TABLE_NAME, false, getRawIndexColumns(), null);

    // Create and upload segments with star tree indexes from Avro data
    createAndUploadSegments(avroFiles, STAR_TREE_TABLE_NAME, true, null, Schema.fromFile(getSchemaFile()));

    // Load data into H2
    _currentTable = DEFAULT_TABLE_NAME;
    loadDataIntoH2(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
    _currentTable = STAR_TREE_TABLE_NAME;
    waitForAllDocsLoaded(600_000L);
  }

  private void loadDataIntoH2(List<File> avroFiles) throws Exception {
    ExecutorService executor = Executors.newCachedThreadPool();
    setUpH2Connection(avroFiles, executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);
  }

  private void createAndUploadSegments(List<File> avroFiles, String tableName, boolean createStarTreeIndex,
      List<String> rawIndexColumns, Schema pinotSchema) throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);

    ExecutorService executor = Executors.newCachedThreadPool();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, tableName,
        createStarTreeIndex, null, rawIndexColumns, pinotSchema, executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    uploadSegments(_tarDir);
  }

  @Test
  public void testDictionaryBasedQueries() throws Exception {

    String pqlQuery;
    String pqlStarTreeQuery;
    String sqlQuery;
    String sqlQuery1;
    String sqlQuery2;
    String sqlQuery3;

    // Test queries with min, max, minmaxrange
    // Dictionary columns
    // int
    pqlQuery = "SELECT MAX(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MAX(ArrTime) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery =  "SELECT MIN(ArrTime) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MIN(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery =  "SELECT MINMAXRANGE(ArrTime) FROM "+ STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(ArrTime)-MIN(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrTime), MAX(ArrTime), MINMAXRANGE(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery =  "SELECT MIN(ArrTime), MAX(ArrTime), MINMAXRANGE(ArrTime) FROM "+ STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT MAX(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery3 = "SELECT MAX(ArrTime)-MIN(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(ArrTime), COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery =  "SELECT MIN(ArrTime), COUNT(*) FROM "+ STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));
    // float
    pqlQuery = "SELECT MAX(DepDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery =  "SELECT MAX(DepDelayMinutes) FROM "+ STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(DepDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(DepDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(DepDelayMinutes) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MIN(DepDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(DepDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery =  "SELECT MINMAXRANGE(DepDelayMinutes) FROM "+ STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(DepDelayMinutes)-MIN(DepDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(DepDelayMinutes), MAX(DepDelayMinutes), MINMAXRANGE(DepDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery =  "SELECT MIN(DepDelayMinutes), MAX(DepDelayMinutes), MINMAXRANGE(DepDelayMinutes) FROM "+ STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(DepDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT MAX(DepDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery3 = "SELECT MAX(DepDelayMinutes)-MIN(DepDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(DepDelayMinutes), COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery =  "SELECT MIN(DepDelayMinutes), COUNT(*) FROM "+ STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(DepDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // double
    pqlQuery = "SELECT MAX(ArrDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery =  "SELECT MAX(ArrDelayMinutes) FROM "+ STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(ArrDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery =  "SELECT MIN(ArrDelayMinutes) FROM "+ STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MIN(ArrDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(ArrDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery =  "SELECT MINMAXRANGE(ArrDelayMinutes) FROM "+ STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(ArrDelayMinutes)-MIN(ArrDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrDelayMinutes), MAX(ArrDelayMinutes), MINMAXRANGE(ArrDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(ArrDelayMinutes), MAX(ArrDelayMinutes), MINMAXRANGE(ArrDelayMinutes) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(ArrDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT MAX(ArrDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery3 = "SELECT MAX(ArrDelayMinutes)-MIN(ArrDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(ArrDelayMinutes), COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(ArrDelayMinutes), COUNT(*) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(ArrDelayMinutes) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // long
    pqlQuery = "SELECT MAX(AirlineID) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery =  "SELECT MAX(AirlineID) FROM "+ STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(AirlineID) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(AirlineID) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(AirlineID) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MIN(AirlineID) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(AirlineID) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MINMAXRANGE(AirlineID) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(AirlineID)-MIN(AirlineID) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(AirlineID), MAX(AirlineID), MINMAXRANGE(AirlineID) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(AirlineID), MAX(AirlineID), MINMAXRANGE(AirlineID) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(AirlineID) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT MAX(AirlineID) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery3 = "SELECT MAX(AirlineID)-MIN(AirlineID) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(AirlineID), COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(AirlineID), COUNT(*) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(AirlineID) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // string
    // TODO: add test cases for string column when we add support for min and max on string datatype columns

    // Non dictionary columns
    // int
    pqlQuery = "SELECT MAX(ActualElapsedTime) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MAX(ActualElapsedTime) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(ActualElapsedTime) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ActualElapsedTime) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(ActualElapsedTime) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MIN(ActualElapsedTime) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(ActualElapsedTime) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MINMAXRANGE(ActualElapsedTime) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(ActualElapsedTime)-MIN(ActualElapsedTime) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ActualElapsedTime), MAX(ActualElapsedTime), MINMAXRANGE(ActualElapsedTime) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(ActualElapsedTime), MAX(ActualElapsedTime), MINMAXRANGE(ActualElapsedTime) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(ActualElapsedTime) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT MAX(ActualElapsedTime) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery3 = "SELECT MAX(ActualElapsedTime)-MIN(ActualElapsedTime) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(ActualElapsedTime), COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(ActualElapsedTime), COUNT(*) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(ActualElapsedTime) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // float
    pqlQuery = "SELECT MAX(ArrDelay) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MAX(ArrDelay) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(ArrDelay) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrDelay) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(ArrDelay) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MIN(ArrDelay) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(ArrDelay) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MINMAXRANGE(ArrDelay) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(ArrDelay)-MIN(ArrDelay) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrDelay), MAX(ArrDelay), MINMAXRANGE(ArrDelay) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(ArrDelay), MAX(ArrDelay), MINMAXRANGE(ArrDelay) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(ArrDelay) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT MAX(ArrDelay) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery3 = "SELECT MAX(ArrDelay)-MIN(ArrDelay) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(ArrDelay), COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(ArrDelay), COUNT(*) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(ArrDelay) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // double
    pqlQuery = "SELECT MAX(DepDelay) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MAX(DepDelay) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(DepDelay) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(DepDelay) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(DepDelay) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MIN(DepDelay) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(DepDelay) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MINMAXRANGE(DepDelay) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(DepDelay)-MIN(DepDelay) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(DepDelay), MAX(DepDelay), MINMAXRANGE(DepDelay) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(DepDelay), MAX(DepDelay), MINMAXRANGE(DepDelay) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(DepDelay) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT MAX(DepDelay) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery3 = "SELECT MAX(DepDelay)-MIN(DepDelay) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(DepDelay), COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MIN(DepDelay), COUNT(*) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery1 = "SELECT MIN(DepDelay) FROM " + DEFAULT_TABLE_NAME;
    sqlQuery2 = "SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));
    testQuery(pqlStarTreeQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // string
    // TODO: add test cases for string column when we add support for min and max on string datatype columns

    // Check execution stats
    JSONObject response;

    // Dictionary column: answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter"), 0);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));

    // Non dictionary column: not answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(DepDelay) FROM " + DEFAULT_TABLE_NAME;
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter"), response.getLong("numDocsScanned"));
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));

    // multiple dictionary based aggregation functions, dictionary columns: answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime),MIN(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter"), 0);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));

    // multiple aggregation functions, mix of dictionary based and non dictionary based: not answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime),COUNT(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter"), response.getLong("numDocsScanned"));
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));

    // group by in query : not answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime) FROM " + DEFAULT_TABLE_NAME + "  group by DaysSinceEpoch";
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter") > 0, true);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));

    // filter in query: not answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime) FROM " + DEFAULT_TABLE_NAME + " where DaysSinceEpoch > 0";
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter") > 0, true);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter") > 0, true);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));
  }


  @Test
  public void testMetadataBasedQueries() throws Exception {

    String pqlQuery;
    String pqlStarTreeQuery;
    String sqlQuery;

    // Test queries with count *
    pqlQuery = "SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT COUNT(*) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));

    // Test queries with max on time column
    pqlQuery = "SELECT MAX(DaysSinceEpoch) FROM " + DEFAULT_TABLE_NAME;
    pqlStarTreeQuery = "SELECT MAX(DaysSinceEpoch) FROM " + STAR_TREE_TABLE_NAME;
    sqlQuery = "SELECT MAX(DaysSinceEpoch) FROM " + DEFAULT_TABLE_NAME;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    testQuery(pqlStarTreeQuery, Collections.singletonList(sqlQuery));

    // Check execution stats
    JSONObject response;

    pqlQuery = "SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME;
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter"), 0);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));

    pqlStarTreeQuery = "SELECT COUNT(*) FROM " + STAR_TREE_TABLE_NAME;
    response = postQuery(pqlStarTreeQuery);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter"), 0);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));


    // group by present in query: not answered by MetadataBasedAggregationOperator
    pqlQuery = "SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME + " GROUP BY DaysSinceEpoch";
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter") > 0, true);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));

    // filter present in query: not answered by MetadataBasedAggregationOperator
    pqlQuery = "SELECT COUNT(*) FROM " + DEFAULT_TABLE_NAME + " WHERE DaysSinceEpoch > 0";
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter"), 0);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter") > 0, true);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));

    // mixed aggregation functions in query: not answered by MetadataBasedAggregationOperator
    pqlQuery = "SELECT COUNT(*),MAX(ArrTime) FROM " + DEFAULT_TABLE_NAME;
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter") > 0, true);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));

    // mixed aggregation functions in star tree query: not answered by MetadataBasedAggregationOperator
    pqlStarTreeQuery = "SELECT COUNT(*),MAX(DaysSinceEpoch) FROM " + STAR_TREE_TABLE_NAME;
    response = postQuery(pqlStarTreeQuery);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter") > 0, true);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));
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
