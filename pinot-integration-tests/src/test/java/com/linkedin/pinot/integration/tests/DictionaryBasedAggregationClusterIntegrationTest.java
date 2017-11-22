/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ServiceStatus;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.util.TestUtils;


/**
 * Integration test to check aggregation functions which use DictionaryBasedAggregationPlan
 */
public class DictionaryBasedAggregationClusterIntegrationTest extends BaseClusterIntegrationTestSet {
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

  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

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

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    ExecutorService executor = Executors.newCachedThreadPool();

    // Create segments from Avro data
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, getTableName(), false,
        getRawIndexColumns(), null, executor);

    // Load data into H2
    setUpH2Connection(avroFiles, executor);

    // Initialize query generator
    setUpQueryGenerator(avroFiles, executor);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Create the table
    addOfflineTable(getTableName(), null, null, null, null, getLoadMode(), SegmentVersion.v1, getInvertedIndexColumns(),
        getTaskConfig());

    // Upload all segments
    uploadSegments(_tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }


  @Test
  public void testDictionaryBasedQueries() throws Exception {

    String pqlQuery;
    String sqlQuery;
    String sqlQuery1;
    String sqlQuery2;
    String sqlQuery3;

    // Test queries with min, max, minmaxrange
    // Dictionary columns
    // int
    pqlQuery = "SELECT MAX(ArrTime) FROM mytable";
    sqlQuery = "SELECT MAX(ArrTime) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrTime) FROM mytable";
    sqlQuery = "SELECT MIN(ArrTime) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(ArrTime) FROM mytable";
    sqlQuery = "SELECT MAX(ArrTime)-MIN(ArrTime) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrTime), MAX(ArrTime), MINMAXRANGE(ArrTime) FROM mytable";
    sqlQuery1 = "SELECT MIN(ArrTime) FROM mytable";
    sqlQuery2 = "SELECT MAX(ArrTime) FROM mytable";
    sqlQuery3 = "SELECT MAX(ArrTime)-MIN(ArrTime) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(ArrTime), COUNT(*) FROM mytable";
    sqlQuery1 = "SELECT MIN(ArrTime) FROM mytable";
    sqlQuery2 = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // float
    pqlQuery = "SELECT MAX(DepDelayMinutes) FROM mytable";
    sqlQuery = "SELECT MAX(DepDelayMinutes) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(DepDelayMinutes) FROM mytable";
    sqlQuery = "SELECT MIN(DepDelayMinutes) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(DepDelayMinutes) FROM mytable";
    sqlQuery = "SELECT MAX(DepDelayMinutes)-MIN(DepDelayMinutes) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(DepDelayMinutes), MAX(DepDelayMinutes), MINMAXRANGE(DepDelayMinutes) FROM mytable";
    sqlQuery1 = "SELECT MIN(DepDelayMinutes) FROM mytable";
    sqlQuery2 = "SELECT MAX(DepDelayMinutes) FROM mytable";
    sqlQuery3 = "SELECT MAX(DepDelayMinutes)-MIN(DepDelayMinutes) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(DepDelayMinutes), COUNT(*) FROM mytable";
    sqlQuery1 = "SELECT MIN(DepDelayMinutes) FROM mytable";
    sqlQuery2 = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // double
    pqlQuery = "SELECT MAX(ArrDelayMinutes) FROM mytable";
    sqlQuery = "SELECT MAX(ArrDelayMinutes) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrDelayMinutes) FROM mytable";
    sqlQuery = "SELECT MIN(ArrDelayMinutes) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(ArrDelayMinutes) FROM mytable";
    sqlQuery = "SELECT MAX(ArrDelayMinutes)-MIN(ArrDelayMinutes) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrDelayMinutes), MAX(ArrDelayMinutes), MINMAXRANGE(ArrDelayMinutes) FROM mytable";
    sqlQuery1 = "SELECT MIN(ArrDelayMinutes) FROM mytable";
    sqlQuery2 = "SELECT MAX(ArrDelayMinutes) FROM mytable";
    sqlQuery3 = "SELECT MAX(ArrDelayMinutes)-MIN(ArrDelayMinutes) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(ArrDelayMinutes), COUNT(*) FROM mytable";
    sqlQuery1 = "SELECT MIN(ArrDelayMinutes) FROM mytable";
    sqlQuery2 = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // long
    pqlQuery = "SELECT MAX(AirlineID) FROM mytable";
    sqlQuery = "SELECT MAX(AirlineID) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(AirlineID) FROM mytable";
    sqlQuery = "SELECT MIN(AirlineID) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(AirlineID) FROM mytable";
    sqlQuery = "SELECT MAX(AirlineID)-MIN(AirlineID) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(AirlineID), MAX(AirlineID), MINMAXRANGE(AirlineID) FROM mytable";
    sqlQuery1 = "SELECT MIN(AirlineID) FROM mytable";
    sqlQuery2 = "SELECT MAX(AirlineID) FROM mytable";
    sqlQuery3 = "SELECT MAX(AirlineID)-MIN(AirlineID) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(AirlineID), COUNT(*) FROM mytable";
    sqlQuery1 = "SELECT MIN(AirlineID) FROM mytable";
    sqlQuery2 = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // string
    // TODO: add test cases for string column when we add support for min and max on string datatype columns

    // Non dictionary columns
    // int
    pqlQuery = "SELECT MAX(ActualElapsedTime) FROM mytable";
    sqlQuery = "SELECT MAX(ActualElapsedTime) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ActualElapsedTime) FROM mytable";
    sqlQuery = "SELECT MIN(ActualElapsedTime) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(ActualElapsedTime) FROM mytable";
    sqlQuery = "SELECT MAX(ActualElapsedTime)-MIN(ActualElapsedTime) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ActualElapsedTime), MAX(ActualElapsedTime), MINMAXRANGE(ActualElapsedTime) FROM mytable";
    sqlQuery1 = "SELECT MIN(ActualElapsedTime) FROM mytable";
    sqlQuery2 = "SELECT MAX(ActualElapsedTime) FROM mytable";
    sqlQuery3 = "SELECT MAX(ActualElapsedTime)-MIN(ActualElapsedTime) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(ActualElapsedTime), COUNT(*) FROM mytable";
    sqlQuery1 = "SELECT MIN(ActualElapsedTime) FROM mytable";
    sqlQuery2 = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // float
    pqlQuery = "SELECT MAX(ArrDelay) FROM mytable";
    sqlQuery = "SELECT MAX(ArrDelay) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrDelay) FROM mytable";
    sqlQuery = "SELECT MIN(ArrDelay) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(ArrDelay) FROM mytable";
    sqlQuery = "SELECT MAX(ArrDelay)-MIN(ArrDelay) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrDelay), MAX(ArrDelay), MINMAXRANGE(ArrDelay) FROM mytable";
    sqlQuery1 = "SELECT MIN(ArrDelay) FROM mytable";
    sqlQuery2 = "SELECT MAX(ArrDelay) FROM mytable";
    sqlQuery3 = "SELECT MAX(ArrDelay)-MIN(ArrDelay) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(ArrDelay), COUNT(*) FROM mytable";
    sqlQuery1 = "SELECT MIN(ArrDelay) FROM mytable";
    sqlQuery2 = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // double
    pqlQuery = "SELECT MAX(DepDelay) FROM mytable";
    sqlQuery = "SELECT MAX(DepDelay) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(DepDelay) FROM mytable";
    sqlQuery = "SELECT MIN(DepDelay) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(DepDelay) FROM mytable";
    sqlQuery = "SELECT MAX(DepDelay)-MIN(DepDelay) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(DepDelay), MAX(DepDelay), MINMAXRANGE(DepDelay) FROM mytable";
    sqlQuery1 = "SELECT MIN(DepDelay) FROM mytable";
    sqlQuery2 = "SELECT MAX(DepDelay) FROM mytable";
    sqlQuery3 = "SELECT MAX(DepDelay)-MIN(DepDelay) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(DepDelay), COUNT(*) FROM mytable";
    sqlQuery1 = "SELECT MIN(DepDelay) FROM mytable";
    sqlQuery2 = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // string
    // TODO: add test cases for string column when we add support for min and max on string datatype columns


    // Check execution stats
    JSONObject response;

    // Dictionary column: answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime) FROM mytable";
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getString("numEntriesScannedPostFilter"), "0");
    Assert.assertEquals(response.getString("numEntriesScannedInFilter"), "0");
    Assert.assertEquals(response.getString("totalDocs"), response.getString("numDocsScanned"));

    // Non dictionary column: not answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(DepDelay) FROM mytable";
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getString("numEntriesScannedPostFilter"), response.getString("numDocsScanned"));
    Assert.assertEquals(response.getString("numEntriesScannedInFilter"), "0");
    Assert.assertEquals(response.getString("totalDocs"), response.getString("numDocsScanned"));

    // multiple dictionary based aggregation functions, dictionary columns: answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime),MIN(ArrTime) FROM mytable";
    response = postQuery(pqlQuery);
    System.out.println(response);
    Assert.assertEquals(response.getString("numEntriesScannedPostFilter"), "0");
    Assert.assertEquals(response.getString("numEntriesScannedInFilter"), "0");
    Assert.assertEquals(response.getString("totalDocs"), response.getString("numDocsScanned"));

    // multiple aggregation functions, mix of dictionary based and non dictionary based: not answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime),COUNT(ArrTime) FROM mytable";
    response = postQuery(pqlQuery);
    Assert.assertEquals(response.getString("numEntriesScannedPostFilter"), response.getString("numDocsScanned"));
    Assert.assertEquals(response.getString("numEntriesScannedInFilter"), "0");
    Assert.assertEquals(response.getString("totalDocs"), response.getString("numDocsScanned"));

    // group by in query : not answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime) FROM mytable group by DaysSinceEpoch";
    response = postQuery(pqlQuery);
    System.out.println(response);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter") > 0, true);
    Assert.assertEquals(response.getString("numEntriesScannedInFilter"), "0");
    Assert.assertEquals(response.getString("totalDocs"), response.getString("numDocsScanned"));

    // filter in query: not answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime) FROM mytable where DaysSinceEpoch > 0";
    response = postQuery(pqlQuery);
    System.out.println(response);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter") > 0, true);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter") > 0, true);
    Assert.assertEquals(response.getString("totalDocs"), response.getString("numDocsScanned"));
  }

  @AfterClass
  public void tearDown() throws Exception {
    // Test instance decommission before tearing down
    testInstanceDecommission();

    // Brokers and servers has been stopped
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  private void testInstanceDecommission() throws Exception {
    // Fetch all instances
    JSONObject response = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forInstanceList()));
    JSONArray instanceList = response.getJSONArray("instances");
    int numInstances = instanceList.length();
    Assert.assertEquals(numInstances, getNumBrokers() + getNumServers());

    // Try to delete a server that does not exist
    String deleteInstanceRequest = _controllerRequestURLBuilder.forInstanceDelete("potato");
    try {
      sendDeleteRequest(deleteInstanceRequest);
      Assert.fail("Delete should have returned a failure status (404)");
    } catch (IOException e) {
      // Expected exception on 404 status code
    }

    // Get the server name
    String serverName = null;
    String brokerName = null;
    for (int i = 0; i < numInstances; i++) {
      String instanceName = instanceList.getString(i);
      if (instanceName.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)) {
        serverName = instanceName;
      } else if (instanceName.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
        brokerName = instanceName;
      }
    }

    // Try to delete a live server
    deleteInstanceRequest = _controllerRequestURLBuilder.forInstanceDelete(serverName);
    try {
      sendDeleteRequest(deleteInstanceRequest);
      Assert.fail("Delete should have returned a failure status (409)");
    } catch (IOException e) {
      // Expected exception on 409 status code
    }

    // Stop servers
    stopServer();

    // Try to delete a server whose information is still on the ideal state
    try {
      sendDeleteRequest(deleteInstanceRequest);
      Assert.fail("Delete should have returned a failure status (409)");
    } catch (IOException e) {
      // Expected exception on 409 status code
    }

    // Delete the table
    dropOfflineTable(getTableName());

    // Now, delete server should work
    response = new JSONObject(sendDeleteRequest(deleteInstanceRequest));
    // TODO Cannot compare messages. We need to compare response code.
//    Assert.assertEquals(response.getString("status"), "success");

    // Try to delete a broker whose information is still live
    try {
      deleteInstanceRequest = _controllerRequestURLBuilder.forInstanceDelete(brokerName);
      sendDeleteRequest(deleteInstanceRequest);
      Assert.fail("Delete should have returned a failure status (409)");
    } catch (IOException e) {
      // Expected exception on 409 status code
    }

    // Stop brokers
    stopBroker();

    // TODO: Add test to delete broker instance. Currently, stopBroker() does not work correctly.

    // Check if '/INSTANCES/<serverName>' has been erased correctly
    String instancePath = "/" + _clusterName + "/INSTANCES/" + serverName;
    Assert.assertFalse(_propertyStore.exists(instancePath, 0));

    // Check if '/CONFIGS/PARTICIPANT/<serverName>' has been erased correctly
    String configPath = "/" + _clusterName + "/CONFIGS/PARTICIPANT/" + serverName;
    Assert.assertFalse(_propertyStore.exists(configPath, 0));
  }
}
