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
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ServiceStatus;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.util.TestUtils;


/**
 * Integration test to check aggregation functions which use MetadataBasedAggregationOperator
 */
public class MetadataBasedAggregationClusterIntegrationTest extends BaseClusterIntegrationTestSet {
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

    // Test queries with count *
    pqlQuery = "SELECT COUNT(*) FROM mytable";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));

    // Check execution stats
    JSONObject response;

    pqlQuery = "SELECT COUNT(*) FROM mytable";
    response = postQuery(pqlQuery);
    System.out.println(response);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter"), 0);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));

    // group by present in query: not answered by MetadataBasedAggregationOperator
    pqlQuery = "SELECT COUNT(*) FROM mytable GROUP BY DaysSinceEpoch";
    response = postQuery(pqlQuery);
    System.out.println(response);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter") > 0, true);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));

    // filter present in query: not answered by MetadataBasedAggregationOperator
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 0";
    response = postQuery(pqlQuery);
    System.out.println(response);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter"), 0);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter") > 0, true);
    Assert.assertEquals(response.getLong("totalDocs"), response.getLong("numDocsScanned"));

    // mixed aggregation functions in query: not answered by MetadataBasedAggregationOperator
    pqlQuery = "SELECT COUNT(*),MAX(ArrTime) FROM mytable";
    response = postQuery(pqlQuery);
    System.out.println(response);
    Assert.assertEquals(response.getLong("numEntriesScannedPostFilter") > 0, true);
    Assert.assertEquals(response.getLong("numEntriesScannedInFilter"), 0);
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
