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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that checks data types for queries with no rows returned.
 */
public class EmptyResponseIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;
  private static final String[] SELECT_STAR_TYPES = new String[]{
      "INT", "INT", "LONG", "INT", "FLOAT", "DOUBLE", "INT", "STRING", "INT", "INT", "INT", "INT", "STRING", "INT",
      "STRING", "INT", "INT", "INT", "INT", "INT", "DOUBLE", "FLOAT", "INT", "STRING", "INT", "STRING", "INT", "INT",
      "INT", "STRING", "STRING", "INT", "STRING", "INT", "INT", "INT", "INT", "INT_ARRAY", "INT", "INT_ARRAY",
      "STRING_ARRAY", "INT", "INT", "FLOAT_ARRAY", "INT", "STRING_ARRAY", "LONG_ARRAY", "INT_ARRAY", "INT_ARRAY",
      "INT", "INT", "STRING", "INT", "INT", "INT", "INT", "INT", "INT", "STRING", "INT", "INT", "INT", "STRING",
      "STRING", "INT", "STRING", "INT", "INT", "STRING_ARRAY", "INT", "STRING", "INT", "INT", "INT", "STRING", "INT",
      "INT", "INT", "INT"
  };

  private final List<ServiceStatus.ServiceStatusCallback> _serviceStatusCallbacks =
      new ArrayList<>(getNumBrokers() + getNumServers());

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  protected int getNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    return Collections.singletonList(
        new FieldConfig("DivAirports", FieldConfig.EncodingType.DICTIONARY, Collections.emptyList(),
            CompressionCodec.MV_ENTRY_DICT, null));
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    // Set hyperloglog log2m value to 12.
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY, Integer.toString(12));
    startBrokers();
    startServers();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments. For exhaustive testing, concurrently upload multiple segments with the same name
    // and validate correctness with parallel push protection enabled.
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    // Create a copy of _tarDir to create multiple segments with the same name.
    File tarDir2 = new File(_tempDir, "tarDir2");
    FileUtils.copyDirectory(_tarDir, tarDir2);

    List<File> tarDirs = new ArrayList<>();
    tarDirs.add(_tarDir);
    tarDirs.add(tarDir2);
    try {
      uploadSegments(getTableName(), TableType.OFFLINE, tarDirs);
    } catch (Exception e) {
      // If enableParallelPushProtection is enabled and the same segment is uploaded concurrently, we could get one
      // of the three exception:
      //   - 409 conflict of the second call enters ProcessExistingSegment
      //   - segmentZkMetadata creation failure if both calls entered ProcessNewSegment
      //   - Failed to copy segment tar file to final location due to the same segment pushed twice concurrently
      // In such cases we upload all the segments again to ensure that the data is set up correctly.
      assertTrue(e.getMessage().contains("Another segment upload is in progress for segment") || e.getMessage()
          .contains("Failed to create ZK metadata for segment") || e.getMessage()
          .contains("java.nio.file.FileAlreadyExistsException"), e.getMessage());
      uploadSegments(getTableName(), _tarDir);
    }

    // Set up service status callbacks
    // NOTE: put this step after creating the table and uploading all segments so that brokers and servers can find the
    // resources to monitor
    registerCallbackHandlers();

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected void startBrokers()
      throws Exception {
    startBrokers(getNumBrokers());
  }

  protected void startServers()
      throws Exception {
    startServers(getNumServers());
  }

  private void registerCallbackHandlers() {
    List<String> instances = _helixAdmin.getInstancesInCluster(getHelixClusterName());
    instances.removeIf(
        instanceId -> !InstanceTypeUtils.isBroker(instanceId) && !InstanceTypeUtils.isServer(instanceId));
    List<String> resourcesInCluster = _helixAdmin.getResourcesInCluster(getHelixClusterName());
    resourcesInCluster.removeIf(resource -> (!TableNameBuilder.isTableResource(resource)
        && !CommonConstants.Helix.BROKER_RESOURCE_INSTANCE.equals(resource)));
    for (String instance : instances) {
      List<String> resourcesToMonitor = new ArrayList<>();
      for (String resourceName : resourcesInCluster) {
        IdealState idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(), resourceName);
        for (String partitionName : idealState.getPartitionSet()) {
          if (idealState.getInstanceSet(partitionName).contains(instance)) {
            resourcesToMonitor.add(resourceName);
            break;
          }
        }
      }
      _serviceStatusCallbacks.add(new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList.of(
          new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_helixManager, getHelixClusterName(),
              instance, resourcesToMonitor, 100.0),
          new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_helixManager, getHelixClusterName(),
              instance, resourcesToMonitor, 100.0))));
    }
  }

  @Test
  public void testInstancesStarted() {
    assertEquals(_serviceStatusCallbacks.size(), getNumBrokers() + getNumServers());
    for (ServiceStatus.ServiceStatusCallback serviceStatusCallback : _serviceStatusCallbacks) {
      assertEquals(serviceStatusCallback.getServiceStatus(), ServiceStatus.Status.GOOD);
    }
  }

  @Test
  public void testStarField()
      throws Exception {
    String sqlQuery = "SELECT * FROM myTable WHERE AirlineID = 0 LIMIT 1";
    JsonNode response = postQuery(sqlQuery);
    assertNoRowsReturned(response);
    assertDataTypes(response, SELECT_STAR_TYPES);
  }

  @Test
  public void testSelectionFields()
      throws Exception {
    String sqlQuery = "SELECT AirlineID, ArrTime, ArrTimeBlk FROM myTable WHERE AirlineID = 0 LIMIT 1";
    JsonNode response = postQuery(sqlQuery);
    assertNoRowsReturned(response);
    assertDataTypes(response, "LONG", "INT", "STRING");
  }

  @Test
  public void testTransformedFields()
      throws Exception {
    String sqlQuery = "SELECT AirlineID, ArrTime, ArrTime+1 FROM myTable WHERE AirlineID = 0 LIMIT 1";
    JsonNode response = postQuery(sqlQuery);
    assertNoRowsReturned(response);
    assertDataTypes(response, "LONG", "INT", "INT");
  }

  @Test
  public void testAggregatedFields()
      throws Exception {
    String sqlQuery = "SELECT AirlineID, avg(ArrTime) FROM myTable WHERE AirlineID = 0 GROUP BY AirlineID LIMIT 1";
    JsonNode response = postQuery(sqlQuery);
    assertNoRowsReturned(response);
    assertDataTypes(response, "LONG", "DOUBLE");
  }

  @Test
  public void testSchemaFallbackStarField()
      throws Exception {
    String sqlQuery = "SELECT * FROM myTable WHERE AirlineID = 0 AND add(AirTime, AirTime, ArrTime) > 0 LIMIT 1";
    JsonNode response = postQuery(sqlQuery);
    assertNoRowsReturned(response);
    assertDataTypes(response, SELECT_STAR_TYPES);
  }

  @Test
  public void testSchemaFallbackSelectionFields()
      throws Exception {
    String sqlQuery = "SELECT AirlineID, ArrTime, ArrTimeBlk FROM myTable"
        + " WHERE AirlineID = 0 AND add(ArrTime, ArrTime, ArrTime) > 0 LIMIT 1";
    JsonNode response = postQuery(sqlQuery);
    assertNoRowsReturned(response);
    assertDataTypes(response, "LONG", "INT", "STRING");
  }

  @Test
  public void testSchemaFallbackTransformedFields()
      throws Exception {
    String sqlQuery = "SELECT AirlineID, ArrTime, ArrTime+1 FROM myTable"
        + " WHERE AirlineID = 0 AND add(ArrTime, ArrTime, ArrTime) > 0 LIMIT 1";
    JsonNode response = postQuery(sqlQuery);
    assertNoRowsReturned(response);
    assertDataTypes(response, "LONG", "INT", "STRING");
  }

  @Test
  public void testSchemaFallbackAggregatedFields()
      throws Exception {
    String sqlQuery = "SELECT AirlineID, avg(ArrTime) FROM myTable"
        + " WHERE AirlineID = 0 AND add(ArrTime, ArrTime, ArrTime) > 0 GROUP BY AirlineID LIMIT 1";
    JsonNode response = postQuery(sqlQuery);
    assertNoRowsReturned(response);
    assertDataTypes(response, "LONG", "DOUBLE");
  }

  @Test
  public void testDataSchemaForBrokerPrunedEmptyResults()
      throws Exception {
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.setRoutingConfig(
        new RoutingConfig(null, Collections.singletonList(RoutingConfig.TIME_SEGMENT_PRUNER_TYPE), null, null));
    updateTableConfig(tableConfig);

    String query =
        "Select DestAirportID, Carrier from myTable WHERE DaysSinceEpoch < -1231231 and FlightNum > 121231231231";

    // Parse the Json response. Assert if DestAirportID has columnDatatype INT and Carrier has columnDatatype STRING.
    JsonNode queryResponse = postQuery(query);
    assertNoRowsReturned(queryResponse);
    assertDataTypes(queryResponse, "INT", "STRING");

    query = "Select DestAirportID, Carrier from myTable WHERE DaysSinceEpoch < -1231231";
    queryResponse = postQuery(query);
    assertNoRowsReturned(queryResponse);
    assertDataTypes(queryResponse, "INT", "STRING");

    // Reset the routing config
    tableConfig.setRoutingConfig(null);
    updateTableConfig(tableConfig);
  }

  private void assertNoRowsReturned(JsonNode response) {
    assertNotNull(response.get("resultTable"));
    assertNotNull(response.get("resultTable").get("rows"));
    assertEquals(response.get("resultTable").get("rows").size(), 0);
  }

  private void assertDataTypes(JsonNode response, String... types)
      throws Exception {
    assertNotNull(response.get("resultTable"));
    assertNotNull(response.get("resultTable").get("dataSchema"));
    assertNotNull(response.get("resultTable").get("dataSchema").get("columnDataTypes"));
    String expected = new ObjectMapper().writeValueAsString(types);
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").toString(), expected);
  }
}
