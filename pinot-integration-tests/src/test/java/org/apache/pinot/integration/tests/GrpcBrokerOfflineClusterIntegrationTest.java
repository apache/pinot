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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that converts Avro data for 12 segments and runs queries against it.
 */
public class GrpcBrokerOfflineClusterIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;

  private final List<ServiceStatus.ServiceStatusCallback> _serviceStatusCallbacks =
      new ArrayList<>(getNumBrokers() + getNumServers());
  private String _schemaFileName = DEFAULT_SCHEMA_FILE_NAME;

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  protected int getNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected String getSchemaFileName() {
    return _schemaFileName;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    Map<String, Object> extraBrokerProps = ImmutableMap.of(CommonConstants.Broker.BROKER_REQUEST_HANDLER_TYPE,
        CommonConstants.Broker.GRPC_BROKER_REQUEST_HANDLER_TYPE);
    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(getNumBrokers(), DEFAULT_BROKER_PORT, getZkUrl(), extraBrokerProps);
    startServers();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Set up service status callbacks
    // NOTE: put this step after creating the table and uploading all segments so that brokers and servers can find the
    // resources to monitor
    registerCallbackHandlers();

    // Wait for all documents loaded
    // TODO: this doesn't work so we simple wait for 5 second here. will be fixed after:
    // https://github.com/apache/pinot/pull/7839
    // waitForAllDocsLoaded(600_000L);
    Thread.sleep(5000);
  }

  protected void startServers() {
    // Enable gRPC server
    PinotConfiguration serverConfig = getDefaultServerConfiguration();
    serverConfig.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_GRPC_SERVER, true);
    startServer(serverConfig);
  }

  private void registerCallbackHandlers() {
    List<String> instances = _helixAdmin.getInstancesInCluster(getHelixClusterName());
    instances.removeIf(
        instance -> (!instance.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE) && !instance.startsWith(
            CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)));
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
  public void testGrpcBrokerRequestHandlerOnSelectionOnlyQuery()
      throws Exception {
    String sql;
    sql = "SELECT * FROM mytable LIMIT 1000000";
    testQuery(sql, Collections.singletonList(sql));
    sql = "SELECT * FROM mytable WHERE DaysSinceEpoch > 16312 LIMIT 10000000";
    testQuery(sql, Collections.singletonList(sql));
    sql = "SELECT ArrTime, DaysSinceEpoch, Carrier FROM mytable LIMIT 10000000";
    testQuery(sql, Collections.singletonList(sql));
  }
}
