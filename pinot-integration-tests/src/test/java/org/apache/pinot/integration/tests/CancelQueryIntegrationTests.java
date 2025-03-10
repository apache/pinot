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
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Integration test that checks the query cancellation feature.
 */
public class CancelQueryIntegrationTests extends BaseClusterIntegrationTestSet {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 4;

  private final List<ServiceStatus.ServiceStatusCallback> _serviceStatusCallbacks =
      new ArrayList<>(getNumBrokers() + getNumServers());

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  protected int getNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    super.overrideBrokerConf(brokerConf);
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_QUERY_CANCELLATION, "true");
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    super.overrideServerConf(serverConf);
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_QUERY_CANCELLATION, "true");
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
    startBrokers(getNumBrokers());
    startServers(getNumServers());

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

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Set up service status callbacks
    // NOTE: put this step after creating the table and uploading all segments so that brokers and servers can find the
    // resources to monitor
    registerCallbackHandlers();

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
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

  @Test(dataProvider = "useBothQueryEngines")
  public void testCancelByClientQueryId(boolean useMultiStageQueryEngine)
    throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String clientRequestId = UUID.randomUUID().toString();
    // tricky query: use sleep with some column data to avoid Calcite from optimizing it on compile time
    String sqlQuery =
        "SET clientQueryId='" + clientRequestId + "'; "
            + "SELECT sleep(ActualElapsedTime+60000) FROM mytable WHERE ActualElapsedTime > 0 limit 1";

    new Timer().schedule(new java.util.TimerTask() {
      @Override
      public void run() {
        try {
          sendCancel(clientRequestId);
        } catch (Exception e) {
          fail("No exception should be thrown", e);
        }
      }
    }, useMultiStageQueryEngine ? 5000 : 500); // TODO: horrible because we have a potential race condition for MSE,
                                               // it will be addressed once we refactor the submit&reduce processing
                                               // so we can track running queries more properly

    JsonNode result = postQuery(sqlQuery);
    // ugly: error message differs from SSQE to MSQE
    assertQueryCancellation(result, useMultiStageQueryEngine ? "InterruptedException" : "QueryCancellationError");
  }

  private void sendCancel(String clientRequestId)
      throws Exception {
    cancelQuery(clientRequestId);
  }

  private void assertQueryCancellation(JsonNode result, String errorText) {
    assertNotNull(result);
    JsonNode exceptions = result.get("exceptions");
    assertNotNull(exceptions);
    assertTrue(exceptions.isArray());
    assertFalse(exceptions.isEmpty());
    for (JsonNode exception: exceptions) {
      JsonNode message = exception.get("message");
      if (message != null && message.asText().contains(errorText)) {
        return;
      }
    }
    fail("At least one QueryCancellationError expected.");
  }
}
