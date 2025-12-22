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
package org.apache.pinot.broker.routing.manager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Test class for {@link RemoteClusterBrokerRoutingManager}.
 * Tests the remote cluster routing manager with real ZooKeeper to validate table discovery,
 * routing updates, concurrent operations, and lifecycle management.
 */
public class RemoteClusterBrokerRoutingManagerTest extends ControllerTest {
  private static final String REMOTE_CLUSTER_NAME = "remoteCluster";
  private static final String RAW_TABLE_NAME = "remoteTable";
  private static final String OFFLINE_TABLE = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String SERVER_INSTANCE_1 = "Server_remote_host1_9000";
  private static final String SERVER_INSTANCE_2 = "Server_remote_host2_9000";

  private BrokerMetrics _brokerMetrics;
  private ServerRoutingStatsManager _serverRoutingStatsManager;
  private PinotConfiguration _pinotConfig;
  private RemoteClusterBrokerRoutingManager _routingManager;

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    startController();

    _brokerMetrics = Mockito.mock(BrokerMetrics.class);
    _serverRoutingStatsManager = Mockito.mock(ServerRoutingStatsManager.class);
    _pinotConfig = Mockito.mock(PinotConfiguration.class);

    Mockito.when(_pinotConfig.getProperty(Mockito.eq("pinot.broker.adaptive.server.selector.type")))
        .thenReturn("UNIFORM_RANDOM");
    Mockito.when(_pinotConfig.getProperty(
        Mockito.eq(CommonConstants.Broker.CONFIG_OF_ROUTING_ASSIGNMENT_CHANGE_PROCESS_PARALLELISM), anyInt()))
        .thenReturn(10);
    Mockito.when(_pinotConfig.getProperty(anyString(), anyString()))
        .thenAnswer(invocation -> invocation.getArgument(1));
    Mockito.when(_pinotConfig.getProperty(anyString(), anyInt()))
        .thenAnswer(invocation -> invocation.getArgument(1));

    _routingManager = new RemoteClusterBrokerRoutingManager(
        REMOTE_CLUSTER_NAME, _brokerMetrics, _serverRoutingStatsManager, _pinotConfig);
    _routingManager.init(_helixManager);

    addServerInstances();
    triggerInstanceConfigProcessing();
  }

  private void triggerInstanceConfigProcessing() {
    try {
      _routingManager.processClusterChange(org.apache.helix.HelixConstants.ChangeType.INSTANCE_CONFIG);
    } catch (Exception e) {
      Assert.fail("Failed to process instance config", e);
    }
  }

  @AfterClass
  public void tearDown() {
    if (_routingManager != null) {
      _routingManager.shutdown();
    }
    stopController();
    stopZk();
  }

  private void addServerInstances() {
    String clusterName = getHelixClusterName();

    for (String serverInstanceId : new String[]{SERVER_INSTANCE_1, SERVER_INSTANCE_2}) {
      if (!_helixAdmin.getInstancesInCluster(clusterName).contains(serverInstanceId)) {
        InstanceConfig instanceConfig = new InstanceConfig(serverInstanceId);
        instanceConfig.setHostName(serverInstanceId.split("_")[1]);
        instanceConfig.setPort("9000");
        instanceConfig.setInstanceEnabled(true);
        _helixAdmin.addInstance(clusterName, instanceConfig);
        _helixAdmin.enableInstance(clusterName, serverInstanceId, true);
      }
    }
  }

  @Test
  public void testTableDiscoveryAddsNewTable() throws Exception {
    Assert.assertFalse(_routingManager.routingExists(OFFLINE_TABLE), "Table should not exist initially");

    createTableInZooKeeper(OFFLINE_TABLE, TableType.OFFLINE);

    _routingManager.processSegmentAssignmentChangeInternal();
    Assert.assertTrue(_routingManager.hasRoutingChangeScheduled(), "Flag should be set after change");

    _routingManager.determineRoutingChangeForTables();

    Assert.assertTrue(_routingManager.routingExists(OFFLINE_TABLE), "Table should be discovered and added");
    Assert.assertFalse(_routingManager.hasRoutingChangeScheduled(), "Flag should be consumed");
  }

  @Test
  public void testTableRemovalDeletesRouting() throws Exception {
    String tableToRemove = "tableToRemove_OFFLINE";
    createTableInZooKeeper(tableToRemove, TableType.OFFLINE);

    _routingManager.processSegmentAssignmentChangeInternal();
    _routingManager.determineRoutingChangeForTables();
    Assert.assertTrue(_routingManager.routingExists(tableToRemove), "Table should exist after discovery");

    deleteTableFromZooKeeper(tableToRemove);

    _routingManager.processSegmentAssignmentChangeInternal();
    _routingManager.determineRoutingChangeForTables();

    Assert.assertFalse(_routingManager.routingExists(tableToRemove), "Table should be removed from routing");
  }

  @Test
  public void testConcurrentTableDiscoveryAndQueries() throws Exception {
    String table1 = "concurrentTable1_OFFLINE";
    String table2 = "concurrentTable2_OFFLINE";

    createTableInZooKeeper(table1, TableType.OFFLINE);
    _routingManager.processSegmentAssignmentChangeInternal();
    _routingManager.determineRoutingChangeForTables();
    Assert.assertTrue(_routingManager.routingExists(table1));

    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(3);

    AtomicReference<Exception> discoveryException = new AtomicReference<>();
    AtomicReference<Exception> query1Exception = new AtomicReference<>();
    AtomicReference<Exception> query2Exception = new AtomicReference<>();

    try {
      // Thread 1: Discover new table
      Future<?> discoveryTask = executor.submit(() -> {
        try {
          startLatch.await();
          createTableInZooKeeper(table2, TableType.OFFLINE);
          _routingManager.processSegmentAssignmentChangeInternal();
          _routingManager.determineRoutingChangeForTables();
        } catch (Exception e) {
          discoveryException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 2: Query existing table
      Future<?> query1Task = executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < 10; i++) {
            boolean exists = _routingManager.routingExists(table1);
            Assert.assertTrue(exists, "Existing table should remain queryable during discovery");
          }
        } catch (Exception e) {
          query1Exception.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Query both tables
      Future<?> query2Task = executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < 5; i++) {
            _routingManager.routingExists(table1);
            _routingManager.routingExists(table2);
          }
        } catch (Exception e) {
          query2Exception.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      startLatch.countDown();
      Assert.assertTrue(finishLatch.await(10, TimeUnit.SECONDS), "Tasks should complete");

      if (discoveryException.get() != null) {
        Assert.fail("Discovery failed", discoveryException.get());
      }
      if (query1Exception.get() != null) {
        Assert.fail("Query 1 failed", query1Exception.get());
      }
      if (query2Exception.get() != null) {
        Assert.fail("Query 2 failed", query2Exception.get());
      }

      Assert.assertTrue(_routingManager.routingExists(table1), "Table 1 should exist");
      Assert.assertTrue(_routingManager.routingExists(table2), "Table 2 should be discovered");
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testMultipleTableDiscoveryInParallel() throws Exception {
    String[] tables = {
        "parallelTable1_OFFLINE",
        "parallelTable2_OFFLINE",
        "parallelTable3_OFFLINE"
    };

    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(3);

    AtomicReference<Exception> exception = new AtomicReference<>();

    try {
      for (int i = 0; i < tables.length; i++) {
        final String tableName = tables[i];
        executor.submit(() -> {
          try {
            startLatch.await();
            createTableInZooKeeper(tableName, TableType.OFFLINE);
          } catch (Exception e) {
            exception.set(e);
          } finally {
            finishLatch.countDown();
          }
        });
      }

      startLatch.countDown();
      Assert.assertTrue(finishLatch.await(10, TimeUnit.SECONDS), "Table creation should complete");

      if (exception.get() != null) {
        Assert.fail("Table creation failed", exception.get());
      }

      _routingManager.processSegmentAssignmentChangeInternal();
      _routingManager.determineRoutingChangeForTables();

      for (String tableName : tables) {
        Assert.assertTrue(_routingManager.routingExists(tableName),
            "Table " + tableName + " should be discovered");
      }
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testRepeatedDiscoveryCalls() throws Exception {
    String testTable = "repeatedTable_OFFLINE";
    createTableInZooKeeper(testTable, TableType.OFFLINE);

    for (int i = 0; i < 5; i++) {
      _routingManager.processSegmentAssignmentChangeInternal();
      _routingManager.determineRoutingChangeForTables();
    }

    Assert.assertTrue(_routingManager.routingExists(testTable),
        "Table should be discovered after repeated calls");
  }

  @Test
  public void testShutdownDuringDiscovery() throws Exception {
    String testTable = "shutdownTable_OFFLINE";
    createTableInZooKeeper(testTable, TableType.OFFLINE);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(2);

    AtomicReference<Exception> discoveryException = new AtomicReference<>();
    AtomicReference<Exception> shutdownException = new AtomicReference<>();

    try {
      // Thread 1: Start discovery
      Future<?> discoveryTask = executor.submit(() -> {
        try {
          startLatch.await();
          _routingManager.processSegmentAssignmentChangeInternal();
          _routingManager.determineRoutingChangeForTables();
        } catch (Exception e) {
          discoveryException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 2: Shutdown
      Future<?> shutdownTask = executor.submit(() -> {
        try {
          startLatch.await();
          _routingManager.shutdown();
        } catch (Exception e) {
          shutdownException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      startLatch.countDown();
      Assert.assertTrue(finishLatch.await(10, TimeUnit.SECONDS), "Tasks should complete");

      if (shutdownException.get() != null) {
        Assert.fail("Shutdown failed", shutdownException.get());
      }

      Assert.assertTrue(_routingManager.isExecutorShutdown(), "Executor should be shutdown");
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private void createTableInZooKeeper(String tableNameWithType, TableType tableType) {
    TableConfig tableConfig = new TableConfigBuilder(tableType)
        .setTableName(TableNameBuilder.extractRawTableName(tableNameWithType))
        .setTimeColumnName("timestamp")
        .build();
    ZKMetadataProvider.setTableConfig(_propertyStore, tableConfig);

    Schema schema = new Schema();
    schema.setSchemaName(TableNameBuilder.extractRawTableName(tableNameWithType));
    schema.addField(new DateTimeFieldSpec("timestamp", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:DAYS"));
    ZKMetadataProvider.setSchema(_propertyStore, schema);

    IdealState idealState = new IdealState(tableNameWithType);
    idealState.setStateModelDefRef("OnlineOffline");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setNumPartitions(1);
    idealState.setPartitionState("segment_0", SERVER_INSTANCE_1, "ONLINE");

    ExternalView externalView = new ExternalView(tableNameWithType);
    externalView.setState("segment_0", SERVER_INSTANCE_1, "ONLINE");

    _helixDataAccessor.setProperty(_helixDataAccessor.keyBuilder().idealStates(tableNameWithType), idealState);
    _helixDataAccessor.setProperty(_helixDataAccessor.keyBuilder().externalView(tableNameWithType), externalView);

    SegmentZKMetadata segmentMetadata = new SegmentZKMetadata("segment_0");
    segmentMetadata.setEndTime(System.currentTimeMillis());
    segmentMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentMetadata.setTotalDocs(1000);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, tableNameWithType, segmentMetadata);
  }

  private void deleteTableFromZooKeeper(String tableNameWithType) {
    _helixDataAccessor.removeProperty(_helixDataAccessor.keyBuilder().idealStates(tableNameWithType));
    _helixDataAccessor.removeProperty(_helixDataAccessor.keyBuilder().externalView(tableNameWithType));
  }
}
