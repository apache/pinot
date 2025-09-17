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
package org.apache.pinot.broker.routing;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class to validate concurrency and race condition handling in BrokerRoutingManager,
 * specifically focusing on TimeBoundaryManager coordination for hybrid tables.
 *
 * This test uses a real ZooKeeper instance and PropertyStore for end-to-end validation.
 */
public class BrokerRoutingManagerConcurrencyTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "testHybridTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  @Mock
  private BrokerMetrics _brokerMetrics;

  @Mock
  private ServerRoutingStatsManager _serverRoutingStatsManager;

  @Mock
  private PinotConfiguration _pinotConfig;

  private BrokerRoutingManager _routingManager;

  @BeforeClass
  public void setUp() throws Exception {
    // Start ZooKeeper and initialize the test infrastructure
    startZk();
    startController();

    // Initialize mocks and configuration
    _brokerMetrics = Mockito.mock(BrokerMetrics.class);
    _serverRoutingStatsManager = Mockito.mock(ServerRoutingStatsManager.class);
    _pinotConfig = Mockito.mock(PinotConfiguration.class);

    // Setup required configuration for BrokerRoutingManager
    Mockito.when(_pinotConfig.getProperty(Mockito.eq("pinot.broker.adaptive.server.selector.type")))
        .thenReturn("UNIFORM_RANDOM");
    Mockito.when(_pinotConfig.getProperty(
        Mockito.eq(CommonConstants.Broker.CONFIG_OF_ROUTING_PROCESS_SEGMENT_ASSIGNMENT_CHANGE_NUM_THREADS), anyInt()))
        .thenReturn(10);
    Mockito.when(_pinotConfig.getProperty(Mockito.anyString(), Mockito.anyString()))
        .thenAnswer(invocation -> invocation.getArgument(1)); // Return default value

    // Initialize BrokerRoutingManager with real components
    _routingManager = new BrokerRoutingManager(_brokerMetrics, _serverRoutingStatsManager, _pinotConfig);
    _routingManager.init(_helixManager);

    // Add server instances to the cluster
    addServerInstancesToCluster();

    // Create and upload test table configs and schemas to ZooKeeper
    setupTestTablesInZooKeeper();

    // Trigger instance config processing to populate _routableServers
    triggerInstanceConfigProcessing();
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }

  private void addServerInstancesToCluster() {
    // Add server instances that will be referenced in IdealState and ExternalView
    String clusterName = getHelixClusterName();

    // Add Server_localhost_8000
    String serverInstanceId1 = "Server_localhost_8000";
    if (!_helixAdmin.getInstancesInCluster(clusterName).contains(serverInstanceId1)) {
      // Create InstanceConfig for the server
      InstanceConfig instanceConfig1 = new InstanceConfig(serverInstanceId1);
      instanceConfig1.setHostName("localhost");
      instanceConfig1.setPort("8000");
      instanceConfig1.setInstanceEnabled(true);

      _helixAdmin.addInstance(clusterName, instanceConfig1);

      // Mark the instance as live (simulate server joining)
      _helixAdmin.enableInstance(clusterName, serverInstanceId1, true);
    }

    // Add Server_localhost_8001
    String serverInstanceId2 = "Server_localhost_8001";
    if (!_helixAdmin.getInstancesInCluster(clusterName).contains(serverInstanceId2)) {
      // Create InstanceConfig for the server
      InstanceConfig instanceConfig2 = new InstanceConfig(serverInstanceId2);
      instanceConfig2.setHostName("localhost");
      instanceConfig2.setPort("8001");
      instanceConfig2.setInstanceEnabled(true);

      _helixAdmin.addInstance(clusterName, instanceConfig2);

      // Mark the instance as live (simulate server joining)
      _helixAdmin.enableInstance(clusterName, serverInstanceId2, true);
    }
  }

  private void triggerInstanceConfigProcessing() {
    // Trigger BrokerRoutingManager to process instance config changes
    // This will populate _routableServers which is needed for buildRouting to work
    try {
      _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);
    } catch (Exception e) {
     Assert.fail("Direct call to processClusterChange failed", e);
    }
  }

  private void clearRoutingEntries() {
    // Clear existing routing entries to ensure test isolation
    try {
      java.lang.reflect.Field routingEntryMapField = BrokerRoutingManager.class.getDeclaredField("_routingEntryMap");
      routingEntryMapField.setAccessible(true);
      Map<?, ?> routingEntryMap = (Map<?, ?>) routingEntryMapField.get(_routingManager);
      routingEntryMap.clear();
    } catch (Exception e) {
      Assert.fail("Failed to clear routing entries", e);
    }
  }

  private void setupTestTablesInZooKeeper() {
    // Create and upload table configs
    TableConfig offlineTableConfig = createTableConfig(OFFLINE_TABLE_NAME, TableType.OFFLINE);
    TableConfig realtimeTableConfig = createTableConfig(REALTIME_TABLE_NAME, TableType.REALTIME);

    ZKMetadataProvider.setTableConfig(_propertyStore, offlineTableConfig);
    ZKMetadataProvider.setTableConfig(_propertyStore, realtimeTableConfig);

    // Create and upload schemas
    Schema testSchema = createMockSchema();
    ZKMetadataProvider.setSchema(_propertyStore, testSchema);

    // Create ideal states and external views for the test
    createIdealStateAndExternalView(OFFLINE_TABLE_NAME);
    createIdealStateAndExternalView(REALTIME_TABLE_NAME);

    // Create segment metadata for TimeBoundaryManager
    createSegmentMetadata(OFFLINE_TABLE_NAME, "segment_0", System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    createSegmentMetadata(REALTIME_TABLE_NAME, "segment_0", System.currentTimeMillis());
  }

  private void createIdealStateAndExternalView(String tableNameWithType) {
    // Create IdealState
    IdealState idealState = new IdealState(tableNameWithType);
    idealState.setStateModelDefRef("OnlineOffline");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setNumPartitions(1);
    idealState.setPartitionState("segment_0", "Server_localhost_8000", "ONLINE");

    // Create ExternalView
    ExternalView externalView = new ExternalView(tableNameWithType);
    externalView.setState("segment_0", "Server_localhost_8000", "ONLINE");

    // Store in ZooKeeper through Helix
    _helixDataAccessor.setProperty(_helixDataAccessor.keyBuilder().idealStates(tableNameWithType), idealState);
    _helixDataAccessor.setProperty(_helixDataAccessor.keyBuilder().externalView(tableNameWithType), externalView);
  }

  private void createIdealStateAndExternalViewWithMultipleServers(String tableNameWithType, String server1,
      String server2) {
    // Create IdealState with multiple servers
    IdealState idealState = new IdealState(tableNameWithType);
    idealState.setStateModelDefRef("OnlineOffline");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setNumPartitions(1);
    idealState.setPartitionState("newSegment_0", server1, "ONLINE");
    idealState.setPartitionState("newSegment_0", server2, "ONLINE");

    // Create ExternalView with multiple servers
    ExternalView externalView = new ExternalView(tableNameWithType);
    externalView.setState("newSegment_0", server1, "ONLINE");
    externalView.setState("newSegment_0", server2, "ONLINE");

    // Store in ZooKeeper through Helix
    _helixDataAccessor.setProperty(_helixDataAccessor.keyBuilder().idealStates(tableNameWithType), idealState);
    _helixDataAccessor.setProperty(_helixDataAccessor.keyBuilder().externalView(tableNameWithType), externalView);
  }

  private void validateDisabledInstanceNotInRouting(String tableNameWithType, String disabledInstance) {
    try {
      Object routingEntry = getRoutingEntry(tableNameWithType);
      Assert.assertNotNull(routingEntry, "Routing entry should exist for table: " + tableNameWithType);

      // Get the InstanceSelector from the routing entry
      java.lang.reflect.Field instanceSelectorField = routingEntry.getClass().getDeclaredField("_instanceSelector");
      instanceSelectorField.setAccessible(true);
      Object instanceSelector = instanceSelectorField.get(routingEntry);
      Assert.assertNotNull(instanceSelector, "InstanceSelector should exist");

      // Get the _enabledInstances field from BaseInstanceSelector
      java.lang.reflect.Field enabledInstancesField =
          instanceSelector.getClass().getSuperclass().getDeclaredField("_enabledInstances");
      enabledInstancesField.setAccessible(true);
      Object enabledInstancesObj = enabledInstancesField.get(instanceSelector);

      if (enabledInstancesObj != null) {
        @SuppressWarnings("unchecked")
        java.util.Set<String> enabledInstances = (java.util.Set<String>) enabledInstancesObj;
        Assert.assertFalse(enabledInstances.contains(disabledInstance),
            "Disabled instance " + disabledInstance + " should NOT be in enabled instances for table "
                + tableNameWithType + ". Enabled instances: " + enabledInstances);
      }
    } catch (Exception e) {
      Assert.fail("Failed to validate disabled instance exclusion for table " + tableNameWithType + ": "
          + e.getMessage());
    }
  }

  private void validateEnabledInstanceInRouting(String tableNameWithType, String enabledInstance) {
    try {
      Object routingEntry = getRoutingEntry(tableNameWithType);
      Assert.assertNotNull(routingEntry, "Routing entry should exist for table: " + tableNameWithType);

      // Get the InstanceSelector from the routing entry
      java.lang.reflect.Field instanceSelectorField = routingEntry.getClass().getDeclaredField("_instanceSelector");
      instanceSelectorField.setAccessible(true);
      Object instanceSelector = instanceSelectorField.get(routingEntry);
      Assert.assertNotNull(instanceSelector, "InstanceSelector should exist");

      // Get the _enabledInstances field from BaseInstanceSelector
      java.lang.reflect.Field enabledInstancesField =
          instanceSelector.getClass().getSuperclass().getDeclaredField("_enabledInstances");
      enabledInstancesField.setAccessible(true);
      Object enabledInstancesObj = enabledInstancesField.get(instanceSelector);

      Assert.assertNotNull(enabledInstancesObj, "Enabled instances should not be null for table " + tableNameWithType);
      @SuppressWarnings("unchecked")
      java.util.Set<String> enabledInstances = (java.util.Set<String>) enabledInstancesObj;
      Assert.assertTrue(enabledInstances.contains(enabledInstance),
          "Enabled instance " + enabledInstance + " should be in enabled instances for table "
              + tableNameWithType + ". Enabled instances: " + enabledInstances);
    } catch (Exception e) {
      Assert.fail("Failed to validate enabled instance inclusion for table " + tableNameWithType + ": "
          + e.getMessage());
    }
  }

  private void createSegmentMetadata(String tableNameWithType, String segmentName, long endTime) {
    SegmentZKMetadata segmentMetadata = new SegmentZKMetadata(segmentName);
    segmentMetadata.setEndTime(endTime);
    segmentMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentMetadata.setTotalDocs(1000);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, tableNameWithType, segmentMetadata);
  }

  /**
   * Test that validates concurrent buildRouting operations for REALTIME and OFFLINE tables
   * of the same hybrid table don't result in missing TimeBoundaryManager due to race conditions.
   *
   * This test uses real ZooKeeper and PropertyStore to validate the new locking mechanism.
   *
   * The test ensures:
   * 1. Both tables can be built concurrently without corruption
   * 2. TimeBoundaryManager is properly set on the offline table when realtime is built
   * 3. No race conditions occur that would leave the offline table without a TimeBoundaryManager
   */
  @Test
  public void testConcurrentHybridTableBuildNoTimeBoundaryManagerRace() throws Exception {
    // Clean any existing routing entries to ensure test isolation
    clearRoutingEntries();

    // Test concurrent execution
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(2);

    AtomicReference<Exception> offlineException = new AtomicReference<>();
    AtomicReference<Exception> realtimeException = new AtomicReference<>();

    try {
      // Build OFFLINE table in thread 1
      @SuppressWarnings("unused")
      Future<?> offlineTask = executor.submit(() -> {
        try {
          startLatch.await();
          _routingManager.buildRouting(OFFLINE_TABLE_NAME);
        } catch (Exception e) {
          offlineException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Build REALTIME table in thread 2
      @SuppressWarnings("unused")
      Future<?> realtimeTask = executor.submit(() -> {
        try {
          startLatch.await();
          _routingManager.buildRouting(REALTIME_TABLE_NAME);
        } catch (Exception e) {
          realtimeException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Release threads to start execution simultaneously
      startLatch.countDown();

      // Wait for both threads to complete
      Assert.assertTrue(finishLatch.await(30, TimeUnit.SECONDS), "Threads didn't complete in time");

      // Check if any thread failed
      if (offlineException.get() != null) {
        Assert.fail("Offline table build failed", offlineException.get());
      }
      if (realtimeException.get() != null) {
        Assert.fail("Realtime table build failed", realtimeException.get());
      }

      // Verify both tables exist
      Assert.assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME), "Offline table routing should exist");
      Assert.assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME), "Realtime table routing should exist");

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // The offline table should have a TimeBoundaryManager when realtime table exists
      Object offlineEntry = getRoutingEntry(OFFLINE_TABLE_NAME);
      Assert.assertNotNull(offlineEntry, "Offline routing entry should exist");

      // If realtime table was built, offline should have TimeBoundaryManager
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      Assert.assertNotNull(timeBoundaryManager, "Offline table should have TimeBoundaryManager when realtime table "
          + "exists - this indicates a race condition in cross-table coordination");
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /**
   * Test that validates sequential table building works correctly and establishes TimeBoundaryManager.
   */
  @Test
  public void testSequentialHybridTableBuildTimeBoundaryManagerCreation() {
    // Clean any existing routing entries to ensure test isolation
    clearRoutingEntries();

    // Step 1: Build OFFLINE table first - should not have TimeBoundaryManager
    _routingManager.buildRouting(OFFLINE_TABLE_NAME);
    Assert.assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME), "Offline table routing should exist");

    Object offlineEntry = getRoutingEntry(OFFLINE_TABLE_NAME);
    TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
    Assert.assertNull(timeBoundaryManager,
        "Offline table should not have TimeBoundaryManager when realtime doesn't exist");

    // Step 2: Build REALTIME table - should add TimeBoundaryManager to existing offline table
    _routingManager.buildRouting(REALTIME_TABLE_NAME);
    Assert.assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME));

    // Verify TimeBoundaryManager was added to offline table
    offlineEntry = getRoutingEntry(OFFLINE_TABLE_NAME);
    timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
    Assert.assertNotNull(timeBoundaryManager, "Offline table should have TimeBoundaryManager after realtime is built");
  }

  @Test
  public void testBuildRoutingSkipsWhenRequestIsOlderThanLastStart()
      throws Exception {
    // Clean any existing routing entries to ensure test isolation
    clearRoutingEntries();

    // Construct with nulls as the build should return early before using these fields
    BrokerRoutingManager manager = new BrokerRoutingManager(null, null, new PinotConfiguration());

    String tableNameWithType = "testTable_OFFLINE";

    // Set a future last build start time to force skipping the current build call
    long futureStart = System.currentTimeMillis() + 10_000L;

    Field startTimesField = BrokerRoutingManager.class.getDeclaredField("_routingTableBuildStartTimeMs");
    startTimesField.setAccessible(true);
    Map<String, Long> startTimes = (Map<String, Long>) startTimesField.get(manager);
    if (startTimes == null) {
      startTimes = new ConcurrentHashMap<>();
      startTimesField.set(manager, startTimes);
    }
    startTimes.put(tableNameWithType, futureStart);

    // Should return without throwing and without attempting to build routing
    manager.buildRouting(tableNameWithType);

    // Ensure routing was not created and the last start time was not overwritten
    Assert.assertFalse(manager.routingExists(tableNameWithType));
    Assert.assertEquals(startTimes.get(tableNameWithType).longValue(), futureStart);
  }

  /**
   * Test concurrent interactions between processSegmentAssignmentChange and buildRouting.
   * This validates that the global read lock (for processSegmentAssignmentChange) and
   * per-table locks (for buildRouting) work correctly together without deadlocks.
   */
  @Test
  public void testConcurrentProcessSegmentAssignmentChangeAndBuildRouting() throws Exception {
    clearRoutingEntries();

    // First, build initial routing entries for both tables
    _routingManager.buildRouting(OFFLINE_TABLE_NAME);
    _routingManager.buildRouting(REALTIME_TABLE_NAME);

    Assert.assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME), "Initial offline routing should exist");
    Assert.assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME), "Initial realtime routing should exist");
    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(3);

    AtomicReference<Exception> segmentAssignmentException = new AtomicReference<>();
    AtomicReference<Exception> buildRoutingOfflineException = new AtomicReference<>();
    AtomicReference<Exception> buildRoutingRealtimeException = new AtomicReference<>();

    try {
      // Thread 1: Process segment assignment change (takes global read lock, per-raw-table-name lock for each table one
      // at a time)
      Future<?> segmentAssignmentTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global read lock
          _routingManager.processClusterChange(ChangeType.IDEAL_STATE);
        } catch (Exception e) {
          segmentAssignmentException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 2: Build routing for offline table (takes per-table lock)
      Future<?> buildOfflineTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take per-raw-table-name lock for OFFLINE table
          _routingManager.buildRouting(OFFLINE_TABLE_NAME);
        } catch (Exception e) {
          buildRoutingOfflineException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Build routing for realtime table (takes same per-table lock)
      Future<?> buildRealtimeTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take per-raw-table-name lock for REALTIME table
          _routingManager.buildRouting(REALTIME_TABLE_NAME);
        } catch (Exception e) {
          buildRoutingRealtimeException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Start all threads simultaneously
      startLatch.countDown();

      // Wait for completion with timeout
      Assert.assertTrue(finishLatch.await(10, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (segmentAssignmentException.get() != null) {
        Assert.fail("Segment assignment change failed: " + segmentAssignmentException.get().getMessage());
      }
      if (buildRoutingOfflineException.get() != null) {
        Assert.fail("Offline table build failed: " + buildRoutingOfflineException.get().getMessage());
      }
      if (buildRoutingRealtimeException.get() != null) {
        Assert.fail("Realtime table build failed: " + buildRoutingRealtimeException.get().getMessage());
      }

      // Verify routing entries still exist after concurrent operations
      Assert.assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME),
          "Offline routing should exist after concurrent operations");
      Assert.assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME),
          "Realtime routing should exist after concurrent operations");

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // The offline table should have a TimeBoundaryManager when realtime table exists
      Object offlineEntry = getRoutingEntry(OFFLINE_TABLE_NAME);
      Assert.assertNotNull(offlineEntry, "Offline routing entry should exist");

      // If realtime table was built, offline should have TimeBoundaryManager
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      Assert.assertNotNull(timeBoundaryManager, "Offline table should have TimeBoundaryManager when realtime table "
          + "exists - this indicates a race condition in cross-table coordination");
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /**
   * Test concurrent interactions between processInstanceConfigChange and buildRouting where buildRouting adds a new
   * table.
   * This validates that the global write lock (for processInstanceConfigChange) and
   * per-table locks (for buildRouting) work correctly together, especially when buildRouting creates new routing
   * entries.
   */
  @Test
  public void testConcurrentProcessInstanceConfigChangeAndBuildRoutingNewTable() throws Exception {
    clearRoutingEntries();

    // Add additional server instances for this test
    String enabledServerInstance = "Server_localhost_8000";  // Already added in setUp()
    String disabledServerInstance = "Server_localhost_8001"; // We'll add and then disable this one

    String clusterName = getHelixClusterName();

    // Add the second server instance
    if (!_helixAdmin.getInstancesInCluster(clusterName).contains(disabledServerInstance)) {
      InstanceConfig disabledInstanceConfig = new InstanceConfig(disabledServerInstance);
      disabledInstanceConfig.setHostName("localhost");
      disabledInstanceConfig.setPort("8001");
      disabledInstanceConfig.setInstanceEnabled(true); // Initially enabled

      _helixAdmin.addInstance(clusterName, disabledInstanceConfig);
      _helixAdmin.enableInstance(clusterName, disabledServerInstance, true);
    }

    // Create test table configs for the new tables we'll add during concurrent operations
    String newOfflineTable = "newTestTable_OFFLINE";
    String newRealtimeTable = "newTestTable_REALTIME";

    TableConfig newOfflineConfig = createTableConfig(newOfflineTable, TableType.OFFLINE);
    TableConfig newRealtimeConfig = createTableConfig(newRealtimeTable, TableType.REALTIME);

    ZKMetadataProvider.setTableConfig(_propertyStore, newOfflineConfig);
    ZKMetadataProvider.setTableConfig(_propertyStore, newRealtimeConfig);

    // Create and upload schemas for the new tables
    Schema newSchema = createMockSchema();
    newSchema.setSchemaName(TableNameBuilder.extractRawTableName(newOfflineTable));
    ZKMetadataProvider.setSchema(_propertyStore, newSchema);

    // Create IdealState and ExternalView for the new tables with both servers
    createIdealStateAndExternalViewWithMultipleServers(newOfflineTable, enabledServerInstance,
        disabledServerInstance);
    createIdealStateAndExternalViewWithMultipleServers(newRealtimeTable, enabledServerInstance,
        disabledServerInstance);

    // Create segment metadata
    createSegmentMetadata(newOfflineTable, "newSegment_0", System.currentTimeMillis());
    createSegmentMetadata(newRealtimeTable, "newSegment_0", System.currentTimeMillis());

    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(3);

    AtomicReference<Exception> instanceConfigException = new AtomicReference<>();
    AtomicReference<Exception> buildNewOfflineException = new AtomicReference<>();
    AtomicReference<Exception> buildNewRealtimeException = new AtomicReference<>();

    // Disable one of the server instances before starting concurrent operations
    _helixAdmin.enableInstance(clusterName, disabledServerInstance, false);

    try {
      // Thread 1: Process instance config change (takes global write lock and per-raw-table-name locks for each table
      // one at a time)
      Future<?> instanceConfigTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global write lock and process the disabled instance
          _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);
        } catch (Exception e) {
          instanceConfigException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 2: Build routing for new offline table (takes per-table lock and adds new entry)
      Future<?> buildNewOfflineTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take per-table lock and create new routing entry
          _routingManager.buildRouting(newOfflineTable);
        } catch (Exception e) {
          buildNewOfflineException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Build routing for new realtime table (takes per-table lock and adds new entry)
      Future<?> buildNewRealtimeTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take per-table lock and create new routing entry
          _routingManager.buildRouting(newRealtimeTable);
        } catch (Exception e) {
          buildNewRealtimeException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Start all threads simultaneously
      startLatch.countDown();

      // Wait for completion with timeout
      Assert.assertTrue(finishLatch.await(15, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (instanceConfigException.get() != null) {
        Assert.fail("Instance config change failed: " + instanceConfigException.get().getMessage());
      }
      if (buildNewOfflineException.get() != null) {
        Assert.fail("New offline table build failed: " + buildNewOfflineException.get().getMessage());
      }
      if (buildNewRealtimeException.get() != null) {
        Assert.fail("New realtime table build failed: " + buildNewRealtimeException.get().getMessage());
      }

      // Verify new routing entries were created successfully
      Assert.assertTrue(_routingManager.routingExists(newOfflineTable),
          "New offline routing should exist after concurrent operations");
      Assert.assertTrue(_routingManager.routingExists(newRealtimeTable),
          "New realtime routing should exist after concurrent operations");

      // Verify TimeBoundaryManager coordination for the new hybrid table
      Object newOfflineEntry = getRoutingEntry(newOfflineTable);
      Assert.assertNotNull(newOfflineEntry, "New offline routing entry should exist");
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(newOfflineEntry);
      Assert.assertNotNull(timeBoundaryManager,
          "New offline table should have TimeBoundaryManager when realtime exists");

      // CRITICAL: Verify that the disabled instance is NOT included in the routing entries
      validateDisabledInstanceNotInRouting(newOfflineTable, disabledServerInstance);
      validateDisabledInstanceNotInRouting(newRealtimeTable, disabledServerInstance);
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  private TableConfig createTableConfig(String tableNameWithType, TableType tableType) {
    return new TableConfigBuilder(tableType)
        .setTableName(TableNameBuilder.extractRawTableName(tableNameWithType))
        .setTimeColumnName("timestamp")
        .build();
  }

  private Schema createMockSchema() {
    Schema schema = new Schema();
    schema.setSchemaName(RAW_TABLE_NAME);
    schema.addField(new DateTimeFieldSpec("timestamp", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:DAYS"));
    return schema;
  }

  private Object getRoutingEntry(String tableNameWithType) {
    try {
      // Use reflection to access the private _routingEntryMap
      java.lang.reflect.Field field = BrokerRoutingManager.class.getDeclaredField("_routingEntryMap");
      field.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.Map<String, Object> routingEntryMap = (java.util.Map<String, Object>) field.get(_routingManager);
      return routingEntryMap.get(tableNameWithType);
    } catch (Exception e) {
      throw new RuntimeException("Failed to access routing entry", e);
    }
  }

  private TimeBoundaryManager getTimeBoundaryManager(Object routingEntry) {
    if (routingEntry == null) {
      return null;
    }
    try {
      java.lang.reflect.Method method = routingEntry.getClass().getDeclaredMethod("getTimeBoundaryManager");
      method.setAccessible(true);
      return (TimeBoundaryManager) method.invoke(routingEntry);
    } catch (Exception e) {
      throw new RuntimeException("Failed to access TimeBoundaryManager", e);
    }
  }

  /**
   * Test concurrent interactions between excludeServerFromRouting (global write lock) and buildRouting
   * (global read lock + per-table lock).
   * This validates that global write lock properly blocks global read lock operations.
   */
  @Test
  public void testConcurrentExcludeServerAndBuildRouting() throws Exception {
    clearRoutingEntries();

    String disabledServerInstance = "Server_localhost_8001"; // We'll disable this one

    // First, build initial routing entries for both tables
    _routingManager.buildRouting(OFFLINE_TABLE_NAME);
    _routingManager.buildRouting(REALTIME_TABLE_NAME);

    Assert.assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME), "Initial offline routing should exist");
    Assert.assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME), "Initial realtime routing should exist");

    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(3);

    AtomicReference<Exception> excludeServerException = new AtomicReference<>();
    AtomicReference<Exception> buildOfflineException = new AtomicReference<>();
    AtomicReference<Exception> buildRealtimeException = new AtomicReference<>();

    // CRITICAL: Verify that the to be disabled instance is currently included in the routing entries
    validateEnabledInstanceInRouting(OFFLINE_TABLE_NAME, disabledServerInstance);
    validateEnabledInstanceInRouting(REALTIME_TABLE_NAME, disabledServerInstance);

    try {
      // Thread 1: Exclude server from routing (takes global write lock)
      Future<?> excludeServerTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global write lock
          _routingManager.excludeServerFromRouting(disabledServerInstance);
        } catch (Exception e) {
          excludeServerException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 2: Build routing for offline table (takes global read lock + per-table lock)
      Future<?> buildOfflineTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global read lock + per-table lock
          _routingManager.buildRouting(OFFLINE_TABLE_NAME);
        } catch (Exception e) {
          buildOfflineException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Build routing for realtime table (takes global read lock + per-table lock)
      Future<?> buildRealtimeTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global read lock + different per-table lock
          _routingManager.buildRouting(REALTIME_TABLE_NAME);
        } catch (Exception e) {
          buildRealtimeException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Start all threads simultaneously
      startLatch.countDown();

      // Wait for completion with timeout
      Assert.assertTrue(finishLatch.await(10, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (excludeServerException.get() != null) {
        Assert.fail("Exclude server failed: " + excludeServerException.get().getMessage());
      }
      if (buildOfflineException.get() != null) {
        Assert.fail("Build offline routing failed: " + buildOfflineException.get().getMessage());
      }
      if (buildRealtimeException.get() != null) {
        Assert.fail("Build realtime routing failed: " + buildRealtimeException.get().getMessage());
      }

      // Verify routing entries still exist after operations
      Assert.assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME),
          "Offline routing should exist after exclude server operation");
      Assert.assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME),
          "Realtime routing should exist after exclude server operation");

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // The offline table should have a TimeBoundaryManager when realtime table exists
      Object offlineEntry = getRoutingEntry(OFFLINE_TABLE_NAME);
      Assert.assertNotNull(offlineEntry, "Offline routing entry should exist");

      // If realtime table was built, offline should have TimeBoundaryManager
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      Assert.assertNotNull(timeBoundaryManager, "Offline table should have TimeBoundaryManager when realtime table "
          + "exists - this indicates a race condition in cross-table coordination");

      // CRITICAL: Verify that the disabled instance is NOT included in the routing entries
      validateDisabledInstanceNotInRouting(OFFLINE_TABLE_NAME, disabledServerInstance);
      validateDisabledInstanceNotInRouting(REALTIME_TABLE_NAME, disabledServerInstance);
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /**
   * Test concurrent interactions between includeServerToRouting (global write lock) and refreshSegment
   * (global read lock + per-table lock).
   * This validates proper coordination between global write operations and segment refresh operations.
   */
  @Test
  public void testConcurrentIncludeServerAndRefreshSegment() throws Exception {
    clearRoutingEntries();

    String includedServerInstance = "Server_localhost_8001"; // We'll include this one

    // First exclude the server so we can include it later and validate the inclusion
    _routingManager.excludeServerFromRouting(includedServerInstance);

    // Build initial routing entries
    _routingManager.buildRouting(OFFLINE_TABLE_NAME);
    _routingManager.buildRouting(REALTIME_TABLE_NAME);

    // Verify server is initially excluded from routing
    validateDisabledInstanceNotInRouting(OFFLINE_TABLE_NAME, includedServerInstance);
    validateDisabledInstanceNotInRouting(REALTIME_TABLE_NAME, includedServerInstance);

    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(3);

    AtomicReference<Exception> includeServerException = new AtomicReference<>();
    AtomicReference<Exception> refreshOfflineException = new AtomicReference<>();
    AtomicReference<Exception> refreshRealtimeException = new AtomicReference<>();

    try {
      // Thread 1: Include server to routing (takes global write lock)
      Future<?> includeServerTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global write lock
          _routingManager.includeServerToRouting(includedServerInstance);
        } catch (Exception e) {
          includeServerException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 2: Refresh segment for offline table (takes global read lock + per-table lock)
      Future<?> refreshOfflineTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global read lock + per-table lock
          _routingManager.refreshSegment(OFFLINE_TABLE_NAME, "segment_0");
        } catch (Exception e) {
          refreshOfflineException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Refresh segment for realtime table (takes global read lock + per-table lock)
      Future<?> refreshRealtimeTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global read lock + different per-table lock
          _routingManager.refreshSegment(REALTIME_TABLE_NAME, "segment_0");
        } catch (Exception e) {
          refreshRealtimeException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Start all threads simultaneously
      startLatch.countDown();

      // Wait for completion with timeout
      Assert.assertTrue(finishLatch.await(10, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (includeServerException.get() != null) {
        Assert.fail("Include server failed: " + includeServerException.get().getMessage());
      }
      if (refreshOfflineException.get() != null) {
        Assert.fail("Refresh offline segment failed: " + refreshOfflineException.get().getMessage());
      }
      if (refreshRealtimeException.get() != null) {
        Assert.fail("Refresh realtime segment failed: " + refreshRealtimeException.get().getMessage());
      }

      // CRITICAL: Verify that the included instance IS now included in the routing entries
      validateEnabledInstanceInRouting(OFFLINE_TABLE_NAME, includedServerInstance);
      validateEnabledInstanceInRouting(REALTIME_TABLE_NAME, includedServerInstance);
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /**
   * Test concurrent query operations (getRoutingTable, getTimeBoundaryInfo, getQueryTimeoutMs) during routing
   * modifications. This validates that query path operations can execute concurrently and are not blocked by
   * routing modifications.
   */
  @Test
  public void testConcurrentQueryOperationsDuringRoutingModifications() throws Exception {
    clearRoutingEntries();

    // Build initial routing entries
    _routingManager.buildRouting(OFFLINE_TABLE_NAME);
    _routingManager.buildRouting(REALTIME_TABLE_NAME);

    ExecutorService executor = Executors.newFixedThreadPool(6);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(6);

    AtomicReference<Exception> buildRoutingException = new AtomicReference<>();
    AtomicReference<Exception> refreshSegmentException = new AtomicReference<>();
    AtomicReference<Exception> getRoutingTableException = new AtomicReference<>();
    AtomicReference<Exception> getTimeBoundaryException = new AtomicReference<>();
    AtomicReference<Exception> getQueryTimeoutException = new AtomicReference<>();
    AtomicReference<Exception> removeRoutingException = new AtomicReference<>();

    // Create a mock broker request for testing
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    QuerySource querySource = mock(QuerySource.class);
    when(brokerRequest.getQuerySource()).thenReturn(querySource);
    when(querySource.getTableName()).thenReturn(OFFLINE_TABLE_NAME);

    try {
      // Thread 1: Build routing (takes global read lock + per-table lock)
      Future<?> buildRoutingTask = executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < 5; i++) {
            _routingManager.buildRouting(OFFLINE_TABLE_NAME);
            Thread.sleep(10);
          }
        } catch (Exception e) {
          buildRoutingException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 2: Refresh segment (takes global read lock + per-table lock)
      Future<?> refreshSegmentTask = executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < 5; i++) {
            _routingManager.refreshSegment(REALTIME_TABLE_NAME, "segment_" + i);
            Thread.sleep(10);
          }
        } catch (Exception e) {
          refreshSegmentException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Get routing table (read-only, no locks in query path)
      Future<?> getRoutingTableTask = executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < 10; i++) {
            _routingManager.getRoutingTable(brokerRequest, i);
            Thread.sleep(5);
          }
        } catch (Exception e) {
          getRoutingTableException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 4: Get time boundary info (read-only, no locks in query path)
      Future<?> getTimeBoundaryTask = executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < 10; i++) {
            _routingManager.getTimeBoundaryInfo(OFFLINE_TABLE_NAME);
            Thread.sleep(5);
          }
        } catch (Exception e) {
          getTimeBoundaryException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 5: Get query timeout (read-only, no locks in query path)
      Future<?> getQueryTimeoutTask = executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < 10; i++) {
            _routingManager.getQueryTimeoutMs(OFFLINE_TABLE_NAME);
            _routingManager.getQueryTimeoutMs(REALTIME_TABLE_NAME);
            Thread.sleep(5);
          }
        } catch (Exception e) {
          getQueryTimeoutException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 6: Remove routing (takes global read lock + per-table lock)
      Future<?> removeRoutingTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(50); // Let other operations run first
          _routingManager.removeRouting(REALTIME_TABLE_NAME);
        } catch (Exception e) {
          removeRoutingException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Start all threads simultaneously
      startLatch.countDown();

      // Wait for completion with timeout
      Assert.assertTrue(finishLatch.await(15, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (buildRoutingException.get() != null) {
        Assert.fail("Build routing failed: " + buildRoutingException.get().getMessage());
      }
      if (refreshSegmentException.get() != null) {
        Assert.fail("Refresh segment failed: " + refreshSegmentException.get().getMessage());
      }
      if (getRoutingTableException.get() != null) {
        Assert.fail("Get routing table failed: " + getRoutingTableException.get().getMessage());
      }
      if (getTimeBoundaryException.get() != null) {
        Assert.fail("Get time boundary failed: " + getTimeBoundaryException.get().getMessage());
      }
      if (getQueryTimeoutException.get() != null) {
        Assert.fail("Get query timeout failed: " + getQueryTimeoutException.get().getMessage());
      }
      if (removeRoutingException.get() != null) {
        Assert.fail("Remove routing failed: " + removeRoutingException.get().getMessage());
      }

      // Verify offline routing still exists but realtime was removed
      Assert.assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME),
          "Offline routing should still exist after concurrent operations");
      Assert.assertFalse(_routingManager.routingExists(REALTIME_TABLE_NAME),
          "Realtime routing should not exist after concurrent operations");

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // The offline table should have a TimeBoundaryManager when realtime table exists
      Object offlineEntry = getRoutingEntry(OFFLINE_TABLE_NAME);
      Assert.assertNotNull(offlineEntry, "Offline routing entry should exist");

      // If realtime table wasn't built, offline shouldn't have TimeBoundaryManager
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      Assert.assertNull(timeBoundaryManager, "Offline table shouldn't have TimeBoundaryManager when realtime table "
          + "doesn't exist - this indicates a race condition in cross-table coordination");
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /**
   * Test concurrent interactions between two global write lock methods: processInstanceConfigChange and
   * includeServerToRouting. This validates that global write lock methods are properly serialized and don't
   * cause deadlocks or race conditions.
   */
  @Test
  public void testConcurrentGlobalWriteLockMethods() throws Exception {
    clearRoutingEntries();

    // Build initial routing entries
    _routingManager.buildRouting(OFFLINE_TABLE_NAME);
    _routingManager.buildRouting(REALTIME_TABLE_NAME);

    // First exclude a server so we can include it later
    _routingManager.excludeServerFromRouting("Server_localhost_8000");

    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(3);

    AtomicReference<Exception> processInstanceConfigException = new AtomicReference<>();
    AtomicReference<Exception> includeServerException = new AtomicReference<>();
    AtomicReference<Exception> excludeServerException = new AtomicReference<>();

    // Track execution order to verify serialization
    List<String> executionOrder = new ArrayList<>();

    try {
      // Thread 1: Process instance config change (takes global write lock)
      Future<?> processInstanceConfigTask = executor.submit(() -> {
        try {
          startLatch.await();
          synchronized (executionOrder) {
            executionOrder.add("processInstanceConfigChange_start");
          }
          // This should take global write lock
          _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);
          synchronized (executionOrder) {
            executionOrder.add("processInstanceConfigChange_end");
          }
        } catch (Exception e) {
          processInstanceConfigException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 2: Include server to routing (takes global write lock)
      Future<?> includeServerTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(10); // Small delay to encourage different ordering
          synchronized (executionOrder) {
            executionOrder.add("includeServerToRouting_start");
          }
          // This should take global write lock and be serialized with processInstanceConfigChange
          _routingManager.includeServerToRouting("Server_localhost_8000");
          synchronized (executionOrder) {
            executionOrder.add("includeServerToRouting_end");
          }
        } catch (Exception e) {
          includeServerException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Exclude another server (takes global write lock)
      Future<?> excludeServerTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(20); // Small delay to encourage different ordering
          synchronized (executionOrder) {
            executionOrder.add("excludeServerFromRouting_start");
          }
          // This should take global write lock and be serialized with other global write operations
          _routingManager.excludeServerFromRouting("Server_localhost_8001");
          synchronized (executionOrder) {
            executionOrder.add("excludeServerFromRouting_end");
          }
        } catch (Exception e) {
          excludeServerException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Start all threads simultaneously
      startLatch.countDown();

      // Wait for completion with timeout
      Assert.assertTrue(finishLatch.await(15, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (processInstanceConfigException.get() != null) {
        Assert.fail("Process instance config failed: " + processInstanceConfigException.get().getMessage());
      }
      if (includeServerException.get() != null) {
        Assert.fail("Include server failed: " + includeServerException.get().getMessage());
      }
      if (excludeServerException.get() != null) {
        Assert.fail("Exclude server failed: " + excludeServerException.get().getMessage());
      }

      // Verify that operations were properly serialized (no interleaving of start/end events)
      Assert.assertEquals(executionOrder.size(), 6, "Should have 6 execution events (3 starts, 3 ends)");

      // Validate that each operation completed before the next one started
      // (no _start event should occur between another operation's _start and _end)
      for (int i = 0; i < executionOrder.size(); i += 2) {
        String startEvent = executionOrder.get(i);
        String endEvent = executionOrder.get(i + 1);
        Assert.assertTrue(startEvent.endsWith("_start"), "Event at position " + i + " should be a start event");
        Assert.assertTrue(endEvent.endsWith("_end"), "Event at position " + (i + 1) + " should be an end event");

        // Extract operation name (everything before the last underscore)
        String startOperation = startEvent.substring(0, startEvent.lastIndexOf("_"));
        String endOperation = endEvent.substring(0, endEvent.lastIndexOf("_"));
        Assert.assertEquals(startOperation, endOperation, "Start and end events should be for the same operation");
      }

      // Verify routing entries still exist and are properly updated
      Assert.assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME),
          "Offline routing should exist after concurrent global write operations");
      Assert.assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME),
          "Realtime routing should exist after concurrent global write operations");

      System.out.println("Global write lock methods executed in order: " + executionOrder);
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /**
   * Test concurrent interactions between buildRoutingForLogicalTable and buildRouting.
   * This validates proper coordination between logical table operations, regular table operations,
   * and global write operations. Uses a hybrid logical table configuration with both offline and realtime tables.
   */
  @Test
  public void testConcurrentLogicalTableBuildAndRegularBuild() throws Exception {
    clearRoutingEntries();

    String logicalTableName = "testLogicalTable";
    String physicalOfflineTable = "testLogicalTable_OFFLINE";
    String physicalRealtimeTable = "testLogicalTable_REALTIME";

    // Create hybrid logical table config with both offline and realtime tables
    LogicalTableConfig logicalTableConfig = createLogicalTableConfig(logicalTableName,
        Map.of(
            "testLogicalTable", new PhysicalTableConfig()
        ), physicalOfflineTable, physicalRealtimeTable);
    ZKMetadataProvider.setLogicalTableConfig(_propertyStore, logicalTableConfig);

    // Create physical table configs and schemas
    TableConfig offlineTableConfig = createTableConfig(physicalOfflineTable, TableType.OFFLINE);
    TableConfig realtimeTableConfig = createTableConfig(physicalRealtimeTable, TableType.REALTIME);
    ZKMetadataProvider.setTableConfig(_propertyStore, offlineTableConfig);
    ZKMetadataProvider.setTableConfig(_propertyStore, realtimeTableConfig);

    // Create schemas for each physical table
    Schema offlineSchema = createMockSchema();
    offlineSchema.setSchemaName(TableNameBuilder.extractRawTableName(physicalOfflineTable));
    ZKMetadataProvider.setSchema(_propertyStore, offlineSchema);

    Schema realtimeSchema = createMockSchema();
    realtimeSchema.setSchemaName(TableNameBuilder.extractRawTableName(physicalRealtimeTable));
    ZKMetadataProvider.setSchema(_propertyStore, realtimeSchema);

    // Create ideal states and external views for physical tables
    createIdealStateAndExternalView(physicalOfflineTable);
    createIdealStateAndExternalView(physicalRealtimeTable);

    ExecutorService executor = Executors.newFixedThreadPool(5);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(5);

    AtomicReference<Exception> logicalBuildException = new AtomicReference<>();
    AtomicReference<Exception> regularBuildException = new AtomicReference<>();
    AtomicReference<Exception> refreshOfflineException = new AtomicReference<>();
    AtomicReference<Exception> refreshRealtimeException = new AtomicReference<>();
    AtomicReference<Exception> excludeServerException = new AtomicReference<>();

    try {
      // Thread 1: Build routing for logical table (global read lock + per-table locks for both physical tables)
      Future<?> logicalBuildTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global read lock + per-table locks for both physical tables
          _routingManager.buildRoutingForLogicalTable(logicalTableName);
        } catch (Exception e) {
          logicalBuildException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 2: Build routing for regular table (global read lock + different per-table lock)
      Future<?> regularBuildTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(5); // Small delay to encourage interleaving
          // This should take global read lock + per-table lock for the regular table
          _routingManager.buildRouting(OFFLINE_TABLE_NAME);
        } catch (Exception e) {
          regularBuildException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Refresh segment on offline physical table (global read lock + same per-table lock as logical)
      Future<?> refreshOfflineTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(10); // Small delay to encourage interleaving
          // This should compete for the same per-table lock as logical table build for offline table
          _routingManager.refreshSegment(physicalOfflineTable, "segment_0");
        } catch (Exception e) {
          refreshOfflineException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 4: Refresh segment on realtime physical table (global read lock + different per-table lock)
      Future<?> refreshRealtimeTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(12); // Small delay to encourage interleaving
          // This should compete for the different per-table lock as logical table build for realtime table
          _routingManager.refreshSegment(physicalRealtimeTable, "segment_0");
        } catch (Exception e) {
          refreshRealtimeException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 5: Exclude server from routing (global write lock - should serialize with all read operations)
      Future<?> excludeServerTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(15); // Small delay to encourage interleaving
          // This should take global write lock and serialize with all other operations
          _routingManager.excludeServerFromRouting("Server_localhost_8001");
        } catch (Exception e) {
          excludeServerException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Start all threads simultaneously
      startLatch.countDown();

      // Wait for completion with timeout
      Assert.assertTrue(finishLatch.await(15, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (logicalBuildException.get() != null) {
        Assert.fail("Logical table build failed: " + logicalBuildException.get().getMessage());
      }
      if (regularBuildException.get() != null) {
        Assert.fail("Regular table build failed: " + regularBuildException.get().getMessage());
      }
      if (refreshOfflineException.get() != null) {
        Assert.fail("Refresh offline segment failed: " + refreshOfflineException.get().getMessage());
      }
      if (refreshRealtimeException.get() != null) {
        Assert.fail("Refresh realtime segment failed: " + refreshRealtimeException.get().getMessage());
      }
      if (excludeServerException.get() != null) {
        Assert.fail("Exclude server failed: " + excludeServerException.get().getMessage());
      }

      // Verify routing entries exist for regular table
      Assert.assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME),
          "Regular table routing should exist after concurrent operations");

      // Verify that the excluded server is not in routing for any existing tables
      validateDisabledInstanceNotInRouting(OFFLINE_TABLE_NAME, "Server_localhost_8001");

      // Verify routing entries exist for regular table
      Assert.assertTrue(_routingManager.routingExists(physicalOfflineTable),
          "Regular table routing should exist after concurrent operations");

      // Verify that the excluded server is not in routing for any existing tables
      validateDisabledInstanceNotInRouting(physicalOfflineTable, "Server_localhost_8001");

      // Note: We don't check physical table routing existence as buildRoutingForLogicalTable only
      // creates routing for offline tables in the time boundary configuration, and only if they
      // don't already exist. The test validates concurrent lock coordination rather than routing creation.

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // If the logical table build actually created routing for physical tables, verify TimeBoundaryManager
      Object offlineEntry = getRoutingEntry(physicalOfflineTable);
      Assert.assertNotNull(offlineEntry, "Physical offline routing entry should exist");

      // Physical offline table should have TimeBoundaryManager due to logical table setup
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      Assert.assertNotNull(timeBoundaryManager, "Physical offline table should have TimeBoundaryManager if part of "
          + "a logical table - this indicates a race condition in cross-table coordination");

      Assert.assertFalse(_routingManager.routingExists(physicalRealtimeTable),
          "Physical realtime routing entry should not exist since we never built routing entry for it");
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /**
   * Test concurrent interactions between buildRoutingForLogicalTable and buildRouting.
   * This validates proper coordination between logical table operations, regular table operations,
   * and global write operations. Uses a hybrid logical table configuration with both offline and realtime tables.
   */
  @Test
  public void testConcurrentLogicalTableBuildAndRegularBuildAndRealtimeBuild() throws Exception {
    clearRoutingEntries();

    String logicalTableName = "testLogicalTable";
    String physicalOfflineTable = "testLogicalTable_OFFLINE";
    String physicalRealtimeTable = "testLogicalTable_REALTIME";

    // Create hybrid logical table config with both offline and realtime tables
    LogicalTableConfig logicalTableConfig = createLogicalTableConfig(logicalTableName,
        Map.of(
            "testLogicalTable", new PhysicalTableConfig()
        ), physicalOfflineTable, physicalRealtimeTable);
    ZKMetadataProvider.setLogicalTableConfig(_propertyStore, logicalTableConfig);

    // Create physical table configs and schemas
    TableConfig offlineTableConfig = createTableConfig(physicalOfflineTable, TableType.OFFLINE);
    TableConfig realtimeTableConfig = createTableConfig(physicalRealtimeTable, TableType.REALTIME);
    ZKMetadataProvider.setTableConfig(_propertyStore, offlineTableConfig);
    ZKMetadataProvider.setTableConfig(_propertyStore, realtimeTableConfig);

    // Create schemas for each physical table
    Schema offlineSchema = createMockSchema();
    offlineSchema.setSchemaName(TableNameBuilder.extractRawTableName(physicalOfflineTable));
    ZKMetadataProvider.setSchema(_propertyStore, offlineSchema);

    Schema realtimeSchema = createMockSchema();
    realtimeSchema.setSchemaName(TableNameBuilder.extractRawTableName(physicalRealtimeTable));
    ZKMetadataProvider.setSchema(_propertyStore, realtimeSchema);

    // Create ideal states and external views for physical tables
    createIdealStateAndExternalView(physicalOfflineTable);
    createIdealStateAndExternalView(physicalRealtimeTable);

    ExecutorService executor = Executors.newFixedThreadPool(6);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(6);

    AtomicReference<Exception> logicalBuildException = new AtomicReference<>();
    AtomicReference<Exception> regularBuildException = new AtomicReference<>();
    AtomicReference<Exception> refreshOfflineException = new AtomicReference<>();
    AtomicReference<Exception> refreshRealtimeException = new AtomicReference<>();
    AtomicReference<Exception> excludeServerException = new AtomicReference<>();
    AtomicReference<Exception> realtimeBuildException = new AtomicReference<>();

    try {
      // Thread 1: Build routing for logical table (global read lock + per-table locks for both physical tables)
      Future<?> logicalBuildTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global read lock + per-table locks for both physical tables
          _routingManager.buildRoutingForLogicalTable(logicalTableName);
        } catch (Exception e) {
          logicalBuildException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 2: Build routing for regular table (global read lock + different per-table lock)
      Future<?> regularBuildTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(5); // Small delay to encourage interleaving
          // This should take global read lock + per-table lock for the regular table
          _routingManager.buildRouting(OFFLINE_TABLE_NAME);
        } catch (Exception e) {
          regularBuildException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Refresh segment on offline physical table (global read lock + same per-table lock as logical)
      Future<?> refreshOfflineTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(10); // Small delay to encourage interleaving
          // This should compete for the same per-table lock as logical table build for offline table
          _routingManager.refreshSegment(physicalOfflineTable, "segment_0");
        } catch (Exception e) {
          refreshOfflineException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 4: Refresh segment on realtime physical table (global read lock + different per-table lock)
      Future<?> refreshRealtimeTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(12); // Small delay to encourage interleaving
          // This should compete for the different per-table lock as logical table build for realtime table
          _routingManager.refreshSegment(physicalRealtimeTable, "segment_0");
        } catch (Exception e) {
          refreshRealtimeException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 5: Exclude server from routing (global write lock - should serialize with all read operations)
      Future<?> excludeServerTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(15); // Small delay to encourage interleaving
          // This should take global write lock and serialize with all other operations
          _routingManager.excludeServerFromRouting("Server_localhost_8001");
        } catch (Exception e) {
          excludeServerException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 6: Build routing for physical realtime table (global read lock + different per-table lock)
      Future<?> physicalRealtimeBuildTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(5); // Small delay to encourage interleaving
          // This should take global read lock + per-table lock for the regular table
          _routingManager.buildRouting(physicalRealtimeTable);
        } catch (Exception e) {
          realtimeBuildException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Start all threads simultaneously
      startLatch.countDown();

      // Wait for completion with timeout
      Assert.assertTrue(finishLatch.await(15, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (logicalBuildException.get() != null) {
        Assert.fail("Logical table build failed: " + logicalBuildException.get().getMessage());
      }
      if (regularBuildException.get() != null) {
        Assert.fail("Regular table build failed: " + regularBuildException.get().getMessage());
      }
      if (refreshOfflineException.get() != null) {
        Assert.fail("Refresh offline segment failed: " + refreshOfflineException.get().getMessage());
      }
      if (refreshRealtimeException.get() != null) {
        Assert.fail("Refresh realtime segment failed: " + refreshRealtimeException.get().getMessage());
      }
      if (excludeServerException.get() != null) {
        Assert.fail("Exclude server failed: " + excludeServerException.get().getMessage());
      }
      if (realtimeBuildException.get() != null) {
        Assert.fail("Realtime table build failed: " + realtimeBuildException.get().getMessage());
      }

      // Verify routing entries exist for regular table
      Assert.assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME),
          "Regular table routing should exist after concurrent operations");

      // Verify that the excluded server is not in routing for any existing tables
      validateDisabledInstanceNotInRouting(OFFLINE_TABLE_NAME, "Server_localhost_8001");

      // Verify routing entries exist for regular table
      Assert.assertTrue(_routingManager.routingExists(physicalOfflineTable),
          "Regular table routing should exist after concurrent operations");

      // Verify that the excluded server is not in routing for any existing tables
      validateDisabledInstanceNotInRouting(physicalOfflineTable, "Server_localhost_8001");

      // Note: We don't check physical table routing existence as buildRoutingForLogicalTable only
      // creates routing for offline tables in the time boundary configuration, and only if they
      // don't already exist. The test validates concurrent lock coordination rather than routing creation.

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // If the logical table build actually created routing for physical tables, verify TimeBoundaryManager
      Object offlineEntry = getRoutingEntry(physicalOfflineTable);
      Assert.assertNotNull(offlineEntry, "Physical offline routing entry should exist");

      // Physical offline table should have TimeBoundaryManager due to logical table setup
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      Assert.assertNotNull(timeBoundaryManager, "Physical offline table should have TimeBoundaryManager if part of "
          + "a logical table - this indicates a race condition in cross-table coordination");

      Assert.assertTrue(_routingManager.routingExists(physicalRealtimeTable),
          "Physical realtime routing entry should exist since we built routing entry for it");
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /**
   * Test concurrent interactions with logical table containing multiple physical tables.
   * This validates coordination when logical table operations affect multiple per-table locks.
   */
  @Test
  public void testConcurrentMultiPhysicalTableLogicalOperations() throws Exception {
    clearRoutingEntries();

    String logicalTableName = "testMultiLogicalTable";
    String physicalTable1 = "multiPhysical1_OFFLINE";
    String physicalTable2 = "multiPhysical2_OFFLINE";
    String physicalTable3 = "multiPhysical3_REALTIME";

    // Create logical table config with multiple physical tables
    LogicalTableConfig logicalTableConfig = createLogicalTableConfig(logicalTableName,
        Map.of(
            "multiPhysical1", new PhysicalTableConfig(),
            "multiPhysical2", new PhysicalTableConfig(),
            "multiPhysical3", new PhysicalTableConfig()
        ),
        physicalTable1, physicalTable3);
    ZKMetadataProvider.setLogicalTableConfig(_propertyStore, logicalTableConfig);

    // Create physical table configs and schemas
    for (String tableNameWithType : Arrays.asList(physicalTable1, physicalTable2, physicalTable3)) {
      TableType tableType = TableNameBuilder.isOfflineTableResource(tableNameWithType)
          ? TableType.OFFLINE : TableType.REALTIME;
      TableConfig physicalTableConfig = createTableConfig(tableNameWithType, tableType);
      ZKMetadataProvider.setTableConfig(_propertyStore, physicalTableConfig);
      ZKMetadataProvider.setSchema(_propertyStore, createMockSchema());
      createIdealStateAndExternalView(tableNameWithType);
    }

    ExecutorService executor = Executors.newFixedThreadPool(5);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(5);

    AtomicReference<Exception> logicalBuildException = new AtomicReference<>();
    AtomicReference<Exception> logicalRemoveException = new AtomicReference<>();
    AtomicReference<Exception> regularBuild1Exception = new AtomicReference<>();
    AtomicReference<Exception> regularBuild2Exception = new AtomicReference<>();
    AtomicReference<Exception> includeServerException = new AtomicReference<>();

    try {
      // Thread 1: Build routing for logical table (global read lock + multiple per-table locks)
      Future<?> logicalBuildTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global read lock + per-table locks for all 3 physical tables
          _routingManager.buildRoutingForLogicalTable(logicalTableName);
        } catch (Exception e) {
          logicalBuildException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 2: Remove routing for logical table (global read lock + multiple per-table locks)
      Future<?> logicalRemoveTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(20); // Delay to let build start first
          // This should take global read lock + per-table locks for all 3 physical tables
          _routingManager.removeRoutingForLogicalTable(logicalTableName);
        } catch (Exception e) {
          logicalRemoveException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Build routing for one of the physical tables directly (competing per-table lock)
      Future<?> regularBuild1Task = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(5);
          // This should compete for per-table lock with logical table operations
          _routingManager.buildRouting(physicalTable1);
        } catch (Exception e) {
          regularBuild1Exception.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 4: Build routing for another physical table (different competing per-table lock)
      Future<?> regularBuild2Task = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(10);
          // This should compete for different per-table lock with logical table operations
          _routingManager.buildRouting(physicalTable2);
        } catch (Exception e) {
          regularBuild2Exception.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 5: Include server to routing (global write lock - should block all read operations)
      Future<?> includeServerTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(15);
          // This should take global write lock and block all other operations
          _routingManager.includeServerToRouting("Server_localhost_8001");
        } catch (Exception e) {
          includeServerException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Start all threads simultaneously
      startLatch.countDown();

      // Wait for completion with timeout
      Assert.assertTrue(finishLatch.await(20, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (logicalBuildException.get() != null) {
        Assert.fail("Logical table build failed: " + logicalBuildException.get().getMessage());
      }
      if (logicalRemoveException.get() != null) {
        Assert.fail("Logical table remove failed: " + logicalRemoveException.get().getMessage());
      }
      if (regularBuild1Exception.get() != null) {
        Assert.fail("Regular table 1 build failed: " + regularBuild1Exception.get().getMessage());
      }
      if (regularBuild2Exception.get() != null) {
        Assert.fail("Regular table 2 build failed: " + regularBuild2Exception.get().getMessage());
      }
      if (includeServerException.get() != null) {
        Assert.fail("Include server failed: " + includeServerException.get().getMessage());
      }

      // Verify that routing can be built for all tables after operations
      _routingManager.buildRouting(physicalTable1);
      _routingManager.buildRouting(physicalTable2);
      _routingManager.buildRouting(physicalTable3);

      Assert.assertTrue(_routingManager.routingExists(physicalTable1),
          "Physical table 1 routing should be buildable after concurrent operations");
      Assert.assertTrue(_routingManager.routingExists(physicalTable2),
          "Physical table 2 routing should be buildable after concurrent operations");
      Assert.assertTrue(_routingManager.routingExists(physicalTable3),
          "Physical table 3 routing should be buildable after concurrent operations");

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // If the logical table build actually created routing for physical tables, verify TimeBoundaryManager
      Object offlineEntry = getRoutingEntry(physicalTable1);
      Assert.assertNotNull(offlineEntry, "Physical offline routing entry should exist");

      // Physical offline table shouldn't have TimeBoundaryManager due to logical table setup and then removal
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      Assert.assertNull(timeBoundaryManager, "Physical offline table shouldn't have TimeBoundaryManager if part of "
          + "a logical table since logical table was removed - this indicates a race condition in cross-table "
          + "coordination");
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /**
   * Test concurrent interactions with logical table containing multiple physical tables.
   * This validates coordination when logical table operations affect multiple per-table locks.
   */
  @Test
  public void testConcurrentMultiPhysicalTableLogicalOperationsWithRealtimeBuild() throws Exception {
    clearRoutingEntries();

    String logicalTableName = "testMultiLogicalTable";
    String physicalTable1 = "multiPhysical1_OFFLINE";
    String physicalTable2 = "multiPhysical2_OFFLINE";
    String physicalTable3 = "multiPhysical3_REALTIME";
    String physicalTable4 = "multiPhysical1_REALTIME";

    // Create logical table config with multiple physical tables
    LogicalTableConfig logicalTableConfig = createLogicalTableConfig(logicalTableName,
        Map.of(
            "multiPhysical1", new PhysicalTableConfig(),
            "multiPhysical2", new PhysicalTableConfig(),
            "multiPhysical3", new PhysicalTableConfig()
        ),
        physicalTable1, physicalTable4);
    ZKMetadataProvider.setLogicalTableConfig(_propertyStore, logicalTableConfig);

    // Create physical table configs and schemas
    for (String tableNameWithType : Arrays.asList(physicalTable1, physicalTable2, physicalTable3, physicalTable4)) {
      TableType tableType = TableNameBuilder.isOfflineTableResource(tableNameWithType)
          ? TableType.OFFLINE : TableType.REALTIME;
      TableConfig physicalTableConfig = createTableConfig(tableNameWithType, tableType);
      ZKMetadataProvider.setTableConfig(_propertyStore, physicalTableConfig);

      // Create schema with proper table name
      Schema schema = createMockSchema();
      schema.setSchemaName(TableNameBuilder.extractRawTableName(tableNameWithType));
      ZKMetadataProvider.setSchema(_propertyStore, schema);
      createIdealStateAndExternalView(tableNameWithType);
    }

    ExecutorService executor = Executors.newFixedThreadPool(6);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(6);

    AtomicReference<Exception> logicalBuildException = new AtomicReference<>();
    AtomicReference<Exception> logicalRemoveException = new AtomicReference<>();
    AtomicReference<Exception> regularBuild1Exception = new AtomicReference<>();
    AtomicReference<Exception> regularBuild2Exception = new AtomicReference<>();
    AtomicReference<Exception> includeServerException = new AtomicReference<>();
    AtomicReference<Exception> realtimeBuildException = new AtomicReference<>();

    try {
      // Thread 1: Build routing for logical table (global read lock + multiple per-table locks)
      Future<?> logicalBuildTask = executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global read lock + per-table locks for all 3 physical tables
          _routingManager.buildRoutingForLogicalTable(logicalTableName);
        } catch (Exception e) {
          logicalBuildException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 2: Remove routing for logical table (global read lock + multiple per-table locks)
      Future<?> logicalRemoveTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(25); // Delay to let build start first
          // This should take global read lock + per-table locks for all 3 physical tables
          _routingManager.removeRoutingForLogicalTable(logicalTableName);
        } catch (Exception e) {
          logicalRemoveException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Build routing for one of the physical tables directly (competing per-table lock)
      Future<?> regularBuild1Task = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(5);
          // This should compete for per-table lock with logical table operations
          _routingManager.buildRouting(physicalTable1);
        } catch (Exception e) {
          regularBuild1Exception.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 4: Build routing for another physical table (different competing per-table lock)
      Future<?> regularBuild2Task = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(10);
          // This should compete for different per-table lock with logical table operations
          _routingManager.buildRouting(physicalTable2);
        } catch (Exception e) {
          regularBuild2Exception.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 5: Include server to routing (global write lock - should block all read operations)
      Future<?> includeServerTask = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(15);
          // This should take global write lock and block all other operations
          _routingManager.includeServerToRouting("Server_localhost_8001");
        } catch (Exception e) {
          includeServerException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 6: Build routing for realtime physical table (competing per-table lock)
      Future<?> regularBuild4Task = executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(20);
          // This should compete for per-table lock with logical table operations
          _routingManager.buildRouting(physicalTable4);
        } catch (Exception e) {
          realtimeBuildException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Start all threads simultaneously
      startLatch.countDown();

      // Wait for completion with timeout
      Assert.assertTrue(finishLatch.await(20, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (logicalBuildException.get() != null) {
        Assert.fail("Logical table build failed: " + logicalBuildException.get().getMessage());
      }
      if (logicalRemoveException.get() != null) {
        Assert.fail("Logical table remove failed: " + logicalRemoveException.get().getMessage());
      }
      if (regularBuild1Exception.get() != null) {
        Assert.fail("Regular table 1 build failed: " + regularBuild1Exception.get().getMessage());
      }
      if (regularBuild2Exception.get() != null) {
        Assert.fail("Regular table 2 build failed: " + regularBuild2Exception.get().getMessage());
      }
      if (includeServerException.get() != null) {
        Assert.fail("Include server failed: " + includeServerException.get().getMessage());
      }
      if (realtimeBuildException.get() != null) {
        Assert.fail("Regular table 4 build failed: " + realtimeBuildException.get().getMessage());
      }

      // Verify that routing can be built for all tables after operations
      _routingManager.buildRouting(physicalTable1);
      _routingManager.buildRouting(physicalTable2);
      _routingManager.buildRouting(physicalTable3);

      Assert.assertTrue(_routingManager.routingExists(physicalTable1),
          "Physical table 1 routing should be buildable after concurrent operations");
      Assert.assertTrue(_routingManager.routingExists(physicalTable2),
          "Physical table 2 routing should be buildable after concurrent operations");
      Assert.assertTrue(_routingManager.routingExists(physicalTable3),
          "Physical table 3 routing should be buildable after concurrent operations");
      Assert.assertTrue(_routingManager.routingExists(physicalTable4),
          "Physical table 4 routing should be buildable after concurrent operations");

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // The logical table was built then removed, so check final state
      Object offlineEntry = getRoutingEntry(physicalTable1);
      Assert.assertNotNull(offlineEntry, "Physical offline routing entry should exist");

      // Since physicalTable4 (realtime) exists and they share the same raw table name (multiPhysical1),
      // the offline table should have a TimeBoundaryManager for hybrid table coordination
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      Assert.assertNotNull(timeBoundaryManager, "Physical offline table should have TimeBoundaryManager when "
          + "realtime table exists - this indicates proper hybrid table coordination");
    } finally {
      executor.shutdown();
      Assert.assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  private LogicalTableConfig createLogicalTableConfig(String logicalTableName,
      Map<String, PhysicalTableConfig> physicalTableConfigMap, String refOfflineTableName,
      String refRealtimeTableName) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("includedTables", List.of(refOfflineTableName));
    TimeBoundaryConfig timeBoundaryConfig = new TimeBoundaryConfig("min", parameters);

    return new LogicalTableConfigBuilder()
        .setTableName(logicalTableName)
        .setPhysicalTableConfigMap(physicalTableConfigMap)
        .setBrokerTenant("DefaultTenant")
        .setRefOfflineTableName(refOfflineTableName)
        .setRefRealtimeTableName(refRealtimeTableName)
        .setTimeBoundaryConfig(timeBoundaryConfig)
        .build();
  }
}
