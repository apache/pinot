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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BiConsumer;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

/// Test class to validate concurrency and race condition handling in BrokerRoutingManager,
/// specifically focusing on TimeBoundaryManager coordination for hybrid tables.
///
/// This test uses a real ZooKeeper instance and PropertyStore for end-to-end validation.
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
  public void setUp()
      throws Exception {
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
            Mockito.eq(CommonConstants.Broker.CONFIG_OF_ROUTING_ASSIGNMENT_CHANGE_PROCESS_PARALLELISM), anyInt()))
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

    // Trigger instance config processing to populate _routableServerInstanceMap
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
    // This will populate _routableServerInstanceMap which is needed for buildRouting to work
    try {
      _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);
    } catch (Exception e) {
      fail("Direct call to processClusterChange failed", e);
    }
  }

  private void clearRoutingEntries() {
    // Clear existing routing entries to ensure test isolation
    try {
      // Access the inherited field from the parent class (BaseBrokerRoutingManager)
      Class<?> baseClass = _routingManager.getClass().getSuperclass();
      java.lang.reflect.Field routingEntryMapField = baseClass.getDeclaredField("_routingEntryMap");
      routingEntryMapField.setAccessible(true);
      Map<?, ?> routingEntryMap = (Map<?, ?>) routingEntryMapField.get(_routingManager);
      routingEntryMap.clear();
    } catch (Exception e) {
      fail("Failed to clear routing entries", e);
    }
  }

  private ReadWriteLock getGlobalLock() {
    try {
      // Access the private lock from the parent class (BaseBrokerRoutingManager)
      Field globalLockField = BaseBrokerRoutingManager.class.getDeclaredField("_globalLock");
      globalLockField.setAccessible(true);
      return (ReadWriteLock) globalLockField.get(_routingManager);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to access the global lock", e);
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
      assertNotNull(routingEntry, "Routing entry should exist for table: " + tableNameWithType);

      // Get the InstanceSelector from the routing entry
      java.lang.reflect.Field instanceSelectorField = routingEntry.getClass().getDeclaredField("_instanceSelector");
      instanceSelectorField.setAccessible(true);
      Object instanceSelector = instanceSelectorField.get(routingEntry);
      assertNotNull(instanceSelector, "InstanceSelector should exist");

      // Get the _enabledInstances field from BaseInstanceSelector
      java.lang.reflect.Field enabledInstancesField =
          instanceSelector.getClass().getSuperclass().getDeclaredField("_enabledInstances");
      enabledInstancesField.setAccessible(true);
      Object enabledInstancesObj = enabledInstancesField.get(instanceSelector);

      if (enabledInstancesObj != null) {
        Set<?> enabledInstances = (Set<?>) enabledInstancesObj;
        assertFalse(enabledInstances.contains(disabledInstance),
            "Disabled instance " + disabledInstance + " should NOT be in enabled instances for table "
                + tableNameWithType + ". Enabled instances: " + enabledInstances);
      }
    } catch (Exception e) {
      fail("Failed to validate disabled instance exclusion for table " + tableNameWithType + ": " + e.getMessage());
    }
  }

  private void validateEnabledInstanceInRouting(String tableNameWithType, String enabledInstance) {
    try {
      Object routingEntry = getRoutingEntry(tableNameWithType);
      assertNotNull(routingEntry, "Routing entry should exist for table: " + tableNameWithType);

      // Get the InstanceSelector from the routing entry
      java.lang.reflect.Field instanceSelectorField = routingEntry.getClass().getDeclaredField("_instanceSelector");
      instanceSelectorField.setAccessible(true);
      Object instanceSelector = instanceSelectorField.get(routingEntry);
      assertNotNull(instanceSelector, "InstanceSelector should exist");

      // Get the _enabledInstances field from BaseInstanceSelector
      java.lang.reflect.Field enabledInstancesField =
          instanceSelector.getClass().getSuperclass().getDeclaredField("_enabledInstances");
      enabledInstancesField.setAccessible(true);
      Object enabledInstancesObj = enabledInstancesField.get(instanceSelector);

      assertNotNull(enabledInstancesObj, "Enabled instances should not be null for table " + tableNameWithType);
      Set<?> enabledInstances = (Set<?>) enabledInstancesObj;
      assertTrue(enabledInstances.contains(enabledInstance),
          "Enabled instance " + enabledInstance + " should be in enabled instances for table " + tableNameWithType
              + ". Enabled instances: " + enabledInstances);
    } catch (Exception e) {
      fail("Failed to validate enabled instance inclusion for table " + tableNameWithType + ": " + e.getMessage());
    }
  }

  private void createSegmentMetadata(String tableNameWithType, String segmentName, long endTime) {
    SegmentZKMetadata segmentMetadata = new SegmentZKMetadata(segmentName);
    segmentMetadata.setEndTime(endTime);
    segmentMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentMetadata.setTotalDocs(1000);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, tableNameWithType, segmentMetadata);
  }

  /// Test that validates concurrent buildRouting operations for REALTIME and OFFLINE tables
  /// of the same hybrid table don't result in missing TimeBoundaryManager due to race conditions.
  ///
  /// This test uses real ZooKeeper and PropertyStore to validate the new locking mechanism.
  ///
  /// The test ensures:
  /// 1. Both tables can be built concurrently without corruption
  /// 2. TimeBoundaryManager is properly set on the offline table when realtime is built
  /// 3. No race conditions occur that would leave the offline table without a TimeBoundaryManager
  @Test
  public void testConcurrentHybridTableBuildNoTimeBoundaryManagerRace()
      throws Exception {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      assertTrue(finishLatch.await(30, TimeUnit.SECONDS), "Threads didn't complete in time");

      // Check if any thread failed
      if (offlineException.get() != null) {
        fail("Offline table build failed", offlineException.get());
      }
      if (realtimeException.get() != null) {
        fail("Realtime table build failed", realtimeException.get());
      }

      // Verify both tables exist
      assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME), "Offline table routing should exist");
      assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME), "Realtime table routing should exist");

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // The offline table should have a TimeBoundaryManager when realtime table exists
      Object offlineEntry = getRoutingEntry(OFFLINE_TABLE_NAME);
      assertNotNull(offlineEntry, "Offline routing entry should exist");

      // If realtime table was built, offline should have TimeBoundaryManager
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      assertNotNull(timeBoundaryManager, "Offline table should have TimeBoundaryManager when realtime table exists - "
          + "this indicates a race condition in cross-table coordination");
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /// Test that validates sequential table building works correctly and establishes TimeBoundaryManager.
  @Test
  public void testSequentialHybridTableBuildTimeBoundaryManagerCreation() {
    // Clean any existing routing entries to ensure test isolation
    clearRoutingEntries();

    // Step 1: Build OFFLINE table first - should not have TimeBoundaryManager
    _routingManager.buildRouting(OFFLINE_TABLE_NAME);
    assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME), "Offline table routing should exist");

    Object offlineEntry = getRoutingEntry(OFFLINE_TABLE_NAME);
    TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
    assertNull(timeBoundaryManager, "Offline table should not have TimeBoundaryManager when realtime doesn't exist");

    // Step 2: Build REALTIME table - should add TimeBoundaryManager to existing offline table
    _routingManager.buildRouting(REALTIME_TABLE_NAME);
    assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME));

    // Verify TimeBoundaryManager was added to offline table
    offlineEntry = getRoutingEntry(OFFLINE_TABLE_NAME);
    timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
    assertNotNull(timeBoundaryManager, "Offline table should have TimeBoundaryManager after realtime is built");
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

    // Access the field from the parent class (BaseBrokerRoutingManager)
    Class<?> baseClass = manager.getClass().getSuperclass();
    Field startTimesField = baseClass.getDeclaredField("_routingTableBuildStartTimeMs");
    startTimesField.setAccessible(true);
    //noinspection unchecked
    Map<String, Long> startTimes = (Map<String, Long>) startTimesField.get(manager);
    if (startTimes == null) {
      startTimes = new ConcurrentHashMap<>();
      startTimesField.set(manager, startTimes);
    }
    startTimes.put(tableNameWithType, futureStart);

    // Should return without throwing and without attempting to build routing
    manager.buildRouting(tableNameWithType);

    // Ensure routing was not created and the last start time was not overwritten
    assertFalse(manager.routingExists(tableNameWithType));
    assertEquals(startTimes.get(tableNameWithType).longValue(), futureStart);
  }

  /// Test concurrent interactions between processSegmentAssignmentChange and buildRouting.
  /// This validates that the global read lock (for processSegmentAssignmentChange) and
  /// per-table locks (for buildRouting) work correctly together without deadlocks.
  @Test
  public void testConcurrentProcessSegmentAssignmentChangeAndBuildRouting()
      throws Exception {
    clearRoutingEntries();

    // First, build initial routing entries for both tables
    _routingManager.buildRouting(OFFLINE_TABLE_NAME);
    _routingManager.buildRouting(REALTIME_TABLE_NAME);

    assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME), "Initial offline routing should exist");
    assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME), "Initial realtime routing should exist");
    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(3);

    AtomicReference<Exception> segmentAssignmentException = new AtomicReference<>();
    AtomicReference<Exception> buildRoutingOfflineException = new AtomicReference<>();
    AtomicReference<Exception> buildRoutingRealtimeException = new AtomicReference<>();

    try {
      // Thread 1: Process segment assignment change (takes global read lock, per-raw-table-name lock for each table one
      // at a time)
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      assertTrue(finishLatch.await(10, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (segmentAssignmentException.get() != null) {
        fail("Segment assignment change failed: " + segmentAssignmentException.get().getMessage());
      }
      if (buildRoutingOfflineException.get() != null) {
        fail("Offline table build failed: " + buildRoutingOfflineException.get().getMessage());
      }
      if (buildRoutingRealtimeException.get() != null) {
        fail("Realtime table build failed: " + buildRoutingRealtimeException.get().getMessage());
      }

      // Verify routing entries still exist after concurrent operations
      assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME),
          "Offline routing should exist after concurrent operations");
      assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME),
          "Realtime routing should exist after concurrent operations");

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // The offline table should have a TimeBoundaryManager when realtime table exists
      Object offlineEntry = getRoutingEntry(OFFLINE_TABLE_NAME);
      assertNotNull(offlineEntry, "Offline routing entry should exist");

      // If realtime table was built, offline should have TimeBoundaryManager
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      assertNotNull(timeBoundaryManager, "Offline table should have TimeBoundaryManager when realtime table exists - "
          + "this indicates a race condition in cross-table coordination");
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /// Test concurrent interactions between processInstanceConfigChange and buildRouting where buildRouting adds a new
  /// table.
  /// This validates that the global write lock (for processInstanceConfigChange) and
  /// per-table locks (for buildRouting) work correctly together, especially when buildRouting creates new routing
  /// entries.
  @Test
  public void testConcurrentProcessInstanceConfigChangeAndBuildRoutingNewTable()
      throws Exception {
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
    createIdealStateAndExternalViewWithMultipleServers(newOfflineTable, enabledServerInstance, disabledServerInstance);
    createIdealStateAndExternalViewWithMultipleServers(newRealtimeTable, enabledServerInstance, disabledServerInstance);

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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      assertTrue(finishLatch.await(15, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (instanceConfigException.get() != null) {
        fail("Instance config change failed: " + instanceConfigException.get().getMessage());
      }
      if (buildNewOfflineException.get() != null) {
        fail("New offline table build failed: " + buildNewOfflineException.get().getMessage());
      }
      if (buildNewRealtimeException.get() != null) {
        fail("New realtime table build failed: " + buildNewRealtimeException.get().getMessage());
      }

      // Verify new routing entries were created successfully
      assertTrue(_routingManager.routingExists(newOfflineTable),
          "New offline routing should exist after concurrent operations");
      assertTrue(_routingManager.routingExists(newRealtimeTable),
          "New realtime routing should exist after concurrent operations");

      // Verify TimeBoundaryManager coordination for the new hybrid table
      Object newOfflineEntry = getRoutingEntry(newOfflineTable);
      assertNotNull(newOfflineEntry, "New offline routing entry should exist");
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(newOfflineEntry);
      assertNotNull(timeBoundaryManager, "New offline table should have TimeBoundaryManager when realtime exists");

      // CRITICAL: Verify that the disabled instance is NOT included in the routing entries
      validateDisabledInstanceNotInRouting(newOfflineTable, disabledServerInstance);
      validateDisabledInstanceNotInRouting(newRealtimeTable, disabledServerInstance);
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  private TableConfig createTableConfig(String tableNameWithType, TableType tableType) {
    return new TableConfigBuilder(tableType).setTableName(TableNameBuilder.extractRawTableName(tableNameWithType))
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
      // Use reflection to access the private _routingEntryMap from the parent class (BaseBrokerRoutingManager)
      Class<?> baseClass = _routingManager.getClass().getSuperclass();
      java.lang.reflect.Field field = baseClass.getDeclaredField("_routingEntryMap");
      field.setAccessible(true);
      Map<?, ?> routingEntryMap = (Map<?, ?>) field.get(_routingManager);
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

  /// Test concurrent interactions between excludeServerFromRouting (global write lock) and buildRouting
  /// (global read lock + per-table lock).
  /// This validates that global write lock properly blocks global read lock operations.
  @Test
  public void testConcurrentExcludeServerAndBuildRouting()
      throws Exception {
    clearRoutingEntries();

    String disabledServerInstance = "Server_localhost_8001"; // We'll disable this one

    // First, build initial routing entries for both tables
    _routingManager.buildRouting(OFFLINE_TABLE_NAME);
    _routingManager.buildRouting(REALTIME_TABLE_NAME);

    assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME), "Initial offline routing should exist");
    assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME), "Initial realtime routing should exist");

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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      assertTrue(finishLatch.await(10, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (excludeServerException.get() != null) {
        fail("Exclude server failed: " + excludeServerException.get().getMessage());
      }
      if (buildOfflineException.get() != null) {
        fail("Build offline routing failed: " + buildOfflineException.get().getMessage());
      }
      if (buildRealtimeException.get() != null) {
        fail("Build realtime routing failed: " + buildRealtimeException.get().getMessage());
      }

      // Verify routing entries still exist after operations
      assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME),
          "Offline routing should exist after exclude server operation");
      assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME),
          "Realtime routing should exist after exclude server operation");

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // The offline table should have a TimeBoundaryManager when realtime table exists
      Object offlineEntry = getRoutingEntry(OFFLINE_TABLE_NAME);
      assertNotNull(offlineEntry, "Offline routing entry should exist");

      // If realtime table was built, offline should have TimeBoundaryManager
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      assertNotNull(timeBoundaryManager, "Offline table should have TimeBoundaryManager when realtime table exists - "
          + "this indicates a race condition in cross-table coordination");

      // CRITICAL: Verify that the disabled instance is NOT included in the routing entries
      validateDisabledInstanceNotInRouting(OFFLINE_TABLE_NAME, disabledServerInstance);
      validateDisabledInstanceNotInRouting(REALTIME_TABLE_NAME, disabledServerInstance);
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /// Test concurrent interactions between includeServerToRouting (global write lock) and refreshSegment
  /// (global read lock + per-table lock).
  /// This validates proper coordination between global write operations and segment refresh operations.
  @Test
  public void testConcurrentIncludeServerAndRefreshSegment()
      throws Exception {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      assertTrue(finishLatch.await(10, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (includeServerException.get() != null) {
        fail("Include server failed: " + includeServerException.get().getMessage());
      }
      if (refreshOfflineException.get() != null) {
        fail("Refresh offline segment failed: " + refreshOfflineException.get().getMessage());
      }
      if (refreshRealtimeException.get() != null) {
        fail("Refresh realtime segment failed: " + refreshRealtimeException.get().getMessage());
      }

      // CRITICAL: Verify that the included instance IS now included in the routing entries
      validateEnabledInstanceInRouting(OFFLINE_TABLE_NAME, includedServerInstance);
      validateEnabledInstanceInRouting(REALTIME_TABLE_NAME, includedServerInstance);
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /// Test concurrent query operations (getRoutingTable, getTimeBoundaryInfo, getQueryTimeoutMs) during routing
  /// modifications. This validates that query path operations can execute concurrently and are not blocked by
  /// routing modifications.
  @Test
  public void testConcurrentQueryOperationsDuringRoutingModifications()
      throws Exception {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      assertTrue(finishLatch.await(15, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (buildRoutingException.get() != null) {
        fail("Build routing failed: " + buildRoutingException.get().getMessage());
      }
      if (refreshSegmentException.get() != null) {
        fail("Refresh segment failed: " + refreshSegmentException.get().getMessage());
      }
      if (getRoutingTableException.get() != null) {
        fail("Get routing table failed: " + getRoutingTableException.get().getMessage());
      }
      if (getTimeBoundaryException.get() != null) {
        fail("Get time boundary failed: " + getTimeBoundaryException.get().getMessage());
      }
      if (getQueryTimeoutException.get() != null) {
        fail("Get query timeout failed: " + getQueryTimeoutException.get().getMessage());
      }
      if (removeRoutingException.get() != null) {
        fail("Remove routing failed: " + removeRoutingException.get().getMessage());
      }

      // Verify offline routing still exists but realtime was removed
      assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME),
          "Offline routing should still exist after concurrent operations");
      assertFalse(_routingManager.routingExists(REALTIME_TABLE_NAME),
          "Realtime routing should not exist after concurrent operations");

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // The offline table should have a TimeBoundaryManager when realtime table exists
      Object offlineEntry = getRoutingEntry(OFFLINE_TABLE_NAME);
      assertNotNull(offlineEntry, "Offline routing entry should exist");

      // If realtime table wasn't built, offline shouldn't have TimeBoundaryManager
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      assertNull(timeBoundaryManager, "Offline table shouldn't have TimeBoundaryManager when realtime table doesn't "
          + "exist - this indicates a race condition in cross-table coordination");
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /// Test that the global write lock methods (processInstanceConfigChange, includeServerToRouting and
  /// excludeServerFromRouting) are serialized by the global write lock and don't deadlock.
  ///
  /// The test holds the global write lock while invoking all three methods concurrently: none of them may complete
  /// while the lock is held, and all of them must complete once it is released. This is deterministic, unlike
  /// observing invocation order from the caller side, which cannot tell whether the callee has actually entered its
  /// critical section.
  @Test
  public void testConcurrentGlobalWriteLockMethods()
      throws Exception {
    clearRoutingEntries();

    // Build initial routing entries
    _routingManager.buildRouting(OFFLINE_TABLE_NAME);
    _routingManager.buildRouting(REALTIME_TABLE_NAME);

    // First exclude a server so we can include it later
    _routingManager.excludeServerFromRouting("Server_localhost_8000");

    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch startedLatch = new CountDownLatch(3);
    CountDownLatch finishLatch = new CountDownLatch(3);
    List<String> completedOperations = Collections.synchronizedList(new ArrayList<>());
    Map<String, Exception> operationExceptions = new ConcurrentHashMap<>();
    BiConsumer<String, Runnable> submitOperation = (operationName, operation) -> executor.submit(() -> {
      try {
        startedLatch.countDown();
        operation.run();
        completedOperations.add(operationName);
      } catch (Exception e) {
        operationExceptions.put(operationName, e);
      } finally {
        finishLatch.countDown();
      }
    });

    ReadWriteLock globalLock = getGlobalLock();
    try {
      // Hold the global write lock so that each method blocks right after being invoked
      globalLock.writeLock().lock();
      try {
        submitOperation.accept("processInstanceConfigChange",
            () -> _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG));
        submitOperation.accept("includeServerToRouting",
            () -> _routingManager.includeServerToRouting("Server_localhost_8000"));
        submitOperation.accept("excludeServerFromRouting",
            () -> _routingManager.excludeServerFromRouting("Server_localhost_8001"));

        // Wait until all three operations have been invoked, then verify that none of them completes while the
        // global write lock is held
        assertTrue(startedLatch.await(15, TimeUnit.SECONDS), "All operations should have been invoked");
        assertFalse(finishLatch.await(500, TimeUnit.MILLISECONDS),
            "No operation should complete while the global write lock is held");
        assertTrue(completedOperations.isEmpty(),
            "No operation should complete while the global write lock is held, completed: " + completedOperations);
      } finally {
        globalLock.writeLock().unlock();
      }

      // Once the lock is released, all operations must complete without deadlock
      assertTrue(finishLatch.await(15, TimeUnit.SECONDS),
          "All operations should complete after the global write lock is released");
      assertTrue(operationExceptions.isEmpty(), "Operations failed: " + operationExceptions);
      assertEquals(completedOperations.size(), 3, "All operations should have completed");

      // Verify routing entries still exist and are properly updated
      assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME),
          "Offline routing should exist after concurrent global write operations");
      assertTrue(_routingManager.routingExists(REALTIME_TABLE_NAME),
          "Realtime routing should exist after concurrent global write operations");
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /// Test concurrent interactions between buildRoutingForLogicalTable and buildRouting.
  /// This validates proper coordination between logical table operations, regular table operations,
  /// and global write operations. Uses a hybrid logical table configuration with both offline and realtime tables.
  @Test
  public void testConcurrentLogicalTableBuildAndRegularBuild()
      throws Exception {
    clearRoutingEntries();

    String logicalTableName = "testLogicalTable";
    String physicalOfflineTable = "testLogicalTable_OFFLINE";
    String physicalRealtimeTable = "testLogicalTable_REALTIME";

    // Create hybrid logical table config with both offline and realtime tables
    LogicalTableConfig logicalTableConfig =
        createLogicalTableConfig(logicalTableName, Map.of("testLogicalTable", new PhysicalTableConfig()),
            physicalOfflineTable, physicalRealtimeTable);
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      assertTrue(finishLatch.await(15, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (logicalBuildException.get() != null) {
        fail("Logical table build failed: " + logicalBuildException.get().getMessage());
      }
      if (regularBuildException.get() != null) {
        fail("Regular table build failed: " + regularBuildException.get().getMessage());
      }
      if (refreshOfflineException.get() != null) {
        fail("Refresh offline segment failed: " + refreshOfflineException.get().getMessage());
      }
      if (refreshRealtimeException.get() != null) {
        fail("Refresh realtime segment failed: " + refreshRealtimeException.get().getMessage());
      }
      if (excludeServerException.get() != null) {
        fail("Exclude server failed: " + excludeServerException.get().getMessage());
      }

      // Verify routing entries exist for regular table
      assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME),
          "Regular table routing should exist after concurrent operations");

      // Verify that the excluded server is not in routing for any existing tables
      validateDisabledInstanceNotInRouting(OFFLINE_TABLE_NAME, "Server_localhost_8001");

      // Verify routing entries exist for regular table
      assertTrue(_routingManager.routingExists(physicalOfflineTable),
          "Regular table routing should exist after concurrent operations");

      // Verify that the excluded server is not in routing for any existing tables
      validateDisabledInstanceNotInRouting(physicalOfflineTable, "Server_localhost_8001");

      // Note: We don't check physical table routing existence as buildRoutingForLogicalTable only
      // creates routing for offline tables in the time boundary configuration, and only if they
      // don't already exist. The test validates concurrent lock coordination rather than routing creation.

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // If the logical table build actually created routing for physical tables, verify TimeBoundaryManager
      Object offlineEntry = getRoutingEntry(physicalOfflineTable);
      assertNotNull(offlineEntry, "Physical offline routing entry should exist");

      // Physical offline table should have TimeBoundaryManager due to logical table setup
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      assertNotNull(timeBoundaryManager, "Physical offline table should have TimeBoundaryManager if part of a logical "
          + "table - this indicates a race condition in cross-table coordination");

      assertFalse(_routingManager.routingExists(physicalRealtimeTable),
          "Physical realtime routing entry should not exist since we never built routing entry for it");
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /// Test concurrent interactions between buildRoutingForLogicalTable and buildRouting.
  /// This validates proper coordination between logical table operations, regular table operations,
  /// and global write operations. Uses a hybrid logical table configuration with both offline and realtime tables.
  @Test
  public void testConcurrentLogicalTableBuildAndRegularBuildAndRealtimeBuild()
      throws Exception {
    clearRoutingEntries();

    String logicalTableName = "testLogicalTable";
    String physicalOfflineTable = "testLogicalTable_OFFLINE";
    String physicalRealtimeTable = "testLogicalTable_REALTIME";

    // Create hybrid logical table config with both offline and realtime tables
    LogicalTableConfig logicalTableConfig =
        createLogicalTableConfig(logicalTableName, Map.of("testLogicalTable", new PhysicalTableConfig()),
            physicalOfflineTable, physicalRealtimeTable);
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      assertTrue(finishLatch.await(15, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (logicalBuildException.get() != null) {
        fail("Logical table build failed: " + logicalBuildException.get().getMessage());
      }
      if (regularBuildException.get() != null) {
        fail("Regular table build failed: " + regularBuildException.get().getMessage());
      }
      if (refreshOfflineException.get() != null) {
        fail("Refresh offline segment failed: " + refreshOfflineException.get().getMessage());
      }
      if (refreshRealtimeException.get() != null) {
        fail("Refresh realtime segment failed: " + refreshRealtimeException.get().getMessage());
      }
      if (excludeServerException.get() != null) {
        fail("Exclude server failed: " + excludeServerException.get().getMessage());
      }
      if (realtimeBuildException.get() != null) {
        fail("Realtime table build failed: " + realtimeBuildException.get().getMessage());
      }

      // Verify routing entries exist for regular table
      assertTrue(_routingManager.routingExists(OFFLINE_TABLE_NAME),
          "Regular table routing should exist after concurrent operations");

      // Verify that the excluded server is not in routing for any existing tables
      validateDisabledInstanceNotInRouting(OFFLINE_TABLE_NAME, "Server_localhost_8001");

      // Verify routing entries exist for regular table
      assertTrue(_routingManager.routingExists(physicalOfflineTable),
          "Regular table routing should exist after concurrent operations");

      // Verify that the excluded server is not in routing for any existing tables
      validateDisabledInstanceNotInRouting(physicalOfflineTable, "Server_localhost_8001");

      // Note: We don't check physical table routing existence as buildRoutingForLogicalTable only
      // creates routing for offline tables in the time boundary configuration, and only if they
      // don't already exist. The test validates concurrent lock coordination rather than routing creation.

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination
      // If the logical table build actually created routing for physical tables, verify TimeBoundaryManager
      Object offlineEntry = getRoutingEntry(physicalOfflineTable);
      assertNotNull(offlineEntry, "Physical offline routing entry should exist");

      // Physical offline table should have TimeBoundaryManager due to logical table setup
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      assertNotNull(timeBoundaryManager, "Physical offline table should have TimeBoundaryManager if part of a logical "
          + "table - this indicates a race condition in cross-table coordination");

      assertTrue(_routingManager.routingExists(physicalRealtimeTable),
          "Physical realtime routing entry should exist since we built routing entry for it");
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /// Test concurrent interactions with logical table containing multiple physical tables.
  /// This validates coordination when logical table operations affect multiple per-table locks.
  @Test
  public void testConcurrentMultiPhysicalTableLogicalOperations()
      throws Exception {
    clearRoutingEntries();

    String logicalTableName = "testMultiLogicalTable";
    String physicalTable1 = "multiPhysical1_OFFLINE";
    String physicalTable2 = "multiPhysical2_OFFLINE";
    String physicalTable3 = "multiPhysical3_REALTIME";

    // Create logical table config with multiple physical tables
    LogicalTableConfig logicalTableConfig = createLogicalTableConfig(logicalTableName,
        Map.of("multiPhysical1", new PhysicalTableConfig(), "multiPhysical2", new PhysicalTableConfig(),
            "multiPhysical3", new PhysicalTableConfig()), physicalTable1, physicalTable3);
    ZKMetadataProvider.setLogicalTableConfig(_propertyStore, logicalTableConfig);

    // Create physical table configs and schemas
    for (String tableNameWithType : Arrays.asList(physicalTable1, physicalTable2, physicalTable3)) {
      TableType tableType =
          TableNameBuilder.isOfflineTableResource(tableNameWithType) ? TableType.OFFLINE : TableType.REALTIME;
      TableConfig physicalTableConfig = createTableConfig(tableNameWithType, tableType);
      ZKMetadataProvider.setTableConfig(_propertyStore, physicalTableConfig);
      ZKMetadataProvider.setSchema(_propertyStore, createMockSchema());
      createIdealStateAndExternalView(tableNameWithType);
    }

    ExecutorService executor = Executors.newFixedThreadPool(5);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(5);
    // Released once the logical table build finishes, so that the logical table remove runs after it
    CountDownLatch logicalBuildDoneLatch = new CountDownLatch(1);

    AtomicReference<Exception> logicalBuildException = new AtomicReference<>();
    AtomicReference<Exception> logicalRemoveException = new AtomicReference<>();
    AtomicReference<Exception> regularBuild1Exception = new AtomicReference<>();
    AtomicReference<Exception> regularBuild2Exception = new AtomicReference<>();
    AtomicReference<Exception> includeServerException = new AtomicReference<>();

    try {
      // Thread 1: Build routing for logical table (global read lock + multiple per-table locks)
      executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global read lock + per-table locks for all 3 physical tables
          _routingManager.buildRoutingForLogicalTable(logicalTableName);
        } catch (Exception e) {
          logicalBuildException.set(e);
        } finally {
          logicalBuildDoneLatch.countDown();
          finishLatch.countDown();
        }
      });

      // Thread 2: Remove routing for logical table (global read lock + multiple per-table locks)
      executor.submit(() -> {
        try {
          startLatch.await();
          // Wait for the logical table build to finish so that the remove detaches the time boundary manager the
          // build attached; without this ordering the final time boundary manager state would depend on scheduling
          if (!logicalBuildDoneLatch.await(15, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting for the logical table build to finish");
          }
          // This should take global read lock + per-table locks for all 3 physical tables
          _routingManager.removeRoutingForLogicalTable(logicalTableName);
        } catch (Exception e) {
          logicalRemoveException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Build routing for one of the physical tables directly (competing per-table lock)
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      assertTrue(finishLatch.await(20, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (logicalBuildException.get() != null) {
        fail("Logical table build failed: " + logicalBuildException.get().getMessage());
      }
      if (logicalRemoveException.get() != null) {
        fail("Logical table remove failed: " + logicalRemoveException.get().getMessage());
      }
      if (regularBuild1Exception.get() != null) {
        fail("Regular table 1 build failed: " + regularBuild1Exception.get().getMessage());
      }
      if (regularBuild2Exception.get() != null) {
        fail("Regular table 2 build failed: " + regularBuild2Exception.get().getMessage());
      }
      if (includeServerException.get() != null) {
        fail("Include server failed: " + includeServerException.get().getMessage());
      }

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination before rebuilding any routing, because a
      // rebuild recomputes the time boundary manager and would mask what the concurrent phase produced. The logical
      // table build attached the manager and the logical table remove (which runs after the build) detached it;
      // rebuilding the physical table directly cannot re-attach it because no realtime counterpart exists.
      Object offlineEntry = getRoutingEntry(physicalTable1);
      assertNotNull(offlineEntry, "Physical offline routing entry should exist");
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      assertNull(timeBoundaryManager, "Physical offline table shouldn't have TimeBoundaryManager if part of a logical "
          + "table since logical table was removed - this indicates a race condition in cross-table coordination");

      // Verify that routing can be built for all tables after operations
      _routingManager.buildRouting(physicalTable1);
      _routingManager.buildRouting(physicalTable2);
      _routingManager.buildRouting(physicalTable3);

      assertTrue(_routingManager.routingExists(physicalTable1),
          "Physical table 1 routing should be buildable after concurrent operations");
      assertTrue(_routingManager.routingExists(physicalTable2),
          "Physical table 2 routing should be buildable after concurrent operations");
      assertTrue(_routingManager.routingExists(physicalTable3),
          "Physical table 3 routing should be buildable after concurrent operations");
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  /// Test concurrent interactions with logical table containing multiple physical tables.
  /// This validates coordination when logical table operations affect multiple per-table locks.
  @Test
  public void testConcurrentMultiPhysicalTableLogicalOperationsWithRealtimeBuild()
      throws Exception {
    clearRoutingEntries();

    String logicalTableName = "testMultiLogicalTable";
    String physicalTable1 = "multiPhysical1_OFFLINE";
    String physicalTable2 = "multiPhysical2_OFFLINE";
    String physicalTable3 = "multiPhysical3_REALTIME";
    String physicalTable4 = "multiPhysical1_REALTIME";

    // Create logical table config with multiple physical tables
    LogicalTableConfig logicalTableConfig = createLogicalTableConfig(logicalTableName,
        Map.of("multiPhysical1", new PhysicalTableConfig(), "multiPhysical2", new PhysicalTableConfig(),
            "multiPhysical3", new PhysicalTableConfig()), physicalTable1, physicalTable4);
    ZKMetadataProvider.setLogicalTableConfig(_propertyStore, logicalTableConfig);

    // Create physical table configs and schemas
    for (String tableNameWithType : Arrays.asList(physicalTable1, physicalTable2, physicalTable3, physicalTable4)) {
      TableType tableType =
          TableNameBuilder.isOfflineTableResource(tableNameWithType) ? TableType.OFFLINE : TableType.REALTIME;
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

    // Released once the logical table build finishes, so that the logical table remove runs after it
    CountDownLatch logicalBuildDoneLatch = new CountDownLatch(1);

    AtomicReference<Exception> logicalBuildException = new AtomicReference<>();
    AtomicReference<Exception> logicalRemoveException = new AtomicReference<>();
    AtomicReference<Exception> regularBuild1Exception = new AtomicReference<>();
    AtomicReference<Exception> regularBuild2Exception = new AtomicReference<>();
    AtomicReference<Exception> includeServerException = new AtomicReference<>();
    AtomicReference<Exception> realtimeBuildException = new AtomicReference<>();

    try {
      // Thread 1: Build routing for logical table (global read lock + multiple per-table locks)
      executor.submit(() -> {
        try {
          startLatch.await();
          // This should take global read lock + per-table locks for all 3 physical tables
          _routingManager.buildRoutingForLogicalTable(logicalTableName);
        } catch (Exception e) {
          logicalBuildException.set(e);
        } finally {
          logicalBuildDoneLatch.countDown();
          finishLatch.countDown();
        }
      });

      // Thread 2: Remove routing for logical table (global read lock + multiple per-table locks)
      executor.submit(() -> {
        try {
          startLatch.await();
          // Wait for the logical table build to finish so that the remove runs after the build, matching the
          // built-then-removed scenario under test
          if (!logicalBuildDoneLatch.await(15, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting for the logical table build to finish");
          }
          // This should take global read lock + per-table locks for all 3 physical tables
          _routingManager.removeRoutingForLogicalTable(logicalTableName);
        } catch (Exception e) {
          logicalRemoveException.set(e);
        } finally {
          finishLatch.countDown();
        }
      });

      // Thread 3: Build routing for one of the physical tables directly (competing per-table lock)
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      executor.submit(() -> {
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
      assertTrue(finishLatch.await(20, TimeUnit.SECONDS), "All tasks should complete within timeout");

      // Verify no exceptions occurred
      if (logicalBuildException.get() != null) {
        fail("Logical table build failed: " + logicalBuildException.get().getMessage());
      }
      if (logicalRemoveException.get() != null) {
        fail("Logical table remove failed: " + logicalRemoveException.get().getMessage());
      }
      if (regularBuild1Exception.get() != null) {
        fail("Regular table 1 build failed: " + regularBuild1Exception.get().getMessage());
      }
      if (regularBuild2Exception.get() != null) {
        fail("Regular table 2 build failed: " + regularBuild2Exception.get().getMessage());
      }
      if (includeServerException.get() != null) {
        fail("Include server failed: " + includeServerException.get().getMessage());
      }
      if (realtimeBuildException.get() != null) {
        fail("Regular table 4 build failed: " + realtimeBuildException.get().getMessage());
      }

      // CRITICAL VERIFICATION: Check TimeBoundaryManager coordination before rebuilding any routing, because a
      // rebuild recomputes the time boundary manager and would mask what the concurrent phase produced. Since
      // physicalTable4 (realtime) routing exists and shares the raw table name (multiPhysical1) with physicalTable1,
      // the offline table must end up with a time boundary manager regardless of how the operations interleave:
      // building the realtime routing attaches it to the offline counterpart, rebuilding the offline routing attaches
      // it while the realtime routing exists, and the logical table remove skips detaching it for hybrid physical
      // tables.
      Object offlineEntry = getRoutingEntry(physicalTable1);
      assertNotNull(offlineEntry, "Physical offline routing entry should exist");
      TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
      assertNotNull(timeBoundaryManager, "Physical offline table should have TimeBoundaryManager when realtime table "
          + "exists - this indicates proper hybrid table coordination");

      // Verify that routing can be built for all tables after operations
      _routingManager.buildRouting(physicalTable1);
      _routingManager.buildRouting(physicalTable2);
      _routingManager.buildRouting(physicalTable3);

      assertTrue(_routingManager.routingExists(physicalTable1),
          "Physical table 1 routing should be buildable after concurrent operations");
      assertTrue(_routingManager.routingExists(physicalTable2),
          "Physical table 2 routing should be buildable after concurrent operations");
      assertTrue(_routingManager.routingExists(physicalTable3),
          "Physical table 3 routing should be buildable after concurrent operations");
      assertTrue(_routingManager.routingExists(physicalTable4),
          "Physical table 4 routing should be buildable after concurrent operations");
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS), "Executor didn't shutdown in time");
    }
  }

  private LogicalTableConfig createLogicalTableConfig(String logicalTableName,
      Map<String, PhysicalTableConfig> physicalTableConfigMap, String refOfflineTableName,
      String refRealtimeTableName) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("includedTables", List.of(refOfflineTableName));
    TimeBoundaryConfig timeBoundaryConfig = new TimeBoundaryConfig("min", parameters);

    return new LogicalTableConfigBuilder().setTableName(logicalTableName)
        .setPhysicalTableConfigMap(physicalTableConfigMap)
        .setBrokerTenant("DefaultTenant")
        .setRefOfflineTableName(refOfflineTableName)
        .setRefRealtimeTableName(refRealtimeTableName)
        .setTimeBoundaryConfig(timeBoundaryConfig)
        .build();
  }
}
