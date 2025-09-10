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
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
    String serverInstanceId = "Server_localhost_8000";
    String clusterName = getHelixClusterName();
    
    // Add server instance to Helix cluster
    if (!_helixAdmin.getInstancesInCluster(clusterName).contains(serverInstanceId)) {
      // Create InstanceConfig for the server
      InstanceConfig instanceConfig = new InstanceConfig(serverInstanceId);
      instanceConfig.setHostName("localhost");
      instanceConfig.setPort("8000");
      instanceConfig.setInstanceEnabled(true);
      
      _helixAdmin.addInstance(clusterName, instanceConfig);
      
      // Mark the instance as live (simulate server joining)
      _helixAdmin.enableInstance(clusterName, serverInstanceId, true);
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
      java.lang.reflect.Field routingEntryMapField = 
          BrokerRoutingManager.class.getDeclaredField("_routingEntryMap");
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
      if (_routingManager.routingExists(REALTIME_TABLE_NAME)) {
        TimeBoundaryManager timeBoundaryManager = getTimeBoundaryManager(offlineEntry);
        Assert.assertNotNull(timeBoundaryManager, "Offline table should have TimeBoundaryManager when realtime table "
            + "exists - this indicates a race condition in cross-table coordination");
      }
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
}
