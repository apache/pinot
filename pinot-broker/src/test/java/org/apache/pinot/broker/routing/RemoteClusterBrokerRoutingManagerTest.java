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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link RemoteClusterBrokerRoutingManager}.
 * Tests focus on lifecycle management, executor behavior, and routing change processing.
 */
public class RemoteClusterBrokerRoutingManagerTest {
  private static final String REMOTE_CLUSTER_NAME = "testRemoteCluster";
  private static final String PROCESS_CHANGE_FIELD_NAME = "_processChangeInRouting";
  private static final String ROUTING_EXECUTOR_FIELD_NAME = "_routingChangeExecutor";
  private static final int EXECUTOR_STARTUP_WAIT_MS = 100;

  @Mock
  private BrokerMetrics _brokerMetrics;

  @Mock
  private ServerRoutingStatsManager _serverRoutingStatsManager;

  @Mock
  private PinotConfiguration _pinotConfig;

  private RemoteClusterBrokerRoutingManager _routingManager;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Setup mock configuration
    when(_pinotConfig.getProperty(anyString(), anyString()))
        .thenAnswer(invocation -> invocation.getArgument(1));
    when(_pinotConfig.getProperty(anyString(), anyInt()))
        .thenAnswer(invocation -> invocation.getArgument(1));

    // Create the routing manager
    _routingManager = new RemoteClusterBrokerRoutingManager(
        REMOTE_CLUSTER_NAME, _brokerMetrics, _serverRoutingStatsManager, _pinotConfig);
  }

  @AfterMethod
  public void tearDown() {
    if (_routingManager != null) {
      _routingManager.shutdown();
    }
  }

  @Test
  public void testConstructor() {
    assertNotNull(_routingManager);
  }

  @Test
  public void testProcessSegmentAssignmentChangeSetsFlag() throws Exception {
    AtomicBoolean changeFlag = getProcessChangeFlag();
    assertFalse(changeFlag.get(), "Initial flag should be false");

    _routingManager.processSegmentAssignmentChangeInternal();

    assertTrue(changeFlag.get(), "Flag should be set to true after processing change");
  }

  @Test
  public void testShutdownStopsExecutor() throws Exception {
    _routingManager.shutdown();

    ScheduledExecutorService executor = getRoutingChangeExecutor();
    assertTrue(executor.isShutdown() || executor.isTerminated(),
        "Executor should be shutdown or terminated");
  }

  @Test
  public void testExecutorIsScheduledOnInitialization() throws Exception {
    RemoteClusterBrokerRoutingManager manager = new RemoteClusterBrokerRoutingManager(
        REMOTE_CLUSTER_NAME, _brokerMetrics, _serverRoutingStatsManager, _pinotConfig);

    try {
      ScheduledExecutorService executor = getRoutingChangeExecutor(manager);

      assertNotNull(executor, "Executor should be initialized");
      assertFalse(executor.isShutdown(), "Executor should not be shutdown initially");

      Thread.sleep(EXECUTOR_STARTUP_WAIT_MS);

      assertFalse(executor.isShutdown(), "Executor should still be running after startup");
    } finally {
      manager.shutdown();
    }
  }

  @Test
  public void testMultipleShutdownCallsAreSafe() {
    _routingManager.shutdown();
    _routingManager.shutdown();
    _routingManager.shutdown();
  }

  @Test
  public void testDetermineRoutingChangeWithoutFlagIsNoOp() {
    _routingManager.determineRoutingChangeForTables();
  }

  @Test
  public void testProcessSegmentAssignmentChangeIdempotent() throws Exception {
    AtomicBoolean changeFlag = getProcessChangeFlag();

    _routingManager.processSegmentAssignmentChangeInternal();
    _routingManager.processSegmentAssignmentChangeInternal();
    _routingManager.processSegmentAssignmentChangeInternal();

    assertTrue(changeFlag.get(), "Flag should remain true after multiple calls");
  }

  /**
   * Helper method to access the private process change flag using reflection.
   */
  private AtomicBoolean getProcessChangeFlag() throws Exception {
    return getProcessChangeFlag(_routingManager);
  }

  /**
   * Helper method to access the private process change flag for a specific manager.
   */
  private AtomicBoolean getProcessChangeFlag(RemoteClusterBrokerRoutingManager manager) throws Exception {
    Field field = RemoteClusterBrokerRoutingManager.class.getDeclaredField(PROCESS_CHANGE_FIELD_NAME);
    field.setAccessible(true);
    return (AtomicBoolean) field.get(manager);
  }

  /**
   * Helper method to access the private routing change executor using reflection.
   */
  private ScheduledExecutorService getRoutingChangeExecutor() throws Exception {
    return getRoutingChangeExecutor(_routingManager);
  }

  /**
   * Helper method to access the private routing change executor for a specific manager.
   */
  private ScheduledExecutorService getRoutingChangeExecutor(RemoteClusterBrokerRoutingManager manager)
      throws Exception {
    Field field = RemoteClusterBrokerRoutingManager.class.getDeclaredField(ROUTING_EXECUTOR_FIELD_NAME);
    field.setAccessible(true);
    return (ScheduledExecutorService) field.get(manager);
  }
}
