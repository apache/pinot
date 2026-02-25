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
package org.apache.pinot.server.starter.helix;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.pinot.core.data.manager.SegmentOperationsTaskContext;
import org.apache.pinot.core.data.manager.SegmentOperationsTaskType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.executor.DecoratorExecutorService;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class DefaultStateTransitionThreadPoolManagerTest {

  @Test
  public void testOnHelixManagerConnectedWithPinotConfig() {
    // Test that Pinot config takes precedence over Helix config
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE, "50");

    HelixManager mockHelixManager = createMockHelixManager("testCluster", "testInstance", true);
    ConfigAccessor mockConfigAccessor = mockHelixManager.getConfigAccessor();
    // Mock Helix config to return different value (should be ignored)
    when(mockConfigAccessor.get(any(HelixConfigScope.class), anyString())).thenReturn("80");

    DefaultStateTransitionThreadPoolManager manager =
        new DefaultStateTransitionThreadPoolManager(config, mockHelixManager);

    // Before connection - executor is available with default size
    ExecutorService preConnectExecutor = manager.getExecutorService("testResource");
    assertNotNull(preConnectExecutor, "Executor should be available immediately after construction");
    ThreadPoolExecutor preConnectPool = getUnderlyingThreadPoolExecutor(preConnectExecutor);
    assertEquals(preConnectPool.getCorePoolSize(), CommonConstants.Server.DEFAULT_STATE_TRANSITION_THREAD_POOL_SIZE,
        "Pool should start with default size before connection");

    // After connection - pool size should be adjusted to configured value
    manager.onHelixManagerConnected();
    ExecutorService executor = manager.getExecutorService("testResource");
    assertNotNull(executor, "Executor should be created after onHelixManagerConnected");

    // Verify thread pool size matches Pinot config (50), not Helix config (80)
    ThreadPoolExecutor threadPoolExecutor = getUnderlyingThreadPoolExecutor(executor);
    assertEquals(threadPoolExecutor.getCorePoolSize(), 50, "Core pool size should match Pinot config");
    assertEquals(threadPoolExecutor.getMaximumPoolSize(), 50, "Max pool size should match Pinot config");

    // Verify Helix config was never read because Pinot config takes precedence
    verify(mockConfigAccessor, never()).get(any(HelixConfigScope.class), anyString());

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testOnHelixManagerConnectedWithDefaultValue() {
    // Test that default value is used when no config is set
    PinotConfiguration config = new PinotConfiguration();

    HelixManager mockHelixManager = createMockHelixManager("testCluster", "testInstance", true);
    // Mock ConfigAccessor to return null for legacy config
    when(mockHelixManager.getConfigAccessor().get(any(HelixConfigScope.class), anyString())).thenReturn(null);

    DefaultStateTransitionThreadPoolManager manager =
        new DefaultStateTransitionThreadPoolManager(config, mockHelixManager);

    manager.onHelixManagerConnected();
    ExecutorService executor = manager.getExecutorService("testResource");
    assertNotNull(executor, "Executor should be created with default value");

    // Verify thread pool size matches default
    ThreadPoolExecutor threadPoolExecutor = getUnderlyingThreadPoolExecutor(executor);
    int defaultSize = CommonConstants.Server.DEFAULT_STATE_TRANSITION_THREAD_POOL_SIZE;
    assertEquals(threadPoolExecutor.getCorePoolSize(), defaultSize, "Core pool size should match default");
    assertEquals(threadPoolExecutor.getMaximumPoolSize(), defaultSize, "Max pool size should match default");

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testOnHelixManagerConnectedWithLegacyHelixInstanceConfig() {
    // Test that legacy Helix instance config is used when Pinot config is not set
    PinotConfiguration config = new PinotConfiguration();

    HelixManager mockHelixManager = createMockHelixManager("testCluster", "testInstance", true);
    // Mock instance-level config
    when(mockHelixManager.getConfigAccessor().get(any(HelixConfigScope.class), anyString())).thenReturn("60");

    DefaultStateTransitionThreadPoolManager manager =
        new DefaultStateTransitionThreadPoolManager(config, mockHelixManager);

    manager.onHelixManagerConnected();
    ExecutorService executor = manager.getExecutorService("testResource");
    assertNotNull(executor, "Executor should be created with legacy Helix config");

    // Verify thread pool size matches legacy Helix instance config
    ThreadPoolExecutor threadPoolExecutor = getUnderlyingThreadPoolExecutor(executor);
    assertEquals(threadPoolExecutor.getCorePoolSize(), 60, "Core pool size should match Helix instance config");
    assertEquals(threadPoolExecutor.getMaximumPoolSize(), 60, "Max pool size should match Helix instance config");

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testOnHelixManagerConnectedWithLegacyHelixClusterConfig() {
    // Test that legacy Helix cluster config is used when instance config is not set
    PinotConfiguration config = new PinotConfiguration();

    HelixManager mockHelixManager = createMockHelixManager("testCluster", "testInstance", true);
    // Mock instance config returns null, cluster config returns value
    when(mockHelixManager.getConfigAccessor().get(any(HelixConfigScope.class), anyString()))
        .thenReturn(null)  // First call for instance config
        .thenReturn("70");  // Second call for cluster config

    DefaultStateTransitionThreadPoolManager manager =
        new DefaultStateTransitionThreadPoolManager(config, mockHelixManager);

    manager.onHelixManagerConnected();
    ExecutorService executor = manager.getExecutorService("testResource");
    assertNotNull(executor, "Executor should be created with cluster-level Helix config");

    // Verify thread pool size matches legacy Helix cluster config
    ThreadPoolExecutor threadPoolExecutor = getUnderlyingThreadPoolExecutor(executor);
    assertEquals(threadPoolExecutor.getCorePoolSize(), 70, "Core pool size should match Helix cluster config");
    assertEquals(threadPoolExecutor.getMaximumPoolSize(), 70, "Max pool size should match Helix cluster config");

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testOnHelixManagerConnectedNotConnected() {
    // Test that executor is still created even if HelixManager is not connected (uses default)
    PinotConfiguration config = new PinotConfiguration();

    HelixManager mockHelixManager = createMockHelixManager("testCluster", "testInstance", false);

    DefaultStateTransitionThreadPoolManager manager =
        new DefaultStateTransitionThreadPoolManager(config, mockHelixManager);

    manager.onHelixManagerConnected();
    ExecutorService executor = manager.getExecutorService("testResource");
    assertNotNull(executor, "Executor should be created with default even if not connected");

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testShutdownWithoutInitialization() {
    // Test that shutdown works even if executor was never created
    PinotConfiguration config = new PinotConfiguration();
    DefaultStateTransitionThreadPoolManager manager = new DefaultStateTransitionThreadPoolManager(config);

    // Should not throw exception
    manager.shutdown();
  }

  @Test
  public void testShutdownAfterInitialization() throws InterruptedException {
    // Test that shutdown properly shuts down the executor
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE, "2");

    DefaultStateTransitionThreadPoolManager manager = new DefaultStateTransitionThreadPoolManager(config);
    manager.onHelixManagerConnected();

    ExecutorService executor = manager.getExecutorService("testResource");
    assertNotNull(executor);
    assertFalse(executor.isShutdown());

    manager.shutdown();
    assertTrue(executor.isShutdown(), "Executor should be shutdown");
  }

  @Test
  public void testExecutorExecutesTasks() throws Exception {
    // Test that the executor can actually execute tasks
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE, "2");

    DefaultStateTransitionThreadPoolManager manager = new DefaultStateTransitionThreadPoolManager(config);
    manager.onHelixManagerConnected();

    ExecutorService executor = manager.getExecutorService("testResource");
    assertNotNull(executor);

    // Submit a simple task
    Callable<String> task = () -> "test-result";
    String result = executor.submit(task).get(5, TimeUnit.SECONDS);
    assertEquals(result, "test-result", "Task should execute successfully");

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testNullHelixManager() {
    // Test that manager works with null HelixManager (uses default)
    PinotConfiguration config = new PinotConfiguration();

    DefaultStateTransitionThreadPoolManager manager = new DefaultStateTransitionThreadPoolManager(config, null);

    manager.onHelixManagerConnected();
    ExecutorService executor = manager.getExecutorService("testResource");
    assertNotNull(executor, "Executor should be created with default value when HelixManager is null");

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testContextWrappingForCallable() throws Exception {
    // Test that Callable tasks are wrapped with proper STATE_TRANSITION context
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE, "2");

    DefaultStateTransitionThreadPoolManager manager = new DefaultStateTransitionThreadPoolManager(config);
    manager.onHelixManagerConnected();

    ExecutorService executor = manager.getExecutorService("testResource");
    assertNotNull(executor);

    AtomicReference<SegmentOperationsTaskType> capturedTaskType = new AtomicReference<>();
    AtomicReference<String> capturedTableName = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    // Submit a plain Callable task
    Callable<String> task = () -> {
      // Capture context during task execution
      capturedTaskType.set(SegmentOperationsTaskContext.getTaskType());
      capturedTableName.set(SegmentOperationsTaskContext.getTableNameWithType());
      latch.countDown();
      return "success";
    };

    // Submit the task
    String result = executor.submit(task).get(5, TimeUnit.SECONDS);

    // Wait for task to complete
    assertTrue(latch.await(5, TimeUnit.SECONDS), "Task should complete");

    // Verify result
    assertEquals(result, "success");

    // Verify context was set during execution
    assertEquals(capturedTaskType.get(), SegmentOperationsTaskType.STATE_TRANSITION,
        "Task type should be STATE_TRANSITION");
    // For non-MessageTask, table name will be null
    assertNull(capturedTableName.get(),
        "Table name should be null for non-MessageTask");

    // Verify context is cleared after execution (in calling thread)
    assertNull(SegmentOperationsTaskContext.getTaskType(),
        "Context should be cleared in calling thread");
    assertNull(SegmentOperationsTaskContext.getTableNameWithType(),
        "Table name should be cleared in calling thread");

    // Cleanup
    manager.shutdown();
  }


  @Test
  public void testContextWrappingForRunnable() throws Exception {
    // Test that Runnable tasks are wrapped with proper STATE_TRANSITION context
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE, "2");

    DefaultStateTransitionThreadPoolManager manager = new DefaultStateTransitionThreadPoolManager(config);
    manager.onHelixManagerConnected();

    ExecutorService executor = manager.getExecutorService("testResource");
    assertNotNull(executor);

    AtomicReference<SegmentOperationsTaskType> capturedTaskType = new AtomicReference<>();
    AtomicReference<String> capturedTableName = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    // Submit a plain Runnable task
    Runnable task = () -> {
      // Capture context during task execution
      capturedTaskType.set(SegmentOperationsTaskContext.getTaskType());
      capturedTableName.set(SegmentOperationsTaskContext.getTableNameWithType());
      latch.countDown();
    };

    // Submit the task as Runnable
    executor.submit(task).get(5, TimeUnit.SECONDS);

    // Wait for task to complete
    assertTrue(latch.await(5, TimeUnit.SECONDS), "Task should complete");

    // Verify context was set during execution
    assertEquals(capturedTaskType.get(), SegmentOperationsTaskType.STATE_TRANSITION,
        "Task type should be STATE_TRANSITION");
    // For non-MessageTask, table name will be null
    assertNull(capturedTableName.get(),
        "Table name should be null for non-MessageTask");

    // Verify context is cleared after execution
    assertNull(SegmentOperationsTaskContext.getTaskType(),
        "Context should be cleared after execution");
    assertNull(SegmentOperationsTaskContext.getTableNameWithType(),
        "Table name should be cleared after execution");

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testContextClearedAfterException() throws Exception {
    // Test that context (both task type and table name) is cleared even when task throws exception
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE, "2");

    DefaultStateTransitionThreadPoolManager manager = new DefaultStateTransitionThreadPoolManager(config);
    manager.onHelixManagerConnected();

    ExecutorService executor = manager.getExecutorService("testResource");
    assertNotNull(executor);

    AtomicReference<SegmentOperationsTaskType> capturedTaskTypeBeforeException = new AtomicReference<>();
    AtomicReference<String> capturedTableNameBeforeException = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    // Submit a task that throws exception
    Callable<String> task = () -> {
      capturedTaskTypeBeforeException.set(SegmentOperationsTaskContext.getTaskType());
      capturedTableNameBeforeException.set(SegmentOperationsTaskContext.getTableNameWithType());
      latch.countDown();
      throw new RuntimeException("Test exception");
    };

    // Submit and expect exception
    try {
      executor.submit(task).get(5, TimeUnit.SECONDS);
      fail("Should have thrown exception");
    } catch (Exception e) {
      // Expected
      assertTrue(e.getCause() instanceof RuntimeException);
      assertEquals(e.getCause().getMessage(), "Test exception");
    }

    // Wait for task to reach the exception point
    assertTrue(latch.await(5, TimeUnit.SECONDS), "Task should have set context before exception");

    // Verify context was set before exception
    assertEquals(capturedTaskTypeBeforeException.get(), SegmentOperationsTaskType.STATE_TRANSITION,
        "Task type should be set before exception");
    // Table name is null for non-MessageTask
    assertNull(capturedTableNameBeforeException.get(),
        "Table name should be null for non-MessageTask");

    // Submit another task to verify context was cleaned up (both task type and table name)
    AtomicReference<SegmentOperationsTaskType> capturedTaskTypeInNextTask = new AtomicReference<>();
    AtomicReference<String> capturedTableNameInNextTask = new AtomicReference<>();
    Callable<String> nextTask = () -> {
      capturedTaskTypeInNextTask.set(SegmentOperationsTaskContext.getTaskType());
      capturedTableNameInNextTask.set(SegmentOperationsTaskContext.getTableNameWithType());
      return "success";
    };

    String result = executor.submit(nextTask).get(5, TimeUnit.SECONDS);
    assertEquals(result, "success");

    // Verify new task has fresh context (not polluted by previous exception)
    assertEquals(capturedTaskTypeInNextTask.get(), SegmentOperationsTaskType.STATE_TRANSITION,
        "New task should have fresh STATE_TRANSITION context");
    assertNull(capturedTableNameInNextTask.get(),
        "New task should have null table name for non-MessageTask");

    // Verify context is cleared in calling thread
    assertNull(SegmentOperationsTaskContext.getTaskType(),
        "Task type should be cleared after execution");
    assertNull(SegmentOperationsTaskContext.getTableNameWithType(),
        "Table name should be cleared after execution");

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testOnChangeResizesPoolDynamically() {
    // Test that onChange dynamically resizes the thread pool
    PinotConfiguration config = new PinotConfiguration();
    DefaultStateTransitionThreadPoolManager manager = new DefaultStateTransitionThreadPoolManager(config);
    manager.onHelixManagerConnected();

    ThreadPoolExecutor threadPoolExecutor = getUnderlyingThreadPoolExecutor(manager.getExecutorService("testResource"));
    int defaultSize = CommonConstants.Server.DEFAULT_STATE_TRANSITION_THREAD_POOL_SIZE;
    assertEquals(threadPoolExecutor.getCorePoolSize(), defaultSize);

    // Increase pool size via cluster config change
    String configKey = CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE;
    manager.onChange(Set.of(configKey), Map.of(configKey, "100"));
    assertEquals(threadPoolExecutor.getCorePoolSize(), 100, "Core pool size should increase to 100");
    assertEquals(threadPoolExecutor.getMaximumPoolSize(), 100, "Max pool size should increase to 100");

    // Decrease pool size via cluster config change
    manager.onChange(Set.of(configKey), Map.of(configKey, "10"));
    assertEquals(threadPoolExecutor.getCorePoolSize(), 10, "Core pool size should decrease to 10");
    assertEquals(threadPoolExecutor.getMaximumPoolSize(), 10, "Max pool size should decrease to 10");

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testOnChangeIgnoresIrrelevantConfigs() {
    // Test that onChange ignores config changes for other keys
    PinotConfiguration config = new PinotConfiguration();
    DefaultStateTransitionThreadPoolManager manager = new DefaultStateTransitionThreadPoolManager(config);
    manager.onHelixManagerConnected();

    ThreadPoolExecutor threadPoolExecutor = getUnderlyingThreadPoolExecutor(manager.getExecutorService("testResource"));
    int defaultSize = CommonConstants.Server.DEFAULT_STATE_TRANSITION_THREAD_POOL_SIZE;
    assertEquals(threadPoolExecutor.getCorePoolSize(), defaultSize);

    // Change an unrelated config key
    manager.onChange(Set.of("some.other.config"), Map.of("some.other.config", "100"));
    assertEquals(threadPoolExecutor.getCorePoolSize(), defaultSize, "Pool size should not change for unrelated config");

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testOnChangeHandlesInvalidValue() {
    // Test that onChange ignores invalid (non-numeric) values
    PinotConfiguration config = new PinotConfiguration();
    DefaultStateTransitionThreadPoolManager manager = new DefaultStateTransitionThreadPoolManager(config);
    manager.onHelixManagerConnected();

    ThreadPoolExecutor threadPoolExecutor = getUnderlyingThreadPoolExecutor(manager.getExecutorService("testResource"));
    int defaultSize = CommonConstants.Server.DEFAULT_STATE_TRANSITION_THREAD_POOL_SIZE;

    String configKey = CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE;

    // Invalid non-numeric value should be ignored
    manager.onChange(Set.of(configKey), Map.of(configKey, "not-a-number"));
    assertEquals(threadPoolExecutor.getCorePoolSize(), defaultSize, "Pool size should not change for invalid value");

    // Non-positive value should be ignored
    manager.onChange(Set.of(configKey), Map.of(configKey, "0"));
    assertEquals(threadPoolExecutor.getCorePoolSize(), defaultSize, "Pool size should not change for zero value");

    manager.onChange(Set.of(configKey), Map.of(configKey, "-5"));
    assertEquals(threadPoolExecutor.getCorePoolSize(), defaultSize, "Pool size should not change for negative value");

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testOnChangeRevertsToDefaultWhenConfigRemoved() {
    // Test that removing the cluster config reverts to server config / default
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE, "25");
    DefaultStateTransitionThreadPoolManager manager = new DefaultStateTransitionThreadPoolManager(config);
    manager.onHelixManagerConnected();

    ThreadPoolExecutor threadPoolExecutor = getUnderlyingThreadPoolExecutor(manager.getExecutorService("testResource"));
    assertEquals(threadPoolExecutor.getCorePoolSize(), 25, "Should use server config after connection");

    // Dynamically override via cluster config
    String configKey = CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE;
    manager.onChange(Set.of(configKey), Map.of(configKey, "80"));
    assertEquals(threadPoolExecutor.getCorePoolSize(), 80, "Should use cluster config value");

    // Remove the cluster config (key in changedConfigs but not in clusterConfigs map)
    manager.onChange(Set.of(configKey), Map.of());
    assertEquals(threadPoolExecutor.getCorePoolSize(), 25,
        "Should revert to server config when cluster config is removed");

    // Cleanup
    manager.shutdown();
  }

  @Test
  public void testInvalidLegacyHelixConfig() {
    // Test that invalid legacy Helix config is handled gracefully
    PinotConfiguration config = new PinotConfiguration();

    HelixManager mockHelixManager = createMockHelixManager("testCluster", "testInstance", true);
    // Mock invalid config value
    when(mockHelixManager.getConfigAccessor().get(any(HelixConfigScope.class), anyString())).thenReturn("not-a-number");

    DefaultStateTransitionThreadPoolManager manager =
        new DefaultStateTransitionThreadPoolManager(config, mockHelixManager);

    manager.onHelixManagerConnected();
    ExecutorService executor = manager.getExecutorService("testResource");
    assertNotNull(executor, "Executor should be created with default when legacy config is invalid");

    // Cleanup
    manager.shutdown();
  }

  private HelixManager createMockHelixManager(String clusterName, String instanceName, boolean isConnected) {
    HelixManager mockHelixManager = mock(HelixManager.class);
    when(mockHelixManager.getClusterName()).thenReturn(clusterName);
    when(mockHelixManager.getInstanceName()).thenReturn(instanceName);
    when(mockHelixManager.isConnected()).thenReturn(isConnected);

    // Mock ConfigAccessor
    ConfigAccessor mockConfigAccessor = mock(ConfigAccessor.class);
    when(mockHelixManager.getConfigAccessor()).thenReturn(mockConfigAccessor);

    return mockHelixManager;
  }

  /**
   * Helper method to extract the underlying ThreadPoolExecutor from the DecoratorExecutorService wrapper.
   */
  private ThreadPoolExecutor getUnderlyingThreadPoolExecutor(ExecutorService executor) {
    try {
      assertNotNull(executor);
      assertTrue(executor instanceof DecoratorExecutorService, "Executor should be a DecoratorExecutorService");
      DecoratorExecutorService decorator = (DecoratorExecutorService) executor;

      // Use reflection to access protected field
      Field field = DecoratorExecutorService.class.getDeclaredField("_executorService");
      field.setAccessible(true);
      ExecutorService underlying = (ExecutorService) field.get(decorator);

      assertTrue(underlying instanceof ThreadPoolExecutor, "Underlying executor should be a ThreadPoolExecutor");
      return (ThreadPoolExecutor) underlying;
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract underlying ThreadPoolExecutor", e);
    }
  }
}
