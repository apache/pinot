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
package org.apache.pinot.controller.helix.core.minion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.task.TaskState;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Test class for PinotTaskManager with distributed locking enabled. Tests scenarios run across multiple controllers
 */
public class PinotTaskManagerDistributedLockingTest extends ControllerTest {

  private static final String RAW_TABLE_NAME_1 = "testTable1";
  private static final String RAW_TABLE_NAME_2 = "testTable2";
  private static final String RAW_TABLE_NAME_3 = "testTable3";

  private static final String TEST_TASK_TYPE = "TestDistributedLockTaskType";

  @BeforeMethod
  public void setUpMethod() throws Exception {
    startZk();
  }

  @AfterMethod
  public void tearDownMethod() {
    try {
      if (_controllerStarter != null) {
        // Clean up any running tasks before stopping controller
        try {
          Map<String, TaskState> taskStates = _controllerStarter.getHelixTaskResourceManager()
              .getTaskStates(TEST_TASK_TYPE);
          for (String taskName : taskStates.keySet()) {
            try {
              _controllerStarter.getHelixTaskResourceManager().deleteTask(taskName, true);
            } catch (Exception e) {
              // Ignore individual task deletion errors
            }
          }
          Thread.sleep(500); // Give time for task cancellation
        } catch (Exception e) {
          // Ignore cleanup errors
        }
        stopController();
      }
    } catch (Exception e) {
      // Ignore
    }
    try {
      stopFakeInstances();
    } catch (Exception e) {
      // Ignore
    }
    stopZk();
  }

  /**
   * Test scenario: Tests the schedule task happy path on a single controller to ensure task generation goes through
   * for all tables
   */
  @Test
  public void testScheduleTaskForSpecificTables() throws Exception {
    // Setup first controller with distributed locking enabled
    Map<String, Object> properties1 = getDefaultControllerConfiguration();
    properties1.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties1.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DISTRIBUTED_LOCKING, true);
    properties1.put(ControllerConf.CONTROLLER_PORT, 21002);

    // Setup second controller with distributed locking enabled (different port)
    Map<String, Object> properties2 = getDefaultControllerConfiguration();
    // Disable scheduler to avoid Quartz conflicts
    properties2.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, false);
    properties2.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DISTRIBUTED_LOCKING, true);
    properties2.put(ControllerConf.CONTROLLER_PORT, 21003);

    // Start both controllers
    startController(properties1);
    BaseControllerStarter controller1 = _controllerStarter;
    PinotTaskManager taskManager1 = controller1.getTaskManager();

    // Start second controller instance
    BaseControllerStarter controller2 = startControllerOnDifferentPort(properties2);
    PinotTaskManager taskManager2 = controller2.getTaskManager();

    try {
      // Setup test environment (using first controller)
      addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
      addFakeServerInstancesToAutoJoinHelixCluster(1, true);
      addFakeMinionInstancesToAutoJoinHelixCluster(1);

      // Create and register task generators on both controllers
      ControllableTaskGenerator generator1 = new ControllableTaskGenerator(TEST_TASK_TYPE);
      generator1.setGenerationDelay(3000); // 3 second delay for createTask
      ClusterInfoAccessor clusterInfoAccessor1 = Mockito.mock(ClusterInfoAccessor.class);
      generator1.init(clusterInfoAccessor1);
      taskManager1.registerTaskGenerator(generator1);

      ControllableTaskGenerator generator2 = new ControllableTaskGenerator(TEST_TASK_TYPE);
      generator2.setGenerationDelay(500); // Shorter delay for scheduleTasks
      ClusterInfoAccessor clusterInfoAccessor2 = Mockito.mock(ClusterInfoAccessor.class);
      generator2.init(clusterInfoAccessor2);
      taskManager2.registerTaskGenerator(generator2);

      // Ensure task queues exist on both controllers
      controller1.getHelixTaskResourceManager().ensureTaskQueueExists(TEST_TASK_TYPE);
      controller2.getHelixTaskResourceManager().ensureTaskQueueExists(TEST_TASK_TYPE);

      // Create schemas and all three tables ONLY on controller1 to avoid Quartz job conflicts
      // The tables will be visible to both controllers via ZooKeeper
      Schema schema1 = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME_1)
          .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
      addSchema(schema1);
      createSingleTestTable(RAW_TABLE_NAME_1);

      Schema schema2 = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME_2)
          .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
      addSchema(schema2);
      createSingleTestTable(RAW_TABLE_NAME_2);

      Schema schema3 = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME_3)
          .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
      addSchema(schema3);
      createSingleTestTable(RAW_TABLE_NAME_3);

      // Wait for controllers to be fully initialized
      Thread.sleep(2000);

      ExecutorService executor = Executors.newFixedThreadPool(1);
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch completionLatch = new CountDownLatch(1);

      AtomicInteger scheduleTasksCompleted = new AtomicInteger(0);
      Map<String, TaskSchedulingInfo> scheduleTasksResult = new HashMap<>();

      // Controller 2: Run scheduleTasks for all tables
      executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(500); // Start after createTask begins

          TaskSchedulingContext context = new TaskSchedulingContext();
          Set<String> tablesToSchedule = new HashSet<>();
          tablesToSchedule.add(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_1)); // This should be blocked
          tablesToSchedule.add(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_2)); // This should succeed
          tablesToSchedule.add(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_3)); // This should succeed
          context.setTablesToSchedule(tablesToSchedule);
          context.setLeader(true);

          Map<String, TaskSchedulingInfo> result = taskManager2.scheduleTasks(context);
          scheduleTasksResult.putAll(result);
          scheduleTasksCompleted.incrementAndGet();
        } catch (Exception ignored) {
        } finally {
          completionLatch.countDown();
        }
      });

      // Start both operations
      startLatch.countDown();

      // Wait for completion
      assertTrue(completionLatch.await(45, TimeUnit.SECONDS), "Tasks should complete within 45 seconds");

      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor should terminate");

      // scheduleTasks currently fails due to RuntimeException when generation errors occur
      // This is the current behavior - it throws exception instead of returning partial results
      assertEquals(scheduleTasksCompleted.get(), 1, "scheduleTasks should complete");

      // Verify that both controllers generated tasks
      int totalGenerations = generator1.getTaskGenerationCount() + generator2.getTaskGenerationCount();
      assertEquals(totalGenerations, 3, "scheduleTasks should've succeeded on one controller for three tables");

      assertNotNull(scheduleTasksResult);
      assertEquals(scheduleTasksResult.size(), 1);

      TaskSchedulingInfo taskSchedulingInfo = scheduleTasksResult.get(TEST_TASK_TYPE);
      assertNotNull(taskSchedulingInfo);
      assertNotNull(taskSchedulingInfo.getScheduledTaskNames());
      assertNotNull(taskSchedulingInfo.getSchedulingErrors());
      assertNotNull(taskSchedulingInfo.getGenerationErrors());
      // Should see one task scheduled for both tables
      assertEquals(taskSchedulingInfo.getScheduledTaskNames().size(), 1);
      // Should see 0 errors
      assertEquals(taskSchedulingInfo.getGenerationErrors().size(), 0);
      assertEquals(taskSchedulingInfo.getSchedulingErrors().size(), 0);
    } finally {
      // Cleanup
      try {
        // Cancel all running tasks before dropping tables
        Map<String, TaskState> taskStates = _controllerStarter.getHelixTaskResourceManager()
            .getTaskStates(TEST_TASK_TYPE);
        for (String taskName : taskStates.keySet()) {
          try {
            _controllerStarter.getHelixTaskResourceManager().deleteTask(taskName, true);
          } catch (Exception e) {
            // Ignore individual task deletion errors
          }
        }
        Thread.sleep(1000); // Give time for task cancellation

        dropOfflineTable(RAW_TABLE_NAME_1);
        dropOfflineTable(RAW_TABLE_NAME_2);
        dropOfflineTable(RAW_TABLE_NAME_3);
      } catch (Exception ignored) {
      }

      // Stop second controller first
      controller2.stop();
    }
  }

  /**
   * Test scenario: Two actual controllers trying to submit createTask for the same table simultaneously.
   * This test starts multiple controller instances to test true multi-controller distributed locking.
   */
  @Test
  public void testConcurrentCreateTaskFromMultipleControllers() throws Exception {
    // Setup first controller with distributed locking enabled
    Map<String, Object> properties1 = getDefaultControllerConfiguration();
    properties1.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties1.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DISTRIBUTED_LOCKING, true);
    properties1.put(ControllerConf.CONTROLLER_PORT, 21000);

    // Setup second controller with distributed locking enabled (different port)
    Map<String, Object> properties2 = getDefaultControllerConfiguration();
    properties2.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties2.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DISTRIBUTED_LOCKING, true);
    properties2.put(ControllerConf.CONTROLLER_PORT, 21001);

    // Start both controllers
    startController(properties1);
    BaseControllerStarter controller1 = _controllerStarter;
    PinotTaskManager taskManager1 = controller1.getTaskManager();

    // Start second controller instance
    BaseControllerStarter controller2 = startControllerOnDifferentPort(properties2);
    PinotTaskManager taskManager2 = controller2.getTaskManager();

    try {
      // Setup test environment (using first controller)
      addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
      addFakeServerInstancesToAutoJoinHelixCluster(1, true);
      addFakeMinionInstancesToAutoJoinHelixCluster(1);

      // Create and register task generators on both controllers FIRST
      ControllableTaskGenerator generator1 = new ControllableTaskGenerator(TEST_TASK_TYPE);
      generator1.setGenerationDelay(2000); // 2 second delay
      ClusterInfoAccessor clusterInfoAccessor1 = Mockito.mock(ClusterInfoAccessor.class);
      generator1.init(clusterInfoAccessor1);
      taskManager1.registerTaskGenerator(generator1);

      ControllableTaskGenerator generator2 = new ControllableTaskGenerator(TEST_TASK_TYPE);
      generator2.setGenerationDelay(2000); // 2 second delay
      ClusterInfoAccessor clusterInfoAccessor2 = Mockito.mock(ClusterInfoAccessor.class);
      generator2.init(clusterInfoAccessor2);
      taskManager2.registerTaskGenerator(generator2);

      // Ensure task queues exist on both controllers
      controller1.getHelixTaskResourceManager().ensureTaskQueueExists(TEST_TASK_TYPE);
      controller2.getHelixTaskResourceManager().ensureTaskQueueExists(TEST_TASK_TYPE);

      // Create schema and table AFTER registering task generators
      Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME_1)
          .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
      addSchema(schema);
      createSingleTestTable(RAW_TABLE_NAME_1);

      // Wait a bit for controllers to be fully initialized
      Thread.sleep(2000);

      // Now test concurrent createTask from both controllers
      ExecutorService executor = Executors.newFixedThreadPool(2);
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch completionLatch = new CountDownLatch(2);

      AtomicInteger successfulCreations = new AtomicInteger(0);
      AtomicInteger failedCreations = new AtomicInteger(0);

      // Controller 1 attempts to create task
      executor.submit(() -> {
        try {
          startLatch.await();
          Map<String, String> result = taskManager1.createTask(TEST_TASK_TYPE, RAW_TABLE_NAME_1, null, new HashMap<>());
          if (result != null && !result.isEmpty()) {
            successfulCreations.incrementAndGet();
          }
        } catch (Exception e) {
          failedCreations.incrementAndGet();
        } finally {
          completionLatch.countDown();
        }
      });

      // Controller 2 attempts to create task for the same table
      executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(100); // Small delay to ensure some overlap
          Map<String, String> result = taskManager2.createTask(TEST_TASK_TYPE, RAW_TABLE_NAME_1, null, new HashMap<>());
          if (result != null && !result.isEmpty()) {
            successfulCreations.incrementAndGet();
          }
        } catch (Exception e) {
          failedCreations.incrementAndGet();
        } finally {
          completionLatch.countDown();
        }
      });

      // Start both controllers simultaneously
      startLatch.countDown();

      // Wait for completion
      assertTrue(completionLatch.await(30, TimeUnit.SECONDS), "Tasks should complete within 30 seconds");

      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor should terminate");

      // With distributed locking between actual controllers, exactly one should succeed
      assertEquals(successfulCreations.get(), 1, "Exactly one createTask should succeed due to distributed locking");
      assertEquals(failedCreations.get(), 1, "Exactly one createTask should fail due to distributed locking");

      int totalGenerations = generator1.getTaskGenerationCount() + generator2.getTaskGenerationCount();
      assertEquals(totalGenerations, 1, "At least one task generation should have occurred");
    } finally {
      // Cleanup
      try {
        // Cancel all running tasks before dropping tables
        Map<String, TaskState> taskStates = _controllerStarter.getHelixTaskResourceManager()
            .getTaskStates(TEST_TASK_TYPE);
        for (String taskName : taskStates.keySet()) {
          try {
            _controllerStarter.getHelixTaskResourceManager().deleteTask(taskName, true);
          } catch (Exception e) {
            // Ignore individual task deletion errors
          }
        }
        Thread.sleep(1000); // Give time for task cancellation

        dropOfflineTable(RAW_TABLE_NAME_1);
      } catch (Exception ignored) {
      }

      // Stop second controller first
      controller2.stop();
    }
  }

  /**
   * Test scenario: One controller runs createTask for a specific table while another controller
   * runs scheduleTasks for multiple tables including the locked table.
   * The scheduleTasks should succeed for other tables but fail for the locked table.
   */
  @Test
  public void testCreateTaskBlocksScheduleTaskForSpecificTable() throws Exception {
    // Setup first controller with distributed locking enabled
    Map<String, Object> properties1 = getDefaultControllerConfiguration();
    properties1.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties1.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DISTRIBUTED_LOCKING, true);
    properties1.put(ControllerConf.CONTROLLER_PORT, 21002);

    // Setup second controller with distributed locking enabled (different port)
    Map<String, Object> properties2 = getDefaultControllerConfiguration();
    // Disable scheduler to avoid Quartz conflicts
    properties2.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, false);
    properties2.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DISTRIBUTED_LOCKING, true);
    properties2.put(ControllerConf.CONTROLLER_PORT, 21003);

    // Start both controllers
    startController(properties1);
    BaseControllerStarter controller1 = _controllerStarter;
    PinotTaskManager taskManager1 = controller1.getTaskManager();

    // Start second controller instance
    BaseControllerStarter controller2 = startControllerOnDifferentPort(properties2);
    PinotTaskManager taskManager2 = controller2.getTaskManager();

    try {
      // Setup test environment (using first controller)
      addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
      addFakeServerInstancesToAutoJoinHelixCluster(1, true);
      addFakeMinionInstancesToAutoJoinHelixCluster(1);

      // Create and register task generators on both controllers
      ControllableTaskGenerator generator1 = new ControllableTaskGenerator(TEST_TASK_TYPE);
      generator1.setGenerationDelay(3000); // 3 second delay for createTask
      ClusterInfoAccessor clusterInfoAccessor1 = Mockito.mock(ClusterInfoAccessor.class);
      generator1.init(clusterInfoAccessor1);
      taskManager1.registerTaskGenerator(generator1);

      ControllableTaskGenerator generator2 = new ControllableTaskGenerator(TEST_TASK_TYPE);
      generator2.setGenerationDelay(500); // Shorter delay for scheduleTasks
      ClusterInfoAccessor clusterInfoAccessor2 = Mockito.mock(ClusterInfoAccessor.class);
      generator2.init(clusterInfoAccessor2);
      taskManager2.registerTaskGenerator(generator2);

      // Ensure task queues exist on both controllers
      controller1.getHelixTaskResourceManager().ensureTaskQueueExists(TEST_TASK_TYPE);
      controller2.getHelixTaskResourceManager().ensureTaskQueueExists(TEST_TASK_TYPE);

      // Create schemas and all three tables ONLY on controller1 to avoid Quartz job conflicts
      // The tables will be visible to both controllers via ZooKeeper
      Schema schema1 = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME_1)
          .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
      addSchema(schema1);
      createSingleTestTable(RAW_TABLE_NAME_1);

      Schema schema2 = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME_2)
          .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
      addSchema(schema2);
      createSingleTestTable(RAW_TABLE_NAME_2);

      Schema schema3 = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME_3)
          .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
      addSchema(schema3);
      createSingleTestTable(RAW_TABLE_NAME_3);

      // Wait for controllers to be fully initialized
      Thread.sleep(2000);

      ExecutorService executor = Executors.newFixedThreadPool(2);
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch completionLatch = new CountDownLatch(2);

      AtomicInteger createTaskSuccessful = new AtomicInteger(0);
      AtomicInteger scheduleTasksCompleted = new AtomicInteger(0);
      Map<String, TaskSchedulingInfo> scheduleTasksResult = new HashMap<>();

      // Controller 1: Run createTask for table1 (this will hold the lock for table1)
      executor.submit(() -> {
        try {
          startLatch.await();
          Map<String, String> result = taskManager1.createTask(TEST_TASK_TYPE, RAW_TABLE_NAME_1, null, new HashMap<>());
          if (result != null && !result.isEmpty()) {
            createTaskSuccessful.incrementAndGet();
          }
        } catch (Exception ignored) {
        } finally {
          completionLatch.countDown();
        }
      });

      // Controller 2: Run scheduleTasks for all tables (should be blocked for table1 but succeed for others)
      executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(500); // Start after createTask begins

          TaskSchedulingContext context = new TaskSchedulingContext();
          Set<String> tablesToSchedule = new HashSet<>();
          tablesToSchedule.add(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_1)); // This should be blocked
          tablesToSchedule.add(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_2)); // This should succeed
          tablesToSchedule.add(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_3)); // This should succeed
          context.setTablesToSchedule(tablesToSchedule);
          context.setLeader(true);

          Map<String, TaskSchedulingInfo> result = taskManager2.scheduleTasks(context);
          scheduleTasksResult.putAll(result);
          scheduleTasksCompleted.incrementAndGet();
        } catch (Exception ignored) {
        } finally {
          completionLatch.countDown();
        }
      });

      // Start both operations
      startLatch.countDown();

      // Wait for completion
      assertTrue(completionLatch.await(45, TimeUnit.SECONDS), "Tasks should complete within 45 seconds");

      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor should terminate");

      // Verify results
      assertEquals(createTaskSuccessful.get(), 1, "createTask should succeed");

      // scheduleTasks currently fails due to RuntimeException when generation errors occur
      // This is the current behavior - it throws exception instead of returning partial results
      assertEquals(scheduleTasksCompleted.get(), 1, "scheduleTasks should complete");

      // Verify that both controllers generated tasks
      int totalGenerations = generator1.getTaskGenerationCount() + generator2.getTaskGenerationCount();
      assertTrue(totalGenerations >= 3, "Both createTask and scheduleTasks should've succeeded on both controllers");

      assertNotNull(scheduleTasksResult);
      assertEquals(scheduleTasksResult.size(), 1);

      TaskSchedulingInfo taskSchedulingInfo = scheduleTasksResult.get(TEST_TASK_TYPE);
      assertNotNull(taskSchedulingInfo);
      assertNotNull(taskSchedulingInfo.getScheduledTaskNames());
      assertNotNull(taskSchedulingInfo.getSchedulingErrors());
      assertNotNull(taskSchedulingInfo.getGenerationErrors());
      // Should see one task scheduled for both tables
      assertEquals(taskSchedulingInfo.getScheduledTaskNames().size(), 1);
      // Should see one generation error, indicating that testTable1's lock couldn't be taken
      assertEquals(taskSchedulingInfo.getGenerationErrors().size(), 1);
      assertEquals(taskSchedulingInfo.getGenerationErrors().get(0), "Could not acquire table level distributed lock "
          + "for scheduled task type: TestDistributedLockTaskType, table: testTable1_OFFLINE. Another controller is "
          + "likely generating tasks for this table. Please try again later.");
      assertEquals(taskSchedulingInfo.getSchedulingErrors().size(), 0);
    } finally {
      // Cleanup
      try {
        // Cancel all running tasks before dropping tables
        Map<String, TaskState> taskStates = _controllerStarter.getHelixTaskResourceManager()
            .getTaskStates(TEST_TASK_TYPE);
        for (String taskName : taskStates.keySet()) {
          try {
            _controllerStarter.getHelixTaskResourceManager().deleteTask(taskName, true);
          } catch (Exception e) {
            // Ignore individual task deletion errors
          }
        }
        Thread.sleep(1000); // Give time for task cancellation

        dropOfflineTable(RAW_TABLE_NAME_1);
        dropOfflineTable(RAW_TABLE_NAME_2);
        dropOfflineTable(RAW_TABLE_NAME_3);
      } catch (Exception ignored) {
      }

      // Stop second controller first
      controller2.stop();
    }
  }

  /**
   * Test scenario: Both controllers scheduleTasks for the same table.
   * The scheduleTasks should succeed for one of the controllers.
   */
  @Test
  public void testMultipleScheduleTaskForSpecificTableOnBothControllers() throws Exception {
    // Setup first controller with distributed locking enabled
    Map<String, Object> properties1 = getDefaultControllerConfiguration();
    properties1.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties1.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DISTRIBUTED_LOCKING, true);
    properties1.put(ControllerConf.CONTROLLER_PORT, 21002);

    // Setup second controller with distributed locking enabled (different port)
    Map<String, Object> properties2 = getDefaultControllerConfiguration();
    // Disable scheduler to avoid Quartz conflicts
    properties2.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, false);
    properties2.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DISTRIBUTED_LOCKING, true);
    properties2.put(ControllerConf.CONTROLLER_PORT, 21003);

    // Start both controllers
    startController(properties1);
    BaseControllerStarter controller1 = _controllerStarter;
    PinotTaskManager taskManager1 = controller1.getTaskManager();

    // Start second controller instance
    BaseControllerStarter controller2 = startControllerOnDifferentPort(properties2);
    PinotTaskManager taskManager2 = controller2.getTaskManager();

    try {
      // Setup test environment (using first controller)
      addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
      addFakeServerInstancesToAutoJoinHelixCluster(1, true);
      addFakeMinionInstancesToAutoJoinHelixCluster(1);

      // Create and register task generators on both controllers
      ControllableTaskGenerator generator1 = new ControllableTaskGenerator(TEST_TASK_TYPE);
      generator1.setGenerationDelay(3000); // 3 second delay for createTask
      ClusterInfoAccessor clusterInfoAccessor1 = Mockito.mock(ClusterInfoAccessor.class);
      generator1.init(clusterInfoAccessor1);
      taskManager1.registerTaskGenerator(generator1);

      ControllableTaskGenerator generator2 = new ControllableTaskGenerator(TEST_TASK_TYPE);
      generator2.setGenerationDelay(500); // Shorter delay for scheduleTasks
      ClusterInfoAccessor clusterInfoAccessor2 = Mockito.mock(ClusterInfoAccessor.class);
      generator2.init(clusterInfoAccessor2);
      taskManager2.registerTaskGenerator(generator2);

      // Ensure task queues exist on both controllers
      controller1.getHelixTaskResourceManager().ensureTaskQueueExists(TEST_TASK_TYPE);
      controller2.getHelixTaskResourceManager().ensureTaskQueueExists(TEST_TASK_TYPE);

      // Create schemas and all three tables ONLY on controller1 to avoid Quartz job conflicts
      // The tables will be visible to both controllers via ZooKeeper
      Schema schema1 = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME_1)
          .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
      addSchema(schema1);
      createSingleTestTable(RAW_TABLE_NAME_1);

      Schema schema2 = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME_2)
          .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
      addSchema(schema2);
      createSingleTestTable(RAW_TABLE_NAME_2);

      Schema schema3 = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME_3)
          .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
      addSchema(schema3);
      createSingleTestTable(RAW_TABLE_NAME_3);

      // Wait for controllers to be fully initialized
      Thread.sleep(2000);

      ExecutorService executor = Executors.newFixedThreadPool(2);
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch completionLatch = new CountDownLatch(2);

      AtomicInteger scheduleTasksCompleted1 = new AtomicInteger(0);
      AtomicInteger scheduleTasksCompleted2 = new AtomicInteger(0);
      Map<String, TaskSchedulingInfo> scheduleTasksResult1 = new HashMap<>();
      Map<String, TaskSchedulingInfo> scheduleTasksResult2 = new HashMap<>();

      AtomicInteger failedCreations = new AtomicInteger(0);

      // Controller 1: Run scheduleTasks for all tables (this will hold the lock for all tables)
      executor.submit(() -> {
        try {
          startLatch.await();

          TaskSchedulingContext context = new TaskSchedulingContext();
          Set<String> tablesToSchedule = new HashSet<>();
          tablesToSchedule.add(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_1));
          tablesToSchedule.add(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_2));
          tablesToSchedule.add(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_3));
          context.setTablesToSchedule(tablesToSchedule);
          context.setLeader(true);

          Map<String, TaskSchedulingInfo> result = taskManager1.scheduleTasks(context);
          scheduleTasksResult1.putAll(result);
          scheduleTasksCompleted1.incrementAndGet();
        } catch (Exception ignored) {
        } finally {
          completionLatch.countDown();
        }
      });

      // Controller 2: Run scheduleTasks for all tables (should fail)
      executor.submit(() -> {
        try {
          startLatch.await();
          Thread.sleep(500); // Start after controller1 begins

          TaskSchedulingContext context = new TaskSchedulingContext();
          Set<String> tablesToSchedule = new HashSet<>();
          tablesToSchedule.add(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_1));
          tablesToSchedule.add(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_2));
          tablesToSchedule.add(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_3));
          context.setTablesToSchedule(tablesToSchedule);
          context.setLeader(true);

          Map<String, TaskSchedulingInfo> result = taskManager2.scheduleTasks(context);
          scheduleTasksResult2.putAll(result);
          scheduleTasksCompleted2.incrementAndGet();
        } catch (Exception e) {
          failedCreations.incrementAndGet();
        } finally {
          completionLatch.countDown();
        }
      });

      // Start both operations
      startLatch.countDown();

      // Wait for completion
      assertTrue(completionLatch.await(45, TimeUnit.SECONDS), "Tasks should complete within 45 seconds");

      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor should terminate");

      // Verify results
      assertEquals(scheduleTasksCompleted1.get(), 1, "scheduleTasks on controller1 should succeed");

      // scheduleTasks currently fails due to RuntimeException when generation errors occur
      // This is the current behavior - it throws exception instead of returning partial results
      assertEquals(scheduleTasksCompleted2.get(), 0, "scheduleTasks on controller2 shouldn't complete");

      // Validate controller 2's scheduleTasks failed due to being unable to get any locks
      assertEquals(failedCreations.get(), 1, "Controller 2's scheduleTasks should've failed");

      // Verify that only 1 controller generated tasks
      int totalGenerations = generator1.getTaskGenerationCount() + generator2.getTaskGenerationCount();
      assertEquals(totalGenerations, 3, "Only 1 scheduleTasks succeeded for all three tables");
      assertEquals(generator2.getTaskGenerationCount(), 0, "Controller 2's task shouldn't have passed");

      assertNotNull(scheduleTasksResult1);
      assertEquals(scheduleTasksResult1.size(), 1);

      assertNotNull(scheduleTasksResult2);
      assertEquals(scheduleTasksResult2.size(), 0);

      TaskSchedulingInfo taskSchedulingInfo = scheduleTasksResult1.get(TEST_TASK_TYPE);
      assertNotNull(taskSchedulingInfo);
      assertNotNull(taskSchedulingInfo.getScheduledTaskNames());
      assertNotNull(taskSchedulingInfo.getSchedulingErrors());
      assertNotNull(taskSchedulingInfo.getGenerationErrors());
      // Should see one task scheduled for both tables
      assertEquals(taskSchedulingInfo.getScheduledTaskNames().size(), 1);
      // Should see 0 errors
      assertEquals(taskSchedulingInfo.getGenerationErrors().size(), 0);
      assertEquals(taskSchedulingInfo.getSchedulingErrors().size(), 0);
    } finally {
      // Cleanup
      try {
        // Cancel all running tasks before dropping tables
        Map<String, TaskState> taskStates = _controllerStarter.getHelixTaskResourceManager()
            .getTaskStates(TEST_TASK_TYPE);
        for (String taskName : taskStates.keySet()) {
          try {
            _controllerStarter.getHelixTaskResourceManager().deleteTask(taskName, true);
          } catch (Exception e) {
            // Ignore individual task deletion errors
          }
        }
        Thread.sleep(1000); // Give time for task cancellation

        dropOfflineTable(RAW_TABLE_NAME_1);
        dropOfflineTable(RAW_TABLE_NAME_2);
        dropOfflineTable(RAW_TABLE_NAME_3);
      } catch (Exception ignored) {
      }

      // Stop second controller first
      controller2.stop();
    }
  }

  /**
   * Helper method to start a second controller instance on a different port
   */
  private BaseControllerStarter startControllerOnDifferentPort(Map<String, Object> properties) throws Exception {
    BaseControllerStarter controllerStarter = createControllerStarter();
    controllerStarter.init(new org.apache.pinot.spi.env.PinotConfiguration(properties));
    controllerStarter.start();
    return controllerStarter;
  }

  /**
   * Helper to create a single test table
   */
  private void createSingleTestTable(String rawTableName) throws Exception {
    Map<String, Map<String, String>> taskTypeConfigsMap = new HashMap<>();
    taskTypeConfigsMap.put(TEST_TASK_TYPE, Map.of("schedule", "0 */10 * ? * * *"));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(rawTableName)
        .setTaskConfig(new TableTaskConfig(taskTypeConfigsMap))
        .build();

    addTableConfig(tableConfig);
  }

  /**
   * Task generator with controllable behavior
   */
  private static class ControllableTaskGenerator implements PinotTaskGenerator {
    private final String _taskType;
    private int _taskGenerationCount = 0;
    private long _generationDelay = 0;

    public ControllableTaskGenerator(String taskType) {
      _taskType = taskType;
    }

    @Override
    public String getTaskType() {
      return _taskType;
    }

    @Override
    public void init(ClusterInfoAccessor clusterInfoAccessor) {
      // No-op
    }

    public void setGenerationDelay(long delayMs) {
      _generationDelay = delayMs;
    }

    public int getTaskGenerationCount() {
      return _taskGenerationCount;
    }

    @Override
    public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
      if (_generationDelay > 0) {
        try {
          Thread.sleep(_generationDelay);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      _taskGenerationCount++;
      List<PinotTaskConfig> tasks = new ArrayList<>();
      for (TableConfig tableConfig : tableConfigs) {
        Map<String, String> configs = new HashMap<>();
        configs.put("tableName", tableConfig.getTableName());
        tasks.add(new PinotTaskConfig(_taskType, configs));
      }
      return tasks;
    }

    @Override
    public List<PinotTaskConfig> generateTasks(TableConfig tableConfig, Map<String, String> taskConfigs) {
      if (_generationDelay > 0) {
        try {
          Thread.sleep(_generationDelay);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      _taskGenerationCount++;
      Map<String, String> configs = new HashMap<>();
      configs.put("tableName", tableConfig.getTableName());
      return Collections.singletonList(new PinotTaskConfig(_taskType, configs));
    }

    @Override
    public void generateTasks(List<TableConfig> tableConfigs, List<PinotTaskConfig> pinotTaskConfigs) {
      if (_generationDelay > 0) {
        try {
          Thread.sleep(_generationDelay);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      _taskGenerationCount++;
      Map<String, String> configs = new HashMap<>();
      configs.put("tableName", tableConfigs.get(0).getTableName());
      pinotTaskConfigs.add(new PinotTaskConfig(_taskType, configs));
    }
  }

  /**
   * Test scenario: Tests the forceReleaseLock API functionality.
   * Verifies that when forceReleaseLock is called during task execution,
   * the lock is released and another createTask can be started for the same table.
   */
  @Test
  public void testForceReleaseLockDuringTaskExecution() throws Exception {
    // Setup controller with distributed locking enabled
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DISTRIBUTED_LOCKING, true);

    startController(properties);
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();

    try {
      // Setup test environment
      addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
      addFakeServerInstancesToAutoJoinHelixCluster(1, true);
      addFakeMinionInstancesToAutoJoinHelixCluster(1);

      // Create and register a slow task generator that will hold the lock for a long time
      ControllableTaskGenerator slowGenerator = new ControllableTaskGenerator(TEST_TASK_TYPE);
      slowGenerator.setGenerationDelay(5000); // 5 second delay to simulate long-running task
      ClusterInfoAccessor clusterInfoAccessor = Mockito.mock(ClusterInfoAccessor.class);
      slowGenerator.init(clusterInfoAccessor);
      taskManager.registerTaskGenerator(slowGenerator);

      // Ensure task queue exists
      _controllerStarter.getHelixTaskResourceManager().ensureTaskQueueExists(TEST_TASK_TYPE);

      // Create schema and table
      Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME_1)
          .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
      addSchema(schema);
      createSingleTestTable(RAW_TABLE_NAME_1);

      String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_1);

      // Wait for controller to be fully initialized
      Thread.sleep(1000);

      ExecutorService executor = Executors.newFixedThreadPool(2);
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch firstTaskStarted = new CountDownLatch(1);
      CountDownLatch completionLatch = new CountDownLatch(2);

      AtomicInteger firstTaskCompleted = new AtomicInteger(0);
      AtomicInteger secondTaskCompleted = new AtomicInteger(0);
      AtomicInteger forceReleaseResult = new AtomicInteger(0);
      AtomicInteger forceReleaseResultException = new AtomicInteger(0);

      // First createTask - this will acquire the lock and run for 5 seconds
      executor.submit(() -> {
        try {
          startLatch.await();
          firstTaskStarted.countDown(); // Signal that first task has started
          Map<String, String> result = taskManager.createTask(TEST_TASK_TYPE, RAW_TABLE_NAME_1, null, new HashMap<>());
          if (result != null && !result.isEmpty()) {
            firstTaskCompleted.incrementAndGet();
          }
        } catch (Exception ignored) {
        } finally {
          completionLatch.countDown();
        }
      });

      // Second thread: Wait for first task to start, then force release lock and try another createTask
      executor.submit(() -> {
        try {
          startLatch.await();
          firstTaskStarted.await(); // Wait for first task to start
          Thread.sleep(1000); // Let first task run for a bit to ensure it has the lock

          // Force release the lock while first task is still running
          try {
            taskManager.forceReleaseLock(tableNameWithType);
            forceReleaseResult.incrementAndGet();
          } catch (Exception e) {
            forceReleaseResultException.incrementAndGet();
          }

          // Now try to create another task for the same table - this should succeed since lock was released
          Thread.sleep(500); // Small delay to ensure lock release is processed
          Map<String, String> result = taskManager.createTask(TEST_TASK_TYPE, RAW_TABLE_NAME_1, null, new HashMap<>());
          if (result != null && !result.isEmpty()) {
            secondTaskCompleted.incrementAndGet();
          }
        } catch (Exception ignored) {
        } finally {
          completionLatch.countDown();
        }
      });

      // Start both operations
      startLatch.countDown();

      // Wait for completion
      assertTrue(completionLatch.await(15, TimeUnit.SECONDS), "Tasks should complete within 15 seconds");

      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should terminate");

      // Verify results
      assertEquals(forceReleaseResult.get(), 1, "forceReleaseLock should succeed");
      assertEquals(forceReleaseResultException.get(), 0, "forceReleaseLock shouldn't throw exception");
      assertEquals(firstTaskCompleted.get(), 1, "First createTask should complete successfully");
      assertEquals(secondTaskCompleted.get(), 1, "Second createTask should succeed after lock was force released");

      // Verify that both tasks were generated (first task continues even after lock release)
      assertEquals(slowGenerator.getTaskGenerationCount(), 2, "Both createTask calls should have generated tasks");
    } finally {
      // Cleanup
      try {
        // Cancel all running tasks before dropping tables
        Map<String, TaskState> taskStates = _controllerStarter.getHelixTaskResourceManager()
            .getTaskStates(TEST_TASK_TYPE);
        for (String taskName : taskStates.keySet()) {
          try {
            _controllerStarter.getHelixTaskResourceManager().deleteTask(taskName, true);
          } catch (Exception e) {
            // Ignore individual task deletion errors
          }
        }
        Thread.sleep(1000); // Give time for task cancellation

        dropOfflineTable(RAW_TABLE_NAME_1);
      } catch (Exception ignored) {
      }
    }
  }

  /**
   * Test scenario: Tests the forceReleaseLock API functionality to ensure it throws exception if no lock is present
   */
  @Test
  public void testForceReleaseLockWhenNoLockExists() throws Exception {
    // Setup controller with distributed locking enabled
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DISTRIBUTED_LOCKING, true);

    startController(properties);
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();

    try {
      // Setup test environment
      addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
      addFakeServerInstancesToAutoJoinHelixCluster(1, true);
      addFakeMinionInstancesToAutoJoinHelixCluster(1);

      // Create and register a slow task generator that will hold the lock for a long time
      ControllableTaskGenerator slowGenerator = new ControllableTaskGenerator(TEST_TASK_TYPE);
      slowGenerator.setGenerationDelay(1000); // 1 second delay to simulate long-running task
      ClusterInfoAccessor clusterInfoAccessor = Mockito.mock(ClusterInfoAccessor.class);
      slowGenerator.init(clusterInfoAccessor);
      taskManager.registerTaskGenerator(slowGenerator);

      // Ensure task queue exists
      _controllerStarter.getHelixTaskResourceManager().ensureTaskQueueExists(TEST_TASK_TYPE);

      // Create schema and table
      Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME_1)
          .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
      addSchema(schema);
      createSingleTestTable(RAW_TABLE_NAME_1);

      String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_1);

      // Wait for controller to be fully initialized
      Thread.sleep(1000);

      ExecutorService executor = Executors.newFixedThreadPool(1);
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch completionLatch = new CountDownLatch(1);

      AtomicInteger firstTaskCompleted = new AtomicInteger(0);
      AtomicInteger forceReleaseResult = new AtomicInteger(0);
      AtomicInteger forceReleaseResultException = new AtomicInteger(0);

      // Force release lock and try a createTask
      executor.submit(() -> {
        try {
          startLatch.await();

          // Force release the lock while first task is still running
          try {
            taskManager.forceReleaseLock(tableNameWithType);
            forceReleaseResult.incrementAndGet();
          } catch (Exception e) {
            forceReleaseResultException.incrementAndGet();
          }

          // Now try to create another task for the same table - this should succeed since lock was released
          Thread.sleep(500); // Small delay to ensure lock release is processed
          Map<String, String> result = taskManager.createTask(TEST_TASK_TYPE, RAW_TABLE_NAME_1, null, new HashMap<>());
          if (result != null && !result.isEmpty()) {
            firstTaskCompleted.incrementAndGet();
          }
        } catch (Exception ignored) {
        } finally {
          completionLatch.countDown();
        }
      });

      // Start operation
      startLatch.countDown();

      // Wait for completion
      assertTrue(completionLatch.await(15, TimeUnit.SECONDS), "Tasks should complete within 15 seconds");

      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should terminate");

      // Verify results
      assertEquals(forceReleaseResult.get(), 0,
          "forceReleaseLock should have thrown exception as no lock should be held");
      assertEquals(forceReleaseResultException.get(), 1, "forceReleaseLock should throw an exception");
      assertEquals(firstTaskCompleted.get(), 1, "First createTask should complete successfully");

      // Verify that both tasks were generated (first task continues even after lock release)
      assertEquals(slowGenerator.getTaskGenerationCount(), 1, "Both createTask calls should have generated tasks");
    } finally {
      // Cleanup
      try {
        // Cancel all running tasks before dropping tables
        Map<String, TaskState> taskStates = _controllerStarter.getHelixTaskResourceManager()
            .getTaskStates(TEST_TASK_TYPE);
        for (String taskName : taskStates.keySet()) {
          try {
            _controllerStarter.getHelixTaskResourceManager().deleteTask(taskName, true);
          } catch (Exception e) {
            // Ignore individual task deletion errors
          }
        }
        Thread.sleep(1000); // Give time for task cancellation

        dropOfflineTable(RAW_TABLE_NAME_1);
      } catch (Exception ignored) {
      }
    }
  }
}
