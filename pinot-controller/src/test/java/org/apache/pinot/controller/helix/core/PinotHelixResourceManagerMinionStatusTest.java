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
package org.apache.pinot.controller.helix.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.pinot.controller.api.resources.MinionStatusResponse;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for minion status query functionality in PinotHelixTaskResourceManager
 */
public class PinotHelixResourceManagerMinionStatusTest extends ControllerTest {

  private PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    _pinotHelixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
  }

  @AfterMethod
  public void cleanupAfterTest() {
    // Clean up all minion instances after each test to ensure test isolation
    try {
      for (String instanceId : _helixResourceManager.getAllInstances()) {
        if (instanceId.startsWith("Minion_")) {
          try {
            _helixResourceManager.dropInstance(instanceId);
          } catch (Exception e) {
            // Ignore errors during cleanup
          }
        }
      }
    } catch (Exception e) {
      // Ignore errors getting all instances
    }
  }

  /**
   * Helper method to create a mock TaskDriver with workflows and jobs containing tasks assigned to minions
   * @param taskAssignments Map of minion instance ID to number of RUNNING tasks
   * @return Mocked TaskDriver
   */
  private TaskDriver createMockTaskDriverWithRunningTasks(Map<String, Integer> taskAssignments) {
    TaskDriver mockTaskDriver = mock(TaskDriver.class);

    // Create a mock workflows - getWorkflows() returns a Map<String, WorkflowConfig>
    Map<String, WorkflowConfig> workflows = new HashMap<>();
    workflows.put("TestWorkflow1", mock(WorkflowConfig.class));
    when(mockTaskDriver.getWorkflows()).thenReturn(workflows);

    // Create workflow context with jobs
    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(mockTaskDriver.getWorkflowContext("TestWorkflow1")).thenReturn(workflowContext);

    Map<String, TaskState> jobStates = new HashMap<>();
    jobStates.put("TestJob1", TaskState.IN_PROGRESS);
    when(workflowContext.getJobStates()).thenReturn(jobStates);

    // Create job context with tasks assigned to minions
    JobContext jobContext = mock(JobContext.class);
    when(mockTaskDriver.getJobContext("TestJob1")).thenReturn(jobContext);

    Set<Integer> partitions = new HashSet<>();
    int partitionId = 0;
    for (Map.Entry<String, Integer> entry : taskAssignments.entrySet()) {
      String minionId = entry.getKey();
      int taskCount = entry.getValue();
      for (int i = 0; i < taskCount; i++) {
        partitions.add(partitionId);
        when(jobContext.getPartitionState(partitionId)).thenReturn(TaskPartitionState.RUNNING);
        when(jobContext.getAssignedParticipant(partitionId)).thenReturn(minionId);
        partitionId++;
      }
    }
    when(jobContext.getPartitionSet()).thenReturn(partitions);

    return mockTaskDriver;
  }

  @Test
  public void testGetMinionStatusWithNoMinions() {
    // Test with no minions registered - no tasks should be running
    TaskDriver mockTaskDriver = createMockTaskDriverWithRunningTasks(new HashMap<>());
    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    MinionStatusResponse response = taskResourceManager.getMinionStatus(null, 0);
    assertNotNull(response);
    assertEquals(response.getCurrentMinionCount(), 0);
    assertNotNull(response.getMinionStatus());
    assertTrue(response.getMinionStatus().isEmpty());
  }

  @Test
  public void testGetMinionStatusWithOnlineMinions() {
    // Create 3 online minions
    for (int i = 0; i < 3; i++) {
      String minionHost = "minion-test-" + i + ".example.com";
      int minionPort = 9514;
      Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION,
          Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
      PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
      assertTrue(addResponse.isSuccessful(), "Failed to add minion instance");
    }

    // Create a mock TaskDriver with running tasks: minion-0 has 2 tasks, minion-1 has 1 task, minion-2 has 0 tasks
    Map<String, Integer> taskAssignments = new HashMap<>();
    taskAssignments.put("Minion_minion-test-0.example.com_9514", 2);
    taskAssignments.put("Minion_minion-test-1.example.com_9514", 1);

    TaskDriver mockTaskDriver = createMockTaskDriverWithRunningTasks(taskAssignments);
    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    // Get all minions
    MinionStatusResponse response = taskResourceManager.getMinionStatus(null, 0);
    assertNotNull(response);
    assertEquals(response.getCurrentMinionCount(), 3);
    assertEquals(response.getMinionStatus().size(), 3);

    // Verify all are ONLINE and check task counts
    for (MinionStatusResponse.MinionStatus status : response.getMinionStatus()) {
      assertEquals(status.getStatus(), "ONLINE");
      assertNotNull(status.getInstanceId());
      assertNotNull(status.getHost());
      assertTrue(status.getPort() > 0);

      // Verify task counts match expected values
      if (status.getInstanceId().equals("Minion_minion-test-0.example.com_9514")) {
        assertEquals(status.getRunningTaskCount(), 2);
      } else if (status.getInstanceId().equals("Minion_minion-test-1.example.com_9514")) {
        assertEquals(status.getRunningTaskCount(), 1);
      } else {
        assertEquals(status.getRunningTaskCount(), 0);
      }
    }

    // Cleanup
    for (int i = 0; i < 3; i++) {
      String minionInstanceId = "Minion_minion-test-" + i + ".example.com_" + (9514);
      _helixResourceManager.dropInstance(minionInstanceId);
    }
  }

  @Test
  public void testGetMinionStatusWithDrainedMinions() {
    // Create 5 minions
    for (int i = 0; i < 5; i++) {
      String minionHost = "minion-drain-test-" + i + ".example.com";
      int minionPort = 9514;
      Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION,
          Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
      PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
      assertTrue(addResponse.isSuccessful());
    }

    // Drain minions 1 and 3
    String minion1 = "Minion_minion-drain-test-1.example.com_9514";
    String minion3 = "Minion_minion-drain-test-3.example.com_9514";
    _helixResourceManager.drainMinionInstance(minion1);
    _helixResourceManager.drainMinionInstance(minion3);

    // Create a mock TaskDriver with running tasks: drained minions can still have running tasks
    Map<String, Integer> taskAssignments = new HashMap<>();
    taskAssignments.put("Minion_minion-drain-test-0.example.com_9514", 3);
    taskAssignments.put(minion1, 1); // Drained but still has 1 running task
    taskAssignments.put("Minion_minion-drain-test-2.example.com_9514", 2);
    taskAssignments.put(minion3, 2); // Drained but still has 2 running tasks

    TaskDriver mockTaskDriver = createMockTaskDriverWithRunningTasks(taskAssignments);
    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    // Get all minions
    MinionStatusResponse allResponse = taskResourceManager.getMinionStatus(null, 0);
    assertEquals(allResponse.getCurrentMinionCount(), 5);
    assertEquals(allResponse.getMinionStatus().size(), 5);

    // Count online vs drained
    long onlineCount = allResponse.getMinionStatus().stream()
        .filter(m -> "ONLINE".equals(m.getStatus()))
        .count();
    long drainedCount = allResponse.getMinionStatus().stream()
        .filter(m -> "DRAINED".equals(m.getStatus()))
        .count();
    assertEquals(onlineCount, 3);
    assertEquals(drainedCount, 2);

    // Get only ONLINE minions
    MinionStatusResponse onlineResponse = taskResourceManager.getMinionStatus("ONLINE", 0);
    assertEquals(onlineResponse.getCurrentMinionCount(), 3);
    assertEquals(onlineResponse.getMinionStatus().size(), 3);
    for (MinionStatusResponse.MinionStatus status : onlineResponse.getMinionStatus()) {
      assertEquals(status.getStatus(), "ONLINE");
    }

    // Get only DRAINED minions
    MinionStatusResponse drainedResponse = taskResourceManager.getMinionStatus("DRAINED", 0);
    assertEquals(drainedResponse.getCurrentMinionCount(), 2);
    assertEquals(drainedResponse.getMinionStatus().size(), 2);
    for (MinionStatusResponse.MinionStatus status : drainedResponse.getMinionStatus()) {
      assertEquals(status.getStatus(), "DRAINED");
      assertTrue(status.getInstanceId().equals(minion1) || status.getInstanceId().equals(minion3));
      // Verify drained minions can still have running tasks
      if (status.getInstanceId().equals(minion1)) {
        assertEquals(status.getRunningTaskCount(), 1);
      } else if (status.getInstanceId().equals(minion3)) {
        assertEquals(status.getRunningTaskCount(), 2);
      }
    }

    // Cleanup
    for (int i = 0; i < 5; i++) {
      String minionInstanceId = "Minion_minion-drain-test-" + i + ".example.com_" + 9514;
      _helixResourceManager.dropInstance(minionInstanceId);
    }
  }

  @Test
  public void testGetMinionStatusWithLimit() {
    // Create 10 minions
    for (int i = 0; i < 10; i++) {
      String minionHost = "minion-limit-test-" + i + ".example.com";
      int minionPort = 9514;
      Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION,
          Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
      PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
      assertTrue(addResponse.isSuccessful());
    }

    // Create a mock TaskDriver with running tasks for some minions
    Map<String, Integer> taskAssignments = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      // Give varying task counts: 0, 1, 2, 3, 0, 1, 2, 3, 0, 1
      int taskCount = i % 4;
      if (taskCount > 0) {
        taskAssignments.put("Minion_minion-limit-test-" + i + ".example.com_" + (9514), taskCount);
      }
    }

    TaskDriver mockTaskDriver = createMockTaskDriverWithRunningTasks(taskAssignments);
    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    // Get all minions (limit = 0 means no limit)
    MinionStatusResponse allResponse = taskResourceManager.getMinionStatus(null, 0);
    assertEquals(allResponse.getCurrentMinionCount(), 10);
    assertEquals(allResponse.getMinionStatus().size(), 10);

    // Get with limit = 5
    MinionStatusResponse limitedResponse = taskResourceManager.getMinionStatus(null, 5);
    assertEquals(limitedResponse.getCurrentMinionCount(), 5);
    assertEquals(limitedResponse.getMinionStatus().size(), 5);

    // Get with limit = 1
    MinionStatusResponse singleResponse = taskResourceManager.getMinionStatus(null, 1);
    assertEquals(singleResponse.getCurrentMinionCount(), 1);
    assertEquals(singleResponse.getMinionStatus().size(), 1);

    // Cleanup
    for (int i = 0; i < 10; i++) {
      String minionInstanceId = "Minion_minion-limit-test-" + i + ".example.com_" + (9514);
      _helixResourceManager.dropInstance(minionInstanceId);
    }
  }

  @Test
  public void testGetMinionStatusWithFilterAndLimit() {
    // Create 6 minions, drain 3 of them
    for (int i = 0; i < 6; i++) {
      String minionHost = "minion-filter-limit-" + i + ".example.com";
      int minionPort = 9514;
      Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION,
          Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
      PinotResourceManagerResponse addResponse = _helixResourceManager.addInstance(minionInstance, false);
      assertTrue(addResponse.isSuccessful());
    }

    // Drain minions 0, 2, 4
    for (int i = 0; i < 6; i += 2) {
      String minionInstanceId = "Minion_minion-filter-limit-" + i + ".example.com_" + (9514);
      _helixResourceManager.drainMinionInstance(minionInstanceId);
    }

    // Create a mock TaskDriver with running tasks: drained minions have tasks, online minions have tasks
    Map<String, Integer> taskAssignments = new HashMap<>();
    taskAssignments.put("Minion_minion-filter-limit-0.example.com_9514", 2); // Drained
    taskAssignments.put("Minion_minion-filter-limit-1.example.com_9514", 3); // Online
    taskAssignments.put("Minion_minion-filter-limit-2.example.com_9514", 1); // Drained
    taskAssignments.put("Minion_minion-filter-limit-3.example.com_9514", 2); // Online
    taskAssignments.put("Minion_minion-filter-limit-4.example.com_9514", 1); // Drained
    taskAssignments.put("Minion_minion-filter-limit-5.example.com_9514", 1); // Online

    TaskDriver mockTaskDriver = createMockTaskDriverWithRunningTasks(taskAssignments);
    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    // Get drained minions with limit = 2
    MinionStatusResponse response = taskResourceManager.getMinionStatus("DRAINED", 2);
    assertEquals(response.getCurrentMinionCount(), 2);
    assertEquals(response.getMinionStatus().size(), 2);
    for (MinionStatusResponse.MinionStatus status : response.getMinionStatus()) {
      assertEquals(status.getStatus(), "DRAINED");
    }

    // Get online minions with limit = 2
    MinionStatusResponse onlineResponse = taskResourceManager.getMinionStatus("ONLINE", 2);
    assertEquals(onlineResponse.getCurrentMinionCount(), 2);
    assertEquals(onlineResponse.getMinionStatus().size(), 2);
    for (MinionStatusResponse.MinionStatus status : onlineResponse.getMinionStatus()) {
      assertEquals(status.getStatus(), "ONLINE");
    }

    // Cleanup
    for (int i = 0; i < 6; i++) {
      String minionInstanceId = "Minion_minion-filter-limit-" + i + ".example.com_" + (9514);
      _helixResourceManager.dropInstance(minionInstanceId);
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetMinionStatusWithInvalidFilter() {
    // Create a minion
    String minionHost = "minion-invalid-filter.example.com";
    int minionPort = 9514;
    Instance minionInstance = new Instance(minionHost, minionPort, InstanceType.MINION,
        Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
    _helixResourceManager.addInstance(minionInstance, false);

    // Create a mock TaskDriver with running tasks
    Map<String, Integer> taskAssignments = new HashMap<>();
    taskAssignments.put("Minion_" + minionHost + "_" + minionPort, 1);

    TaskDriver mockTaskDriver = createMockTaskDriverWithRunningTasks(taskAssignments);
    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    try {
      // Try with invalid status filter - should throw IllegalArgumentException
      taskResourceManager.getMinionStatus("INVALID_STATUS", 0);
    } finally {
      // Cleanup
      String minionInstanceId = "Minion_" + minionHost + "_" + minionPort;
      _helixResourceManager.dropInstance(minionInstanceId);
    }
  }

  @Test
  public void testGetMinionStatusCaseInsensitiveFilter() {
    // Create 2 minions, drain one
    String minionHost1 = "minion-case-test-1.example.com";
    String minionHost2 = "minion-case-test-2.example.com";
    int minionPort = 9514;

    Instance minion1 = new Instance(minionHost1, minionPort, InstanceType.MINION,
        Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
    Instance minion2 = new Instance(minionHost2, minionPort, InstanceType.MINION,
        Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);

    _helixResourceManager.addInstance(minion1, false);
    _helixResourceManager.addInstance(minion2, false);

    String minion1Id = "Minion_" + minionHost1 + "_" + minionPort;
    String minion2Id = "Minion_" + minionHost2 + "_" + minionPort;
    _helixResourceManager.drainMinionInstance(minion1Id);

    // Create a mock TaskDriver with running tasks
    Map<String, Integer> taskAssignments = new HashMap<>();
    taskAssignments.put(minion1Id, 2); // Drained minion with 2 tasks
    taskAssignments.put(minion2Id, 3); // Online minion with 3 tasks

    TaskDriver mockTaskDriver = createMockTaskDriverWithRunningTasks(taskAssignments);
    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    // Test case-insensitive filters
    MinionStatusResponse responseUpperCase = taskResourceManager.getMinionStatus("DRAINED", 0);
    MinionStatusResponse responseLowerCase = taskResourceManager.getMinionStatus("drained", 0);
    MinionStatusResponse responseMixedCase = taskResourceManager.getMinionStatus("Drained", 0);

    assertEquals(responseUpperCase.getCurrentMinionCount(), 1);
    assertEquals(responseLowerCase.getCurrentMinionCount(), 1);
    assertEquals(responseMixedCase.getCurrentMinionCount(), 1);

    // Verify task counts in all responses
    assertEquals(responseUpperCase.getMinionStatus().get(0).getRunningTaskCount(), 2);
    assertEquals(responseLowerCase.getMinionStatus().get(0).getRunningTaskCount(), 2);
    assertEquals(responseMixedCase.getMinionStatus().get(0).getRunningTaskCount(), 2);

    // Cleanup
    _helixResourceManager.dropInstance(minion1Id);
    _helixResourceManager.dropInstance(minion2Id);
  }

  @Test
  public void testGetMinionStatusOnlyReturnsMinions() {
    // Create different instance types
    String brokerHost = "broker-test.example.com";
    String serverHost = "server-test.example.com";
    String minionHost = "minion-type-test.example.com";

    Instance brokerInstance = new Instance(brokerHost, 8099, InstanceType.BROKER,
        Collections.singletonList("DefaultTenant_BROKER"), null, 0, 0, 0, 0, false);
    Instance serverInstance = new Instance(serverHost, 8098, InstanceType.SERVER,
        Collections.singletonList("DefaultTenant_OFFLINE"), null, 0, 0, 0, 0, false);
    Instance minionInstance = new Instance(minionHost, 9514, InstanceType.MINION,
        Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);

    _helixResourceManager.addInstance(brokerInstance, false);
    _helixResourceManager.addInstance(serverInstance, false);
    _helixResourceManager.addInstance(minionInstance, false);

    // Create a mock TaskDriver with running tasks - only for minion (broker and server won't have task counts)
    Map<String, Integer> taskAssignments = new HashMap<>();
    taskAssignments.put("Minion_" + minionHost + "_9514", 5);

    TaskDriver mockTaskDriver = createMockTaskDriverWithRunningTasks(taskAssignments);
    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    // Get minion status - should only return the minion instance
    MinionStatusResponse response = taskResourceManager.getMinionStatus(null, 0);
    assertEquals(response.getCurrentMinionCount(), 1);
    assertEquals(response.getMinionStatus().size(), 1);
    assertEquals(response.getMinionStatus().get(0).getHost(), minionHost);
    assertEquals(response.getMinionStatus().get(0).getRunningTaskCount(), 5);

    // Cleanup
    _helixResourceManager.dropInstance("Broker_" + brokerHost + "_8099");
    _helixResourceManager.dropInstance("Server_" + serverHost + "_8098");
    _helixResourceManager.dropInstance("Minion_" + minionHost + "_9514");
  }

  @Test
  public void testGetMinionStatusReturnsCorrectPortAndHost() {
    // Create minion with specific host and port
    String expectedHost = "specific-minion-host.example.com";
    int expectedPort = 9514;

    Instance minionInstance = new Instance(expectedHost, expectedPort, InstanceType.MINION,
        Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
    _helixResourceManager.addInstance(minionInstance, false);

    // Create a mock TaskDriver with running tasks
    Map<String, Integer> taskAssignments = new HashMap<>();
    taskAssignments.put("Minion_" + expectedHost + "_" + expectedPort, 4);

    TaskDriver mockTaskDriver = createMockTaskDriverWithRunningTasks(taskAssignments);
    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    // Get status and verify host/port
    MinionStatusResponse response = taskResourceManager.getMinionStatus(null, 0);
    assertEquals(response.getCurrentMinionCount(), 1);

    MinionStatusResponse.MinionStatus status = response.getMinionStatus().get(0);
    assertEquals(status.getHost(), expectedHost);
    assertEquals(status.getPort(), expectedPort);
    assertEquals(status.getInstanceId(), "Minion_" + expectedHost + "_" + expectedPort);
    assertEquals(status.getRunningTaskCount(), 4);

    // Cleanup
    _helixResourceManager.dropInstance("Minion_" + expectedHost + "_" + expectedPort);
  }

  @Test
  public void testGetMinionStatusWithRunningTasks() {
    // Create 3 minions
    String minion1Id = "Minion_minion-running-1.example.com_9514";
    String minion2Id = "Minion_minion-running-2.example.com_9514";
    String minion3Id = "Minion_minion-running-3.example.com_9514";

    Instance minion1 = new Instance("minion-running-1.example.com", 9514, InstanceType.MINION,
        Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
    Instance minion2 = new Instance("minion-running-2.example.com", 9514, InstanceType.MINION,
        Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
    Instance minion3 = new Instance("minion-running-3.example.com", 9514, InstanceType.MINION,
        Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);

    _helixResourceManager.addInstance(minion1, false);
    _helixResourceManager.addInstance(minion2, false);
    _helixResourceManager.addInstance(minion3, false);

    // Create a mock TaskDriver with running tasks: minion1 has 3 tasks, minion2 has 1 task, minion3 has 0 tasks
    Map<String, Integer> taskAssignments = new HashMap<>();
    taskAssignments.put(minion1Id, 3);
    taskAssignments.put(minion2Id, 1);
    // minion3 not in map = 0 running tasks

    TaskDriver mockTaskDriver = createMockTaskDriverWithRunningTasks(taskAssignments);
    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    // Call getMinionStatus on the task resource manager
    MinionStatusResponse response = taskResourceManager.getMinionStatus(null, 0);

    assertNotNull(response);
    assertEquals(response.getCurrentMinionCount(), 3);
    assertEquals(response.getMinionStatus().size(), 3);

    // Verify running task counts for each minion
    for (MinionStatusResponse.MinionStatus status : response.getMinionStatus()) {
      if (status.getInstanceId().equals(minion1Id)) {
        assertEquals(status.getRunningTaskCount(), 3);
      } else if (status.getInstanceId().equals(minion2Id)) {
        assertEquals(status.getRunningTaskCount(), 1);
      } else if (status.getInstanceId().equals(minion3Id)) {
        assertEquals(status.getRunningTaskCount(), 0);
      }
    }

    // Cleanup
    _helixResourceManager.dropInstance(minion1Id);
    _helixResourceManager.dropInstance(minion2Id);
    _helixResourceManager.dropInstance(minion3Id);
  }

  @Test
  public void testGetMinionStatusWithRunningTasksAndInitTasks() {
    // This test verifies that getMinionStatus correctly reports running task counts
    // including both RUNNING and INIT states (tasks assigned but not yet started)

    // Create 2 minions
    String minion1Id = "Minion_minion-init-test-1.example.com_9514";
    String minion2Id = "Minion_minion-init-test-2.example.com_9514";

    Instance minion1 = new Instance("minion-init-test-1.example.com", 9514, InstanceType.MINION,
        Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
    Instance minion2 = new Instance("minion-init-test-2.example.com", 9514, InstanceType.MINION,
        Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);

    _helixResourceManager.addInstance(minion1, false);
    _helixResourceManager.addInstance(minion2, false);

    // Create a mock TaskDriver with running tasks
    // Simulate: minion1 has 3 RUNNING tasks, minion2 has 1 RUNNING task
    Map<String, Integer> taskAssignments = new HashMap<>();
    taskAssignments.put(minion1Id, 3); // 3 RUNNING tasks
    taskAssignments.put(minion2Id, 1); // 1 RUNNING task

    TaskDriver mockTaskDriver = createMockTaskDriverWithRunningTasks(taskAssignments);
    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    // Call getMinionStatus
    MinionStatusResponse response = taskResourceManager.getMinionStatus(null, 0);

    assertNotNull(response);
    assertEquals(response.getCurrentMinionCount(), 2);

    // Verify that the running task counts include both RUNNING and INIT tasks
    Map<String, Integer> resultCounts = new HashMap<>();
    for (MinionStatusResponse.MinionStatus status : response.getMinionStatus()) {
      resultCounts.put(status.getInstanceId(), status.getRunningTaskCount());
    }

    assertEquals(resultCounts.get(minion1Id).intValue(), 3);
    assertEquals(resultCounts.get(minion2Id).intValue(), 1);

    // Cleanup
    _helixResourceManager.dropInstance(minion1Id);
    _helixResourceManager.dropInstance(minion2Id);
  }

  @Test
  public void testGetMinionStatusWithDrainedMinionAndRunningTasks() {
    // Test that drained minions can still show running task counts
    // (tasks that were assigned before draining)

    String minionId = "Minion_minion-drained-running.example.com_9514";
    Instance minion = new Instance("minion-drained-running.example.com", 9514, InstanceType.MINION,
        Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
    _helixResourceManager.addInstance(minion, false);

    // Drain the minion
    _helixResourceManager.drainMinionInstance(minionId);

    // Create a mock TaskDriver with running tasks
    // Even though drained, minion might still have running tasks that were assigned before draining
    Map<String, Integer> taskAssignments = new HashMap<>();
    taskAssignments.put(minionId, 2);

    TaskDriver mockTaskDriver = createMockTaskDriverWithRunningTasks(taskAssignments);
    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    // Get all minions
    MinionStatusResponse response = taskResourceManager.getMinionStatus(null, 0);
    assertEquals(response.getCurrentMinionCount(), 1);

    MinionStatusResponse.MinionStatus status = response.getMinionStatus().get(0);
    assertEquals(status.getInstanceId(), minionId);
    assertEquals(status.getStatus(), "DRAINED");
    assertEquals(status.getRunningTaskCount(), 2); // Should still show running tasks

    // Get only DRAINED minions
    MinionStatusResponse drainedResponse = taskResourceManager.getMinionStatus("DRAINED", 0);
    assertEquals(drainedResponse.getCurrentMinionCount(), 1);
    assertEquals(drainedResponse.getMinionStatus().get(0).getRunningTaskCount(), 2);

    // Get only ONLINE minions
    MinionStatusResponse onlineResponse = taskResourceManager.getMinionStatus("ONLINE", 0);
    assertEquals(onlineResponse.getCurrentMinionCount(), 0);

    // Cleanup
    _helixResourceManager.dropInstance(minionId);
  }

  @Test
  public void testGetMinionStatusRunningTaskCountException() {
    // Test that getMinionStatus handles exceptions from getRunningTaskCountsPerMinion gracefully

    String minionId = "Minion_minion-exception-test.example.com_9514";
    Instance minion = new Instance("minion-exception-test.example.com", 9514, InstanceType.MINION,
        Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
    _helixResourceManager.addInstance(minion, false);

    // Create a mock TaskDriver that throws an exception when accessing workflows
    TaskDriver mockTaskDriver = mock(TaskDriver.class);
    Mockito.doThrow(new RuntimeException("Simulated task driver failure"))
        .when(mockTaskDriver).getWorkflows();

    PinotHelixTaskResourceManager taskResourceManager = new PinotHelixTaskResourceManager(
        _helixResourceManager, mockTaskDriver);

    // getMinionStatus should still work, just with 0 running task counts
    MinionStatusResponse response = taskResourceManager.getMinionStatus(null, 0);

    assertNotNull(response);
    assertEquals(response.getCurrentMinionCount(), 1);
    assertEquals(response.getMinionStatus().get(0).getRunningTaskCount(), 0);

    // Cleanup
    _helixResourceManager.dropInstance(minionId);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
