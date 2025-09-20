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

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DistributedTaskLockManagerTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testEphemeralLockAcquisitionAndRelease() {
    // Mock the property store and data accessor
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);

    // Define the specific UUID to use for the lock
    UUID expectedUuid = UUID.fromString("123e4567-e89b-42d3-a456-426614174000");

    // Configure mocks for ephemeral sequential node creation
    when(mockPropertyStore.exists(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    when(mockPropertyStore.create(anyString(), any(ZNRecord.class), eq(AccessOption.EPHEMERAL_SEQUENTIAL)))
        .thenReturn(true);
    // Use the expectedUuid in the lock name to be returned
    // Return just one controller, with the first sequence number
    when(mockPropertyStore.getChildNames(anyString(), eq(AccessOption.PERSISTENT)))
        .thenReturn(Arrays.asList("controller1-" + expectedUuid + "-lock-0000000001"));
    when(mockPropertyStore.remove(anyString(), eq(AccessOption.EPHEMERAL))).thenReturn(true);
    when(mockPropertyStore.set(anyString(), any(ZNRecord.class), eq(AccessOption.PERSISTENT))).thenReturn(true);

    DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, "controller1");

    try (MockedStatic<UUID> mockedUuid = mockStatic(UUID.class)) {
      // Configure the mock to return the specific UUID when randomUUID() is called
      mockedUuid.when(UUID::randomUUID).thenReturn(expectedUuid);

      // Test lock acquisition
      DistributedTaskLockManager.TaskLock lock = lockManager.acquireLock("testTable");
      Assert.assertNotNull(lock, "Should successfully acquire lock");
      assertEquals(lock.getOwner(), "controller1");
      assertTrue(lock.getAge() >= 0, "Lock should have valid age");

      // Test lock release
      boolean released = lockManager.releaseLock(lock, true);
      Assert.assertTrue(released, "Should successfully release lock");
    }

    // Verify ephemeral node interactions
    verify(mockPropertyStore, times(1)).create(anyString(), any(ZNRecord.class),
        eq(AccessOption.EPHEMERAL_SEQUENTIAL));
    verify(mockPropertyStore, times(1)).remove(anyString(), eq(AccessOption.EPHEMERAL));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConcurrentEphemeralLockAcquisition() {
    // Mock the property store and data accessor
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);

    // Define the specific UUID to use for the lock
    UUID expectedUuid1 = UUID.fromString("123e4567-e89b-42d3-a456-426614174000");
    UUID expectedUuid2 = UUID.fromString("543e4239-e89b-53d4-b474-24423374adf6");

    // Configure mocks to simulate another controller already has the lock
    when(mockPropertyStore.exists(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    when(mockPropertyStore.create(anyString(), any(ZNRecord.class), eq(AccessOption.EPHEMERAL_SEQUENTIAL)))
        .thenReturn(true);
    // Use the expectedUuid in the lock name to be returned
    // Have controller2 have the lower sequence number
    when(mockPropertyStore.getChildNames(anyString(), eq(AccessOption.PERSISTENT)))
        .thenReturn(Arrays.asList("controller2-" + expectedUuid2 + "-lock-0000000001",
            "controller1-" + expectedUuid1 + "-lock-0000000002"));
    when(mockPropertyStore.remove(anyString(), eq(AccessOption.EPHEMERAL))).thenReturn(true);

    DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, "controller1");

    try (MockedStatic<UUID> mockedUuid = mockStatic(UUID.class)) {
      // Configure the mock to return the specific UUID when randomUUID() is called
      mockedUuid.when(UUID::randomUUID).thenReturn(expectedUuid1);

      // Test lock acquisition should fail because controller2 has lower sequence number
      DistributedTaskLockManager.TaskLock lock = lockManager.acquireLock("testTable");
      Assert.assertNull(lock, "Should fail to acquire lock when another controller has lower sequence number");
    }

    // Verify that we cleaned up our node after failing to get the lock
    verify(mockPropertyStore, times(1)).create(anyString(), any(ZNRecord.class),
        eq(AccessOption.EPHEMERAL_SEQUENTIAL));
    verify(mockPropertyStore, times(1)).remove(anyString(), eq(AccessOption.EPHEMERAL));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEphemeralNodeAutomaticCleanup() {
    // Mock the property store and data accessor
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);

    // Define the specific UUID to use for the lock
    UUID expectedUuid = UUID.fromString("123e4567-e89b-42d3-a456-426614174000");

    // Simulate a scenario where ephemeral nodes from dead sessions are automatically cleaned up
    when(mockPropertyStore.exists(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    // First return: No existing locks - dead sessions cleaned up automatically
    // Then return: lock which we created with provided UUID
    when(mockPropertyStore.getChildNames(anyString(), eq(AccessOption.PERSISTENT)))
        .thenReturn(Collections.emptyList())
        .thenReturn(Arrays.asList("controller1-" + expectedUuid + "-lock-0000000001"));
    when(mockPropertyStore.create(anyString(), any(ZNRecord.class), eq(AccessOption.EPHEMERAL_SEQUENTIAL)))
        .thenReturn(true);

    when(mockPropertyStore.set(anyString(), any(ZNRecord.class), eq(AccessOption.PERSISTENT))).thenReturn(true);

    DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, "controller1");

    try (MockedStatic<UUID> mockedUuid = mockStatic(UUID.class)) {
      // Configure the mock to return the specific UUID when randomUUID() is called
      mockedUuid.when(UUID::randomUUID).thenReturn(expectedUuid);

      // Test that we can acquire lock when no other ephemeral nodes exist (automatic cleanup)
      DistributedTaskLockManager.TaskLock lock = lockManager.acquireLock("testTable");
      Assert.assertNotNull(lock, "Should successfully acquire lock when dead sessions are automatically cleaned up");
      assertEquals(lock.getOwner(), "controller1");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTaskGenerationInProgressDetection() {
    // Mock the property store and data accessor
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);

    // Define the specific UUID to use for the lock
    UUID expectedUuid = UUID.fromString("123e4567-e89b-42d3-a456-426614174000");

    // Simulate active ephemeral nodes indicating task generation in progress
    when(mockPropertyStore.exists(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    // Use the expectedUuid in the lock name to be returned
    when(mockPropertyStore.getChildNames(anyString(), eq(AccessOption.PERSISTENT)))
        .thenReturn(Arrays.asList("controller2-" + expectedUuid + "-lock-0000000001"));
    when(mockPropertyStore.exists(contains("controller2-" + expectedUuid + "-lock-0000000001"),
        eq(AccessOption.EPHEMERAL))).thenReturn(true);

    DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, "controller1");

    // Test that we can detect task generation in progress
    boolean inProgress = lockManager.isTaskGenerationInProgress("testTable");
    Assert.assertTrue(inProgress, "Should detect task generation in progress when ephemeral nodes exist");

    // Also create a state node that matches the lock
    ZNRecord staleState = new ZNRecord("testTable");
    staleState.setSimpleField("status", "IN_PROGRESS");
    staleState.setSimpleField("startTimeMillis", String.valueOf(System.currentTimeMillis())); // 24 hours ago
    staleState.setSimpleField("lockPath", "/MINION_TASK_METADATA/testTable-Lock/controller2-" + expectedUuid
        + "-lock-0000000001");
    when(mockPropertyStore.get(anyString(), any(), eq(AccessOption.PERSISTENT))).thenReturn(staleState);

    // Test that we can detect task generation in progress
    inProgress = lockManager.isTaskGenerationInProgress("testTable");
    Assert.assertTrue(inProgress, "Should detect task generation in progress when ephemeral nodes exist");

    // Should detect task generation is completed if state ZNode indicates completed / failed
    staleState.setSimpleField("status", "COMPLETED");
    when(mockPropertyStore.get(anyString(), any(), eq(AccessOption.PERSISTENT))).thenReturn(staleState);
    inProgress = lockManager.isTaskGenerationInProgress("testTable");
    Assert.assertFalse(inProgress, "Should detect task generation is not in progress when ephemeral nodes "
        + "exist but state node indicates completed and matches ephemeral node");

    staleState.setSimpleField("status", "FAILED");
    when(mockPropertyStore.get(anyString(), any(), eq(AccessOption.PERSISTENT))).thenReturn(staleState);
    inProgress = lockManager.isTaskGenerationInProgress("testTable");
    Assert.assertFalse(inProgress, "Should detect task generation is not in progress when ephemeral nodes "
        + "exist but state node indicates completed and matches ephemeral node");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLockManagerStateCleanup() {
    // Mock the property store and data accessor
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);

    // Define the specific UUID to use for the lock
    UUID expectedUuid = UUID.fromString("123e4567-e89b-42d3-a456-426614174000");

    // Configure mocks for state cleanup
    when(mockPropertyStore.exists(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    when(mockPropertyStore.getChildNames(anyString(), eq(AccessOption.PERSISTENT)))
        .thenReturn(Arrays.asList("testTable-State"));

    // Create a stale state record
    ZNRecord staleState = new ZNRecord("testTable");
    staleState.setSimpleField("status", "COMPLETED");
    // Set a time larger than the 24 hours staleness timeout
    staleState.setSimpleField("startTimeMillis", String.valueOf(System.currentTimeMillis() - 86410000));
    staleState.setSimpleField("lockPath", "/MINION_TASK_METADATA/testTable-Lock/controller2-" + expectedUuid
        + "-lock-0000000001");
    when(mockPropertyStore.get(anyString(), any(), eq(AccessOption.PERSISTENT))).thenReturn(staleState);
    when(mockPropertyStore.remove(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);

    DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, "controller1");

    // Test state cleanup
    lockManager.cleanupStaleStates();

    // Verify that stale state was removed
    verify(mockPropertyStore, times(1)).remove(anyString(), eq(AccessOption.PERSISTENT));
  }
}
