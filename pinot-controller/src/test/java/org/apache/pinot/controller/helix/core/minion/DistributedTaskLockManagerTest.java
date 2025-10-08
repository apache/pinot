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

import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DistributedTaskLockManagerTest {

  @Test
  public void testEphemeralLockAcquisitionAndRelease() {
    // Mock the property store and data accessor
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);

    // Configure mocks for ephemeral node creation
    when(mockPropertyStore.exists(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    when(mockPropertyStore.create(anyString(), any(ZNRecord.class), eq(AccessOption.EPHEMERAL))).thenReturn(true);
    when(mockPropertyStore.remove(anyString(), eq(AccessOption.EPHEMERAL))).thenReturn(true);

    DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, "controller1");

    // Test lock acquisition
    DistributedTaskLockManager.TaskLock lock = lockManager.acquireLock("testTable");
    Assert.assertNotNull(lock, "Should successfully acquire lock");
    assertEquals(lock.getOwner(), "controller1");
    Assert.assertNotNull(lock.getLockZNodePath(), "Lock ZNode path should not be null");
    assertEquals(lock.getLockZNodePath(), "/MINION_TASK_METADATA/testTable-Lock");
    assertTrue(lock.getAge() >= 0, "Lock should have valid age");

    // Test lock release
    when(mockPropertyStore.exists(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL))).thenReturn(true);
    boolean released = lockManager.releaseLock(lock);
    Assert.assertTrue(released, "Should successfully release lock");

    // Verify ephemeral node interactions
    verify(mockPropertyStore, times(1)).create(eq(lock.getLockZNodePath()), any(ZNRecord.class),
        eq(AccessOption.EPHEMERAL));
    verify(mockPropertyStore, times(1)).remove(eq(lock.getLockZNodePath()),
        eq(AccessOption.EPHEMERAL));
  }

  @Test
  public void testConcurrentEphemeralLockAcquisition() {
    // Mock the property store and data accessor
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);

    // Configure mocks to simulate another controller already has the lock
    when(mockPropertyStore.exists(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    when(mockPropertyStore.create(anyString(), any(ZNRecord.class), eq(AccessOption.EPHEMERAL))).thenReturn(false);
    when(mockPropertyStore.remove(anyString(), eq(AccessOption.EPHEMERAL))).thenReturn(true);

    DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, "controller1");

    // Test lock acquisition should fail because create returned false
    DistributedTaskLockManager.TaskLock lock = lockManager.acquireLock("testTable");
    Assert.assertNull(lock, "Should fail to acquire lock when create returns false");

    // Verify that create was called. No need to clean-up the lock so remove should not have been called
    verify(mockPropertyStore, times(1)).create(anyString(), any(ZNRecord.class),
        eq(AccessOption.EPHEMERAL));
    verify(mockPropertyStore, times(0)).remove(anyString(), eq(AccessOption.EPHEMERAL));
  }

  @Test
  public void testTaskGenerationInProgressDetection() {
    // Mock the property store and data accessor
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);

    String controllerId = "controller1";

    // Construct a record to return when propertyStore.get() is called on the lock ZNode
    ZNRecord record = new ZNRecord(controllerId);
    record.setSimpleField("lockCreationTimeMs", String.valueOf(System.currentTimeMillis()));
    record.setSimpleField("lockOwner", controllerId);

    // Simulate active ephemeral nodes indicating task generation in progress
    when(mockPropertyStore.exists(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    when(mockPropertyStore.get(contains("testTable"), any(Stat.class), eq(AccessOption.EPHEMERAL))).thenReturn(record);

    DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, controllerId);

    // Test that we can detect task generation in progress
    boolean inProgress = lockManager.isTaskGenerationInProgress("testTable");
    Assert.assertTrue(inProgress, "Should detect task generation in progress when ephemeral nodes exist");

    // Test that we detect that the task generation is not in progress when we don't have any task lock
    when(mockPropertyStore.get(contains("testTable"), any(Stat.class), eq(AccessOption.EPHEMERAL))).thenReturn(null);
    inProgress = lockManager.isTaskGenerationInProgress("testTable");
    Assert.assertFalse(inProgress, "Should detect task generation in not progress when ephemeral nodes don't exist");
  }

  @Test
  public void testMinionTaskGenerationLockHeldMetric() {
    // Mock the property store
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);

    // Mock ControllerMetrics to verify metric calls
    ControllerMetrics mockControllerMetrics = mock(ControllerMetrics.class);

    String controllerId = "controller1";
    String tableNameWithType = "testTable";

    // Test case 1: Lock is held (record exists with creation time)
    long lockCreationTime = System.currentTimeMillis() - 5000; // 5 seconds ago
    ZNRecord lockRecord = new ZNRecord(controllerId);
    lockRecord.setSimpleField("lockCreationTimeMs", String.valueOf(lockCreationTime));
    lockRecord.setSimpleField("lockOwner", controllerId);

    when(mockPropertyStore.get(contains(tableNameWithType), any(Stat.class), eq(AccessOption.EPHEMERAL)))
        .thenReturn(lockRecord);

    // Mock ControllerMetrics.get() to return our mock
    try (var mockedStatic = mockStatic(ControllerMetrics.class)) {
      mockedStatic.when(ControllerMetrics::get).thenReturn(mockControllerMetrics);

      DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, controllerId);

      // Call the method that should update the metric
      boolean inProgress = lockManager.isTaskGenerationInProgress(tableNameWithType);
      Assert.assertTrue(inProgress, "Should detect task generation in progress when lock record exists");

      // Verify that addTimedValue was called with a non-zero duration (approximately 5000ms)
      verify(mockControllerMetrics, times(1)).addTimedValue(
          eq(tableNameWithType),
          eq(ControllerTimer.MINION_TASK_GENERATION_LOCK_HELD_ELAPSED_TIME_MS),
          anyLong(), // We can't predict the exact value due to timing, but it should be > 0
          eq(TimeUnit.MILLISECONDS)
      );
    }

    // Test case 2: No lock held (record is null)
    when(mockPropertyStore.get(contains(tableNameWithType), any(Stat.class), eq(AccessOption.EPHEMERAL)))
        .thenReturn(null);

    try (var mockedStatic = mockStatic(ControllerMetrics.class)) {
      mockedStatic.when(ControllerMetrics::get).thenReturn(mockControllerMetrics);

      DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, controllerId);

      // Call the method that should update the metric
      boolean inProgress = lockManager.isTaskGenerationInProgress(tableNameWithType);
      Assert.assertFalse(inProgress, "Should detect task generation not in progress when no lock record exists");


      // Verify that addTimedValue was called with 0 duration (no lock held)
      verify(mockControllerMetrics, times(1)).addTimedValue(
          eq(tableNameWithType),
          eq(ControllerTimer.MINION_TASK_GENERATION_LOCK_HELD_ELAPSED_TIME_MS),
          eq(0L), // Should be 0 when no lock is held
          eq(TimeUnit.MILLISECONDS)
      );
    }
  }

  @Test
  public void testLockReleaseRetriesOnFailure() throws InterruptedException {
    // Mock the property store
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);
    String controllerId = "controller1";
    String tableNameWithType = "testTable";

    // Configure mocks for lock acquisition
    when(mockPropertyStore.exists(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    when(mockPropertyStore.create(anyString(), any(ZNRecord.class), eq(AccessOption.EPHEMERAL))).thenReturn(true);
    // Mock isTaskGenerationInProgress to return null (no task in progress)
    when(mockPropertyStore.get(anyString(), any(Stat.class), eq(AccessOption.EPHEMERAL))).thenReturn(null);

    DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, controllerId);

    // Acquire a lock first
    DistributedTaskLockManager.TaskLock lock = lockManager.acquireLock(tableNameWithType);
    Assert.assertNotNull(lock, "Should successfully acquire lock");

    // Configure mock for lock release - simulate failure on first 2 attempts, success on 3rd attempt
    when(mockPropertyStore.exists(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL)))
        .thenReturn(true); // Lock exists for all attempts

    when(mockPropertyStore.remove(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL)))
        .thenReturn(false) // First attempt fails
        .thenReturn(false) // Second attempt fails
        .thenReturn(true); // Third attempt succeeds

    // Record start time to verify retry delays
    long startTime = System.currentTimeMillis();

    // Test lock release with retries
    boolean released = lockManager.releaseLock(lock);

    long elapsedTime = System.currentTimeMillis() - startTime;

    // Verify that the lock was eventually released
    Assert.assertTrue(released, "Should successfully release lock after retries");

    // Verify that remove was called 3 times (2 failures + 1 success)
    verify(mockPropertyStore, times(3)).remove(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL));

    // Verify that the total time includes retry delays
    // Expected delays: 100ms + 200ms = 300ms minimum (plus some execution overhead)
    Assert.assertTrue(elapsedTime >= 250,
        "Should have waited for retry delays. Elapsed time: " + elapsedTime + "ms");

    // Should not take too long (max 3 seconds for safety, accounting for test environment variability)
    Assert.assertTrue(elapsedTime < 3000,
        "Should not take too long. Elapsed time: " + elapsedTime + "ms");
  }

  @Test
  public void testLockReleaseFailsAfterMaxRetries() {
    // Mock the property store
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);
    String controllerId = "controller1";
    String tableNameWithType = "testTable";

    // Configure mocks for lock acquisition
    when(mockPropertyStore.exists(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    when(mockPropertyStore.create(anyString(), any(ZNRecord.class), eq(AccessOption.EPHEMERAL))).thenReturn(true);
    // Mock isTaskGenerationInProgress to return null (no task in progress)
    when(mockPropertyStore.get(anyString(), any(Stat.class), eq(AccessOption.EPHEMERAL))).thenReturn(null);

    DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, controllerId);

    // Acquire a lock first
    DistributedTaskLockManager.TaskLock lock = lockManager.acquireLock(tableNameWithType);
    Assert.assertNotNull(lock, "Should successfully acquire lock");

    // Configure mock for lock release - simulate failure on all attempts
    when(mockPropertyStore.exists(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL)))
        .thenReturn(true); // Lock exists for all attempts

    when(mockPropertyStore.remove(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL)))
        .thenReturn(false); // All attempts fail

    // Test lock release with retries - should fail after max retries
    boolean released = lockManager.releaseLock(lock);

    // Verify that the lock release failed
    Assert.assertFalse(released, "Should fail to release lock after max retries");

    // Verify that remove was called exactly MAX_RETRIES times (3 times)
    verify(mockPropertyStore, times(3)).remove(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL));
  }

  @Test
  public void testLockReleaseSucceedsWhenLockDisappearsBeforeRetry() {
    // Mock the property store
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);
    String controllerId = "controller1";
    String tableNameWithType = "testTable";

    // Configure mocks for lock acquisition
    when(mockPropertyStore.exists(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    when(mockPropertyStore.create(anyString(), any(ZNRecord.class), eq(AccessOption.EPHEMERAL))).thenReturn(true);
    // Mock isTaskGenerationInProgress to return null (no task in progress)
    when(mockPropertyStore.get(anyString(), any(Stat.class), eq(AccessOption.EPHEMERAL))).thenReturn(null);

    DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, controllerId);

    // Acquire a lock first
    DistributedTaskLockManager.TaskLock lock = lockManager.acquireLock(tableNameWithType);
    Assert.assertNotNull(lock, "Should successfully acquire lock");

    // Configure mock for lock release - simulate:
    // 1. First exists() call in releaseLock(): lock exists, so it calls removeWithRetries()
    // 2. Second exists() call in removeWithRetries(): lock no longer exists (disappeared between attempts)
    when(mockPropertyStore.exists(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL)))
        .thenReturn(true)  // First check in releaseLock(): lock exists
        .thenReturn(false); // Second check in removeWithRetries(): lock disappeared

    // Since the lock disappears, remove() should never be called

    // Test lock release - should succeed when lock disappears
    boolean released = lockManager.releaseLock(lock);

    // Verify that the lock release succeeded
    Assert.assertTrue(released, "Should succeed when lock disappears between retry attempts");

    // Verify that remove was never called because lock disappeared before retry
    verify(mockPropertyStore, times(0)).remove(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL));

    // Verify that exists was called twice (releaseLock() + removeWithRetries() first attempt)
    verify(mockPropertyStore, times(2)).exists(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL));
  }

  @Test
  public void testLockReleaseWithMultipleExistsChecks() {
    // Mock the property store
    ZkHelixPropertyStore<ZNRecord> mockPropertyStore = mock(ZkHelixPropertyStore.class);
    String controllerId = "controller1";
    String tableNameWithType = "testTable";

    // Configure mocks for lock acquisition
    when(mockPropertyStore.exists(anyString(), eq(AccessOption.PERSISTENT))).thenReturn(true);
    when(mockPropertyStore.create(anyString(), any(ZNRecord.class), eq(AccessOption.EPHEMERAL))).thenReturn(true);
    // Mock isTaskGenerationInProgress to return null (no task in progress)
    when(mockPropertyStore.get(anyString(), any(Stat.class), eq(AccessOption.EPHEMERAL))).thenReturn(null);

    DistributedTaskLockManager lockManager = new DistributedTaskLockManager(mockPropertyStore, controllerId);

    // Acquire a lock first
    DistributedTaskLockManager.TaskLock lock = lockManager.acquireLock(tableNameWithType);
    Assert.assertNotNull(lock, "Should successfully acquire lock");

    // Configure mock for lock release - simulate:
    // 1. First exists() call in releaseLock(): lock exists
    // 2. Second exists() call in removeWithRetries() attempt 1: lock exists, remove fails
    // 3. Third exists() call in removeWithRetries() attempt 2: lock exists, remove succeeds
    when(mockPropertyStore.exists(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL)))
        .thenReturn(true)  // First check in releaseLock(): lock exists
        .thenReturn(true)  // Second check in removeWithRetries() attempt 1: lock exists
        .thenReturn(true); // Third check in removeWithRetries() attempt 2: lock exists

    when(mockPropertyStore.remove(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL)))
        .thenReturn(false) // First remove attempt fails
        .thenReturn(true); // Second remove attempt succeeds

    // Test lock release - should succeed after retry
    boolean released = lockManager.releaseLock(lock);

    // Verify that the lock release succeeded
    Assert.assertTrue(released, "Should succeed after retry");

    // Verify that remove was called twice (first fails, second succeeds)
    verify(mockPropertyStore, times(2)).remove(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL));

    // Verify that exists was called 3 times (releaseLock() + removeWithRetries() attempt 1 + attempt 2)
    verify(mockPropertyStore, times(3)).exists(eq(lock.getLockZNodePath()), eq(AccessOption.EPHEMERAL));
  }
}
