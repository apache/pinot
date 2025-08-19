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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MinionTaskGenerationLockManagerTest extends ControllerTest {
  private static final String TABLE_NAME_WITH_TYPE = "testTable_OFFLINE";
  private static final String TASK_TYPE = "TestTaskType";

  private MinionTaskGenerationLockManager _lockManager;
  private PinotHelixResourceManager _helixResourceManager;
  private HelixPropertyStore<ZNRecord> _propertyStore;

  @BeforeMethod
  public void setUp() throws Exception {
    startZk();
    startController();
    _helixResourceManager = _controllerStarter.getHelixResourceManager();
    _propertyStore = _helixResourceManager.getPropertyStore();
    _lockManager = new MinionTaskGenerationLockManager(_helixResourceManager);
  }

  @AfterMethod
  public void tearDown() {
    stopController();
    stopZk();
  }

  @Test
  public void testSuccessfulLockAcquisitionAndExecution() throws Exception {
    AtomicBoolean taskExecuted = new AtomicBoolean(false);

    Callable<Void> task = () -> {
      taskExecuted.set(true);
      return null;
    };

    _lockManager.generateWithLock(TABLE_NAME_WITH_TYPE, TASK_TYPE, task);

    Assert.assertTrue(taskExecuted.get());

    String lockPath = ZKMetadataProvider.constructPropertyStorePathForMinionTaskGenerationLock(
        TABLE_NAME_WITH_TYPE, TASK_TYPE);
    ZNRecord lockRecord = _propertyStore.get(lockPath, null, AccessOption.PERSISTENT);
    Assert.assertNull(lockRecord);
  }

  @Test
  public void testLockReleaseOnTaskException() {
    Callable<Void> failingTask = () -> {
      throw new RuntimeException("Task failed");
    };

    try {
      _lockManager.generateWithLock(TABLE_NAME_WITH_TYPE, TASK_TYPE, failingTask);
      Assert.fail("Expected RuntimeException");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Task failed");
    }

    String lockPath = ZKMetadataProvider.constructPropertyStorePathForMinionTaskGenerationLock(
        TABLE_NAME_WITH_TYPE, TASK_TYPE);
    ZNRecord lockRecord = _propertyStore.get(lockPath, null, AccessOption.PERSISTENT);
    Assert.assertNull(lockRecord);
  }

  @Test
  public void testConcurrentLockAcquisitionFailure() throws Exception {
    String lockPath = ZKMetadataProvider.constructPropertyStorePathForMinionTaskGenerationLock(
        TABLE_NAME_WITH_TYPE, TASK_TYPE);

    ZNRecord existingLock = new ZNRecord("existing-lock");
    existingLock.setLongField(MinionTaskGenerationLockManager.ACQUIRED_AT, System.currentTimeMillis());
    _propertyStore.create(lockPath, existingLock, AccessOption.PERSISTENT);

    AtomicBoolean taskExecuted = new AtomicBoolean(false);
    Callable<Void> task = () -> {
      taskExecuted.set(true);
      return null;
    };

    try {
      _lockManager.generateWithLock(TABLE_NAME_WITH_TYPE, TASK_TYPE, task);
      Assert.fail("Expected RuntimeException for lock acquisition failure");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("Unable to acquire task generation lock"));
      Assert.assertFalse(taskExecuted.get());
    }

    _propertyStore.remove(lockPath, AccessOption.PERSISTENT);
  }

  @Test
  public void testExpiredLockCleanupAndAcquisition() throws Exception {
    String lockPath = ZKMetadataProvider.constructPropertyStorePathForMinionTaskGenerationLock(
        TABLE_NAME_WITH_TYPE, TASK_TYPE);

    long expiredTime = System.currentTimeMillis() - MinionTaskGenerationLockManager.TASK_GENERATION_LOCK_TTL - 1000;
    ZNRecord expiredLock = new ZNRecord("expired-lock");
    expiredLock.setLongField(MinionTaskGenerationLockManager.ACQUIRED_AT, expiredTime);
    _propertyStore.create(lockPath, expiredLock, AccessOption.PERSISTENT);

    AtomicBoolean taskExecuted = new AtomicBoolean(false);
    Callable<Void> task = () -> {
      taskExecuted.set(true);
      return null;
    };

    _lockManager.generateWithLock(TABLE_NAME_WITH_TYPE, TASK_TYPE, task);

    Assert.assertTrue(taskExecuted.get());

    ZNRecord lockRecord = _propertyStore.get(lockPath, null, AccessOption.PERSISTENT);
    Assert.assertNull(lockRecord);
  }

  @Test
  public void testLockTTLBoundary() throws Exception {
    String lockPath = ZKMetadataProvider.constructPropertyStorePathForMinionTaskGenerationLock(
        TABLE_NAME_WITH_TYPE, TASK_TYPE);

    long boundaryTime = System.currentTimeMillis() - MinionTaskGenerationLockManager.TASK_GENERATION_LOCK_TTL + 1000;
    ZNRecord boundaryLock = new ZNRecord("boundary-lock");
    boundaryLock.setLongField(MinionTaskGenerationLockManager.ACQUIRED_AT, boundaryTime);
    _propertyStore.create(lockPath, boundaryLock, AccessOption.PERSISTENT);

    AtomicBoolean taskExecuted = new AtomicBoolean(false);
    Callable<Void> task = () -> {
      taskExecuted.set(true);
      return null;
    };

    try {
      _lockManager.generateWithLock(TABLE_NAME_WITH_TYPE, TASK_TYPE, task);
      Assert.fail("Expected RuntimeException for non-expired lock");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("Unable to acquire task generation lock"));
      Assert.assertFalse(taskExecuted.get());
    }

    _propertyStore.remove(lockPath, AccessOption.PERSISTENT);
  }

  @Test
  public void testLockWithMissingAcquiredAtField() throws Exception {
    String lockPath = ZKMetadataProvider.constructPropertyStorePathForMinionTaskGenerationLock(
        TABLE_NAME_WITH_TYPE, TASK_TYPE);

    ZNRecord lockWithoutTimestamp = new ZNRecord("no-timestamp-lock");
    _propertyStore.create(lockPath, lockWithoutTimestamp, AccessOption.PERSISTENT);

    AtomicBoolean taskExecuted = new AtomicBoolean(false);
    Callable<Void> task = () -> {
      taskExecuted.set(true);
      return null;
    };

    _lockManager.generateWithLock(TABLE_NAME_WITH_TYPE, TASK_TYPE, task);

    Assert.assertTrue(taskExecuted.get());

    ZNRecord lockRecord = _propertyStore.get(lockPath, null, AccessOption.PERSISTENT);
    Assert.assertNull(lockRecord);
  }

  @Test
  public void testMultipleTablesAndTaskTypes() throws Exception {
    String table1 = "table1_OFFLINE";
    String table2 = "table2_OFFLINE";
    String taskType1 = "TaskType1";
    String taskType2 = "TaskType2";

    AtomicBoolean task1Executed = new AtomicBoolean(false);
    AtomicBoolean task2Executed = new AtomicBoolean(false);
    AtomicBoolean task3Executed = new AtomicBoolean(false);
    AtomicBoolean task4Executed = new AtomicBoolean(false);

    Callable<Void> task1 = getCallable(task1Executed);
    Callable<Void> task2 = getCallable(task2Executed);
    Callable<Void> task3 = getCallable(task3Executed);
    Callable<Void> task4 = getCallable(task4Executed);

    ExecutorService executorService = Executors.newFixedThreadPool(4);
    List<Future> futures = new ArrayList<>(4);
    generateTask(futures, executorService, table1, taskType1, task1);
    generateTask(futures, executorService, table1, taskType2, task2);
    generateTask(futures, executorService, table2, taskType1, task3);
    generateTask(futures, executorService, table2, taskType2, task4);
    for (Future future : futures) {
      future.get();
    }
    Assert.assertTrue(task1Executed.get());
    Assert.assertTrue(task2Executed.get());
    Assert.assertTrue(task3Executed.get());
    Assert.assertTrue(task4Executed.get());
  }

  private static Callable<Void> getCallable(AtomicBoolean task1Executed) {
    return () -> {
      Thread.sleep(System.currentTimeMillis() % 10);
      task1Executed.set(true);
      return null;
    };
  }

  private void generateTask(List<Future> futures, ExecutorService executorService, String table1, String taskType1,
      Callable<Void> task1) {
    futures.add(executorService.submit(() -> {
      try {
        _lockManager.generateWithLock(table1, taskType1, task1);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }));
  }
}
