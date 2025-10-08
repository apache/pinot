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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages distributed locks for minion task generation using ZooKeeper ephemeral nodes that automatically disappear
 * when the controller session ends or when the lock is explicitly released. This approach provides automatic cleanup
 * and is suitable for long-running task generation.
 * Locks are at the table level, to ensure that only one type of task can be generated per table at any given time.
 * This is to prevent task types which shouldn't run in parallel from being generated at the same time.
 * <p>
 * ZK EPHEMERAL Lock Node:
 *   <ul>
 *     <li>Every lock is created at the table level with the name: {tableName}-Lock, under the base path
 *     MINION_TASK_METADATA within the PROPERTYSTORE.
 *     <li>If the propertyStore::create() call returns true, that means the lock node was successfully created and the
 *     lock belongs to the current controller, otherwise it was not. If the lock node already exists, this will return
 *     false. No clean-up of the lock node is needed if the propertyStore::create() call returns false.
 *     <li>The locks are EPHEMERAL in nature, meaning that once the session with ZK is lost, the lock is automatically
 *     cleaned up. Scenarios when the ZK session can be lost: a) controller shutdown, b) controller crash, c) ZK session
 *     expiry (e.g. long GC pauses can cause this). This property helps ensure that the lock is released under
 *     controller failure.
 *   </ul>
 * <p>
 */
public class DistributedTaskLockManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedTaskLockManager.class);

  // Lock paths are constructed using ZKMetadataProvider
  private static final String LOCK_OWNER_KEY = "lockOwner";
  private static final String LOCK_CREATION_TIME_MS = "lockCreationTimeMs";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _controllerInstanceId;
  private final ControllerMetrics _controllerMetrics;

  public DistributedTaskLockManager(ZkHelixPropertyStore<ZNRecord> propertyStore, String controllerInstanceId) {
    _propertyStore = propertyStore;
    _controllerInstanceId = controllerInstanceId;
    _controllerMetrics = ControllerMetrics.get();
  }

  /**
   * Attempts to acquire a distributed lock at the table level for task generation using session-based locking.
   * The lock is at the table level instead of the task level to ensure that only a single task can be generated for
   * a given table at any time. Certain tasks depend on other tasks not being generated at the same time
   * The lock is held until explicitly released or the controller session ends.
   *
   * @param tableNameWithType the table name with type
   * @return TaskLock object if successful, null if lock could not be acquired
   */
  @Nullable
  public TaskLock acquireLock(String tableNameWithType) {
    LOGGER.info("Attempting to acquire task generation lock for table: {} by controller: {}", tableNameWithType,
        _controllerInstanceId);

    try {
      // Check if task generation is already in progress
      if (isTaskGenerationInProgress(tableNameWithType)) {
        LOGGER.info("Task generation already in progress for: {} by this or another controller", tableNameWithType);
        return null;
      }

      // Try to acquire the lock using ephemeral node
      TaskLock lock = tryAcquireSessionBasedLock(tableNameWithType);
      if (lock != null) {
        LOGGER.info("Successfully acquired task generation lock for table: {} by controller: {}", tableNameWithType,
            _controllerInstanceId);
        return lock;
      } else {
        LOGGER.warn("Could not acquire lock for table: {} - another controller must hold it", tableNameWithType);
      }
    } catch (Exception e) {
      LOGGER.error("Error while trying to acquire task lock for table: {}", tableNameWithType, e);
    }
    return null;
  }

  private String getLockPath(String tableNameForPath) {
    return ZKMetadataProvider.constructPropertyStorePathForMinionTaskGenerationLock(tableNameForPath);
  }

  /**
   * Releases a previously acquired session-based lock and marks task generation as completed.
   *
   * @param lock the lock to release
   * @return true if successfully released, false otherwise
   */
  public boolean releaseLock(TaskLock lock) {
    if (lock == null) {
      return true;
    }

    String tableNameWithType = lock.getTableNameWithType();
    String lockNode = lock.getLockZNodePath();

    // Remove the ephemeral lock node
    boolean status = true;
    if (lockNode != null) {
      try {
        if (_propertyStore.exists(lockNode, AccessOption.EPHEMERAL)) {
          status = _propertyStore.remove(lockNode, AccessOption.EPHEMERAL);
          LOGGER.info("Tried to removed ephemeral lock node: {}, for table: {} by controller: {}, removal success: {}",
              lockNode, tableNameWithType, _controllerInstanceId, status);
        } else {
          LOGGER.warn("Ephemeral lock node: {} does not exist for table: {}, nothing to remove",
              lockNode, tableNameWithType);
        }
      } catch (Exception e) {
        status = false;
        LOGGER.warn("Exception while trying to remove ephemeral lock node: {}", lockNode, e);
      }
    } else {
      LOGGER.warn("Lock node path seems to be null for task lock: {}, treating release as a no-op", lock);
    }

    return status;
  }

  /**
   * Force release the lock without checking if any tasks are in progress
   */
  public void forceReleaseLock(String tableNameWithType) {
    LOGGER.info("Trying to force release the lock for table: {}", tableNameWithType);
    String lockPath = getLockPath(tableNameWithType);

    if (!_propertyStore.exists(lockPath, AccessOption.EPHEMERAL)) {
      String message = "No lock ZNode: " + lockPath + " found for table: " + tableNameWithType
          + ", nothing to force release";
      LOGGER.warn(message);
      throw new RuntimeException(message);
    }

    LOGGER.info("Lock for table: {} found at path: {}, trying to remove", tableNameWithType, lockPath);
    boolean result = _propertyStore.remove(lockPath, AccessOption.EPHEMERAL);
    if (!result) {
      String message = "Could not force release lock: " + lockPath + " for table: " + tableNameWithType;
      LOGGER.error(message);
      throw new RuntimeException(message);
    }
  }

  /**
   * Checks if any task generation is currently in progress for the given table.
   *
   * @param tableNameWithType the table name with type
   * @return true if task generation is in progress, false otherwise
   */
  @VisibleForTesting
  boolean isTaskGenerationInProgress(String tableNameWithType) {
    String lockPath = getLockPath(tableNameWithType);

    try {
      long durationLockHeldMs = 0;
      Stat stat = new Stat();
      // Get the node instead of checking for existence to update the time held metric in case the node exists
      ZNRecord zNRecord = _propertyStore.get(lockPath, stat, AccessOption.EPHEMERAL);
      if (zNRecord != null) {
        String creationTimeMsString = zNRecord.getSimpleField(LOCK_CREATION_TIME_MS);
        long creationTimeMs = stat.getCtime();
        if (creationTimeMsString != null) {
          try {
            creationTimeMs = Long.parseLong(creationTimeMsString);
          } catch (NumberFormatException e) {
            LOGGER.warn("Could not parse creationTimeMs string: {} into long from ZNode, using ZNode creation time",
                creationTimeMsString);
            creationTimeMs = stat.getCtime();
          }
        }
        durationLockHeldMs = System.currentTimeMillis() - creationTimeMs;
      }
      _controllerMetrics.addTimedValue(tableNameWithType,
          ControllerTimer.MINION_TASK_GENERATION_LOCK_HELD_ELAPSED_TIME_MS, durationLockHeldMs, TimeUnit.MILLISECONDS);
      return zNRecord != null;
    } catch (Exception e) {
      LOGGER.error("Error checking task generation status for: {} with lock path: {}", tableNameWithType, lockPath, e);
      return false;
    }
  }

  /**
   * Attempts to acquire a lock using ephemeral nodes.
   */
  @VisibleForTesting
  TaskLock tryAcquireSessionBasedLock(String tableNameWithType) {
    String lockPath = getLockPath(tableNameWithType);

    try {
      long currentTimeMs = System.currentTimeMillis();

      // Create ephemeral node for this table, owned by this controller
      ZNRecord lockRecord = new ZNRecord(_controllerInstanceId);
      lockRecord.setSimpleField(LOCK_OWNER_KEY, _controllerInstanceId);
      lockRecord.setSimpleField(LOCK_CREATION_TIME_MS, String.valueOf(currentTimeMs));

      boolean created = _propertyStore.create(lockPath, lockRecord, AccessOption.EPHEMERAL);

      if (created) {
        LOGGER.info("Successfully created lock node at path: {}, for controller: {}, for table: {}", lockPath,
            _controllerInstanceId, tableNameWithType);
        return new TaskLock(tableNameWithType, _controllerInstanceId, currentTimeMs, lockPath);
      }

      // We could not create the lock node, returning null
      LOGGER.warn("Could not create lock node at path: {} for controller: {}, for table: {}", lockPath,
          _controllerInstanceId, tableNameWithType);
    } catch (Exception e) {
      LOGGER.error("Error creating lock under path: {}, for controller: {}, for table: {}", lockPath,
          _controllerInstanceId, tableNameWithType, e);
    }

    return null;
  }

  /**
   * Represents a session-based distributed lock for task generation.
   * The lock is automatically released when the controller session ends.
   * The state node is periodically cleaned up
   */
  public static class TaskLock {
    private final String _tableNameWithType;
    private final String _owner;
    private final long _creationTimeMs;
    private final String _lockZNodePath; // Path to the ephemeral lock node

    public TaskLock(String tableNameWithType, String owner, long creationTimeMs, String lockZNodePath) {
      _tableNameWithType = tableNameWithType;
      _owner = owner;
      _creationTimeMs = creationTimeMs;
      _lockZNodePath = lockZNodePath;
    }

    public String getTableNameWithType() {
      return _tableNameWithType;
    }

    public String getOwner() {
      return _owner;
    }

    public long getCreationTimeMs() {
      return _creationTimeMs;
    }

    public String getLockZNodePath() {
      return _lockZNodePath;
    }

    public long getAge() {
      return System.currentTimeMillis() - _creationTimeMs;
    }

    @Override
    public String toString() {
      return "TaskLock{tableNameWithType='" + _tableNameWithType + "', owner='" + _owner + "', creationTimeMs="
          + _creationTimeMs + ", age=" + getAge() + ", lockZNodePath='" + _lockZNodePath + "'}";
    }
  }
}
