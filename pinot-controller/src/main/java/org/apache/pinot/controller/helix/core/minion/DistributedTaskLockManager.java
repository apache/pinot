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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages distributed locks for minion task generation using ZooKeeper ephemeral sequential nodes.
 * Uses ephemeral nodes that automatically disappear when the controller session ends.
 * This approach provides automatic cleanup and is suitable for long-running task generation.
 * Locks are held until explicitly released or the controller session terminates.
 * Locks are at the table level, to ensure that only one type of task can be generated per table at any given time.
 */
public class DistributedTaskLockManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedTaskLockManager.class);

  // Lock and state paths are constructed using ZKMetadataProvider
  private static final String LOCK_SUFFIX = "-Lock";
  private static final String STATE_SUFFIX = "-State";
  private static final String LOCK_OWNER_KEY = "lockOwner";
  private static final String LOCK_PATH_KEY = "lockPath";
  private static final String LOCK_UUID_KEY = "lockUuid";
  private static final String LOCK_TIMESTAMP_MILLIS_KEY = "lockTimestampMillis";
  private static final String TASK_GENERATION_STATUS_KEY = "status";
  private static final String TASK_GENERATION_START_TIME_MILLIS_KEY = "startTimeMillis";
  private static final String TASK_GENERATION_COMPLETION_TIME_MILLIS_KEY = "completionTimeMillis";
  private static final long STALE_THRESHOLD_MILLIS = 24 * 60 * 60 * 1000L; // 24 hours;

  // Task generation states
  private enum Status {
    // IN_PROGRESS if the task generation is currently in progress;
    // COMPLETED if the task generation completed successfully;
    // FAILED if the task generation failed.
    IN_PROGRESS, COMPLETED, FAILED
  }

  // Define a custom comparator to compare strings of format '<controllerName>-lock-<sequenceNumber>' and sort them by
  // the sequence number at the end
  private static final Comparator<String> TASK_LOCK_SEQUENCE_ID_COMPARATOR = (s1, s2) -> {
    // Regex to find the trailing sequence of digits
    Pattern p = Pattern.compile("\\d+$");

    // Extract the number from the first string
    Matcher m1 = p.matcher(s1);
    long num1 = m1.find() ? Long.parseLong(m1.group()) : 0;

    // Extract the number from the second string
    Matcher m2 = p.matcher(s2);
    long num2 = m2.find() ? Long.parseLong(m2.group()) : 0;

    return Long.compare(num1, num2);
  };

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _controllerInstanceId;

  public DistributedTaskLockManager(ZkHelixPropertyStore<ZNRecord> propertyStore, String controllerInstanceId) {
    _propertyStore = propertyStore;
    _controllerInstanceId = controllerInstanceId;

    // Ensure base paths exist
    ensureBasePaths();
  }

  /**
   * Attempts to acquire a distributed lock for task generation using session-based locking.
   * The lock is held until explicitly released or the controller session ends.
   * The lock is created at the table level
   *
   * @param tableName the table name (can be null for all-table operations)
   * @return TaskLock object if successful, null if lock could not be acquired
   */
  @Nullable
  public TaskLock acquireLock(@Nullable String tableName) {
    String tableNameForPath = (tableName != null) ? tableName : "ALL_TABLES";
    String lockBasePath = getLockBasePath(tableNameForPath);
    String statePath = lockBasePath.replace(LOCK_SUFFIX, STATE_SUFFIX);

    LOGGER.info("Attempting to acquire task generation lock: {} by controller: {}", tableNameForPath,
        _controllerInstanceId);

    try {
      // Check if task generation is already in progress
      if (isTaskGenerationInProgress(tableNameForPath, lockBasePath, statePath)) {
        LOGGER.info("Task generation already in progress for: {} by this or another controller", tableNameForPath);
        return null;
      }

      // Try to acquire the lock using ephemeral sequential node
      TaskLock lock = tryAcquireSessionBasedLock(lockBasePath, statePath, tableNameForPath);
      if (lock != null) {
        LOGGER.info("Successfully acquired task generation lock: {} by controller: {}", tableNameForPath,
            _controllerInstanceId);
        return lock;
      } else {
        LOGGER.warn("Could not acquire lock: {} - another controller holds it", tableNameForPath);
        return null;
      }
    } catch (Exception e) {
      LOGGER.error("Error while trying to acquire lock: {}", tableNameForPath, e);
      return null;
    }
  }

  private String getLockBasePath(String tableNameForPath) {
    return ZKMetadataProvider.constructPropertyStorePathForMinionTaskGenerationLock(tableNameForPath);
  }

  private String getBasePath() {
    return ZKMetadataProvider.getPropertyStorePathForMinionTaskMetadataPrefix();
  }

  /**
   * Releases a lock assuming successful completion.
   */
  public boolean releaseLock(TaskLock lock) {
    return releaseLock(lock, true);
  }

  /**
   * Releases a previously acquired session-based lock and marks task generation as completed.
   *
   * @param lock the lock to release
   * @param success whether task generation completed successfully
   * @return true if successfully released, false otherwise
   */
  public boolean releaseLock(TaskLock lock, boolean success) {
    if (lock == null) {
      return true;
    }

    String lockKey = lock.getLockKey();

    try {
      // Mark task generation as completed/failed
      markTaskGenerationCompleted(lock.getStatePath(), lockKey, success);

      // Remove the ephemeral lock node
      if (lock.getLockNodePath() != null) {
        try {
          boolean status = _propertyStore.remove(lock.getLockNodePath(), AccessOption.EPHEMERAL);
          LOGGER.info("Removed ephemeral lock node: {}, removal success: {}", lock.getLockNodePath(), status);
        } catch (Exception e) {
          // Lock node might have already been removed due to session timeout - this is OK
          LOGGER.warn("Ephemeral lock node already removed or session expired: {}", lock.getLockNodePath(), e);
        }
      }

      LOGGER.info("Successfully released task generation lock: {} by controller: {} (success: {})", lockKey,
          _controllerInstanceId, success);
      return true;
    } catch (Exception e) {
      LOGGER.error("Error while releasing lock: {}", lockKey, e);
      return false;
    }
  }

  /**
   * Force release the lock without checking if any tasks are in progress
   */
  public boolean forceReleaseLock(String tableNameWithType) {
    LOGGER.info("Trying to force release the lock for table: {}", tableNameWithType);
    String lockBasePath = getLockBasePath(tableNameWithType);

    boolean released = true;
    if (_propertyStore.exists(lockBasePath, AccessOption.PERSISTENT)) {
      List<String> lockNodes = _propertyStore.getChildNames(lockBasePath, AccessOption.PERSISTENT);
      if (lockNodes != null && !lockNodes.isEmpty()) {
        // There are active ephemeral lock nodes, check if any are still valid and delete them
        for (String nodeName : lockNodes) {
          String nodePath = lockBasePath + "/" + nodeName;
          if (_propertyStore.exists(nodePath, AccessOption.EPHEMERAL)) {
            LOGGER.info("Lock for table: {} found at path: {}, trying to remove", tableNameWithType, nodePath);
            boolean result = _propertyStore.remove(nodePath, AccessOption.EPHEMERAL);
            if (!result) {
              LOGGER.warn("Could not force release lock: {}", nodePath);
              released = false;
            }
          }
        }
      } else {
        LOGGER.info("No locks to force release, no child lock ZNodes found for table: {} under base: {}",
            tableNameWithType, lockBasePath);
      }
    } else {
      LOGGER.info("No locks to force release, no base lock ZNode: {} found for table: {}", lockBasePath,
          tableNameWithType);
    }
    return released;
  }

  /**
   * Checks if task generation is currently in progress for the given task type and table.
   *
   * @param tableName the table name
   * @return true if task generation is in progress, false otherwise
   */
  @VisibleForTesting
  boolean isTaskGenerationInProgress(@Nullable String tableName) {
    String tableNameForPath = (tableName != null) ? tableName : "ALL_TABLES";
    String lockBasePath = getLockBasePath(tableNameForPath);
    String statePath = lockBasePath.replace(LOCK_SUFFIX, STATE_SUFFIX);
    return isTaskGenerationInProgress(tableNameForPath, lockBasePath, statePath);
  }

  /**
   * Internal method to check if task generation is in progress for a lock key.
   */
  private boolean isTaskGenerationInProgress(String lockKey, String lockBasePath, String statePath) {
    try {
      // Check if there are any active ephemeral lock nodes
      if (_propertyStore.exists(lockBasePath, AccessOption.PERSISTENT)) {
        List<String> lockNodes = _propertyStore.getChildNames(lockBasePath, AccessOption.PERSISTENT);
        if (lockNodes != null && !lockNodes.isEmpty()) {
          // There are active ephemeral lock nodes, check if any are still valid
          boolean anyEphemeralLockExists = false;
          for (String nodeName : lockNodes) {
            String nodePath = lockBasePath + "/" + nodeName;
            if (_propertyStore.exists(nodePath, AccessOption.EPHEMERAL)) {
              anyEphemeralLockExists = true;
              // Ephemeral node exists, meaning session is still alive and task could be in progress (we update the
              // state to COMPLETED / FAILED before removing the lock, so better to double-check the task generation
              // state if the lock exists)
              ZNRecord stateRecord = _propertyStore.get(statePath, null, AccessOption.PERSISTENT);
              if (stateRecord != null) {
                String status = stateRecord.getSimpleField(TASK_GENERATION_STATUS_KEY);
                String lockPath = stateRecord.getSimpleField(LOCK_PATH_KEY);
                if (lockPath != null && lockPath.equals(nodePath)) {
                  return Status.IN_PROGRESS.name().equals(status);
                }
              }
            }
          }

          // If we cannot find a matching statePath, but found valid locks, return true just in case the state path
          // wasn't created yet
          return anyEphemeralLockExists;
        }
      }

      return false;
    } catch (Exception e) {
      LOGGER.error("Error checking task generation status for: {}", lockKey, e);
      return false;
    }
  }

  /**
   * Cleans up stale task generation state records.
   * Since we use session-based locking, expired locks clean themselves up automatically.
   * This method only cleans up stale state records.
   */
  public void cleanupStaleStates() {
    try {
      LOGGER.info("Start cleaning up stale states");
      // Get the base path for minion task metadata
      String basePath = getBasePath();
      if (!_propertyStore.exists(basePath, AccessOption.PERSISTENT)) {
        return;
      }

      // Traverse table directories under the base path
      List<String> tableNameEntries = _propertyStore.getChildNames(basePath, AccessOption.PERSISTENT);
      if (tableNameEntries == null) {
        return;
      }

      long currentTimeMs = System.currentTimeMillis();

      for (String tableNameEntry : tableNameEntries) {
        try {
          if (tableNameEntry.endsWith(STATE_SUFFIX)) {
            String statePath = basePath + "/" + tableNameEntry;
            ZNRecord stateRecord = _propertyStore.get(statePath, null, AccessOption.PERSISTENT);

            if (stateRecord != null) {
              String status = stateRecord.getSimpleField(TASK_GENERATION_STATUS_KEY);
              String startTimeMsStr = stateRecord.getSimpleField(TASK_GENERATION_START_TIME_MILLIS_KEY);

              if (startTimeMsStr != null) {
                long startTimeMs = Long.parseLong(startTimeMsStr);
                boolean isStale = (currentTimeMs - startTimeMs) > STALE_THRESHOLD_MILLIS;

                int lastIndexOfTableName = tableNameEntry.lastIndexOf(STATE_SUFFIX);

                // Clean up completed/failed states older than threshold, or in-progress states without active locks
                if ((Status.COMPLETED.name().equals(status) || Status.FAILED.name().equals(status)) && isStale) {
                  _propertyStore.remove(statePath, AccessOption.PERSISTENT);
                  LOGGER.info("Cleaned up stale table state: {} (status: {}, age: {}ms)",
                      tableNameEntry, status, currentTimeMs - startTimeMs);
                } else if (Status.IN_PROGRESS.name().equals(status)
                    && !hasActiveLocks(tableNameEntry.substring(0, lastIndexOfTableName))) {
                  // In-progress state without active locks - likely a dead session
                  _propertyStore.remove(statePath, AccessOption.PERSISTENT);
                  LOGGER.info("Cleaned up orphaned in-progress task state: {}, for table: {}", tableNameEntry,
                      tableNameEntry.substring(0, lastIndexOfTableName));
                }
              }
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Error cleaning up state for table: {}", tableNameEntry, e);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error during state cleanup", e);
    }
  }

  /**
   * Attempts to acquire a lock using ephemeral sequential nodes.
   * Uses the ZooKeeper recipe for distributed locking with automatic cleanup.
   */
  @VisibleForTesting
  TaskLock tryAcquireSessionBasedLock(String lockBasePath, String statePath, String lockKey) {
    try {
      long currentTimeMs = System.currentTimeMillis();

      // Ensure the base lock directory exists
      if (!_propertyStore.exists(lockBasePath, AccessOption.PERSISTENT)) {
        ZNRecord baseRecord = new ZNRecord(lockKey);
        _propertyStore.create(lockBasePath, baseRecord, AccessOption.PERSISTENT);
      }

      // Create ephemeral sequential node for this controller, add an UUID to ensure that the path is unique in case
      // multiple controller threads run at the same time
      UUID uuid = UUID.randomUUID();
      String lockNodePrefix = lockBasePath + "/" + _controllerInstanceId + "-" + uuid + "-lock-";
      ZNRecord lockRecord = new ZNRecord(_controllerInstanceId);
      lockRecord.setSimpleField(LOCK_OWNER_KEY, _controllerInstanceId);
      lockRecord.setSimpleField(LOCK_TIMESTAMP_MILLIS_KEY, String.valueOf(currentTimeMs));
      lockRecord.setSimpleField(LOCK_UUID_KEY, uuid.toString());

      // ZK will assign the sequence when creating EPHEMERAL_SEQUENTIAL ZNodes
      boolean created = _propertyStore.create(lockNodePrefix, lockRecord, AccessOption.EPHEMERAL_SEQUENTIAL);

      if (created) {
        // Find our actual node path by listing children and finding the one we just created, the UUID makes the path
        // unique, even if we have multiple requests from the same controller
        List<String> children = _propertyStore.getChildNames(lockBasePath, AccessOption.PERSISTENT);
        List<String> allLockNodePathsForController = new ArrayList<>();
        String lockNodePath = null;
        if (children != null) {
          // Find any node that starts with our controller ID and contains "-lock-"
          for (String child : children) {
            if (child.startsWith(_controllerInstanceId) && child.contains("-lock-")) {
              if (child.startsWith(_controllerInstanceId + "-" + uuid)) {
                // If the node also contains the UUID, it's the lock we created
                lockNodePath = lockBasePath + "/" + child;
              }
              allLockNodePathsForController.add(lockBasePath + "/" + child);
            }
          }
        }

        LOGGER.info("Found {} lockNodePaths for controller instance: {}, list: {}, first lockNodePath cached: {}",
            allLockNodePathsForController.size(), _controllerInstanceId, allLockNodePathsForController, lockNodePath);

        if (lockNodePath != null && allLockNodePathsForController.size() == 1) {
          // Check if we got the lowest sequence number (i.e., we're first in line)
          List<String> allChildren = _propertyStore.getChildNames(lockBasePath, AccessOption.PERSISTENT);
          if (allChildren != null && !allChildren.isEmpty()) {
            allChildren.sort(TASK_LOCK_SEQUENCE_ID_COMPARATOR); // Sort by sequence number
            String ourNode = lockNodePath.substring(lockNodePath.lastIndexOf('/') + 1);
            if (ourNode.equals(allChildren.get(0))) {
              // We have the lock! Mark task generation as in progress
              markTaskGenerationInProgress(statePath, lockNodePath, lockKey);
              LOGGER.info("Acquired lock with ephemeral node: {}", lockNodePath);
              return new TaskLock(lockKey, _controllerInstanceId, currentTimeMs, lockNodePath, statePath);
            } else {
              // Someone else has the lock, clean up our node
              boolean status = _propertyStore.remove(lockNodePath, AccessOption.EPHEMERAL);
              LOGGER.info("Did not get lock, removing ephemeral node: {}, return status: {}", lockNodePath, status);
              return null;
            }
          } else {
            // No children found, something went wrong, clean up
            boolean status = _propertyStore.remove(lockNodePath, AccessOption.EPHEMERAL);
            LOGGER.warn("No children found under {}. Remove lockNodePath status: {} for node: {}. Something must have "
                    + "gone wrong", lockBasePath, status, lockNodePath);
            return null;
          }
        } else {
          // Could not find our node path, or found too many paths for the same controller, cleanup failed creation
          LOGGER.warn("Either lockNodePath: {} wasn't found, or too many locks ({}) found for the same controller: {},"
                  + "list of locks: {}", lockNodePath, allLockNodePathsForController.size(), _controllerInstanceId,
              allLockNodePathsForController);

          if (lockNodePath != null) {
            boolean status = _propertyStore.remove(lockNodePath, AccessOption.EPHEMERAL);
            LOGGER.warn("Remove lockNodePath status: {} for path: {}", status, lockNodePath);
          }
          return null;
        }
      }
      return null;
    } catch (Exception e) {
      LOGGER.error("Error creating ephemeral lock under path: {}, lockKey: {}", lockBasePath, lockKey, e);
      return null;
    }
  }

  /**
   * Marks task generation as in progress.
   */
  private void markTaskGenerationInProgress(String statePath, String lockNodePath, String lockKey) {
    try {
      ZNRecord stateRecord = new ZNRecord(lockKey);
      stateRecord.setSimpleField(TASK_GENERATION_STATUS_KEY, Status.IN_PROGRESS.name());
      stateRecord.setSimpleField(TASK_GENERATION_START_TIME_MILLIS_KEY, String.valueOf(System.currentTimeMillis()));
      stateRecord.setSimpleField(LOCK_OWNER_KEY, _controllerInstanceId);
      stateRecord.setSimpleField(LOCK_PATH_KEY, lockNodePath);

      _propertyStore.set(statePath, stateRecord, AccessOption.PERSISTENT);
    } catch (Exception e) {
      LOGGER.warn("Error marking task generation in progress for path: {}, lockKey: {}", statePath, lockKey, e);
    }
  }

  /**
   * Marks task generation as completed or failed.
   */
  private void markTaskGenerationCompleted(String statePath, String lockKey, boolean success) {
    try {
      ZNRecord stateRecord = _propertyStore.get(statePath, null, AccessOption.PERSISTENT);

      if (stateRecord == null) {
        LOGGER.info("Could not find ZNode record for state path: {}, creating a new one", statePath);
        stateRecord = new ZNRecord(lockKey);
      }

      stateRecord.setSimpleField(TASK_GENERATION_STATUS_KEY, success ? Status.COMPLETED.name() : Status.FAILED.name());
      stateRecord.setSimpleField(TASK_GENERATION_COMPLETION_TIME_MILLIS_KEY,
          String.valueOf(System.currentTimeMillis()));

      _propertyStore.set(statePath, stateRecord, AccessOption.PERSISTENT);
    } catch (Exception e) {
      LOGGER.warn("Error marking task generation completed for path: {}, lockKey: {}", statePath, lockKey, e);
    }
  }

  /**
   * Checks if there are active ephemeral locks for the given key.
   */
  private boolean hasActiveLocks(String tableName) {
    try {
      String lockBasePath = getLockBasePath(tableName);
      if (!_propertyStore.exists(lockBasePath, AccessOption.PERSISTENT)) {
        return false;
      }

      List<String> children = _propertyStore.getChildNames(lockBasePath, AccessOption.PERSISTENT);
      if (children != null && !children.isEmpty()) {
        // Check if any ephemeral nodes still exist (session still alive)
        for (String childName : children) {
          String childPath = lockBasePath + "/" + childName;
          if (_propertyStore.exists(childPath, AccessOption.EPHEMERAL)) {
            return true;
          }
        }
      }
      return false;
    } catch (Exception e) {
      LOGGER.warn("Error checking active locks for table: {}", tableName, e);
      return false;
    }
  }

  private void ensureBasePaths() {
    try {
      // Ensure minion task metadata base path exists
      String basePath = getBasePath();
      if (!_propertyStore.exists(basePath, AccessOption.PERSISTENT)) {
        ZNRecord baseRecord = new ZNRecord("MINION_TASK_METADATA");
        _propertyStore.create(basePath, baseRecord, AccessOption.PERSISTENT);
        LOGGER.info("Created base path for minion task metadata: {}", basePath);
      }
    } catch (Exception e) {
      LOGGER.warn("Error ensuring base paths exist", e);
    }
  }

  /**
   * Represents a session-based distributed lock for task generation.
   * The lock is automatically released when the controller session ends.
   * The state node is periodically cleaned up
   */
  public static class TaskLock {
    private final String _lockKey;
    private final String _owner;
    private final long _timestamp;
    private final String _lockNodePath; // Path to the ephemeral lock node
    private final String _statePath; // Path to the state record

    public TaskLock(String lockKey, String owner, long timestamp, String lockNodePath, String statePath) {
      _lockKey = lockKey;
      _owner = owner;
      _timestamp = timestamp;
      _lockNodePath = lockNodePath;
      _statePath = statePath;
    }

    public String getLockKey() {
      return _lockKey;
    }

    public String getOwner() {
      return _owner;
    }

    public long getTimestamp() {
      return _timestamp;
    }

    public String getLockNodePath() {
      return _lockNodePath;
    }

    public String getStatePath() {
      return _statePath;
    }

    public long getAge() {
      return System.currentTimeMillis() - _timestamp;
    }

    @Override
    public String toString() {
      return String.format("TaskLock{key='%s', owner='%s', timestamp=%d, age=%dms, nodePath='%s'}",
          _lockKey, _owner, _timestamp, getAge(), _lockNodePath);
    }
  }
}
