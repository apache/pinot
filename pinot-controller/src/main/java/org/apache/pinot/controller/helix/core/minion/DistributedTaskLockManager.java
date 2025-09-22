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
 * <p>
 * ZK EPHEMERAL_SEQUENTIAL Locks (see <a href="https://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_Locks">
 *   ZooKeeper Lock Recipe.</a> for more details):
 *   <ul>
 *     <li>Every lock is created with a lock prefix. Lock prefix used: [controllerName]-lock-[UUID]. The UUID helps
 *     differentiate between requests originating from the same controller at the same time
 *     <li>When ZK creates the ZNode, it appends a sequence number at the end. E.g.
 *     [controllerName]-lock-[UUID]-00000001
 *     <li>The sequence number is used to identify the lock winner in case more than one lock node is created at the
 *     same time. The smallest sequence number always wins
 *     <li>The locks are EPHEMERAL in nature, meaning that once the session with ZK is lost, the lock is automatically
 *     cleaned up. Scenarios when the ZK session can be lost: a) controller shutdown, b) controller crash, c) ZK session
 *     expiry (e.g. long GC pauses can cause this)
 *     <li>This implementation does not set up watches as described in the recipe as the task lock is released whenever
 *     we identify that the lock is already acquired. Do not expect lock ownership to automatically change for the
 *     time being. If such support is needed in the future, this can be enhanced to add a watch on the neighboring
 *     lock node
 *   </ul>
 * <p>
 * Example of how the locks will work:
 * <p>
 * Say we have two controllers, and one controller happens to run 2 threads at the same time, all of which need to take
 * the distributed lock. Each thread will create a distributed lock node, and the "-Lock" ZNode getChildren will return:
 * <ul>
 *   <li>controller2-lock-xyzwx-00000002
 *   <li>controller1-lock-abcde-00000001
 *   <li>controller1-lock-ab345-00000003
 * </ul>
 * <p>
 * In the above, the controller1 with UUID abcde will win the lock as it has the smallest sequence number. The other
 * two threads will clean up their locks and return error that the distributed lock could not be acquired. Controller1
 * will proceed with performing its tasks, and when done will release the lock.
 */
public class DistributedTaskLockManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedTaskLockManager.class);

  // Lock paths are constructed using ZKMetadataProvider
  private static final String LOCK_OWNER_KEY = "lockOwner";
  private static final String LOCK_UUID_KEY = "lockUuid";
  private static final String LOCK_TIMESTAMP_MILLIS_KEY = "lockTimestampMillis";

  // Define a custom comparator to compare strings of format '<controllerName>-lock-<uuid>-<sequenceNumber>' and sort
  // them by the sequence number at the end
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
   * Attempts to acquire a distributed lock at the table level for task generation using session-based locking.
   * The lock is held until explicitly released or the controller session ends.
   *
   * @param tableNameWithType the table name with type
   * @return TaskLock object if successful, null if lock could not be acquired
   */
  @Nullable
  public TaskLock acquireLock(String tableNameWithType) {
    String lockBasePath = getLockBasePath(tableNameWithType);

    LOGGER.info("Attempting to acquire task generation lock: {} by controller: {}", tableNameWithType,
        _controllerInstanceId);

    try {
      // Check if task generation is already in progress
      if (isTaskGenerationInProgress(tableNameWithType, lockBasePath)) {
        LOGGER.info("Task generation already in progress for: {} by this or another controller", tableNameWithType);
        return null;
      }

      // Try to acquire the lock using ephemeral sequential node
      TaskLock lock = tryAcquireSessionBasedLock(lockBasePath, tableNameWithType);
      if (lock != null) {
        LOGGER.info("Successfully acquired task generation lock: {} by controller: {}", tableNameWithType,
            _controllerInstanceId);
        return lock;
      } else {
        LOGGER.warn("Could not acquire lock: {} - another controller holds it", tableNameWithType);
        return null;
      }
    } catch (Exception e) {
      LOGGER.error("Error while trying to acquire lock: {}", tableNameWithType, e);
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
   * Checks if any task generation is currently in progress for the given table.
   *
   * @param tableNameWithType the table name with type
   * @return true if task generation is in progress, false otherwise
   */
  @VisibleForTesting
  boolean isTaskGenerationInProgress(String tableNameWithType) {
    String lockBasePath = getLockBasePath(tableNameWithType);
    return isTaskGenerationInProgress(tableNameWithType, lockBasePath);
  }

  /**
   * Internal method to check if task generation is in progress for a lock key.
   */
  private boolean isTaskGenerationInProgress(String tableNameWithType, String lockBasePath) {
    try {
      if (!_propertyStore.exists(lockBasePath, AccessOption.PERSISTENT)) {
        return false;
      }

      // Check if there are any active ephemeral lock nodes
      List<String> lockNodes = _propertyStore.getChildNames(lockBasePath, AccessOption.PERSISTENT);
      if (lockNodes != null && !lockNodes.isEmpty()) {
        // There are active ephemeral lock nodes, check if any are still valid
        for (String nodeName : lockNodes) {
          String nodePath = lockBasePath + "/" + nodeName;
          if (_propertyStore.exists(nodePath, AccessOption.EPHEMERAL)) {
            // Ephemeral node exists, meaning session is still alive and task should be in progress
            return true;
          }
        }
      }
      return false;
    } catch (Exception e) {
      LOGGER.error("Error checking task generation status for: {}", tableNameWithType, e);
      return false;
    }
  }

  /**
   * Attempts to acquire a lock using ephemeral sequential nodes.
   * Uses the ZooKeeper recipe for distributed locking with automatic cleanup.
   */
  @VisibleForTesting
  TaskLock tryAcquireSessionBasedLock(String lockBasePath, String lockKey) {
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
              // We have the lock!
              LOGGER.info("Acquired lock with ephemeral node: {}", lockNodePath);
              return new TaskLock(lockKey, _controllerInstanceId, currentTimeMs, lockNodePath);
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
    private final long _creationTimeMs;
    private final String _lockNodePath; // Path to the ephemeral lock node

    public TaskLock(String lockKey, String owner, long creationTimeMs, String lockNodePath) {
      _lockKey = lockKey;
      _owner = owner;
      _creationTimeMs = creationTimeMs;
      _lockNodePath = lockNodePath;
    }

    public String getLockKey() {
      return _lockKey;
    }

    public String getOwner() {
      return _owner;
    }

    public long getCreationTimeMs() {
      return _creationTimeMs;
    }

    public String getLockNodePath() {
      return _lockNodePath;
    }

    public long getAge() {
      return System.currentTimeMillis() - _creationTimeMs;
    }

    @Override
    public String toString() {
      return String.format("TaskLock{key='%s', owner='%s', creationTimeMs=%d, age=%dms, nodePath='%s'}",
          _lockKey, _owner, _creationTimeMs, getAge(), _lockNodePath);
    }
  }
}
