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

import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MinionTaskGenerationLockManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinionTaskGenerationLockManager.class);
  public static final String ACQUIRED_AT = "acquiredAt";
  public static final int TASK_GENERATION_LOCK_TTL = 300_000; // 5 minutes
  private final HelixPropertyStore<ZNRecord> _propertyStore;
  public MinionTaskGenerationLockManager(PinotHelixResourceManager helixResourceManager) {
    _propertyStore = helixResourceManager.getPropertyStore();
  }

  public void generateWithLock(String tableNameWithType, String taskType, Callable<Void> generate)
      throws Exception {
    String lockNodePath =
        ZKMetadataProvider.constructPropertyStorePathForMinionTaskGenerationLock(tableNameWithType, taskType);
    Stat stat = new Stat();
    ZNRecord znRecord = _propertyStore.get(lockNodePath, stat, AccessOption.PERSISTENT);
    // If lock node already exists check if it has gone beyond TTL
    if (znRecord != null) {
      long acquiredAt = znRecord.getLongField(ACQUIRED_AT, 0);
      if (acquiredAt + TASK_GENERATION_LOCK_TTL > System.currentTimeMillis()) {
        fail(tableNameWithType, taskType);
      } else {
        _propertyStore.remove(lockNodePath, AccessOption.PERSISTENT);
      }
    }

    // Create new lock node
    znRecord = new ZNRecord(String.valueOf(UUID.randomUUID()));
    znRecord.setLongField(ACQUIRED_AT, System.currentTimeMillis());
    if (_propertyStore.create(lockNodePath, znRecord, AccessOption.PERSISTENT)) {
      LOGGER.info("Acquired task generation lock on table {} for task type {} with id {}", tableNameWithType, taskType,
          znRecord.getId());
      try {
        generate.call();
      } finally {
        // release lock by deleting the lock node
        _propertyStore.remove(lockNodePath, AccessOption.PERSISTENT);
      }
    } else {
      fail(tableNameWithType, taskType);
    }
  }

  private static void fail(String tableNameWithType, String taskType) {
    throw new RuntimeException("Unable to acquire task generation lock on table " + tableNameWithType
        + " for task type " + taskType);
  }
}
