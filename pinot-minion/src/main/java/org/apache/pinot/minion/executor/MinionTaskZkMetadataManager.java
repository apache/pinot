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
package org.apache.pinot.minion.executor;

import org.apache.helix.HelixManager;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.minion.BaseTaskMetadata;
import org.apache.pinot.common.minion.MinionTaskMetadataUtils;

/**
 * An abstraction on top of {@link HelixManager}, created for the {@link PinotTaskExecutor}, restricted to only
 * get/update minion task metadata
 */
public class MinionTaskZkMetadataManager {
  private final HelixManager _helixManager;

  public MinionTaskZkMetadataManager(HelixManager helixManager) {
    _helixManager = helixManager;
  }

  /**
   * Fetch the ZNRecord under MINION_TASK_METADATA/${tableNameWithType}/${taskType} for
   * the given tableNameWithType
   */
  public ZNRecord getTaskMetadataZNRecord(String tableNameWithType, String taskType) {
    return MinionTaskMetadataUtils.fetchTaskMetadata(_helixManager.getHelixPropertyStore(), taskType,
        tableNameWithType);
  }

  /**
   * Sets the {@link BaseTaskMetadata} ito the ZNode at
   * MINION_TASK_METADATA/${tableNameWithType}/${taskType}
   * for the corresponding tableNameWitType
   * @param taskMetadata Task metadata which is to be written
   * @param taskType taskType for which metadata is to be updated
   * @param expectedVersion Version expected to be updating, failing the call if there's a mismatch
   */
  public void setTaskMetadataZNRecord(BaseTaskMetadata taskMetadata, String taskType, int expectedVersion) {
    MinionTaskMetadataUtils.persistTaskMetadata(_helixManager.getHelixPropertyStore(), taskType, taskMetadata,
        expectedVersion);
  }
}
