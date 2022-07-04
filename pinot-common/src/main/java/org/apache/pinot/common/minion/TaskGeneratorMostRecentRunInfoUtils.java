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
package org.apache.pinot.common.minion;

import java.util.function.Consumer;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;


/**
 * TaskGeneratorMostRecentRunInfoUtils is a tool used to save PinotTaskGenerator success run timestamp or error run
 * messages to ZK.
 */
public class TaskGeneratorMostRecentRunInfoUtils {
  private TaskGeneratorMostRecentRunInfoUtils() {
  }
  /**
   * Saves a success run timestamp to ZK path MINION_TASK_GENERATOR_INFO/tableNameWthType/taskName.
   * @param propertyStore the {@link HelixPropertyStore} to save {@link BaseTaskGeneratorInfo} into
   * @param tableNameWithType the table name with type
   * @param taskType the task type
   * @param ts the timestamp
   */
  public static void saveSuccessRunTsToZk(HelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      String taskType, long ts) {
    saveToZk(propertyStore, tableNameWithType, taskType,
        taskGeneratorMostRecentRunInfo -> taskGeneratorMostRecentRunInfo.addSuccessRunTs(ts));
  }

  /**
   * Saves an error run message to ZK path MINION_TASK_GENERATOR_INFO/tableNameWthType/taskName.
   * @param propertyStore the {@link HelixPropertyStore} to save {@link BaseTaskGeneratorInfo} into
   * @param tableNameWithType  the table name with type
   * @param taskType the task type
   * @param ts the timestamp
   * @param message the error message
   */
  public static void saveErrorRunMessageToZk(HelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      String taskType, long ts, String message) {
    saveToZk(propertyStore, tableNameWithType, taskType,
        taskGeneratorMostRecentRunInfo -> taskGeneratorMostRecentRunInfo.addErrorRunMessage(ts, message));
  }

  private static void saveToZk(HelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType, String taskType,
      Consumer<TaskGeneratorMostRecentRunInfo> taskGeneratorMostRecentRunInfoUpdater) {
    ZNRecord znRecord = TaskGeneratorInfoUtils.fetchTaskGeneratorInfo(propertyStore, tableNameWithType, taskType);
    TaskGeneratorMostRecentRunInfo taskGeneratorMostRecentRunInfo;
    if (znRecord != null) {
      taskGeneratorMostRecentRunInfo =
          TaskGeneratorMostRecentRunInfo.fromZNRecord(znRecord, tableNameWithType, taskType);
    } else {
      taskGeneratorMostRecentRunInfo = TaskGeneratorMostRecentRunInfo.newInstance(tableNameWithType, taskType);
    }
    taskGeneratorMostRecentRunInfoUpdater.accept(taskGeneratorMostRecentRunInfo);
    TaskGeneratorInfoUtils.persistTaskGeneratorInfo(propertyStore, tableNameWithType, taskGeneratorMostRecentRunInfo,
        taskGeneratorMostRecentRunInfo.getVersion());
  }
}
