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
package org.apache.pinot.controller.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.ws.rs.core.Response;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.helix.task.TaskState;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableStatus;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to fetch ingestion status for realtime and offline table
 */
public class TableIngestionStatusHelper {
  private TableIngestionStatusHelper() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableIngestionStatusHelper.class);

  public static TableStatus.IngestionStatus getRealtimeTableIngestionStatus(String tableNameWithType, int timeoutMs,
      Executor executor, HttpClientConnectionManager connectionManager, PinotHelixResourceManager pinotHelixResourceManager) {
    ConsumingSegmentInfoReader consumingSegmentInfoReader =
        new ConsumingSegmentInfoReader(executor, connectionManager, pinotHelixResourceManager);
    return consumingSegmentInfoReader.getIngestionStatus(tableNameWithType, timeoutMs);
  }

  public static TableStatus.IngestionStatus getOfflineTableIngestionStatus(String tableNameWithType,
      PinotHelixResourceManager pinotHelixResourceManager,
      PinotHelixTaskResourceManager pinotHelixTaskResourceManager) {
    // Check if this offline table uses the built-in segment generation and push task type
    // Offline table ingestion status for ingestion via other task types is not supported.
    TableConfig tableConfig = pinotHelixResourceManager.getTableConfig(tableNameWithType);
    TableTaskConfig taskConfig = tableConfig.getTaskConfig();
    if (taskConfig == null
        || taskConfig.getConfigsForTaskType(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE) == null) {
      throw new ControllerApplicationException(LOGGER,
          "Cannot retrieve ingestion status for Table : " + tableNameWithType
              + " since it does not use the built-in SegmentGenerationAndPushTask task", Response.Status.BAD_REQUEST);
    }

    TableStatus.IngestionState ingestionState = TableStatus.IngestionState.HEALTHY;
    String errorMessage = "";

    // Retrieve all the Minion tasks and corresponding states for this table
    Map<String, TaskState> taskStateMap = pinotHelixTaskResourceManager
        .getTaskStatesByTable(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, tableNameWithType);
    List<String> failedTasks = new ArrayList<>();

    // Check if any of the tasks are in error state
    for (Map.Entry<String, TaskState> taskStateEntry : taskStateMap.entrySet()) {
      switch (taskStateEntry.getValue()) {
        case FAILED:
        case ABORTED:
          failedTasks.add(taskStateEntry.getKey());
          break;
        default:
          continue;
      }
    }
    if (!failedTasks.isEmpty()) {
      ingestionState = TableStatus.IngestionState.UNHEALTHY;
      errorMessage = "Follow ingestion tasks have failed: " + failedTasks.toString();
    }
    return new TableStatus.IngestionStatus(ingestionState.toString(), errorMessage);
  }
}
