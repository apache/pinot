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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorRegistry;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.data.Schema;
import org.quartz.CronScheduleBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskConfigUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskConfigUtils.class);
  private static final String SCHEDULE_KEY = "schedule";
  private static final String ALL_VALIDATION_TYPE = "ALL";
  private static final String TASK_VALIDATION_TYPE = "TASK";

  private TaskConfigUtils() {
  }

  public static void validateTaskConfigs(TableConfig tableConfig, Schema schema, PinotTaskManager pinotTaskManager,
      String validationTypesToSkip) {
    if (tableConfig == null || tableConfig.getTaskConfig() == null) {
      return;
    }
    if (ALL_VALIDATION_TYPE.equalsIgnoreCase(validationTypesToSkip) || TASK_VALIDATION_TYPE.equalsIgnoreCase(
        validationTypesToSkip)) {
      LOGGER.info("Skipping task validation as validationTypesToSkip is set to: {}", validationTypesToSkip);
      return;
    }
    TableTaskConfig taskConfig = tableConfig.getTaskConfig();
    for (Map.Entry<String, Map<String, String>> taskConfigEntry : taskConfig.getTaskTypeConfigsMap().entrySet()) {
      String taskType = taskConfigEntry.getKey();
      TaskGeneratorRegistry taskRegistry = pinotTaskManager.getTaskGeneratorRegistry();
      if (taskRegistry != null) {
        PinotTaskGenerator taskGenerator = taskRegistry.getTaskGenerator(taskType);
        if (taskGenerator != null) {
          Map<String, String> taskConfigs = taskConfigEntry.getValue();
          doCommonTaskValidations(tableConfig, taskType, taskConfigs);
          taskGenerator.validateTaskConfigs(tableConfig, schema, taskConfigs);
        } else {
          throw new RuntimeException(String.format("Task generator not found for task type: %s, while validating table "
              + "configs for table: %s", taskType, tableConfig.getTableName()));
        }
      }
    }
  }

  @VisibleForTesting
  static void doCommonTaskValidations(TableConfig tableConfig, String taskType,
      Map<String, String> taskConfigs) {
    Preconditions.checkNotNull(taskConfigs, String.format("Task configuration for %s cannot be null.", taskType));
    // Schedule key for task config has to be set.
    if (taskConfigs.containsKey(SCHEDULE_KEY)) {
      String cronExprStr = taskConfigs.get(SCHEDULE_KEY);
      try {
        CronScheduleBuilder.cronSchedule(cronExprStr);
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format("Task %s contains an invalid cron schedule: %s", taskType, cronExprStr), e);
      }
    }
    boolean isAllowDownloadFromServer = Boolean.parseBoolean(
        taskConfigs.getOrDefault(TableTaskConfig.MINION_ALLOW_DOWNLOAD_FROM_SERVER,
            String.valueOf(TableTaskConfig.DEFAULT_MINION_ALLOW_DOWNLOAD_FROM_SERVER)));
    if (isAllowDownloadFromServer) {
      Preconditions.checkState(tableConfig.getValidationConfig().getPeerSegmentDownloadScheme() != null,
          String.format("Table %s has task %s with allowDownloadFromServer set to true, but "
                  + "peerSegmentDownloadScheme is not set in the table config", tableConfig.getTableName(),
              taskType));
    }
  }
}
