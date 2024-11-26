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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorRegistry;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


public class TaskConfigUtilsTest {
  PinotTaskManager _mockTaskManager;
  TaskGeneratorRegistry _mockTaskRegistry;
  PinotTaskGenerator _taskGenerator;
  private static final String TEST_TASK_TYPE = "myTask";
  private static final String TEST_TABLE_NAME = "myTable";

  @BeforeMethod
  public void setup() {
    _mockTaskManager = Mockito.mock(PinotTaskManager.class);
    _mockTaskRegistry = Mockito.mock(TaskGeneratorRegistry.class);

    _taskGenerator = new BaseTaskGenerator() {

      @Override
      public String getTaskType() {
        return TEST_TASK_TYPE;
      }

      @Override
      public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
        return List.of(new PinotTaskConfig(TEST_TASK_TYPE, new HashMap<>()));
      }

      @Override
      public void validateTaskConfigs(TableConfig tableConfig, Map<String, String> taskConfigs) {
        throw new RuntimeException("TableConfig validation failed");
      }
    };

    when(_mockTaskRegistry.getTaskGenerator(TEST_TASK_TYPE)).thenReturn(_taskGenerator);
    when(_mockTaskManager.getTaskGeneratorRegistry()).thenReturn(_mockTaskRegistry);
  }

  @Test (expectedExceptions = RuntimeException.class)
  public void testValidateTableTaskConfigsValidationException() {
    TableTaskConfig tableTaskConfig =
        new TableTaskConfig(ImmutableMap.of(TEST_TASK_TYPE, ImmutableMap.of("schedule", "0 */10 * ? * * *")));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TEST_TABLE_NAME).setTaskConfig(tableTaskConfig).build();
    TaskConfigUtils.validateTaskConfigs(tableConfig, _mockTaskManager, null);
  }

  @Test (expectedExceptions = RuntimeException.class)
  public void testValidateTableTaskConfigsUnknownTaskType() {
    TableTaskConfig tableTaskConfig =
        new TableTaskConfig(ImmutableMap.of("otherTask", ImmutableMap.of("schedule", "0 */10 * ? * * *")));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TEST_TABLE_NAME).setTaskConfig(tableTaskConfig).build();
    TaskConfigUtils.validateTaskConfigs(tableConfig, _mockTaskManager, null);
  }

  @Test
  public void testCommonTaskValidations() {
    // invalid schedule
    HashMap<String, String> invalidScheduleConfig = new HashMap<>();
    invalidScheduleConfig.put("schedule", "invalidSchedule");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TEST_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of(TEST_TASK_TYPE, invalidScheduleConfig))).build();

    try {
      TaskConfigUtils.doCommonTaskValidations(tableConfig, TEST_TASK_TYPE, invalidScheduleConfig);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("contains an invalid cron schedule"));
    }

    // invalid allowDownloadFromServer config
    HashMap<String, String> invalidAllowDownloadFromServerConfig = new HashMap<>();
    invalidAllowDownloadFromServerConfig.put("allowDownloadFromServer", "true");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TEST_TABLE_NAME).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of(TEST_TASK_TYPE, invalidAllowDownloadFromServerConfig))).build();
    try {
      TaskConfigUtils.doCommonTaskValidations(tableConfig, TEST_TASK_TYPE, invalidAllowDownloadFromServerConfig);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains(
          "allowDownloadFromServer set to true, but " + "peerSegmentDownloadScheme is not set in the table config"));
    }
  }
}
