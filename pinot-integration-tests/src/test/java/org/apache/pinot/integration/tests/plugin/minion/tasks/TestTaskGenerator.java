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
package org.apache.pinot.integration.tests.plugin.minion.tasks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.integration.tests.SimpleMinionClusterIntegrationTest;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;

import static org.testng.Assert.assertEquals;


/**
 * Task generator for {@link SimpleMinionClusterIntegrationTest}.
 */
@TaskGenerator
public class TestTaskGenerator extends BaseTaskGenerator {

  @Override
  public String getTaskType() {
    return SimpleMinionClusterIntegrationTest.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    assertEquals(tableConfigs.size(), 1);

    // Generate at most 2 tasks
    if (_clusterInfoAccessor.getTaskStates(SimpleMinionClusterIntegrationTest.TASK_TYPE).size()
        >= SimpleMinionClusterIntegrationTest.NUM_TASKS) {
      return Collections.emptyList();
    }

    List<PinotTaskConfig> taskConfigs = new ArrayList<>();
    for (TableConfig tableConfig : tableConfigs) {
      Map<String, String> configs = new HashMap<>();
      configs.put("tableName", tableConfig.getTableName());
      configs.put("tableType", tableConfig.getTableType().toString());
      taskConfigs.add(new PinotTaskConfig(SimpleMinionClusterIntegrationTest.TASK_TYPE, configs));
    }
    return taskConfigs;
  }
}
