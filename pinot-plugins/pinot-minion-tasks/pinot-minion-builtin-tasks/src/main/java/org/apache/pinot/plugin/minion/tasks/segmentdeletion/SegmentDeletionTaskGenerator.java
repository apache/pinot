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
package org.apache.pinot.plugin.minion.tasks.segmentdeletion;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@TaskGenerator
public class SegmentDeletionTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDeletionTaskGenerator.class);

  @Override
  public String getTaskType() {
    return MinionConstants.SegmentDeletionTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(TableConfig tableConfig, Map<String, String> taskConfigs)
      throws Exception {

    String taskType = MinionConstants.SegmentDeletionTask.TASK_TYPE;

    Map<String, String> configs = new HashMap<>(
        getBaseTaskConfigs(tableConfig, List.of(taskConfigs.get("segmentName"))));
    configs.put(MinionConstants.OPERATOR_KEY, taskConfigs.get("operator"));
    return List.of(new PinotTaskConfig(taskType, configs));
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    throw new UnsupportedOperationException("not supported");
  }
}
