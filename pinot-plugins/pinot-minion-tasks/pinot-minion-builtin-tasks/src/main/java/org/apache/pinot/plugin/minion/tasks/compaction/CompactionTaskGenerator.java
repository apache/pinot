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
package org.apache.pinot.plugin.minion.tasks.compaction;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@TaskGenerator
public class CompactionTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompactionTaskGenerator.class);
  @Override
  public String getTaskType() { return MinionConstants.CompactionTask.TASK_TYPE; }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = MinionConstants.CompactionTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
    for (TableConfig tableConfig: tableConfigs) {
      if (!validate(tableConfig, taskType))
      {
        continue;
      }

      String tableNameWithType = tableConfig.getTableName();
      LOGGER.info("Start generating task configs for table: {} for task: {}",
          tableNameWithType, taskType);

      // Get all segment metadata
      List<SegmentZKMetadata> allSegments = _clusterInfoAccessor.getSegmentsZKMetadata(tableNameWithType);

      // TODO
    }
    return null;
  }

  @VisibleForTesting
  static boolean validate(TableConfig tableConfig, String taskType) {
    String tableNameWithType = tableConfig.getTableName();
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      LOGGER.warn("Skip generation task: {} for table: {}, offline table is not supported", taskType, tableNameWithType);
      return false;
    }
    if (!tableConfig.isUpsertEnabled()) {
      LOGGER.warn("Skip generation task: {} for table: {}, table without upsert enabled is not supported", taskType, tableNameWithType);
      return false;
    }
    TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
    if (tableTaskConfig == null) {
      LOGGER.warn("Skip generation task: {} for table: {}, unable to find task config", taskType, tableNameWithType);
      return false;
    }
    Map<String, String> taskConfigs = tableTaskConfig.getConfigsForTaskType(MinionConstants.CompactionTask.TASK_TYPE);
    if (taskConfigs == null) {
      LOGGER.warn("Skip generation task: {} for table: {}, unable to find compaction task config", taskType, tableNameWithType);
      return false;
    }
    return true;
  }
}
