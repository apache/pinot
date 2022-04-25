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
package org.apache.pinot.plugin.minion.tasks.purge;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.data.Segment;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@TaskGenerator
public class PurgeTaskGenerator implements PinotTaskGenerator {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PurgeTaskGenerator.class);

    private static final PurgeTaskGenerator INSTANCE = new PurgeTaskGenerator();

    protected ClusterInfoAccessor _clusterInfoAccessor;

    public static Object newInstance() {
        return INSTANCE;
    }

    @Override
    public void init(ClusterInfoAccessor clusterInfoAccessor) {
        LOGGER.info("I'm in init for the purge");
        _clusterInfoAccessor = clusterInfoAccessor;
    }


    @Override
    public String getTaskType() {
        return MinionConstants.PurgeTask.TASK_TYPE;
    }



    @Override
    public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
        String taskType = MinionConstants.PurgeTask.TASK_TYPE;
        List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
        Set<Segment> runningSegments =
                TaskGeneratorUtils.getRunningSegments(MinionConstants.PurgeTask.TASK_TYPE, _clusterInfoAccessor);
        for (TableConfig tableConfig : tableConfigs) {
            String tableName = tableConfig.getTableName();
            TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
            Preconditions.checkNotNull(tableTaskConfig);
            Map<String, String> taskConfigs =
                    tableTaskConfig.getConfigsForTaskType(MinionConstants.PurgeTask.TASK_TYPE);
            Preconditions.checkNotNull(taskConfigs, "Task config shouldn't be null for Table: {}", tableName);

            LOGGER.info("Start generating task configs for table: {} for task: {}", tableName, taskType);

            if (tableConfig.getTableType() == TableType.REALTIME) {
                LOGGER.info("Task type : {}, cannot be run on table of type  {}",
                        taskType, TableType.REALTIME);
                continue;
            }
                // Get max number of tasks for this table
            int tableMaxNumTasks;
            String tableMaxNumTasksConfig = taskConfigs.get(MinionConstants.TABLE_MAX_NUM_TASKS_KEY);
            if (tableMaxNumTasksConfig != null) {
                try {
                    tableMaxNumTasks = Integer.parseInt(tableMaxNumTasksConfig);
                } catch (Exception e) {
                    tableMaxNumTasks = Integer.MAX_VALUE;
                }
            } else {
                tableMaxNumTasks = Integer.MAX_VALUE;
            }
            List<SegmentZKMetadata> offlineSegmentsZKMetadata = _clusterInfoAccessor.getSegmentsZKMetadata(tableName);
            int tableNumTasks = 0;
            for (SegmentZKMetadata segmentZKMetadata : offlineSegmentsZKMetadata) {
                Map<String, String> configs = new HashMap<>();
                String segmentName = segmentZKMetadata.getSegmentName();
                //skip running segment
                if (runningSegments.contains(new Segment(tableName, segmentName))) {
                    continue;
                }
                if (tableNumTasks == tableMaxNumTasks) {
                    break;
                }

                configs.put(MinionConstants.TABLE_NAME_KEY, tableName);
                configs.put(MinionConstants.SEGMENT_NAME_KEY, segmentName);
                configs.put(MinionConstants.DOWNLOAD_URL_KEY, segmentZKMetadata.getDownloadUrl());
                configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");
                configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY, String.valueOf(segmentZKMetadata.getCrc()));
                pinotTaskConfigs.add(new PinotTaskConfig(taskType, configs));
                tableNumTasks++;
            }
            LOGGER.info("Finished generating task configs for table: {} for task: {}", tableName, taskType);
        }
        return pinotTaskConfigs;
    }
}
