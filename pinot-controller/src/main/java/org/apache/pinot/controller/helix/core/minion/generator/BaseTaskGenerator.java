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
package org.apache.pinot.controller.helix.core.minion.generator;

import java.util.List;
import java.util.Map;
import org.apache.helix.task.JobConfig;
import org.apache.pinot.controller.api.exception.UnknownTaskTypeException;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation of the {@link PinotTaskGenerator} which reads the 'taskTimeoutMs' and
 * 'numConcurrentTasksPerInstance' from the cluster config.
 */
public abstract class BaseTaskGenerator implements PinotTaskGenerator {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTaskGenerator.class);

  protected ClusterInfoAccessor _clusterInfoAccessor;

  @Override
  public void init(ClusterInfoAccessor clusterInfoAccessor) {
    _clusterInfoAccessor = clusterInfoAccessor;
  }

  @Override
  public long getTaskTimeoutMs() {
    String taskType = getTaskType();
    String configKey = taskType + MinionConstants.TIMEOUT_MS_KEY_SUFFIX;
    String configValue = _clusterInfoAccessor.getClusterConfig(configKey);
    if (configValue != null) {
      try {
        return Long.parseLong(configValue);
      } catch (Exception e) {
        LOGGER.error("Invalid cluster config {}: '{}'", configKey, configValue, e);
      }
    }
    return JobConfig.DEFAULT_TIMEOUT_PER_TASK;
  }

  @Override
  public int getNumConcurrentTasksPerInstance() {
    String taskType = getTaskType();
    String configKey = taskType + MinionConstants.NUM_CONCURRENT_TASKS_PER_INSTANCE_KEY_SUFFIX;
    String configValue = _clusterInfoAccessor.getClusterConfig(configKey);
    if (configValue != null) {
      try {
        return Integer.parseInt(configValue);
      } catch (Exception e) {
        LOGGER.error("Invalid config {}: '{}'", configKey, configValue, e);
      }
    }
    return JobConfig.DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(TableConfig tableConfig, Map<String, String> taskConfigs)
      throws Exception {
    throw new UnknownTaskTypeException("Adhoc task generation is not supported for task type - " + this.getTaskType());
  }
}
