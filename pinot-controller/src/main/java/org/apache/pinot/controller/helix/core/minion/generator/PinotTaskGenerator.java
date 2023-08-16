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
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * The interface <code>PinotTaskGenerator</code> defines the APIs for task generators.
 */
public interface PinotTaskGenerator {

  /**
   * Initializes the task generator.
   */
  void init(ClusterInfoAccessor clusterInfoAccessor);

  /**
   * Returns the task type of the generator.
   */
  String getTaskType();

  /**
   * Generates a list of tasks to schedule based on the given table configs.
   */
  List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs);

  /**
   * Generates a list of adhoc tasks to schedule based on the given table configs and task configs.
   */
  List<PinotTaskConfig> generateTasks(TableConfig tableConfig, Map<String, String> taskConfigs)
      throws Exception;

  /**
   * Returns the timeout in milliseconds for each task, 3600000 (1 hour) by default.
   */
  default long getTaskTimeoutMs() {
    return JobConfig.DEFAULT_TIMEOUT_PER_TASK;
  }

  /**
   * Returns the maximum number of concurrent tasks allowed per instance, 1 by default.
   */
  default int getNumConcurrentTasksPerInstance() {
    return JobConfig.DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE;
  }

  /**
   * Returns the maximum number of attempts per task, 1 by default.
   */
  default int getMaxAttemptsPerTask() {
    return MinionConstants.DEFAULT_MAX_ATTEMPTS_PER_TASK;
  }

  /**
   * Performs necessary cleanups (e.g. remove metrics) when the controller leadership changes.
   */
  default void nonLeaderCleanUp() {
  }
}
