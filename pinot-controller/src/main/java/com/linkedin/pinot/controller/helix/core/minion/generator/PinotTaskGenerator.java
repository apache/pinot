/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core.minion.generator;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.config.TableConfig;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.helix.task.JobConfig;


/**
 * The interface <code>PinotTaskGenerator</code> defines the APIs for task generators.
 */
public interface PinotTaskGenerator {
  int DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE = JobConfig.DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE;

  /**
   * Returns the task type of the generator.
   *
   * @return Task type of the generator
   */
  @Nonnull
  String getTaskType();

  /**
   * Generates a list of tasks to schedule based on the given table configs.
   *
   * @return List of tasks to schedule
   */
  @Nonnull
  List<PinotTaskConfig> generateTasks(@Nonnull List<TableConfig> tableConfigs);

  /**
   * Returns the maximum number of concurrent tasks allowed per instance.
   *
   * @return Maximum number of concurrent tasks allowed per instance
   */
  int getNumConcurrentTasksPerInstance();

  /**
   * Performs necessary cleanups (e.g. remove metrics) when the controller leadership changes.
   */
  void nonLeaderCleanUp();
}
