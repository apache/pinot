/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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


/**
 * The interface <code>PinotTaskGenerator</code> defines the APIs for task generators.
 */
public interface PinotTaskGenerator {

  /**
   * The type of tasks to generate. Should match the task type in task executor.
   */
  @Nonnull
  String getTaskType();

  /**
   * Given a list of table configs, generate a list of new tasks to be scheduled.
   */
  @Nonnull
  List<PinotTaskConfig> generateTasks(@Nonnull List<TableConfig> tableConfigs);
}
