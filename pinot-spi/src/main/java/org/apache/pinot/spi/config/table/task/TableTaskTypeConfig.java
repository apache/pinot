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
package org.apache.pinot.spi.config.table.task;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.exception.InvalidTableConfigException;
import org.apache.pinot.spi.exception.InvalidTableTaskConfigException;
import org.apache.pinot.spi.exception.TaskNotAllowedForTableException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.quartz.CronScheduleBuilder;


/**
 * Base class for all table task type configurations.
 * <p>
 * Attributes in this class are generally managed by the Pinot controller rather than the task itself.
 * Eg: schedule, priority, minionInstanceTag, etc
 *
 * Though some may be managed by the task but are still common across all task types. Eg: maxNumTasks.
 *
 * Attributes of subclass can be of 2 types
 * 1. Required -> Such attributes should either have defaults or should fail validation if not set.
 * 2. Optional -> Such attributes should be Optional<> so that the implementation knows that it can be empty.
 *
 * This setup helps -
 * 1. Provide visibility and documentation of all configs and their defaults available for a task type.
 * 2. Enable easier validation and correction of configs through above awareness.
 * 3. Abstract backward compatibility / config names of configs making the generator / executor simpler.
 */
public abstract class TableTaskTypeConfig {

  /**
   * Schedule in quartz cron format. eg: "0 0/5 * * * ?" to run every 5 minutes
   * The default value is null, which means the task is not scheduled by default (except if it's a default task)
   */
  protected final Optional<String> schedule;

  /**
   * Minion Tag identifier to schedule a task on a dedicated pool of minion hosts which match the tag.
   * Default value matches the default minion instance tag.
   */
  protected final String minionInstanceTag;

  /**
   * Maximum number of sub-tasks that can be executed in a single trigger of the segment refresh task.
   * This is used to control helix resource usage on controller.
   * If more sub-tasks are needed to be merged / reloaded, multiple triggers of the task will be needed.
   * This is going to be bounded by the cluster limit of max number of subtasks possible
   */
  private final int maxNumSubTasks;

  /**
   * The actual user defined configuration for the task type.
   * This is not supposed to be used by any task generator / executor but is used by controller for persisting user defined configs
   * This is required because the attribute values in the subclass are modified based on defaults, corrections, etc
   */
  protected Map<String, String> configs;

  protected TableTaskTypeConfig(Map<String, String> configs) {
    // TODO - Move the constants from pinot-controller to pinot-spi and use them here.
    this.schedule = Optional.ofNullable(configs.get("schedule"));
    this.minionInstanceTag = configs.getOrDefault("minionInstanceTag", CommonConstants.Helix.UNTAGGED_MINION_INSTANCE);
    this.maxNumSubTasks = Integer.parseInt(configs.getOrDefault("tableMaxNumTasks", String.valueOf(getDefaultMaxNumSubTasks())));
    this.configs = configs;
  }

  /**
   * Validates the task type configuration.
   * TODO - This should be called by PinotTaskGenerator before generating tasks. It shouldn't be called by each task.
   * @return true if the configuration is valid, false otherwise.
   */
  public final void checkValidity(TableConfig tableConfig) throws InvalidTableConfigException {
    validateCronSchedule();
    // TODO - check minion instance tag is valid.
    checkIfAllowedForTable(tableConfig);
    checkTaskConfigValiditiy();
  }

  /**
   * Corrects the task type configuration if needed to make it pass validations
   * This will only work if the validation failed due to {@link InvalidTableTaskConfigException}.
   * This is typically used when task generator runs but the current task type configuration is invalid
   * due to modifications in validation logic across Pinot versions.
   * TODO - This should be called by PinotTaskGenerator before generating tasks. It shouldn't be called by each task.
   */
  public abstract TableTaskTypeConfig getCorrectedConfig();

  /**
   * Returns an unmodifiable copy of the task type configuration.
   */
  public Map<String, String> getConfigs() {
    return Collections.unmodifiableMap(configs);
  }

  public int getMaxNumSubTasks() {
    return maxNumSubTasks;
  }

  // Can be overridden by subclasses
  protected int getDefaultMaxNumSubTasks() {
    return 1000;
  }

  /**
   * Checks if the task type is allowed for the given table configuration.
   * @throws TaskNotAllowedForTableException with message on why the configuration is not valid.
   */
  protected abstract void checkIfAllowedForTable(TableConfig tableConfig) throws TaskNotAllowedForTableException;

  /**
   * Validates the task type configuration.
   * @throws InvalidTableTaskConfigException with message on why the configuration is not valid.
   */
  protected abstract void checkTaskConfigValiditiy() throws InvalidTableTaskConfigException;

  private void validateCronSchedule() throws InvalidTableTaskConfigException {
    if (schedule.isPresent()) {
      String cronExprStr = schedule.get();
      try {
        CronScheduleBuilder.cronSchedule(cronExprStr);
      } catch (Exception e) {
        throw new InvalidTableTaskConfigException(
            String.format("Task contains an invalid cron schedule: %s. Cron schedule is required in quartz format",
                cronExprStr), e);
      }
    }
  }

}
