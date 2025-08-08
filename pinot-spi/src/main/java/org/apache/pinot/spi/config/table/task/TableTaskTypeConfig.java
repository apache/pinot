package org.apache.pinot.spi.config.table.task;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Base class for all table task type configurations.
 * <p>
 * Attributes in this class are generally managed by the Pinot controller rather than the task itself.
 * Eg: schedule, priority, minionInstanceTag, etc
 *
 * Attributes of subclass can be of 2 types
 * 1. Required -> Such attributes should either have defaults or should fail validation if not set.
 * 2. Optional -> Such attributes should be Optional<> so that the implementation knows that it can be null.
 */
public abstract class TableTaskTypeConfig {

  /**
   * Schedule in quartz cron format. eg: "0 0/5 * * * ?" to run every 5 minutes
   * The default value is null, which means the task is not scheduled by default (except if it's a default task)
   */
  protected final String schedule;

  /**
   * Minion Tag identifier to schedule a task on a dedicated pool of minion hosts which match the tag.
   * Default value matches the default minion instance tag.
   */
  protected final String minionInstanceTag;

  /**
   * The actual user defined configuration for the task type.
   * This is not supposed to be used by any task generator / executor but is used by controller for persisting user defined configs
   * This is required because the attribute values in the subclass are modified based on defaults, corrections, etc
   */
  protected Map<String, String> configs;

  protected TableTaskTypeConfig(Map<String, String> configs) {
    // TODO - Move the constants from pinot-controller to pinot-spi and use them here.
    this.schedule = configs.get("schedule");
    this.minionInstanceTag = configs.getOrDefault("minionInstanceTag", CommonConstants.Helix.UNTAGGED_MINION_INSTANCE);
    this.configs = configs;
  }

  /**
   * Validates the task type configuration.
   * TODO - This should be called by PinotTaskGenerator before generating tasks. It shouldn't be called by each task.
   * @return true if the configuration is valid, false otherwise.
   */
  public final boolean isValidConfig(TableConfig tableConfig) throws InvalidTableConfigException {
    // TODO - Check if schedule is in valid quartz cron format and that minion instance tag is valid.
    return isAllowedForTable(tableConfig) && isValidConfig();
  }

  /**
   * Corrects the task type configuration if needed to make it pass validations
   * This will only work if the validation failed due to TASK_CONFIG_INVALID in {@link InvalidTableConfigExceptionType}.
   * This is typically used when task generator runs but the current task type configuration is invalid
   * due to modifications in validation logic across Pinot versions.
   * TODO - This should be called by PinotTaskGenerator before generating tasks. It shouldn't be called by each task.
   */
  public abstract TableTaskTypeConfig getCorrectedConfig();

  public abstract TableTaskTypeConfig getScaledDownConfig();

  /**
   * Checks if the task type is allowed for the given table configuration.
   * @throws InvalidTableConfigException of type TASK_NOT_ALLOWED in {@link InvalidTableConfigExceptionType}
   *    *          with message on why the configuration is not valid.
   */
  protected abstract boolean isAllowedForTable(TableConfig tableConfig) throws InvalidTableConfigException;

  /**
   * Validates the task type configuration.
   * @throws InvalidTableConfigException of type TASK_CONFIG_INVALID in {@link InvalidTableConfigExceptionType}
   *          with message on why the configuration is not valid.
   */
  protected abstract boolean isValidConfig() throws InvalidTableConfigException;

  /**
   * Returns an unmodifiable copy of the task type configuration.
   */
  public Map<String, String> getConfigs() {
    return Collections.unmodifiableMap(configs);
  }

}
