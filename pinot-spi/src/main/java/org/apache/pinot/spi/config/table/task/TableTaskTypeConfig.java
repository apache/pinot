package org.apache.pinot.spi.config.table.task;

import java.util.Map;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Base class for all table task type configurations.
 * <p>
 * Attributes in this class are generally managed by the Pinot controller rather than the task itself.
 * Eg: schedule, priority, minionInstanceTag, etc
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

  protected TableTaskTypeConfig(Map<String, String> configs) {
    // TODO - Move the constants from pinot-controller to pinot-spi and use them here.
    this.schedule = configs.get("schedule");
    this.minionInstanceTag = configs.getOrDefault("minionInstanceTag", CommonConstants.Helix.UNTAGGED_MINION_INSTANCE);
  }

  /**
   * Validates the task type configuration.
   * @return true if the configuration is valid, false otherwise.
   */
  protected abstract boolean validateConfig();

}
