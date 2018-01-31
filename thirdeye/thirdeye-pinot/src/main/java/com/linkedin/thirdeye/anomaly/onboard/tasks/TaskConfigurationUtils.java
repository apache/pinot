package com.linkedin.thirdeye.anomaly.onboard.tasks;

import java.util.List;
import joptsimple.internal.Strings;
import org.apache.commons.configuration.Configuration;


public class TaskConfigurationUtils {
  /**
   * As there is an issue when fetching configuration value as String. When there is comma in the configuration value,
   * e.g. UP,DOWN, the configuration getString() only return UP. This util function is a quick fix to resolve the problem.
   * @param configuration
   * @param key
   * @param defaultString
   * @param separator
   * @return
   */
  public static String getString(Configuration configuration, String key, String defaultString, String separator) {
    if (configuration.getProperty(key) instanceof List) {
      return Strings.join(configuration.getStringArray(key), separator);
    } else {
      return configuration.getString(key, defaultString);
    }
  }
}
