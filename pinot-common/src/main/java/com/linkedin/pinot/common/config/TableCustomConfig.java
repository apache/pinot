package com.linkedin.pinot.common.config;

import java.lang.reflect.Field;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableCustomConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentsValidationAndRetentionConfig.class);

  private Map<String, String> customConfigs;

  public Map<String, String> getCustomConfigs() {
    return customConfigs;
  }

  public void setCustomConfigs(Map<String, String> customConfigs) {
    this.customConfigs = customConfigs;
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    final String newLine = System.getProperty("line.separator");

    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newLine);

    //determine fields declared in this class only (no fields of superclass)
    final Field[] fields = this.getClass().getDeclaredFields();

    //print field names paired with their values
    for (final Field field : fields) {
      result.append("  ");
      try {
        result.append(field.getName());
        result.append(": ");
        //requires access to private field:
        result.append(field.get(this));
      } catch (final IllegalAccessException ex) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Caught exception while processing field " + field, ex);
        }
      }
      result.append(newLine);
    }
    result.append("}");

    return result.toString();
  }
}
