package com.linkedin.thirdeye.tools.migrate;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Properties;


public abstract class BaseAnomalyFunctionMigrater implements AnomalyFunctionMigrater {
  protected ImmutableMap<String, String> defaultProperties;
  protected ImmutableMap<String, String> directKeyMap;

  protected static final String FUNCTION = "function";
  protected static final String WORKFLOW = "workflow";
  protected static final String DATA = "data";
  protected static final String DOWNGRADE_PREFIX = "downgrade";
  protected static final String TESTING_PREPROCESS = "testingPreprocessors";
  protected static final String TRAINING_PREPROCESS = "trainingPreprocessors";
  protected static final String TRAINING = "training";
  protected static final String DETECTION = "detection";
  protected static final String WRAPPER_FUNCTION_CLASS_TYPE = "ANOMALY_FUNCTION_WRAPPER";

  protected Properties applyDefaultProperties(Properties properties) {
    Preconditions.checkNotNull(properties);

    Properties newProperties = new Properties(properties);
    newProperties.putAll(defaultProperties);
    return newProperties;
  }

  protected String moduleConfigKey(String str) {
    return String.format("module.%s", Preconditions.checkNotNull(str));
  }

  protected String variableConfigKey(String str) {
    return String.format("variables.%s", Preconditions.checkNotNull(str));
  }

  protected Properties mapNewKeys(Properties oldProperties, Properties newProperties) {
    Preconditions.checkNotNull(newProperties);

    Properties properties = new Properties();
    properties.putAll(newProperties);

    if (oldProperties != null && oldProperties.size() > 0) {
      for (Map.Entry<String, String> keyMap : directKeyMap.entrySet()) {
        if (oldProperties.containsKey(keyMap.getKey())) {
          String value = oldProperties.getProperty(keyMap.getKey());
          properties.put(keyMap.getValue(), value);
        }
      }
    }
    return properties;
  }

  @Override
  public Map<String, String> getDefaultProperties() {
    return defaultProperties;
  }
}
