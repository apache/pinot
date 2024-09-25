package org.apache.pinot.controller.helix.core.realtime;

import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

public class SegmentCompletionConfig {
  private final Map<String, String> fsmSchemes = new HashMap<>();

  public SegmentCompletionConfig(Properties properties) {
    // Parse properties to extract FSM schemes
    // Assuming properties keys are in the format scheme=className
    for (String key : properties.stringPropertyNames()) {
      if (key.startsWith("fsm.scheme.")) {
        String scheme = key.substring("fsm.scheme.".length());
        String className = properties.getProperty(key);
        fsmSchemes.put(scheme, className);
      }
    }
  }

  public Map<String, String> getFsmSchemes() {
    return fsmSchemes;
  }
}

