package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;

public class DetectionOnboardJob {

  public Configuration generateConfiguration(Map<String, String> properties) {
    Preconditions.checkNotNull(properties);
    //TODO: convert properties to configuration for tasks
    Configuration configuration = new MapConfiguration(properties);
    return configuration;
  }

  public List<DetectionOnboardTask> createTasks() {
    // TODO: create a list of onboard tasks
    return new ArrayList<>();
  }
}
