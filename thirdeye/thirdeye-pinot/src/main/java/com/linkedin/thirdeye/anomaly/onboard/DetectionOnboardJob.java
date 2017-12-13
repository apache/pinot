package com.linkedin.thirdeye.anomaly.onboard;

import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;

public interface DetectionOnboardJob {

  String getName();

  void initialize(Map<String, String> properties);

  Configuration getTaskConfiguration();

  List<DetectionOnboardTask> getTasks();
}
