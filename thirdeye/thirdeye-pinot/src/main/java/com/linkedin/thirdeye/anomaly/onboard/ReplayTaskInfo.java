package com.linkedin.thirdeye.anomaly.onboard;

import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import java.util.Map;


public class ReplayTaskInfo implements TaskInfo {
  private String jobName;
  private Map<String, String> properties;

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }
}
