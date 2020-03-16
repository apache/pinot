package org.apache.pinot.thirdeye.model.download;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;


public class ModelDownloaderConfiguration {
  private TimeGranularity runFrequency;
  private String className;
  private String destinationPath;
  private Map<String, Object> properties = new HashMap<>();

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  public TimeGranularity getRunFrequency() {
    return runFrequency;
  }

  public void setRunFrequency(TimeGranularity runFrequency) {
    this.runFrequency = runFrequency;
  }

  public String getDestinationPath() {
    return destinationPath;
  }

  public void setDestinationPath(String destinationPath) {
    this.destinationPath = destinationPath;
  }
}
