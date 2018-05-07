package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import java.util.Objects;


/**
 * ConfigBean holds namespaced key-value configuration values.  Values are serialized into the
 * database using the default object mapper.  ConfigBean serves as a light-weight
 * alternative to existing configuration mechanisms to (a) allow at-runtime changes to configuration
 * traditionally stored in config files, and (b) alleviate the need for introducing new bean classes
 * to handle simple configuration tasks.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DetectionConfigBean extends AbstractBean {
  String cron;
  String name;
  long lastTimestamp;
  Map<String, Object> properties;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCron() {
    return cron;
  }

  public void setCron(String cron) {
    this.cron = cron;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  public long getLastTimestamp() {
    return lastTimestamp;
  }

  public void setLastTimestamp(long lastTimestamp) {
    this.lastTimestamp = lastTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DetectionConfigBean that = (DetectionConfigBean) o;
    return lastTimestamp == that.lastTimestamp && Objects.equals(cron, that.cron)
        && Objects.equals(name, that.name) && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cron, name, lastTimestamp, properties);
  }
}
