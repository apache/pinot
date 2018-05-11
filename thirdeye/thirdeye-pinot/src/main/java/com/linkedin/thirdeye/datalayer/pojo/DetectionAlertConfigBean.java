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
public class DetectionAlertConfigBean extends AbstractBean {

  boolean active;
  long lastTimeStamp;
  String name;
  String fromAddress;
  String cronExpression;

  Map<String, Object> properties;

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCronExpression() {
    return cronExpression;
  }

  public void setCronExpression(String cronExpression) {
    this.cronExpression = cronExpression;
  }

  public String getFromAddress() {
    return fromAddress;
  }

  public void setFromAddress(String fromAddress) {
    this.fromAddress = fromAddress;
  }

  public long getLastTimeStamp() {
    return lastTimeStamp;
  }

  public void setLastTimeStamp(long lastTimeStamp) {
    this.lastTimeStamp = lastTimeStamp;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DetectionAlertConfigBean)) {
      return false;
    }
    DetectionAlertConfigBean that = (DetectionAlertConfigBean) o;
    return active == that.active && lastTimeStamp == that.lastTimeStamp && Objects.equals(name, that.name)
        && Objects.equals(fromAddress, that.fromAddress) && Objects.equals(cronExpression, that.cronExpression)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {

    return Objects.hash(active, lastTimeStamp, name, fromAddress, cronExpression, properties);
  }
}
