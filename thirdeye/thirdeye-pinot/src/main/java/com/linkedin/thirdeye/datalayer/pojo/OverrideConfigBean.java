package com.linkedin.thirdeye.datalayer.pojo;

import java.util.List;
import java.util.Map;

public class OverrideConfigBean extends AbstractBean {

  private long startTime;

  private long endTime;

  private Map<String, List<String>> targetLevel;

  private String targetEntity;

  private Map<String, String> overrideProperties;

  private boolean active;


  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public Map<String, List<String>> getTargetLevel() {
    return targetLevel;
  }

  public void setTargetLevel(Map<String, List<String>> targetLevel) {
    this.targetLevel = targetLevel;
  }

  public String getTargetEntity() {
    return targetEntity;
  }

  public void setTargetEntity(String targetEntity) {
    this.targetEntity = targetEntity;
  }

  public Map<String, String> getOverrideProperties() {
    return overrideProperties;
  }

  public void setOverrideProperties(Map<String, String> overrideProperties) {
    this.overrideProperties = overrideProperties;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean isActive) {
    this.active = isActive;
  }
}
