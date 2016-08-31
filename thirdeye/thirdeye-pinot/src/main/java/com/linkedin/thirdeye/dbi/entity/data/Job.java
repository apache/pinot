package com.linkedin.thirdeye.dbi.entity.data;

import com.linkedin.thirdeye.dbi.entity.base.AbstractJsonEntity;

public class Job extends AbstractJsonEntity {
  String name;
  String status;
  long startTime;
  long endTime;

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }
}
