package com.linkedin.thirdeye.impl.storage;

import java.io.Serializable;

public class IndexMetadata implements Serializable {

  private Long minDataTime;
  private Long maxDataTime;

  public Long getMinDataTime() {
    return minDataTime;
  }
  public void setMinDataTime(Long minDataTime) {
    this.minDataTime = minDataTime;
  }
  public Long getMaxDataTime() {
    return maxDataTime;
  }
  public void setMaxDataTime(Long maxDataTime) {
    this.maxDataTime = maxDataTime;
  }



}
