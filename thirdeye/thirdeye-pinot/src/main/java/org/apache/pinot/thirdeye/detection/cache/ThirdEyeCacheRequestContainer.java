package org.apache.pinot.thirdeye.detection.cache;

import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;


public class ThirdEyeCacheRequestContainer {
  private final ThirdEyeRequest thirdEyeRequest;
  private final String detectionId;

  public ThirdEyeCacheRequestContainer(String detectionId, ThirdEyeRequest thirdEyeRequest) {
    this.detectionId = detectionId;
    this.thirdEyeRequest = thirdEyeRequest;
  }

  public ThirdEyeRequest getRequest() { return thirdEyeRequest; }

  public String getDetectionId() { return detectionId; }
}
