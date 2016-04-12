package com.linkedin.thirdeye.hadoop;

public enum ThirdEyeJobConstants {
  THIRDEYE_FLOW_SCHEDULE("thirdeye.flow.schedule"), // HOURLY, DAILY
  THIRDEYE_PHASE("thirdeye.phase"), // segment_creation, segment_push
  THIRDEYE_ROOT("thirdeye.root"),
  THIRDEYE_COLLECTION("thirdeye.collection"),
  THIRDEYE_TIME_MIN("thirdeye.time.min"), // YYYY-mm-ddThh
  THIRDEYE_TIME_MAX("thirdeye.time.max"),
  INPUT_PATHS("input.paths"),
  THIRDEYE_MR_CONF("thirdeye.mr.conf"),
  THIRDEYE_PINOT_CONTROLLER_HOSTS("thirdeye.pinot.controller.hosts"),
  THIRDEYE_PINOT_CONTROLLER_PORT("thirdeye.pinot.controller.port");

  private final String propertyName;

  ThirdEyeJobConstants(String propertyName) {
    this.propertyName = propertyName;
  }

  public String getName() {
    return propertyName;
  }
}
