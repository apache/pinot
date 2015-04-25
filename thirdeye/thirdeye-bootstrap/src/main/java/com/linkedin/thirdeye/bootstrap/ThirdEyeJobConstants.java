package com.linkedin.thirdeye.bootstrap;

public enum ThirdEyeJobConstants {
  THIRDEYE_FLOW("thirdeye.flow"),
  THIRDEYE_FLOW_SCHEDULE("thirdeye.flow.schedule"),
  THIRDEYE_PHASE("thirdeye.phase"),
  THIRDEYE_ROOT("thirdeye.root"),
  THIRDEYE_COLLECTION("thirdeye.collection"),
  THIRDEYE_SERVER_URI("thirdeye.server.uri"),
  THIRDEYE_TIME_PATH("thirdeye.time.path"),
  THIRDEYE_TIME_MIN("thirdeye.time.min"),
  THIRDEYE_TIME_MAX("thirdeye.time.max"),
  THIRDEYE_DIMENSION_INDEX_REF("thirdeye.dimension.index.ref"),
  INPUT_PATHS("input.paths");

  private final String propertyName;

  ThirdEyeJobConstants(String propertyName) {
    this.propertyName = propertyName;
  }

  public String getName() {
    return propertyName;
  }
}
