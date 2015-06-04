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
  INPUT_PATHS("input.paths"),
  THIRDEYE_CLEANUP_DAYSAGO("thirdeye.cleanup.daysago"),
  THIRDEYE_CLEANUP_SKIP("thirdeye.cleanup.skip"),
  THIRDEYE_SKIP_MISSING("thirdeye.skip.missing"),
  THIRDEYE_FOLDER_PERMISSION("thirdeye.folder.permission"),
  THIRDEYE_POLL_ENABLE("thirdeye.poll.enable"),
  THIRDEYE_POLL_FREQUENCY("thirdeye.poll.frequency"),
  THIRDEYE_POLL_TIMEOUT("thirdeye.poll.timeout");

  private final String propertyName;

  ThirdEyeJobConstants(String propertyName) {
    this.propertyName = propertyName;
  }

  public String getName() {
    return propertyName;
  }
}
