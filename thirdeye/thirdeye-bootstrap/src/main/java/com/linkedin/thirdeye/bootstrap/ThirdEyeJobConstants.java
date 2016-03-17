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
  THIRDEYE_POLL_TIMEOUT("thirdeye.poll.timeout"),
  THIRDEYE_CHECK_COMPLETENESS_CLASS("thirdeye.check.completeness.class"),
  THIRDEYE_COMPACTION("thirdeye.compaction"),
  THIRDEYE_PRESERVE_TIME_COMPACTION("thirdeye.preserve.time.compaction"),
  THIRDEYE_MR_CONF("thirdeye.mr.conf"),
  THIRDEYE_INPUT_CONVERTER_CLASS("thirdeye.input.converter.class"),
  THIRDEYE_TOPK("thirdeye.topk"),
  THIRDEYE_NUM_PARTITIONS("thirdeye.num.partitions"),
  THIRDEYE_PINOT_CONTROLLER_HOSTS("thirdeye.pinot.controller.hosts"),
  THIRDEYE_PINOT_CONTROLLER_PORT("thirdeye.pinot.controller.port"),
  THIRDEYE_AGG_JOB_DUMP_STATISTICS("thirdeye.agg.job.dump.statistics");

  private final String propertyName;

  ThirdEyeJobConstants(String propertyName) {
    this.propertyName = propertyName;
  }

  public String getName() {
    return propertyName;
  }
}
