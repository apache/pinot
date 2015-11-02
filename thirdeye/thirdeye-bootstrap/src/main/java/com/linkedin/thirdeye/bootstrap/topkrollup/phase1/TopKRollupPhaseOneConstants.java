package com.linkedin.thirdeye.bootstrap.topkrollup.phase1;

public enum TopKRollupPhaseOneConstants {
  TOPK_ROLLUP_PHASE1_INPUT_PATH("topk.rollup.phase1.input.path"), //
  TOPK_ROLLUP_PHASE1_OUTPUT_PATH("topk.rollup.phase1.output.path"), //
  TOPK_ROLLUP_PHASE1_CONFIG_PATH("topk.rollup.phase1.config.path"),
  TOPK_ROLLUP_PHASE1_SCHEMA_PATH("topk.rollup.phase1.schema.path"),
  TOPK_ROLLUP_PHASE1_METRIC_SUMS_PATH("topk.rollup.phase1.metric.sums.path");//

  String name;

  TopKRollupPhaseOneConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
