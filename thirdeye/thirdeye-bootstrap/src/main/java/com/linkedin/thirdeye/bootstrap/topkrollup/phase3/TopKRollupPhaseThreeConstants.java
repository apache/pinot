package com.linkedin.thirdeye.bootstrap.topkrollup.phase3;

public enum TopKRollupPhaseThreeConstants {
  TOPK_ROLLUP_PHASE3_INPUT_PATH("topk.rollup.phase3.input.path"), //
  TOPK_ROLLUP_PHASE3_OUTPUT_PATH("topk.rollup.phase3.output.path"), //
  TOPK_ROLLUP_PHASE3_CONFIG_PATH("topk.rollup.phase3.config.path"),
  TOPK_DIMENSIONS_PATH("topk.dimensions.path");

  String name;

  TopKRollupPhaseThreeConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
