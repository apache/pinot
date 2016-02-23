package com.linkedin.thirdeye.bootstrap.topkrollup.phase2;

public enum TopKRollupPhaseTwoConstants {
  TOPK_ROLLUP_PHASE2_INPUT_PATH("topk.rollup.phase2.input.path"), //
  TOPK_ROLLUP_PHASE2_OUTPUT_PATH("topk.rollup.phase2.output.path"), //
  TOPK_ROLLUP_PHASE2_CONFIG_PATH("topk.rollup.phase2.config.path");

  String name;

  TopKRollupPhaseTwoConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
