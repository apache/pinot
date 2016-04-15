package com.linkedin.thirdeye.hadoop.topk;

/**
 * This class contains the properties to be set for topk phase
 */
public enum TopKPhaseConstants {
  TOPK_ROLLUP_PHASE_INPUT_PATH("topk.rollup.phase.input.path"),
  TOPK_ROLLUP_PHASE_OUTPUT_PATH("topk.rollup.phase.output.path"),
  TOPK_ROLLUP_PHASE_CONFIG_PATH("topk.rollup.phase.config.path"),
  TOPK_ROLLUP_PHASE_SCHEMA_PATH("topk.rollup.phase.schema.path");

  String name;

  TopKPhaseConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
