package com.linkedin.thirdeye.bootstrap.rollup.phase2;

public enum RollupPhaseTwoConstants {
  ROLLUP_PHASE2_INPUT_PATH("rollup.phase2.input.path"), //
  ROLLUP_PHASE2_OUTPUT_PATH("rollup.phase2.output.path"), //
  ROLLUP_PHASE2_CONFIG_PATH("rollup.phase2.config.path");//

  String name;

  RollupPhaseTwoConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
