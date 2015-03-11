package com.linkedin.thirdeye.bootstrap.rollup.phase4;

public enum RollupPhaseFourConstants {
  ROLLUP_PHASE4_INPUT_PATH("rollup.phase4.input.path"), //
  ROLLUP_PHASE4_OUTPUT_PATH("rollup.phase4.output.path"), //
  ROLLUP_PHASE4_CONFIG_PATH("rollup.phase4.config.path");//

  String name;

  RollupPhaseFourConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }
}
