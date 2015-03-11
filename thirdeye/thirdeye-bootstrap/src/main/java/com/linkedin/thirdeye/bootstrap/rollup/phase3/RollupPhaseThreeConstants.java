package com.linkedin.thirdeye.bootstrap.rollup.phase3;

public enum RollupPhaseThreeConstants {

  ROLLUP_PHASE3_INPUT_PATH("rollup.phase3.input.path"), //
  ROLLUP_PHASE3_OUTPUT_PATH("rollup.phase3.output.path"), //
  ROLLUP_PHASE3_CONFIG_PATH("rollup.phase3.config.path");//

  String name;

  RollupPhaseThreeConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }
}
