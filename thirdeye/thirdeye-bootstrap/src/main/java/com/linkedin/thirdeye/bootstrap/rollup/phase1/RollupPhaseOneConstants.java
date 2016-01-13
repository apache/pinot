package com.linkedin.thirdeye.bootstrap.rollup.phase1;

/**
 * @author kgopalak
 */
public enum RollupPhaseOneConstants {
  ROLLUP_PHASE1_INPUT_PATH("rollup.phase1.input.path"), //
  ROLLUP_PHASE1_OUTPUT_PATH("rollup.phase1.output.path"), //
  ROLLUP_PHASE1_CONFIG_PATH("rollup.phase1.config.path");//

  String name;

  RollupPhaseOneConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
