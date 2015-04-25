package com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2;

public enum StarTreeBootstrapPhaseTwoConstants {
  STAR_TREE_GENERATION_OUTPUT_PATH("startree.generation.output.path"), //
  STAR_TREE_BOOTSTRAP_PHASE2_INPUT_PATH("startree.bootstrap.phase2.input.path"), //
  STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH("startree.bootstrap.phase2.output.path"), //
  STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH("startree.bootstrap.phase2.config.path");

  String name;

  StarTreeBootstrapPhaseTwoConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }
}
