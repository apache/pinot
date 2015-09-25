package com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1;

public enum StarTreeBootstrapPhaseOneConstants {
  STAR_TREE_GENERATION_OUTPUT_PATH("startree.generation.output.path"), //
  STAR_TREE_BOOTSTRAP_INPUT_PATH("startree.bootstrap.phase1.input.path"), //
  STAR_TREE_BOOTSTRAP_OUTPUT_PATH("startree.bootstrap.phase1.output.path"), //
  STAR_TREE_BOOTSTRAP_CONFIG_PATH("startree.bootstrap.phase1.config.path"),
  STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA("startree.bootstrap.phase1.input.avro.schema"),
  STAR_TREE_BOOTSTRAP_COMPACTION("startree.bootstrap.phase1.compaction");//
  String name;

  StarTreeBootstrapPhaseOneConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }
}
