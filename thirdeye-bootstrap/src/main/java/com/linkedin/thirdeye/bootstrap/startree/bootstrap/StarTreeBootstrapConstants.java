package com.linkedin.thirdeye.bootstrap.startree.bootstrap;

public enum StarTreeBootstrapConstants {
  STAR_TREE_GENERATION_OUTPUT_PATH("startree.generation.output.path"), //
  STAR_TREE_BOOTSTRAP_INPUT_PATH("startree.bootstrap.input.path"), //
  STAR_TREE_BOOTSTRAP_OUTPUT_PATH("startree.bootstrap.output.path"), //
  STAR_TREE_BOOTSTRAP_CONFIG_PATH("startree.bootstrap.config.path"), 
  STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA("startree.bootstrap.input.avro.schema");//
  String name;

  StarTreeBootstrapConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }
}
