package com.linkedin.thirdeye.bootstrap.startree.generation;

public enum StarTreeGenerationConstants {

  STAR_TREE_GEN_INPUT_PATH("startree.generation.input.path"), //
  STAR_TREE_GEN_OUTPUT_PATH("startree.generation.output.path"), //
  STAR_TREE_GEN_CONFIG_PATH("startree.generation.config.path");//
  String name;

  StarTreeGenerationConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }
}
