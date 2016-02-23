package com.linkedin.thirdeye.bootstrap.join;

public enum JoinPhaseJobConstants {
  JOIN_CONFIG_PATH("join.config.path"),
  // SCHEMA AND INPUT PER SOURCE actual property access would be {source}.join.input.path
  JOIN_INPUT_AVRO_SCHEMA("join.input.schema"), // one input for each source
  JOIN_INPUT_PATH("join.input.path"),
  JOIN_OUTPUT_PATH("join.output.path"),
  JOIN_OUTPUT_AVRO_SCHEMA("join.output.schema");

  String name;

  JoinPhaseJobConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
