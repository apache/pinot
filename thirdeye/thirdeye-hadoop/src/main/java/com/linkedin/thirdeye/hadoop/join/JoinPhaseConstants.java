package com.linkedin.thirdeye.hadoop.join;

public enum JoinPhaseConstants {
  // SCHEMA AND INPUT PER SOURCE actual property access would be {source}.join.input.path
  JOIN_INPUT_SCHEMA("join.input.schema"), // one schema for each source
  JOIN_INPUT_PATH("join.input.path"), // one input for each source
  JOIN_OUTPUT_PATH("join.output.path"),
  JOIN_OUTPUT_SCHEMA("join.output.schema"),
  JOIN_SOURCE_NAMES("join.source.names"), // comma separated list of sources
  JOIN_CONFIG_UDF_CLASS("join.config.udf.class"),
  JOIN_UDF_CLASS("join.udf.class"),
  JOIN_KEY_EXTRACTOR_CLASS("join.key.extractor.class"),
  JOIN_KEY_EXTRACTOR_CONFIG("join.key.extractor.config"), // one for each source
  JOIN_UDF_CONFIG("join.udf.config"); // one for each source

  String name;

  JoinPhaseConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
