package com.linkedin.thirdeye.bootstrap.transform;

public enum TransformPhaseJobConstants {
  TRANSFORM_INPUT_AVRO_SCHEMA("transform.input.schema"),
  TRANSFORM_INPUT_PATH("transform.input.path"),
  TRANSFORM_OUTPUT_PATH("transform.output.path"),
  TRANSFORM_OUTPUT_AVRO_SCHEMA("transform.output.schema"),
  TRANSFORM_SOURCE_NAMES("transform.source.names"),
  TRANSFORM_UDF("transform.udf.class"),
  TRANSFORM_CONFIG_UDF("transform.config.udf.class"),
  TRANSFORM_NUM_REDUCERS("transform.num.reducers");

  String name;

  TransformPhaseJobConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
