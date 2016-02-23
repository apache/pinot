package com.linkedin.thirdeye.bootstrap.analysis;

public enum AnalysisJobConstants {
  ANALYSIS_INPUT_AVRO_SCHEMA("analysis.input.avro.schema"),
  ANALYSIS_INPUT_PATH("analysis.input.path"),
  ANALYSIS_CONFIG_PATH("analysis.config.path"),
  ANALYSIS_OUTPUT_PATH("analysis.output.path"),
  ANALYSIS_FILE_NAME("results.json");

  String name;

  AnalysisJobConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }
}
