package com.linkedin.thirdeye.bootstrap.segment.create;

public enum SegmentCreationPhaseConstants {

  SEGMENT_CREATION_SCHEMA_PATH("segment.creation.schema.path"),
  SEGMENT_CREATION_INPUT_PATH("segment.creation.input.path"), //
  SEGMENT_CREATION_OUTPUT_PATH("segment.creation.output.path"), //
  SEGMENT_CREATION_CONFIG_PATH("segment.creation.config.path"),
  SEGMENT_CREATION_SEGMENT_TABLE_NAME("segment.creation.segment.table.name");

  String name;

  SegmentCreationPhaseConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}