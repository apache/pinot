package com.linkedin.thirdeye.bootstrap.segment.push;

public enum SegmentPushPhaseConstants {

  SEGMENT_PUSH_INPUT_PATH("segment.push.input.path"), //
  SEGMENT_PUSH_CONTROLLER_HOSTS("segment.push.controller.hosts"),
  SEGMENT_PUSH_CONTROLLER_PORT("segment.push.controller.port"),
  SEGMENT_PUSH_TABLENAME("segment.push.tablename");

  String name;

  SegmentPushPhaseConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}