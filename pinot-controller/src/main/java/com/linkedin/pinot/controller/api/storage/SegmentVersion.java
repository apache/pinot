package com.linkedin.pinot.controller.api.storage;

public class SegmentVersion {
  private String _uuid;
  private String _timestamp;

  private static final String SEPARATOR = "_";

  public SegmentVersion(String uuid, String timestamp) {
    _uuid = uuid;
    _timestamp = timestamp;
  }

  public String toString() {
    return _uuid + SEPARATOR + _timestamp;
  }
}
