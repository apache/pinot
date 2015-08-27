package com.linkedin.thirdeye.anomaly.util;

import java.io.File;

import org.joda.time.DateTime;

/**
 * A copy of dashboard's segment descriptor used locally to avoid circular dependencies
 */
public class SegmentDescriptor {
  private File file;
  private DateTime startWallTime;
  private DateTime endWallTime;
  private DateTime startDataTime;
  private DateTime endDataTime;

  public SegmentDescriptor() {}

  public SegmentDescriptor(
      File file,
      DateTime startWallTime,
      DateTime endWallTime,
      DateTime startDataTime,
      DateTime endDataTime) {
    this.file = file;
    this.startWallTime = startWallTime;
    this.endWallTime = endWallTime;
    this.startDataTime = startDataTime;
    this.endDataTime = endDataTime;
  }

  public File getFile() {
    return file;
  }

  public DateTime getStartWallTime() {
    return startWallTime;
  }

  public DateTime getEndWallTime() {
    return endWallTime;
  }

  public DateTime getStartDataTime() {
    return startDataTime;
  }

  public DateTime getEndDataTime() {
    return endDataTime;
  }

  @Override
  public String toString() {
    if (file == null) {
      return "null";
    }
    return file.getName();
  }
}