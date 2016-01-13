package com.linkedin.thirdeye.api;

import org.joda.time.DateTime;

import java.io.File;

public class SegmentDescriptor {
  private final File file;
  private final DateTime startWallTime;
  private final DateTime endWallTime;
  private final DateTime startDataTime;
  private final DateTime endDataTime;

  public SegmentDescriptor(File file, DateTime startWallTime, DateTime endWallTime,
      DateTime startDataTime, DateTime endDataTime) {
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
}
