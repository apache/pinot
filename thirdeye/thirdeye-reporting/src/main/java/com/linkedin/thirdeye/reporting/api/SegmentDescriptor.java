package com.linkedin.thirdeye.reporting.api;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.base.BaseDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.thirdeye.api.StarTreeConstants;

import java.io.File;

public class SegmentDescriptor{
  private File file;
  private DateTime startWallTime;
  private DateTime endWallTime;
  private DateTime startDataTime;
  private DateTime endDataTime;


  public SegmentDescriptor() {

  }

  public SegmentDescriptor(File file,
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
    String[] tokens = file.getName().split("_");
    return StarTreeConstants.DATE_TIME_FORMATTER.parseDateTime(tokens[2]);

  }


  public BaseDateTime getStartWallTime(String timezone) {
    String[] tokens = file.getName().split("_");
    return StarTreeConstants.DATE_TIME_FORMATTER.withZone(DateTimeZone.forID(timezone)).parseDateTime(tokens[2]);
  }

  public DateTime getEndWallTime() {
    String[] tokens = file.getName().split("_");
    return StarTreeConstants.DATE_TIME_FORMATTER.parseDateTime(tokens[3]);
  }

  public DateTime getEndWallTime(String timezone) {
    String[] tokens = file.getName().split("_");
    return StarTreeConstants.DATE_TIME_FORMATTER.withZone(DateTimeZone.forID(timezone)).parseDateTime(tokens[3]);
  }

  public DateTime getStartDataTime() {
    return startDataTime;
  }

  public DateTime getEndDataTime() {
    return endDataTime;
  }

  public boolean includesTime(long time, String timezone) {
    String[] tokens = file.getName().split("_");
    long wallStartTime = StarTreeConstants.DATE_TIME_FORMATTER.withZone(DateTimeZone.forID(timezone)).parseDateTime(tokens[2]).getMillis();
    long wallEndTime = StarTreeConstants.DATE_TIME_FORMATTER.withZone(DateTimeZone.forID(timezone)).parseDateTime(tokens[3]).getMillis();
    if (time >= wallStartTime && time < wallEndTime) {
      return true;
    }
    return false;
  }


}
