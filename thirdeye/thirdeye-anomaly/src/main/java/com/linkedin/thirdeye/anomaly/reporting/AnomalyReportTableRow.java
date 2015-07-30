package com.linkedin.thirdeye.anomaly.reporting;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 */
public class AnomalyReportTableRow {

  private long timeWindow;
  private String date;
  private String dimensions;
  private String description;
  private boolean scoreIsPercent;
  private Number anomalyScore;
  private Number anomalyVolume;

  private static final DateFormat DATE_FORMAT;
  static {
    DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  }

  public AnomalyReportTableRow(long timeWindow, String dimensions, String description, boolean scoreIsPercent,
      Number anomalyScore, Number anomalyVolume) {
    super();
    this.timeWindow = timeWindow;
    this.date = DATE_FORMAT.format(new Date(timeWindow));
    this.dimensions = dimensions;
    this.description = description;
    this.scoreIsPercent = scoreIsPercent;
    this.anomalyScore = anomalyScore;
    this.anomalyVolume = anomalyVolume;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public long getTimeWindow() {
    return timeWindow;
  }

  public void setTimeWindow(long timeWindow) {
    this.timeWindow = timeWindow;
    this.date = DATE_FORMAT.format(new Date(timeWindow));
  }

  public String getDimensions() {
    return dimensions;
  }

  public void setDimensions(String dimensions) {
    this.dimensions = dimensions;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public boolean isScoreIsPercent() {
    return scoreIsPercent;
  }

  public void setScoreIsPercent(boolean scoreIsPercent) {
    this.scoreIsPercent = scoreIsPercent;
  }

  public Number getAnomalyScore() {
    return anomalyScore;
  }

  public void setAnomalyScore(Number anomalyScore) {
    this.anomalyScore = anomalyScore;
  }

  public Number getAnomalyVolume() {
    return anomalyVolume;
  }

  public void setAnomalyVolume(Number anomalyVolume) {
    this.anomalyVolume = anomalyVolume;
  }

  @Override
  public String toString() {
    return "\nAnomalyReportTableRow [timeWindow=" + timeWindow + ", date=" + date + ", dimensions=" + dimensions
        + ", description=" + description + ", scoreIsPercent=" + scoreIsPercent + ", anomalyScore=" + anomalyScore
        + ", anomalyVolume=" + anomalyVolume + "]";
  }

}
