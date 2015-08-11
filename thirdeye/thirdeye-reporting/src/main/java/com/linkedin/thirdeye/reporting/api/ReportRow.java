package com.linkedin.thirdeye.reporting.api;


import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.api.DimensionKey;

public class ReportRow {

  private DateTime currentStart;
  private DateTime currentEnd;
  private DateTime baselineStart;
  private DateTime baselineEnd;
  private String date;
  private DimensionKey dimensionKey;
  private String dimension;
  private String metric;
  private Number baseline;
  private Number current;
  private String ratio;

  private static DateFormat DATE_FORMAT = new SimpleDateFormat("HH a");

  public ReportRow(DateTime currentStart, DateTime currentEnd, DateTime baselineStart, DateTime baselineEnd,
      DimensionKey dimensionKey, String dimension, String metric, Number baseline, Number current) {
    this.currentStart = currentStart;
    this.date = DATE_FORMAT.format(currentStart.toDate());
    this.currentEnd = currentEnd;
    this.baselineStart = baselineStart;
    this.baselineEnd = baselineEnd;
    this.dimensionKey = dimensionKey;
    this.dimension = dimension;
    this.metric = metric;
    this.baseline = baseline;
    this.current = current;
  }

  public ReportRow(ReportRow copyReportRow) {
    this.currentStart = copyReportRow.currentStart;
    this.date = DATE_FORMAT.format(currentStart.toDate());
    this.currentEnd = copyReportRow.currentEnd;
    this.baselineStart = copyReportRow.baselineStart;
    this.baselineEnd = copyReportRow.baselineEnd;
    this.metric = copyReportRow.metric;
  }

  public ReportRow() {

  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public String getDimension() {
    return dimension;
  }

  public void setDimension(String dimension) {
    this.dimension = dimension;
  }

  public DateTime getCurrentStart() {
    return currentStart;
  }

  public void setCurrentStart(DateTime currentStart) {
    this.currentStart = currentStart;
  }

  public DateTime getCurrentEnd() {
    return currentEnd;
  }

  public void setCurrentEnd(DateTime currentEnd) {
    this.currentEnd = currentEnd;
  }

  public DateTime getBaselineStart() {
    return baselineStart;
  }

  public void setBaselineStart(DateTime baselineStart) {
    this.baselineStart = baselineStart;
  }

  public DateTime getBaselineEnd() {
    return baselineEnd;
  }

  public void setBaselineEnd(DateTime baselineEnd) {
    this.baselineEnd = baselineEnd;
  }

  public DimensionKey getDimensionKey() {
    return dimensionKey;
  }

  public void setDimensionKey(DimensionKey dimensionKey) {
    this.dimensionKey = dimensionKey;
  }

  public Number getBaseline() {
    return baseline;
  }

  public void setBaseline(Number baseline) {
    this.baseline = baseline;
  }

  public Number getCurrent() {
    return current;
  }

  public void setCurrent(Number current) {
    this.current = current;
  }

  public String getRatio() {
    return ratio;
  }

  public void setRatio(String ratio) {
    this.ratio = ratio;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

}
