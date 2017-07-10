package com.linkedin.thirdeye.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;
import javax.swing.*;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.title.TextTitle;
import org.jfree.chart.title.Title;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.joda.time.DateTime;


public class TimeSeriesLineChart extends JFrame {
  private TimeSeriesCollection xyDataset;
  private JPanel chartPanel;
  private JFreeChart chart;
  private final static String DEFAULT_SUBTITLE_FORMAT = "Severity: %.2f %%, Current Value: %.2f, Baseline Value %.2f";

  private boolean showLegend = true;
  private boolean createURL = false;
  private boolean createTooltip = false;
  private boolean autoSort = false;
  private boolean allowDuplicateXValues = false;
  private String timezoneCode = "America/Los_Angeles";

  public TimeSeriesLineChart(String panelTitle) {
    super(panelTitle);

    TimeZone timeZone = TimeZone.getTimeZone(timezoneCode);

    this.xyDataset = new TimeSeriesCollection(timeZone);
  }

  public void loadTimeSeries(String legendTitle, Map<DateTime, Double> wowTimeSeries) {
    TimeSeries timeSeries = new TimeSeries(legendTitle);

    ArrayList<DateTime> timestamps = new ArrayList<>(wowTimeSeries.keySet());
    Collections.sort(timestamps);


    for(int i = 0; i < timestamps.size(); i++) {
      DateTime timestamp = timestamps.get(i);
      Millisecond millisecondTick = new Millisecond(timestamp.toDate());
      double timeSeriesValue = wowTimeSeries.get(timestamp);
      timeSeries.add(millisecondTick, timeSeriesValue);
    }

    xyDataset.addSeries(timeSeries);
  }

  public void createChartPanel(String chartTitle, String subTitleString) {
    String xAxisLabel = "Date";
    String yAxisLabel = "Count";

    chart = ChartFactory.createTimeSeriesChart(chartTitle,
        xAxisLabel, yAxisLabel, xyDataset, showLegend, createTooltip, createURL);

    Title subTitle = new TextTitle(subTitleString);
    chart.addSubtitle(subTitle);

    chartPanel = new ChartPanel(chart);
  }

  public void markAnomaly(DateTime start, DateTime end) {
    //TODO

  }

  public static String defaultSubtitle(double currentValue, double baselineValue) {
    double severity = currentValue/baselineValue - 1;
    return String.format(DEFAULT_SUBTITLE_FORMAT, severity * 100, currentValue, baselineValue);
  }

  public void saveAsPNG(File imageFile) {
    int width = 640;
    int height = 480;

    saveAsPNG(imageFile, width, height);
  }

  public void saveAsPNG(File imageFile, int width, int height) {
    try {
      ChartUtilities.saveChartAsPNG(imageFile, chart, width, height);
    } catch (IOException ex) {
      System.err.println(ex);
    }
  }

  public JFreeChart getChart() {
    return chart;
  }

  public boolean isShowLegend() {
    return showLegend;
  }

  public void setShowLegend(boolean showLegend) {
    this.showLegend = showLegend;
  }

  public boolean isCreateURL() {
    return createURL;
  }

  public void setCreateURL(boolean createURL) {
    this.createURL = createURL;
  }

  public boolean isCreateTooltip() {
    return createTooltip;
  }

  public void setCreateTooltip(boolean createTooltip) {
    this.createTooltip = createTooltip;
  }

  public boolean isAutoSort() {
    return autoSort;
  }

  public void setAutoSort(boolean autoSort) {
    this.autoSort = autoSort;
  }

  public boolean isAllowDuplicateXValues() {
    return allowDuplicateXValues;
  }

  public void setAllowDuplicateXValues(boolean allowDuplicateXValues) {
    this.allowDuplicateXValues = allowDuplicateXValues;
  }

  public TimeSeriesCollection getXyDataset() {
    return xyDataset;
  }

  public String getTimezoneCode() {
    return timezoneCode;
  }

  public void setTimezoneCode(String timezoneCode) {
    this.timezoneCode = timezoneCode;
  }
}
