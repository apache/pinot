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
import scala.Tuple2;


public class TimeSeriesLineChart extends JFrame {
  private TimeSeriesCollection xyDataset;
  private JPanel chartPanel;
  private JFreeChart chart;
  private double severity;

  private boolean showLegend = true;
  private boolean createURL = false;
  private boolean createTooltip = false;
  private boolean autoSort = false;
  private boolean allowDuplicateXValues = false;

  public TimeSeriesLineChart(String panelTitle) {
    super(panelTitle);

    TimeZone timeZone = TimeZone.getTimeZone("America/Los_Angeles");

    this.xyDataset = new TimeSeriesCollection(timeZone);
  }

  public void loadData(Map<DateTime, Tuple2<Double, Double>> timeseries) {
    TimeSeries currentSeries = new TimeSeries("Current Value");
    TimeSeries baselineSeries = new TimeSeries("Baseline Value");

    ArrayList<DateTime> timestamps = new ArrayList<>(timeseries.keySet());
    Collections.sort(timestamps);
    for(int i = 0; i < timestamps.size(); i++) {
      DateTime timestamp = timestamps.get(i);
      Millisecond millisecondTick = new Millisecond(timestamp.toDate());
      currentSeries.add(millisecondTick, timeseries.get(timestamp)._1());
      baselineSeries.add(millisecondTick, timeseries.get(timestamp)._2());
      severity = timeseries.get(timestamp)._1() / timeseries.get(timestamp)._2();
    }

    xyDataset.addSeries(currentSeries);
    xyDataset.addSeries(baselineSeries);
  }

  public void createChartPanel(String chartTitle) {
    String xAxisLabel = "Date";
    String yAxisLabel = "Count";

    chart = ChartFactory.createTimeSeriesChart(chartTitle,
        xAxisLabel, yAxisLabel, xyDataset, showLegend, createTooltip, createURL);
    Title subTitle = new TextTitle(String.format("Severity: %.2f %%", (severity - 1) * 100));
    chart.addSubtitle(subTitle);

    chartPanel = new ChartPanel(chart);
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
}
