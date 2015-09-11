package com.linkedin.thirdeye.reporting.api;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

public class ReportEmailCssSpec {

  private String timeTitleStyle;
  private String titleStyle2;
  private String titleStyle3;

  private String hourTitleStyle;
  private String groupByTitleStyle;
  private String metricTitleStyle;
  private String currentTitleStyle;
  private String baselineTitleStyle;
  private String ratioTitleStyle;

  private String cellStyle1;
  private String cellStyle2;
  private String cellStyle3;
  private String cellStyle4;

  private String metricCellTotalPositiveStyle;
  private String metricCellTotalNegativeStyle;
  private String metricCellPositiveStyle;
  private String metricCellNegativeStyle;

  private String tableStyle1;
  private String tableStyle2;

  private String anomalyMetric;
  private String anomalyTitleStyle;
  private String anomalyViolations;
  private String anomalyViolationsCount;
  private String anomalyDimensionSchema;
  private String anomalyCell1;
  private String anomalyCell2;
  private String anomalyCell3;

  public ReportEmailCssSpec() throws IOException {

    timeTitleStyle = loadCss("time-title-style.css");
    titleStyle2 = loadCss("title-style-2.css");
    titleStyle3 = loadCss("title-style-3.css");

    hourTitleStyle = loadCss("hour-title-style.css");
    groupByTitleStyle = loadCss("groupby-title-style.css");
    metricTitleStyle = loadCss("metric-title-style.css");
    currentTitleStyle = loadCss("current-title-style.css");
    baselineTitleStyle = loadCss("baseline-title-style.css");
    ratioTitleStyle = loadCss("ratio-title-style.css");

    cellStyle1 = loadCss("cell-style-1.css");
    cellStyle2 = loadCss("cell-style-2.css");
    cellStyle3 = loadCss("cell-style-3.css");
    cellStyle4 = loadCss("cell-style-4.css");

    metricCellTotalPositiveStyle = loadCss("metric-cell-total-positive-style.css");
    metricCellTotalNegativeStyle = loadCss("metric-cell-total-negative-style.css");
    metricCellNegativeStyle = loadCss("metric-cell-negative-style.css");
    metricCellPositiveStyle = loadCss("metric-cell-positive-style.css");

    tableStyle1 = loadCss("table-style-1.css");
    tableStyle2 = loadCss("table-style-2.css");

    anomalyMetric = loadCss("anomaly-metric.css");
    anomalyTitleStyle = loadCss("anomaly-title-style.css");
    anomalyViolations = loadCss("anomaly-violations.css");
    anomalyViolationsCount = loadCss("anomaly-violations-count.css");
    anomalyDimensionSchema = loadCss("anomaly-dimension-schema.css");
    anomalyCell1 = loadCss("anomaly-cell-1.css");
    anomalyCell2 = loadCss("anomaly-cell-2.css");
    anomalyCell3 = loadCss("anomaly-cell-3.css");
  }

  public String getTimeTitleStyle() {
    return timeTitleStyle;
  }

  public String getTitleStyle2() {
    return titleStyle2;
  }

  public String getTitleStyle3() {
    return titleStyle3;
  }

  public String getHourTitleStyle() {
    return hourTitleStyle;
  }

  public String getGroupByTitleStyle() {
    return groupByTitleStyle;
  }

  public String getMetricTitleStyle() {
    return metricTitleStyle;
  }

  public String getCurrentTitleStyle() {
    return currentTitleStyle;
  }

  public String getBaselineTitleStyle() {
    return baselineTitleStyle;
  }

  public String getRatioTitleStyle() {
    return ratioTitleStyle;
  }

  public String getCellStyle1() {
    return cellStyle1;
  }

  public String getCellStyle2() {
    return cellStyle2;
  }

  public String getCellStyle3() {
    return cellStyle3;
  }

  public String getCellStyle4() {
    return cellStyle4;
  }


  public String getMetricCellTotalPositiveStyle() {
    return metricCellTotalPositiveStyle;
  }

  public String getMetricCellTotalNegativeStyle() {
    return metricCellTotalNegativeStyle;
  }

  public String getMetricCellPositiveStyle() {
    return metricCellPositiveStyle;
  }

  public String getMetricCellNegativeStyle() {
    return metricCellNegativeStyle;
  }

  public String getTableStyle1() {
    return tableStyle1;
  }

  public String getTableStyle2() {
    return tableStyle2;
  }

  public String getAnomalyMetric() {
    return anomalyMetric;
  }

  public String getAnomalyTitleStyle() {
    return anomalyTitleStyle;
  }

  public String getAnomalyViolations() {
    return anomalyViolations;
  }

  public String getAnomalyViolationsCount() {
    return anomalyViolationsCount;
  }

  public String getAnomalyDimensionSchema() {
    return anomalyDimensionSchema;
  }

  public String getAnomalyCell1() {
    return anomalyCell1;
  }

  public String getAnomalyCell2() {
    return anomalyCell2;
  }

  public String getAnomalyCell3() {
    return anomalyCell3;
  }

  private String loadCss(String cssFile) throws IOException {

    String css = null;
    InputStream is = ClassLoader.getSystemResourceAsStream("assets/css/" + cssFile);

    try {
      css = IOUtils.toString(is);
    } finally {
      IOUtils.closeQuietly(is);
    }

    return css;
  }



}
