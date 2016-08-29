package com.linkedin.thirdeye.dashboard.configs;

import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.client.MetricExpression;

public class CollectionConfig extends AbstractConfig {

  public static double DEFAULT_THRESHOLD = 0.01;

  String collectionName;
  String collectionAlias;

  double metricThreshold = DEFAULT_THRESHOLD;

  boolean isActive = true;
  boolean enableCount = false; // Default __COUNT metric
  boolean metricAsDimension = false;
  String metricNamesColumn = null;
  String metricValuesColumn = null;

  Map<String, String> derivedMetrics;

  List<String> invertColorMetrics; // Invert colors of heatmap and table cells

  Map<String, MetricExpression> cellSizeExpression; // Expression defining how to compute cell size

  public CollectionConfig() {

  }

  public String getCollectionName() {
    return collectionName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }

  public String getCollectionAlias() {
    return collectionAlias;
  }

  public void setCollectionAlias(String collectionAlias) {
    this.collectionAlias = collectionAlias;
  }

  public double getMetricThreshold() {
    return metricThreshold;
  }

  public void setMetricThreshold(double metricThreshold) {
    this.metricThreshold = metricThreshold;
  }

  public boolean isActive() {
    return isActive;
  }

  public void setActive(boolean isActive) {
    this.isActive = isActive;
  }

  public boolean isEnableCount() {
    return enableCount;
  }

  public void setEnableCount(boolean enableCount) {
    this.enableCount = enableCount;
  }

  public boolean isMetricAsDimension() {
    return metricAsDimension;
  }

  public void setMetricAsDimension(boolean metricAsDimension) {
    this.metricAsDimension = metricAsDimension;
  }

  public String getMetricValuesColumn() {
    return metricValuesColumn;
  }

  public void setMetricValuesColumn(String metricValuesColumn) {
    this.metricValuesColumn = metricValuesColumn;
  }

  public String getMetricNamesColumn() {
    return metricNamesColumn;
  }

  public void setMetricNamesColumn(String metricNamesColumn) {
    this.metricNamesColumn = metricNamesColumn;
  }

  public Map<String, String> getDerivedMetrics() {
    return derivedMetrics;
  }

  public void setDerivedMetrics(Map<String, String> derivedMetrics) {
    this.derivedMetrics = derivedMetrics;
  }

  public List<String> getInvertColorMetrics() {
    return invertColorMetrics;
  }

  public void setInvertColorMetrics(List<String> invertColorMetrics) {
    this.invertColorMetrics = invertColorMetrics;
  }


  public Map<String, MetricExpression> getCellSizeExpression() {
    return cellSizeExpression;
  }

  public void setCellSizeExpression(Map<String, MetricExpression> cellSizeExpression) {
    this.cellSizeExpression = cellSizeExpression;
  }

  @Override
  public String toJSON() throws Exception {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  @Override
  public String getConfigName() {
    return collectionName;
  }


}
