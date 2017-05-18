package com.linkedin.thirdeye.dashboard.configs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.datasource.MetricExpression;

import java.util.concurrent.TimeUnit;


@JsonIgnoreProperties(ignoreUnknown = true)
public class CollectionConfig extends AbstractConfig {

  public static double DEFAULT_THRESHOLD = 0.01;
  public static String DEFAULT_TIMEZONE = "UTC";
  public static String DEFAULT_PREAGGREGATED_DIMENSION_VALUE = "all";
  public static int DEFAULT_NONADDITIVE_BUCKET_SIZE = 5;
  public static String DEFAULT_NONADDITIVE_BUCKET_UNIT = TimeUnit.MINUTES.toString();

  String collectionName;
  String collectionAlias;
  double metricThreshold = DEFAULT_THRESHOLD;
  boolean isActive = true;
  boolean enableCount = false; // Default __COUNT metric
  String timezone = null;
  boolean metricAsDimension = false;
  String metricNamesColumn = null;
  String metricValuesColumn = null;

  boolean isNonAdditive = false;
  List<String> dimensionsHaveNoPreAggregation = Collections.emptyList();
  String preAggregatedKeyword = DEFAULT_PREAGGREGATED_DIMENSION_VALUE;
  int nonAdditiveBucketSize = DEFAULT_NONADDITIVE_BUCKET_SIZE;
  String nonAdditiveBucketUnit = DEFAULT_NONADDITIVE_BUCKET_UNIT;

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

  public String getTimezone() {
    return timezone;
  }

  public void setTimezone(String timezone) {
    this.timezone = timezone;
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

  public boolean isNonAdditive() {
    return isNonAdditive;
  }

  public void setIsNonAdditive(boolean isNonAdditive) {
    this.isNonAdditive = isNonAdditive;
  }

  public List<String> getDimensionsHaveNoPreAggregation() {
    return dimensionsHaveNoPreAggregation;
  }

  public void setDimensionsHaveNoPreAggregation(List<String> dimensionsHaveNoPreAggregation) {
    this.dimensionsHaveNoPreAggregation =
        (dimensionsHaveNoPreAggregation == null) ? Collections.<String>emptyList() : dimensionsHaveNoPreAggregation;
  }

  public String getPreAggregatedKeyword() {
    return preAggregatedKeyword;
  }

  public void setPreAggregatedKeyword(String preAggregatedKeyword) {
    this.preAggregatedKeyword = preAggregatedKeyword == null ? DEFAULT_PREAGGREGATED_DIMENSION_VALUE : preAggregatedKeyword;
  }

  public int getNonAdditiveBucketSize() {
    return nonAdditiveBucketSize;
  }

  public void setNonAdditiveBucketSize(int size) {
    this.nonAdditiveBucketSize = size;
  }

  public String getNonAdditiveBucketUnit() {
    return nonAdditiveBucketUnit;
  }

  public void setNonAdditiveBucketUnit(String bucketUnit) {
    this.nonAdditiveBucketUnit = bucketUnit;
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
