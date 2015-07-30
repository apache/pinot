package com.linkedin.thirdeye.anomaly.database;

import java.util.List;

/**
 *
 */
public class AnomalyTableRow {

  int id;
  int functionId;
  String functionName;
  String functionDescription;
  String collection;
  long timeWindow;
  int nonStarCount;
  String dimensions;
  double dimensionsContribution;
  List<String> metrics;
  double anomalyScore;
  double anomalyVolume;
  String properties;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getFunctionId() {
    return functionId;
  }

  public void setFunctionId(int functionId) {
    this.functionId = functionId;
  }

  public String getFunctionName() {
    return functionName;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionDescription() {
    return functionDescription;
  }

  public void setFunctionDescription(String functionDescription) {
    this.functionDescription = functionDescription;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public long getTimeWindow() {
    return timeWindow;
  }

  public void setTimeWindow(long timeWindow) {
    this.timeWindow = timeWindow;
  }

  public int getNonStarCount() {
    return nonStarCount;
  }

  public void setNonStarCount(int nonStarCount) {
    this.nonStarCount = nonStarCount;
  }

  public String getDimensions() {
    return dimensions;
  }

  public void setDimensions(String dimensions) {
    this.dimensions = dimensions;
  }

  public double getAnomalyScore() {
    return anomalyScore;
  }

  public void setAnomalyScore(double anomalyScore) {
    this.anomalyScore = anomalyScore;
  }

  public double getAnomalyVolume() {
    return anomalyVolume;
  }

  public void setAnomalyVolume(double anomalyVolume) {
    this.anomalyVolume = anomalyVolume;
  }

  public String getProperties() {
    return properties;
  }

  public void setProperties(String properties) {
    this.properties = properties;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<String> metrics) {
    this.metrics = metrics;
  }

  public double getDimensionsContribution() {
    return dimensionsContribution;
  }

  public void setDimensionsContribution(double dimensionsContribution) {
    this.dimensionsContribution = dimensionsContribution;
  }
}
