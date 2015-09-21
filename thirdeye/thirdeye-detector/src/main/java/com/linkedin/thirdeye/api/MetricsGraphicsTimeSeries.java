package com.linkedin.thirdeye.api;

import io.dropwizard.jackson.JsonSnakeCase;

import java.util.List;
import java.util.Map;

@JsonSnakeCase
public class MetricsGraphicsTimeSeries {
  private String description;
  private String title;
  private Object data;
  private String xAccessor;
  private String yAccessor;
  private List<String> legend;
  private List<Map<String, Object>> markers;
  private List<AnomalyResult> anomalyResults;

  public MetricsGraphicsTimeSeries() {
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public Object getData() {
    return data;
  }

  public void setData(Object data) {
    this.data = data;
  }

  public String getxAccessor() {
    return xAccessor;
  }

  public void setxAccessor(String xAccessor) {
    this.xAccessor = xAccessor;
  }

  public String getyAccessor() {
    return yAccessor;
  }

  public void setyAccessor(String yAccessor) {
    this.yAccessor = yAccessor;
  }

  public List<Map<String, Object>> getMarkers() {
    return markers;
  }

  public void setMarkers(List<Map<String, Object>> markers) {
    this.markers = markers;
  }

  public List<String> getLegend() {
    return legend;
  }

  public void setLegend(List<String> legend) {
    this.legend = legend;
  }

  public List<AnomalyResult> getAnomalyResults() {
    return anomalyResults;
  }

  public void setAnomalyResults(List<AnomalyResult> anomalyResults) {
    this.anomalyResults = anomalyResults;
  }
}

