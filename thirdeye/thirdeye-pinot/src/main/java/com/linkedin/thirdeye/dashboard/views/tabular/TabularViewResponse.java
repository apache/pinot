package com.linkedin.thirdeye.dashboard.views.tabular;

import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.dashboard.views.GenericResponse;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.dashboard.views.ViewResponse;

public class TabularViewResponse implements ViewResponse {
  List<String> metrics;
  List<TimeBucket> timeBuckets;
  Map<String, String> summary;
  Map<String, GenericResponse> data;

  public TabularViewResponse() {
    super();
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<String> metrics) {
    this.metrics = metrics;
  }

  public void setTimeBuckets(List<TimeBucket> timeBuckets) {
    this.timeBuckets = timeBuckets;
  }

  public List<TimeBucket> getTimeBuckets() {
    return timeBuckets;
  }

  public Map<String, String> getSummary() {
    return summary;
  }

  public void setSummary(Map<String, String> summary) {
    this.summary = summary;
  }

  public Map<String, GenericResponse> getData() {
    return data;
  }

  public void setData(Map<String, GenericResponse> data) {
    this.data = data;
  }

}
