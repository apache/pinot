package com.linkedin.thirdeye.anomaly.views;

import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AnomalyTimelinesView {
  List<TimeBucket> timeBuckets = new ArrayList<>();
  Map<String, String> summary = new HashMap<>();
  List<Double> currentValues = new ArrayList<>();
  List<Double> baselineValues = new ArrayList<>();

  public List<TimeBucket> getTimeBuckets() {
    return timeBuckets;
  }

  public void addTimeBuckets(TimeBucket timeBucket) {
    this.timeBuckets.add(timeBucket);
  }

  public Map<String, String> getSummary() {
    return summary;
  }

  public void addSummary(String key, String value) {
    this.summary.put(key, value);
  }

  public List<Double> getCurrentValues() {
    return currentValues;
  }

  public void addCurrentValues(Double currentValue) {
    this.currentValues.add(currentValue);
  }

  public List<Double> getBaselineValues() {
    return baselineValues;
  }

  public void addBaselineValues(Double baselineValue) {
    this.baselineValues.add(baselineValue);
  }
}
