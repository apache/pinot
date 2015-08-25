package com.linkedin.thirdeye.anomaly.util;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.api.MetricSpec;

/**
 *
 */
public class MetricSpecUtils {

  public static MetricSpec findMetricSpec(String metricName, List<MetricSpec> metricSpecs) {
    for (MetricSpec metricSpec : metricSpecs) {
      if (metricName.equals(metricSpec.getName())) {
        return metricSpec;
      }
    }
    return null;
  }

  public static List<String> getMetricNames(List<MetricSpec> metricSpecs) {
    List<String> metricNames = new ArrayList<String>(metricSpecs.size());
    for (MetricSpec metricSpec : metricSpecs) {
      metricNames.add(metricSpec.getName());
    }
    return metricNames;
  }

}


