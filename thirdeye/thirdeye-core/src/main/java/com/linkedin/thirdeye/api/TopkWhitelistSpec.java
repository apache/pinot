package com.linkedin.thirdeye.api;

import java.util.List;
import java.util.Map;

/**
 * Config class to define topk and whitelist
 * threshold: dimension values which do not satisfy metric thresholds will be ignored
 * topKDimensionToMetricsSpec: list of dimension and a map of metric to topk value for that dimension
 * whitelist: values to whitelist for given dimension
 */
public class TopkWhitelistSpec {

  Map<String, Double> threshold;
  List<TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec;
  Map<String, String> whitelist;

  public TopkWhitelistSpec() {

  }

  public Map<String, Double> getThreshold() {
    return threshold;
  }

  public void setThreshold(Map<String, Double> threshold) {
    this.threshold = threshold;
  }

  public List<TopKDimensionToMetricsSpec> getTopKDimensionToMetricsSpec() {
    return topKDimensionToMetricsSpec;
  }

  public void setTopKDimensionToMetricsSpec(List<TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec) {
    this.topKDimensionToMetricsSpec = topKDimensionToMetricsSpec;
  }

  public Map<String, String> getWhitelist() {
    return whitelist;
  }

  public void setWhitelist(Map<String, String> whitelist) {
    this.whitelist = whitelist;
  }



}
