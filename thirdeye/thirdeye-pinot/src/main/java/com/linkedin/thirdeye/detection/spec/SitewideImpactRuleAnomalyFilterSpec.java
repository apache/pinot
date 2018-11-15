package com.linkedin.thirdeye.detection.spec;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class SitewideImpactRuleAnomalyFilterSpec extends AbstractSpec {
  private String timezone = "UTC";
  private double threshold = Double.NaN;
  private String offset;
  private String pattern;
  private String sitewideMetricName;
  private String sitewideCollection;
  private Map<String, Collection<String>> filters = new HashMap<>();

  public String getSitewideMetricName() {
    return sitewideMetricName;
  }

  public void setSitewideMetricName(String sitewideMetricName) {
    this.sitewideMetricName = sitewideMetricName;
  }

  public String getSitewideCollection() {
    return sitewideCollection;
  }

  public void setSitewideCollection(String sitewideCollection) {
    this.sitewideCollection = sitewideCollection;
  }

  public Map<String, Collection<String>> getFilters() {
    return filters;
  }

  public void setFilters(Map<String, Collection<String>> filters) {
    this.filters = filters;
  }

  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  public String getOffset() {
    return offset;
  }

  public void setOffset(String offset) {
    this.offset = offset;
  }

  public String getTimezone() {
    return timezone;
  }

  public void setTimezone(String timezone) {
    this.timezone = timezone;
  }

  public double getThreshold() {
    return threshold;
  }

  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }
}
