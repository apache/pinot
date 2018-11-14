package com.linkedin.thirdeye.detection.spec;

public class SitewideImpactRuleAnomalyFilterSpec extends AbstractSpec {
  private String timezone = "UTC";
  private String sitewideMetricUrn;
  private double threshold = Double.NaN;
  private String offset = "wo1w";

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

  public String getSitewideMetricUrn() {
    return sitewideMetricUrn;
  }

  public void setSitewideMetricUrn(String sitewideMetricUrn) {
    this.sitewideMetricUrn = sitewideMetricUrn;
  }

  public double getThreshold() {
    return threshold;
  }

  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }
}
