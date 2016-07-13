package com.linkedin.thirdeye.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class MetricFunction implements Comparable<MetricFunction> {

  public enum Function {
    SUM, COUNT, AVG
  }

  private Function functionName;
  private String metricName;

  public MetricFunction(){

  }

  public MetricFunction(@JsonProperty("functionName") Function functionName,
      @JsonProperty("metricName") String metricName) {
    this.functionName = functionName;
    this.metricName = metricName;
  }

  private String format(String functionName, String metricName) {
    return String.format("%s_%s", functionName, metricName);
  }

  @Override
  public String toString() {
    // TODO this is hardcoded for pinot's return column name, but there's no binding contract that
    // clients need to return response objects with these keys.
   return format(functionName.name(), metricName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(functionName, metricName);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MetricFunction)) {
      return false;
    }
    MetricFunction that = (MetricFunction) obj;
    return Objects.equal(this.functionName, that.functionName)
        && Objects.equal(this.metricName, that.metricName);
  }

  @Override
  public int compareTo(MetricFunction o) {
    return this.toString().compareTo(o.toString());
  }

  public Function getFunctionName() {
    return functionName;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public void setFunctionName(Function functionName) {
    this.functionName = functionName;
  }
}
