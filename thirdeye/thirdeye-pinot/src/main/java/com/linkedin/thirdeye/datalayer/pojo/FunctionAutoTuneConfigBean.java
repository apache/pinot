package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType;
import com.linkedin.thirdeye.anomaly.detection.lib.PerformanceEvaluationMethod;
import java.util.Map;
import java.util.Objects;


@JsonIgnoreProperties(ignoreUnknown=true)
public class FunctionAutoTuneConfigBean extends AbstractBean {
  // The function id which is autotuned
  private long functionId;
  // The start time of the autotune monitoring window
  private long startTime;
  // The end time of the autotune monitoring window
  private long endTime;
  // The autotune method we used
  private AutotuneMethodType autotuneMethod;
  // The performance evaluation method which is used for performance evaluation
  private PerformanceEvaluationMethod performanceEvaluationMethod;
  // The performance value, can be Number or any Comparable class
  private Map<String, String> performance;
  // The tuned properties configuration
  private Map<String, String> configuration;

  public long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(long functionId) {
    this.functionId = functionId;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public AutotuneMethodType getAutotuneMethod() {
    return autotuneMethod;
  }

  public void setAutotuneMethod(AutotuneMethodType autotuneMethod) {
    this.autotuneMethod = autotuneMethod;
  }

  public PerformanceEvaluationMethod getPerformanceEvaluationMethod() {
    return performanceEvaluationMethod;
  }

  public void setPerformanceEvaluationMethod(PerformanceEvaluationMethod performanceEvaluationMethod) {
    this.performanceEvaluationMethod = performanceEvaluationMethod;
  }

  public Map<String, String> getPerformance() {
    return performance;
  }

  public void setPerformance(Map<String, String> performance) {
    this.performance = performance;
  }

  public Map<String, String> getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Map<String, String> configuration) {
    this.configuration = configuration;
  }

  @Override
  public boolean equals(Object o){
    if(!(o instanceof FunctionAutoTuneConfigBean)){
      return false;
    }
    FunctionAutoTuneConfigBean fac = (FunctionAutoTuneConfigBean) o;
    return Objects.equals(getFunctionId(), fac.getFunctionId()) && Objects.equals(getStartTime(), fac.getStartTime())
        && Objects.equals(getEndTime(), fac.getEndTime()) && Objects.equals(getAutotuneMethod(), fac.getAutotuneMethod())
        && Objects.equals(getConfiguration(), fac.getConfiguration()) && Objects.equals(getPerformance(), fac.getPerformance())
        && Objects.equals(getPerformanceEvaluationMethod(), fac.getPerformanceEvaluationMethod());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), functionId, startTime, endTime, autotuneMethod, performanceEvaluationMethod,
        configuration, performance);
  }

  @Override
  public String toString(){
    return MoreObjects.toStringHelper(this).add("functionId", functionId).add("configuration", configuration)
        .add("startTime", startTime).add("endTime", endTime).add("autotuneMethod", autotuneMethod)
        .add("performanceEvaluationMethod", performanceEvaluationMethod).add("performance", performance).toString();
  }
}
