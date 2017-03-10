package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
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
  // The performance value
  private double performance;
  // The tuned properties configuration
  private Map<String, String> configuration;
  // The average running time for each thread while doing autotune
  private long avgRunningTime;
  // The overall running time  while doing autotune
  private long overallRunningTime;

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

  public double getPerformance() {
    return performance;
  }

  public void setPerformance(double performance) {
    this.performance = performance;
  }

  public Map<String, String> getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Map<String, String> configuration) {
    this.configuration = configuration;
  }

  public long getAvgRunningTime() {
    return avgRunningTime;
  }

  public void setAvgRunningTime(long avgRunningTime) {
    this.avgRunningTime = avgRunningTime;
  }

  public long getOverallRunningTime() {
    return overallRunningTime;
  }

  public void setOverallRunningTime(long overallRunningTime) {
    this.overallRunningTime = overallRunningTime;
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
        && Objects.equals(getPerformanceEvaluationMethod(), fac.getPerformanceEvaluationMethod())
        && Objects.equals(getAvgRunningTime(), fac.getAvgRunningTime())
        && Objects.equals(getOverallRunningTime(), fac.getOverallRunningTime());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), functionId, startTime, endTime, autotuneMethod, performanceEvaluationMethod,
        configuration, performance, avgRunningTime, overallRunningTime);
  }

  @Override
  public String toString(){
    return MoreObjects.toStringHelper(this).add("functionId", functionId).add("configuration", configuration)
        .add("startTime", startTime).add("endTime", endTime).add("autotuneMethod", autotuneMethod)
        .add("performanceEvaluationMethod", performanceEvaluationMethod).add("performance", performance)
        .add("avgRunningTime", avgRunningTime).add("overallRunningTime", overallRunningTime).toString();
  }
}
