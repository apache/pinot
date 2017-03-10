package com.linkedin.thirdeye.anomaly.detection.lib;

import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import java.util.Map;
import org.joda.time.DateTime;


public class FunctionAutotuneReturn {
  private Map<String, String> configuration;
  private long targetFunctionId;
  private DateTime startTime;
  private DateTime endTime;
  private AutotuneMethodType autotuneMethod;
  private PerformanceEvaluationMethod performanceEvaluationMethod;
  private double performance;

  public FunctionAutotuneReturn(long targetFunctionId, DateTime startTime, DateTime endTime,
      Map<String, String> configuration, AutotuneMethodType autotuneMethod,
      PerformanceEvaluationMethod performanceEvaluationMethod, double performance) {
    this.configuration = configuration;
    this.targetFunctionId = targetFunctionId;
    this.startTime = startTime;
    this.endTime = endTime;
    this.autotuneMethod = autotuneMethod;
    this.performanceEvaluationMethod = performanceEvaluationMethod;
    this.performance = performance;
  }

  public Map<String, String> getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Map<String, String> configuration) {
    this.configuration = configuration;
  }

  public long getTargetFunctionId() {
    return targetFunctionId;
  }

  public void setTargetFunctionId(long targetFunctionId) {
    this.targetFunctionId = targetFunctionId;
  }

  public DateTime getStartTime() {
    return startTime;
  }

  public void setStartTime(DateTime startTime) {
    this.startTime = startTime;
  }

  public DateTime getEndTime() {
    return endTime;
  }

  public void setEndTime(DateTime endTime) {
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
}
