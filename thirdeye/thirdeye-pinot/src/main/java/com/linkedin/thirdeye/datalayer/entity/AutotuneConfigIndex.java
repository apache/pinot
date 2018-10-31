package com.linkedin.thirdeye.datalayer.entity;

public class AutotuneConfigIndex extends AbstractIndexEntity{
  String functionId;
  String autotuneMethod;
  String performanceEvaluationMethod;
  double goal;
  long startTime;
  long endTime;

  public String getFunctionId() {
    return functionId;
  }

  public void setFunctionId(String functionId) {
    this.functionId = functionId;
  }

  public String getAutotuneMethod() {
    return autotuneMethod;
  }

  public void setAutotuneMethod(String autotuneMethod) {
    this.autotuneMethod = autotuneMethod;
  }

  public String getPerformanceEvaluationMethod() {
    return performanceEvaluationMethod;
  }

  public void setPerformanceEvaluationMethod(String performanceEvaluationMethod) {
    this.performanceEvaluationMethod = performanceEvaluationMethod;
  }

  public double getGoal() {
    return goal;
  }

  public void setGoal(double goal) {
    this.goal = goal;
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
}
