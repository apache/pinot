/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.joda.time.DateTime;


@JsonIgnoreProperties(ignoreUnknown=true)
public class AutotuneConfigBean extends AbstractBean {
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
  private Map<String, Double> performance;
  // The goal of the autotune
  private double goal;
  // The tuned properties configuration
  private Map<String, String> configuration = new HashMap<>();
  // The average running time for each thread while doing autotune
  private long avgRunningTime;
  // The overall running time  while doing autotune
  private long overallRunningTime;
  // the message containing the intermediate results
  private String message;
  // the last update timestamp
  private long lastUpdateTimestamp;

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

  public Map<String, Double> getPerformance() {
    return performance;
  }

  public void setPerformance(Map<String, Double> performance) {
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

  public double getGoal() {
    return goal;
  }

  public void setGoal(double goal) {
    this.goal = goal;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public long getLastUpdateTimestamp() {
    return lastUpdateTimestamp;
  }

  public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
    this.lastUpdateTimestamp = lastUpdateTimestamp;
  }

  @Override
  public boolean equals(Object o){
    if(!(o instanceof AutotuneConfigBean)){
      return false;
    }
    AutotuneConfigBean fac = (AutotuneConfigBean) o;
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
}
