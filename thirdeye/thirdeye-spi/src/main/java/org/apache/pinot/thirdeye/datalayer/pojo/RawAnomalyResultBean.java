/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.pinot.thirdeye.datalayer.pojo;

import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import java.util.Objects;
import org.apache.commons.lang3.ObjectUtils;
import org.joda.time.DateTime;

@Deprecated
public class RawAnomalyResultBean extends AbstractBean implements Comparable<RawAnomalyResultBean> {

  private Long functionId;
  private Long AnomalyFeedbackId;
  private Long startTime;
  private Long endTime;
  private DimensionMap dimensions;
  private Long jobId;
  private AnomalyResultSource anomalyResultSource = AnomalyResultSource.DEFAULT_ANOMALY_DETECTION;

  // significance level
  private double score;

  // severity
  private double weight;
  private String properties;
  private String message;
  private Long creationTimeUtc;
  private boolean dataMissing;
  private boolean merged;
  private double avgCurrentVal;
  private double avgBaselineVal;

  public Long getJobId() {
    return jobId;
  }

  public void setJobId(Long jobId) {
    this.jobId = jobId;
  }

  public double getAvgCurrentVal(){
    return this.avgCurrentVal;
  }

  public double getAvgBaselineVal(){
    return this.avgBaselineVal;
  }

  public void setAvgCurrentVal(double val){
    this.avgCurrentVal = val;
  }

  public void setAvgBaselineVal(double val){
    this.avgBaselineVal = val;
  }

  public RawAnomalyResultBean() {
    creationTimeUtc = DateTime.now().getMillis();
  }

  public DimensionMap getDimensions() {
    return dimensions;
  }

  public void setDimensions(DimensionMap dimensions) {
    this.dimensions = dimensions;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTimeUtc) {
    this.startTime = startTimeUtc;
  }

  public Long getEndTime() {
    return endTime;
  }

  public void setEndTime(Long endTimeUtc) {
    this.endTime = endTimeUtc;
  }

  public double getScore() {
    return score;
  }

  public void setScore(double score) {
    this.score = score;
  }

  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  public String getProperties() {
    return properties;
  }

  public void setProperties(String properties) {
    this.properties = properties;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Long getCreationTimeUtc() {
    return creationTimeUtc;
  }

  public void setCreationTimeUtc(Long creationTimeUtc) {
    this.creationTimeUtc = creationTimeUtc;
  }


  public boolean isDataMissing() {
    return dataMissing;
  }

  public void setDataMissing(boolean dataMissing) {
    this.dataMissing = dataMissing;
  }

  public boolean isMerged() {
    return merged;
  }

  public void setMerged(boolean merged) {
    this.merged = merged;
  }

  public Long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(Long functionId) {
    this.functionId = functionId;
  }

  public Long getAnomalyFeedbackId() {
    return AnomalyFeedbackId;
  }

  public void setAnomalyFeedbackId(Long anomalyFeedbackId) {
    AnomalyFeedbackId = anomalyFeedbackId;
  }

  public AnomalyResultSource getAnomalyResultSource() {
    return anomalyResultSource;
  }

  public void setAnomalyResultSource(AnomalyResultSource anomalyResultSource) {
    this.anomalyResultSource = anomalyResultSource;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RawAnomalyResultBean)) {
      return false;
    }
    RawAnomalyResultBean r = (RawAnomalyResultBean) o;
    return Objects.equals(startTime, r.getStartTime())
        && Objects.equals(dimensions, r.getDimensions()) && Objects.equals(endTime, r.getEndTime())
        && Objects.equals(score, r.getScore()) && Objects.equals(weight, r.getWeight())
        && Objects.equals(properties, r.getProperties()) && Objects.equals(message, r.getMessage())
        && Objects.equals(avgBaselineVal, r.getAvgBaselineVal()) && Objects.equals(avgCurrentVal, r.getAvgCurrentVal())
        && Objects.equals(anomalyResultSource, r.anomalyResultSource);
    // Intentionally omit creationTimeUtc, since start/end are the truly significant dates for
    // anomalies
  }

  @Override
  public int hashCode() {
    return Objects.hash(dimensions, startTime, endTime, score, weight, properties,
        message, avgBaselineVal, avgCurrentVal, anomalyResultSource);
    // Intentionally omit creationTimeUtc, since start/end are the truly significant dates for
    // anomalies
  }

  @Override
  public int compareTo(RawAnomalyResultBean o) {
    // compare by dimension, -startTime, functionId, id
    int diff = ObjectUtils.compare(getDimensions(), o.getDimensions());
    if (diff != 0) {
      return diff;
    }
    diff = -ObjectUtils.compare(startTime, o.getStartTime()); // inverted to sort by
    // decreasing time
    if (diff != 0) {
      return diff;
    }
    return ObjectUtils.compare(getId(), o.getId());
  }
}
