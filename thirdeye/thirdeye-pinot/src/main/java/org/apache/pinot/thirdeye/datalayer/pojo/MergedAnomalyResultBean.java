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
 */

package org.apache.pinot.thirdeye.datalayer.pojo;

import java.io.Serializable;
import org.apache.pinot.thirdeye.anomaly.AnomalySeverity;
import org.apache.pinot.thirdeye.anomaly.AnomalyType;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.ObjectUtils;


public class MergedAnomalyResultBean extends AbstractBean implements Comparable<MergedAnomalyResultBean>, Serializable {
  private Long functionId;
  private Long anomalyFeedbackId;
  private String collection;
  private String metric;
  private DimensionMap dimensions = new DimensionMap();
  private long startTime;
  private long endTime;

  private double avgCurrentVal; // actual value
  private double avgBaselineVal; // expected value
  private double score; // confidence level
  private double weight; // change percentage, whose absolute value is severity
  private double impactToGlobal; // the impact of this anomaly to the global metric
  private AnomalyResultSource anomalyResultSource = AnomalyResultSource.DEFAULT_ANOMALY_DETECTION;
  // Additional anomaly detection properties (e.g., patter=UP, etc.)
  // Being used as identifying if two merged anomalies have same anomaly detection properties, thus can be mergeable
  private Map<String, String> properties = new HashMap<>();

  private Long createdTime;
  private boolean notified;

  private String message;

  private String metricUrn;
  private Long detectionConfigId;
  private Set<Long> childIds; // ids of the anomalies this anomaly merged from
  private boolean isChild;
  private AnomalyType type;
  private AnomalySeverity severityLabel;


  public Set<Long> getChildIds() {
    return childIds;
  }

  public void setChildIds(Set<Long> childIds) {
    this.childIds = childIds;
  }

  public boolean isChild() {
    return isChild;
  }

  public void setChild(boolean child) {
    isChild = child;
  }

  // TODO: Remove raw anomaly id list after old merged anomalies are cleaned up
  @Deprecated
  private List<Long> rawAnomalyIdList;

  public Long getDetectionConfigId() {
    return detectionConfigId;
  }

  public void setDetectionConfigId(Long detectionConfigId) {
    this.detectionConfigId = detectionConfigId;
  }

  public Long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(Long functionId) {
    this.functionId = functionId;
  }

  public Long getAnomalyFeedbackId() {
    return anomalyFeedbackId;
  }

  public void setAnomalyFeedbackId(Long anomalyFeedbackId) {
    this.anomalyFeedbackId = anomalyFeedbackId;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
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

  /**
   * DimensionMap class is deprecated.
   *
   * Please use MetricEntity.fromURN(anomaly.getMetricUrn()).getFilters()
   */
  @Deprecated
  public DimensionMap getDimensions() {
    return dimensions;
  }

  @Deprecated
  public void setDimensions(DimensionMap dimensions) {
    this.dimensions = dimensions;
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

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public Long getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(Long createdTime) {
    this.createdTime = createdTime;
  }

  public boolean isNotified() {
    return notified;
  }

  public void setNotified(boolean notified) {
    this.notified = notified;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  // TODO: Remove this method after old merged anomalies are cleaned up
  @Deprecated
  public List<Long> getRawAnomalyIdList() {
    return rawAnomalyIdList;
  }

  // TODO: Remove this method after old merged anomalies are cleaned up
  @Deprecated
  public void setRawAnomalyIdList(List<Long> rawAnomalyIdList) {
    this.rawAnomalyIdList = rawAnomalyIdList;
  }

  public double getImpactToGlobal() {
    return impactToGlobal;
  }

  public void setImpactToGlobal(double impactToGlobal) {
    this.impactToGlobal = impactToGlobal;
  }

  public AnomalyResultSource getAnomalyResultSource() {
    return anomalyResultSource;
  }

  public void setAnomalyResultSource(AnomalyResultSource anomalyResultSource) {
    this.anomalyResultSource = anomalyResultSource;
  }

  public String getMetricUrn() {
    return metricUrn;
  }

  public void setMetricUrn(String metricUrn) {
    this.metricUrn = metricUrn;
  }

  public AnomalyType getType() {
    // default type is deviation
    if (type == null) {
      return AnomalyType.DEVIATION;
    }
    return type;
  }

  public void setType(AnomalyType type) {
    this.type = type;
  }

  public void setSeverityLabel(AnomalySeverity severityLabel) {
    this.severityLabel = severityLabel;
  }

  public AnomalySeverity getSeverityLabel() {
    // default severity level is debug
    if (severityLabel == null) {
      return AnomalySeverity.DEFAULT;
    }
    return severityLabel;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), startTime, endTime, collection, metric, dimensions, score, impactToGlobal, avgBaselineVal, avgCurrentVal, anomalyResultSource, metricUrn, detectionConfigId, childIds, isChild);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MergedAnomalyResultBean)) {
      return false;
    }
    MergedAnomalyResultBean m = (MergedAnomalyResultBean) o;
    return Objects.equals(getId(), m.getId()) && Objects.equals(startTime, m.getStartTime()) && Objects
        .equals(endTime, m.getEndTime()) && Objects.equals(collection, m.getCollection()) && Objects
        .equals(metric, m.getMetric()) && Objects.equals(dimensions, m.getDimensions()) && Objects
        .equals(score, m.getScore()) && Objects.equals(avgBaselineVal, m.getAvgBaselineVal()) && Objects
        .equals(avgCurrentVal, m.getAvgCurrentVal()) && Objects.equals(impactToGlobal, m.getImpactToGlobal()) &&
        Objects.equals(anomalyResultSource, m.getAnomalyResultSource()) && Objects.equals(metricUrn, m.getMetricUrn()) &&
        Objects.equals(detectionConfigId, m.getDetectionConfigId()) && Objects.equals(childIds, m.getChildIds()) &&
        Objects.equals(isChild, m.isChild());
  }

  @Override
  public int compareTo(MergedAnomalyResultBean o) {
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
