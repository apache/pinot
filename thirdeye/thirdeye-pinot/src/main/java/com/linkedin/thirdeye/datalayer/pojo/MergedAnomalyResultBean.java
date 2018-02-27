package com.linkedin.thirdeye.datalayer.pojo;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.constant.AnomalyResultSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang.ObjectUtils;


public class MergedAnomalyResultBean extends AbstractBean implements Comparable<MergedAnomalyResultBean> {
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

  //TODO: deprecate raw anomaly list and message
  private String message;
  private List<Long> rawAnomalyIdList;


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

  public DimensionMap getDimensions() {
    return dimensions;
  }

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

  public List<Long> getRawAnomalyIdList() {
    return rawAnomalyIdList;
  }

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

  @Override
  public int hashCode() {
    return Objects.hash(getId(), startTime, endTime, collection, metric, dimensions, score, impactToGlobal, avgBaselineVal, avgCurrentVal, anomalyResultSource);
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
        Objects.equals(anomalyResultSource, m.getAnomalyResultSource());
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
