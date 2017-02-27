package com.linkedin.thirdeye.datalayer.pojo;

import com.linkedin.thirdeye.api.DimensionMap;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang.ObjectUtils;


public class MergedAnomalyResultBean extends AbstractBean
    implements Comparable<MergedAnomalyResultBean> {
  private Long functionId;
  private Long anomalyFeedbackId;
  private List<Long> rawAnomalyIdList;
  private String collection;
  private String metric;
  private DimensionMap dimensions;
  private Long startTime;
  private Long endTime;
  // significance level
  private double score;
  // severity
  private double weight;
  private Long createdTime;
  private String message;
  private boolean notified;
  private double avgCurrentVal;
  private double avgBaselineVal;

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

  public List<Long> getRawAnomalyIdList() {
    return rawAnomalyIdList;
  }

  public void setRawAnomalyIdList(List<Long> rawAnomalyIdList) {
    this.rawAnomalyIdList = rawAnomalyIdList;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public void setEndTime(Long endTime) {
    this.endTime = endTime;
  }

  public DimensionMap getDimensions() {
    return dimensions;
  }

  public void setDimensions(DimensionMap dimensions) {
    this.dimensions = dimensions;
  }

  public double getScore() {
    return score;
  }

  public void setScore(double score) {
    this.score = score;
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

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  /**
   * Weight is change ratio. The absolute value of weight is severity.
   */
  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), startTime, endTime, collection, metric, dimensions, score, avgBaselineVal, avgCurrentVal);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MergedAnomalyResultBean)) {
      return false;
    }
    MergedAnomalyResultBean m = (MergedAnomalyResultBean) o;
    return Objects.equals(getId(), m.getId()) && Objects.equals(startTime, m.getStartTime())
        && Objects.equals(endTime, m.getEndTime()) && Objects.equals(collection, m.getCollection())
        && Objects.equals(metric, m.getMetric()) && Objects.equals(dimensions, m.getDimensions())
        && Objects.equals(avgBaselineVal, m.getAvgBaselineVal())
        && Objects.equals(avgCurrentVal, m.getAvgCurrentVal());
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

  @Override
  public String toString() {
    return "MergedAnomalyResultBean{" + "id=" + getId() + ", anomalyFeedbackId=" + anomalyFeedbackId + ", functionId="
        + functionId + ", rawAnomalyIdList=" + rawAnomalyIdList + ", collection='" + collection
        + '\'' + ", metric='" + metric + '\'' + ", dimensions='" + dimensions + '\''
        + ", startTime=" + startTime + ", endTime=" + endTime + ", score=" + score + ", weight="
        + weight + ", createdTime=" + createdTime + ", message='" + message + '\''
        + ", currentVal=" + avgCurrentVal + ", baseLineVal=" + avgBaselineVal
        + ", notified=" + notified + '}';
  }
}
