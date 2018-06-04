package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import java.util.Map;


/**
 * Container object for user dashboard summarized anomaly view
 */
public class AnomalySummary {
  private long id;
  private long start;
  private long end;
  private Map<String, String> dimensions;
  private double severity;
  private double current;
  private double baseline;
  private AnomalyFeedbackType feedback;
  private String comment;
  private String metricName;
  private String metricUrn;
  private long metricId;
  private String functionName;
  private long functionId;
  private String dataset;

  public AnomalySummary() {
    // left blank
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public long getEnd() {
    return end;
  }

  public void setEnd(long end) {
    this.end = end;
  }

  public Map<String, String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(Map<String, String> dimensions) {
    this.dimensions = dimensions;
  }

  public double getSeverity() {
    return severity;
  }

  public void setSeverity(double severity) {
    this.severity = severity;
  }

  public double getCurrent() {
    return current;
  }

  public void setCurrent(double current) {
    this.current = current;
  }

  public double getBaseline() {
    return baseline;
  }

  public void setBaseline(double baseline) {
    this.baseline = baseline;
  }

  public AnomalyFeedbackType getFeedback() {
    return feedback;
  }

  public void setFeedback(AnomalyFeedbackType feedback) {
    this.feedback = feedback;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public String getFunctionName() {
    return functionName;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  public long getMetricId() {
    return metricId;
  }

  public void setMetricId(long metricId) {
    this.metricId = metricId;
  }

  public String getMetricUrn() {
    return metricUrn;
  }

  public void setMetricUrn(String metricUrn) {
    this.metricUrn = metricUrn;
  }

  public long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(long functionId) {
    this.functionId = functionId;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }
}
