package com.linkedin.thirdeye.anomalydetection.context;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import java.util.Collections;
import java.util.Map;


/**
 * An simple class for storing the detection result from anomaly functions. An instance of RawAnomalyResult is
 * supposed to be used to update or create an existing or new merged anomaly, respectively.
 */
public class RawAnomalyResult implements AnomalyResult {
  private long startTime;
  private long endTime;
  private DimensionMap dimensions = new DimensionMap();

  private double score; // confidence level
  private double weight; // severity
  private double avgCurrentVal;
  private double avgBaselineVal;
  private Map<String, String> properties = Collections.emptyMap();
  private AnomalyFeedbackDTO feedback = null;


  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @Override
  public long getEndTime() {
    return endTime;
  }

  @Override
  public void setDimensions(DimensionMap dimensionMap) {
    this.dimensions = Preconditions.checkNotNull(dimensionMap);
  }

  @Override
  public DimensionMap getDimensions() {
    return dimensions;
  }

  @Override
  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  @Override
  public double getScore() {
    return score;
  }

  @Override
  public void setScore(double score) {
    this.score = score;
  }

  @Override
  public double getWeight() {
    return weight;
  }

  @Override
  public void setWeight(double weight) {
    this.weight = weight;
  }

  @Override
  public double getAvgCurrentVal() {
    return avgCurrentVal;
  }

  @Override
  public void setAvgCurrentVal(double avgCurrentVal) {
    this.avgCurrentVal = avgCurrentVal;
  }

  @Override
  public double getAvgBaselineVal() {
    return avgBaselineVal;
  }

  @Override
  public void setAvgBaselineVal(double avgBaselineVal) {
    this.avgBaselineVal = avgBaselineVal;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    this.properties = Preconditions.checkNotNull(properties);
  }

  @Override
  public AnomalyFeedback getFeedback() {
    return feedback;
  }

  @Override
  public void setFeedback(AnomalyFeedback feedback) {
    if (feedback == null) {
      this.feedback = null;
    } else if (feedback instanceof AnomalyFeedbackDTO) {
      this.feedback = (AnomalyFeedbackDTO) feedback;
    } else {
      this.feedback = new AnomalyFeedbackDTO(feedback);
    }
  }

}
