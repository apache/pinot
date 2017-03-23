package com.linkedin.thirdeye.anomalydetection.context;

import java.util.Map;

public interface AnomalyResult {
  /**
   * Set start time in millis, inclusive.
   * @param startTime start time in millis, inclusive.
   */
  void setStartTime(long startTime);

  /**
   * Get start time in millis, inclusive.
   * @return start time in millis, inclusive.
   */
  long getStartTime();

  /**
   * Set end time in millis, exclusive.
   * @param endTime end time in millis, exclusive.
   */
  void setEndTime(long endTime);

  /**
   * Get end time in millis, exclusive.
   * @return end time in millis, exclusive.
   */
  long getEndTime();

  /**
   * Set score (e.g., confidence level) of this anomaly.
   * @param score score of this anomaly.
   */
  void setScore(double score);

  /**
   * Get score (e.g., confidence level) of this anomaly.
   * @return score of this anomaly.
   */
  double getScore();

  /**
   * Set weight (e.g., change percentage) of this anomaly.
   * @param weight weight of this anomaly.
   */
  void setWeight(double weight);

  /**
   * Get weight (e.g., change percentage) of this anomaly.
   * @return weight of this anomaly.
   */
  double getWeight();

  /**
   * Set average current value of this anomaly.
   * @param avgCurrentVal average current value of this anomaly.
   */
  void setAvgCurrentVal(double avgCurrentVal);

  /**
   * Get average current value of this anomaly.
   * @return average current value of this anomaly.
   */
  double getAvgCurrentVal();

  /**
   * Set average baseline value of this anomaly.
   * @param avgBaselineVal average baseline value of this anomaly.
   */
  void setAvgBaselineVal(double avgBaselineVal);

  /**
   * Get average baseline value of this anomaly.
   * @return average baseline value of this anomaly.
   */
  double getAvgBaselineVal();

  /**
   * Set anomaly feedback (i.e., user label) of this anomaly.
   * @param anomalyFeedback anomaly feedback of this anomaly.
   */
  void setFeedback(AnomalyFeedback anomalyFeedback);

  /**
   * Return anomaly feedback (i.e., user label) of this anomaly.
   * @return anomaly feedback of this anomaly.
   */
  AnomalyFeedback getFeedback();

  /**
   * Set the properties (e.g., pattern=UP, baselineLift=1.7, etc.) of this anomaly.
   * @param properties the properties of this anomaly.
   */
  void setProperties(Map<String, String> properties);

  /**
   * Return the properties (e.g., pattern=UP, baselineLift=1.7, etc.) of this anomaly.
   * @return the properties of this anomaly.
   */
  Map<String, String> getProperties();
}
