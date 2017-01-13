package com.linkedin.thirdeye.anomalydetection.model;

import com.linkedin.thirdeye.anomalydetection.model.detection.AnomalyDetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.transform.TransformationFunction;
import java.util.List;
import org.joda.time.Interval;

/**
 * The anomaly detection function defines the necessary element for detection anomalies on a single
 * observed time series, i.e., an anomaly function does not handle an anomaly that is defined upon
 * time series from various metric and dimensions.
 */
public interface AnomalyDetectionFunction {
  /**
   * Returns the intervals of time series that is used by this anomaly function. This method is
   * useful when additional time series are needed for predicting the expected time series.
   *
   * @param monitoringWindowStartTime inclusive
   * @param monitoringWindowEndTime exclusive
   *
   * @return intervals of time series that are used by this anomaly function
   */
  List<Interval> getTimeSeriesIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime);

  /**
   * Returns the chain of transformation functions for the current time series.
   * @return the chain of transformation functions for the current time series.
   */
  List<TransformationFunction> getCurrentTimeSeriesTransformationChain();

  /**
   * Returns the chain of transformation functions for baseline time series.
   * @return the chain of transformation functions for baseline time series.
   */
  List<TransformationFunction> getBaselineTimeSeriesTransformationChain();

  /**
   * Returns the prediction model for computing the expected time series using the provided history
   * time series.
   * @return the prediction model for computing the expected time series.
   */
  PredictionModel getPredictionModel();

  /**
   * Returns the anomaly detection model that defines the anomaly upon the current and baseline
   * time series.
   * @return the anomaly detection model that defines the anomaly upon the current and baseline
   * time series.
   */
  AnomalyDetectionModel getAnomalyDetectionModel();
}
