package com.linkedin.thirdeye.anomalydetection.model.detection;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.List;
import java.util.Properties;

public interface DetectionModel {
  /**
   * Initializes this model with the given properties.
   * @param properties the given properties.
   */
  void init(Properties properties);

  /**
   * Returns the properties of this model.
   */
  Properties getProperties();

  /**
   * Detects anomalies on the observed (current) time series and returns a list of raw anomalies.
   *
   * @param metricName the name of the metric on which this detection model should detect anomalies
   * @param anomalyDetectionContext the context that contains the observed time series and
   *                                prediction model, which could provide an expected time series or
   *                                additional parameters (e.g., sigma) for anomaly detection.
   * @return list of raw anomalies.
   */
  List<RawAnomalyResultDTO> detect(String metricName, AnomalyDetectionContext anomalyDetectionContext);
}
