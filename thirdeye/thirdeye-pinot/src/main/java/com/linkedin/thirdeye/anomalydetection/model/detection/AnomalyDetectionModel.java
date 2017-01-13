package com.linkedin.thirdeye.anomalydetection.model.detection;

import com.linkedin.thirdeye.anomalydetection.data.AnomalyDetectionContext;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.List;

public interface AnomalyDetectionModel {
  /**
   * Detects anomalies on the observed (current) time series and returns a list of raw anomalies.
   *
   * @param adc the context that contains the observed time series and prediction model, which
   *            provides the expected time series and additional parameters (e.g., sigma) for
   *            anomaly detection.
   * @return list of raw anomalies.
   */
  List<RawAnomalyResultDTO> detect(AnomalyDetectionContext adc);
}
