package com.linkedin.thirdeye.anomaly.classification.classifier;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import java.util.Map;

public interface AnomalyClassifier {
  /**
   * Sets the properties of this classifier.
   *
   * @param props the properties for this classifier.
   */
  void setParameters(Map<String, String> props);

  /**
   * Returns a list of main anomalies that have issue type updated.
   *
   * @param classificationConfig the configuration of the current classification job that triggers this method.
   * @param anomalyFunctionSpecMap the anomaly function of the main and correlated anomalies.
   * @param anomalies the collection of main and correlated anomalies for this classification task.
   *
   * @return a list of main anomalies that have issue type updated.
   */
  List<MergedAnomalyResultDTO> classify(ClassificationConfigDTO classificationConfig,
      Map<Long, AnomalyFunctionDTO> anomalyFunctionSpecMap, Map<Long, List<MergedAnomalyResultDTO>> anomalies);
}
