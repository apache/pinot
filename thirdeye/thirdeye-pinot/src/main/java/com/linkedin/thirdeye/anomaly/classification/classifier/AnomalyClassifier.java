package com.linkedin.thirdeye.anomaly.classification.classifier;

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
   * @param anomalies the collection of main and correlated anomalies for this classification task.
   *
   * @return a list of main anomalies that have issue type updated.
   */
  List<MergedAnomalyResultDTO> classify(Map<Long, List<MergedAnomalyResultDTO>> anomalies, ClassificationConfigDTO classificationConfig);
}
