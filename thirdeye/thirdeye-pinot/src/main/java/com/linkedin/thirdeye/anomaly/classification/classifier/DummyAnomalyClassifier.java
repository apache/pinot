package com.linkedin.thirdeye.anomaly.classification.classifier;

import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DummyAnomalyClassifier extends BaseAnomalyClassifier {
  @Override
  public void setParameters(Map<String, String> props) {
    // Does nothing
  }

  @Override
  public List<MergedAnomalyResultDTO> classify(Map<Long, List<MergedAnomalyResultDTO>> anomalies,
      ClassificationConfigDTO classificationConfig) {
    return Collections.emptyList();
  }
}
