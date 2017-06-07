package com.linkedin.thirdeye.anomaly.classification.classifier;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import java.util.Map;

public class DummyAnomalyClassifier extends BaseAnomalyClassifier {
  @Override
  public void setParameters(Map<String, String> props) {
    // Does nothing
  }

  @Override
  public String classify(MergedAnomalyResultDTO mainAnomaly, Map<String, List<MergedAnomalyResultDTO>> auxAnomalies) {
    return null;
  }
}
