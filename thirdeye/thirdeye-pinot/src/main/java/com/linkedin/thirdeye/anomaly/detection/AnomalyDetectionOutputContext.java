package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.ListMultimap;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

public class AnomalyDetectionOutputContext {
  ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalies;

  public ListMultimap<DimensionMap, MergedAnomalyResultDTO> getMergedAnomalies() {
    return mergedAnomalies;
  }

  public void setMergedAnomalies(
      ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalies) {
    this.mergedAnomalies = mergedAnomalies;
  }
}
