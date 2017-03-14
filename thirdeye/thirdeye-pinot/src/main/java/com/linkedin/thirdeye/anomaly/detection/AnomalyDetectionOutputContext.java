package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.ListMultimap;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

public class AnomalyDetectionOutputContext {
  ListMultimap<DimensionMap, RawAnomalyResultDTO> rawAnomalies;
  ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalies;

  public ListMultimap<DimensionMap, RawAnomalyResultDTO> getRawAnomalies() {
    return rawAnomalies;
  }

  public void setRawAnomalies(ListMultimap<DimensionMap, RawAnomalyResultDTO> rawAnomalies) {
    this.rawAnomalies = rawAnomalies;
  }

  public ListMultimap<DimensionMap, MergedAnomalyResultDTO> getMergedAnomalies() {
    return mergedAnomalies;
  }

  public void setMergedAnomalies(
      ListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalies) {
    this.mergedAnomalies = mergedAnomalies;
  }
}
