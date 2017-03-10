package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.ArrayListMultimap;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

public class AnomalyDetectionOutputContext {
  ArrayListMultimap<DimensionMap, RawAnomalyResultDTO> rawAnomalies;
  ArrayListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalies;

  public ArrayListMultimap<DimensionMap, RawAnomalyResultDTO> getRawAnomalies() {
    return rawAnomalies;
  }

  public void setRawAnomalies(ArrayListMultimap<DimensionMap, RawAnomalyResultDTO> rawAnomalies) {
    this.rawAnomalies = rawAnomalies;
  }

  public ArrayListMultimap<DimensionMap, MergedAnomalyResultDTO> getMergedAnomalies() {
    return mergedAnomalies;
  }

  public void setMergedAnomalies(
      ArrayListMultimap<DimensionMap, MergedAnomalyResultDTO> mergedAnomalies) {
    this.mergedAnomalies = mergedAnomalies;
  }
}
