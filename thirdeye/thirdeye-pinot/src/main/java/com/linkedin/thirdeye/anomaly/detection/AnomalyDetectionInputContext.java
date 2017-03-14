package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.ArrayListMultimap;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import java.util.List;
import java.util.Map;

public class AnomalyDetectionInputContext {
  Map<DimensionMap, MetricTimeSeries> dimensionKeyMetricTimeSeriesMap;
  ArrayListMultimap<DimensionMap, RawAnomalyResultDTO> existingRawAnomalies;
  ArrayListMultimap<DimensionMap, MergedAnomalyResultDTO> knownMergedAnomalies;
  List<ScalingFactor> scalingFactors;

  public Map<DimensionMap, MetricTimeSeries> getDimensionKeyMetricTimeSeriesMap() {
    return dimensionKeyMetricTimeSeriesMap;
  }

  public void setDimensionKeyMetricTimeSeriesMap(
      Map<DimensionMap, MetricTimeSeries> dimensionKeyMetricTimeSeriesMap) {
    this.dimensionKeyMetricTimeSeriesMap = dimensionKeyMetricTimeSeriesMap;
  }

  public ArrayListMultimap<DimensionMap, RawAnomalyResultDTO> getExistingRawAnomalies() {
    return existingRawAnomalies;
  }

  public void setExistingRawAnomalies(ArrayListMultimap<DimensionMap, RawAnomalyResultDTO> existingRawAnomalies) {
    this.existingRawAnomalies = existingRawAnomalies;
  }

  public ArrayListMultimap<DimensionMap, MergedAnomalyResultDTO> getKnownMergedAnomalies() {
    return knownMergedAnomalies;
  }

  public void setKnownMergedAnomalies(ArrayListMultimap<DimensionMap, MergedAnomalyResultDTO> knownMergedAnomalies) {
    this.knownMergedAnomalies = knownMergedAnomalies;
  }

  public List<ScalingFactor> getScalingFactors() {
    return scalingFactors;
  }

  public void setScalingFactors(List<ScalingFactor> scalingFactors) {
    this.scalingFactors = scalingFactors;
  }
}
