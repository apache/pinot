package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class AnomalyDetectionInputContext {
  Map<DimensionMap, MetricTimeSeries> dimensionMapMetricTimeSeriesMap = Collections.emptyMap();
  MetricTimeSeries globalMetric;
  ListMultimap<DimensionMap, RawAnomalyResultDTO> existingRawAnomalies = ArrayListMultimap.create();;
  ListMultimap<DimensionMap, MergedAnomalyResultDTO> knownMergedAnomalies = ArrayListMultimap.create();;
  List<ScalingFactor> scalingFactors = Collections.emptyList();

  public Map<DimensionMap, MetricTimeSeries> getDimensionMapMetricTimeSeriesMap() {
    return dimensionMapMetricTimeSeriesMap;
  }

  public void setDimensionMapMetricTimeSeriesMap(
      Map<DimensionMap, MetricTimeSeries> dimensionMapMetricTimeSeriesMap) {
    this.dimensionMapMetricTimeSeriesMap = dimensionMapMetricTimeSeriesMap;
  }

  public ListMultimap<DimensionMap, RawAnomalyResultDTO> getExistingRawAnomalies() {
    return existingRawAnomalies;
  }

  public void setExistingRawAnomalies(ListMultimap<DimensionMap, RawAnomalyResultDTO> existingRawAnomalies) {
    this.existingRawAnomalies = existingRawAnomalies;
  }

  public ListMultimap<DimensionMap, MergedAnomalyResultDTO> getKnownMergedAnomalies() {
    return knownMergedAnomalies;
  }

  public void setKnownMergedAnomalies(ListMultimap<DimensionMap, MergedAnomalyResultDTO> knownMergedAnomalies) {
    this.knownMergedAnomalies = knownMergedAnomalies;
  }

  public List<ScalingFactor> getScalingFactors() {
    return scalingFactors;
  }

  public void setScalingFactors(List<ScalingFactor> scalingFactors) {
    this.scalingFactors = scalingFactors;
  }

  public MetricTimeSeries getGlobalMetric() {
    return globalMetric;
  }

  public void setGlobalMetric(MetricTimeSeries globalMetric) {
    this.globalMetric = globalMetric;
  }
}
