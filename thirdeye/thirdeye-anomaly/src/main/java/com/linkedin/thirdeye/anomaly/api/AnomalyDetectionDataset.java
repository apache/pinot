package com.linkedin.thirdeye.anomaly.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;

/**
 *
 */
public abstract class AnomalyDetectionDataset {

  /** List of dimension names */
  protected List<String> dimensions;

  /** List of metric names */
  protected List<String> metrics;

  protected Map<DimensionKey, MetricTimeSeries> data = new HashMap<>();

  /**
   * @return
   *  A list of dimension names
   */
  public List<String> getDimensions() {
    return dimensions;
  }

  /**
   * @return
   *  A list of metric names
   */
  public List<String> getMetrics() {
    return metrics;
  }

  /**
   * @return
   *  The set of dimension keys represented in the dataset
   */
  public Set<DimensionKey> getDimensionKeys() {
    return data.keySet();
  }

  /**
   * @param key
   * @return
   *  The metric time series associated with the dimension key
   */
  public MetricTimeSeries getMetricTimeSeries(DimensionKey key) {
    return data.get(key);
  }
}
