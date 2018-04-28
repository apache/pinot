package com.linkedin.thirdeye.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;


/**
 * Selector for anomalies based on (optionally) detector id, start time, end time, and
 * dimension filters.
 */
public class AnomalySlice {
  final long configId;
  final long start;
  final long end;
  final Multimap<String, String> filters;

  public AnomalySlice() {
    this.configId = -1;
    this.start = -1;
    this.end = -1;
    this.filters = ArrayListMultimap.create();
  }

  public AnomalySlice(long configId, long start, long end, Multimap<String, String> filters) {
    this.configId = configId;
    this.start = start;
    this.end = end;
    this.filters = filters;
  }

  public long getConfigId() {
    return configId;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public Multimap<String, String> getFilters() {
    return filters;
  }

  public AnomalySlice withConfigId(long configId) {
    return new AnomalySlice(configId, this.start, this.end, this.filters);
  }

  public AnomalySlice withStart(long start) {
    return new AnomalySlice(this.configId, start, this.end, this.filters);
  }

  public AnomalySlice withEnd(long end) {
    return new AnomalySlice(this.configId, this.start, end, this.filters);
  }

  public AnomalySlice withFilters(Multimap<String, String> filters) {
    return new AnomalySlice(this.configId, this.start, this.end, filters);
  }

  public boolean match(MergedAnomalyResultDTO anomaly) {
    if (this.start >= 0 && anomaly.getEndTime() < this.start)
      return false;
    if (this.end >= 0 && anomaly.getStartTime() > this.end)
      return false;
    if (this.configId >= 0 && (anomaly.getDetectionConfigId() == null || anomaly.getDetectionConfigId() != this.configId))
      return false;

    for (String dimName : this.filters.keySet()) {
      if (anomaly.getDimensions().containsKey(dimName)) {
        String dimValue = anomaly.getDimensions().get(dimName);
        if (!this.filters.get(dimName).contains(dimValue))
          return false;
      }
    }

    return true;
  }
}
