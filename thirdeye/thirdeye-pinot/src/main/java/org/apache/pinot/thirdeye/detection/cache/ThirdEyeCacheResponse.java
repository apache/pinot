package org.apache.pinot.thirdeye.detection.cache;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;

public class ThirdEyeCacheResponse {
  private final ThirdEyeCacheRequest request;
  private List<TimeSeriesDataPoint> rows;

  public ThirdEyeCacheResponse(ThirdEyeCacheRequest request, List<TimeSeriesDataPoint> rows) {
    this.request = request;
    this.rows = rows;
  }

  public ThirdEyeCacheRequest getCacheRequest() { return request; }
  public List<TimeSeriesDataPoint> getRows() { return rows; }

  public int getNumRows() { return rows.size(); }
  public boolean hasNoRows() { return rows.isEmpty(); }
  public long getFirstTimestamp() { return rows.get(0).getTimestamp(); }
  public long getLastTimestamp() { return rows.get(rows.size() - 1).getTimestamp(); }

  public boolean isMissingSlice(long sliceStart, long sliceEnd) {
    return isMissingStartSlice(sliceStart) || isMissingEndSlice(sliceEnd);
  }

  public boolean isMissingStartAndEndSlice(long sliceStart, long sliceEnd) {
    return isMissingStartSlice(sliceStart) && isMissingEndSlice(sliceEnd);
  }

  public boolean isMissingStartSlice(long sliceStart) {
    return sliceStart < getFirstTimestamp();
  }

  public boolean isMissingEndSlice(long sliceEnd) {
    return sliceEnd > getLastTimestamp();
  }

  public void mergeSliceIntoRows(ThirdEyeResponse slice, MergeSliceType type) {

    List<TimeSeriesDataPoint> sliceRows = Arrays.asList(new TimeSeriesDataPoint[slice.getNumRows()]);

    for (MetricFunction metric : slice.getMetricFunctions()) {
      String metricUrn = MetricEntity.fromMetric(slice.getRequest().getFilterSet().asMap(), metric.getMetricId()).getUrn();
      for (int i = 0; i < slice.getNumRowsFor(metric); i++) {
        Map<String, String> row = slice.getRow(metric, i);
        int timeBucketId = Integer.parseInt(row.get("Date"));
        sliceRows.set(timeBucketId,
            new TimeSeriesDataPoint(metricUrn, Long.valueOf(row.get("timestamp")), metric.getMetricId(), row.get(metric.getMetricName())));
      }
    }

    if (type == MergeSliceType.PREPEND) {
      sliceRows.addAll(rows);
      this.rows = sliceRows;
    } else if (type == MergeSliceType.APPEND) {
      rows.addAll(sliceRows);
    }
  }
}
