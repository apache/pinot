package org.apache.pinot.thirdeye.detection.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.RelationalThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


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
