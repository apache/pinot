package org.apache.pinot.thirdeye.detection.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


public class ThirdEyeCacheResponse {
  private final ThirdEyeRequest request;
  private final long startInclusive;
  private final long endExclusive;
  private final List<TimeSeriesDataPoint> rows;

  public ThirdEyeCacheResponse(ThirdEyeRequest request, long startInclusive, long endExclusive, List<TimeSeriesDataPoint> rows) {
    this.request = request;
    this.startInclusive = startInclusive;
    this.endExclusive = endExclusive;
    this.rows = rows;
  }

  public ThirdEyeRequest getRequest() { return request; }

  public long getStartInclusive() { return startInclusive; }
  public long getEndExclusive() { return endExclusive; }
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
}
