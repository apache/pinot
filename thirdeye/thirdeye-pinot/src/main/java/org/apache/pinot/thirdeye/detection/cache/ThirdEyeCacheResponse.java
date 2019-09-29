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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


public class ThirdEyeCacheResponse {
  @JsonProperty("start")
  private final String start;
  @JsonProperty("end")
  private final String end;
  @JsonProperty("metrics")
  private final List<String[]> metrics;
  @JsonProperty("timeSpec")
  private final Map<String, String> timeSpec;

  public ThirdEyeCacheResponse(String start, String end, List<String[]> metrics, Map<String, String> timeSpec) {
    this.start = start;
    this.end = end;
    this.metrics = metrics;
    this.timeSpec = timeSpec;
  }

  public String getStart() { return start; }
  public String getEnd() { return end; }
  public List<String[]> getMetrics() { return metrics; }
  public TimeSpec getTimeSpec() { return this.buildTimeSpec(); }

  public boolean isMissingSlice(DateTime sliceStart, DateTime sliceEnd) {
    return isMissingStartSlice(sliceStart) || isMissingEndSlice(sliceEnd);
  }

  public boolean isMissingStartAndEndSlice(DateTime sliceStart, DateTime sliceEnd) {
    return isMissingStartSlice(sliceStart) && isMissingEndSlice(sliceEnd);
  }

  public boolean isMissingStartSlice(DateTime sliceStart) {
    return sliceStart.isBefore(Long.valueOf(start));
  }

  public boolean isMissingEndSlice(DateTime sliceEnd) {
    return sliceEnd.isAfter(Long.valueOf(end));
  }

  public TimeSpec buildTimeSpec() { return new TimeSpec(getFormat(), getDataGranularity(), getColumnName()); }

  public String getFormat() { return timeSpec.get("format"); }

  public TimeGranularity getDataGranularity() {
    String[] granularity = timeSpec.get("dataGranularity").split("-");
    return new TimeGranularity(Integer.valueOf(granularity[0]), TimeUnit.valueOf(granularity[1]));
  }

  public String getColumnName() { return timeSpec.get("Date"); }
}
