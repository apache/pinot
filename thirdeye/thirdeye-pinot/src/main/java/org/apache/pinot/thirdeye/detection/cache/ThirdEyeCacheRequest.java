package org.apache.pinot.thirdeye.detection.cache;

import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.CacheUtils;


public class ThirdEyeCacheRequest {
  ThirdEyeRequest request;
  private long metricId;
  private String metricUrn;
  private long startTimeInclusive;
  private long endTimeExclusive;

  public ThirdEyeCacheRequest(ThirdEyeRequest request, long metricId, String metricUrn, long startTimeInclusive, long endTimeExclusive) {
    this.request = request;
    this.metricId = metricId;
    this.metricUrn = metricUrn;
    this.startTimeInclusive = startTimeInclusive;
    this.endTimeExclusive = endTimeExclusive;
  }

  public static ThirdEyeCacheRequest from(ThirdEyeRequest request) {

    long metricId = request.getMetricFunctions().get(0).getMetricId();
    String metricUrn = MetricEntity.fromMetric(request.getFilterSet().asMap(), metricId).getUrn();
    long startTime = request.getStartTimeInclusive().getMillis();
    long endTime = request.getEndTimeExclusive().getMillis();

    return new ThirdEyeCacheRequest(request, metricId, metricUrn, startTime, endTime);
  }

  public ThirdEyeRequest getRequest() { return request; }

  public long getMetricId() { return metricId; }

  public String getMetricUrn() { return metricUrn; }

  public long getStartTimeInclusive() { return startTimeInclusive; }

  public long getEndTimeExclusive() { return endTimeExclusive; }

  public String getDimensionKey() {
    return CacheUtils.hashMetricUrn(metricUrn);
  }
}
