package org.apache.pinot.thirdeye.detection.cache;

import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;


public interface TimeSeriesCache {
  public ThirdEyeResponse fetchTimeSeries(ThirdEyeCacheRequestContainer request) throws Exception;
  public void insertTimeSeriesIntoCache(String detectionId, ThirdEyeResponse response);
  public boolean detectionIdExistsInCache(long detectionId);
}
