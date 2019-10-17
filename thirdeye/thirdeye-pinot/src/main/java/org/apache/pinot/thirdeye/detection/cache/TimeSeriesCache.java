package org.apache.pinot.thirdeye.detection.cache;

import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;

public interface TimeSeriesCache {
  public ThirdEyeResponse fetchTimeSeries(ThirdEyeRequest request) throws Exception;
  public void insertTimeSeriesIntoCache(ThirdEyeResponse response);
  public boolean detectionIdExistsInCache(String key);
}
