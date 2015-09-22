package com.linkedin.thirdeye.client;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;

import java.util.Map;

public interface ThirdEyeClient {
  Map<DimensionKey, MetricTimeSeries> execute(ThirdEyeRequest request) throws Exception;

  ThirdEyeRawResponse getRawResponse(String sql) throws Exception;

  StarTreeConfig getStarTreeConfig(String collection) throws Exception;

  void close() throws Exception;
}
