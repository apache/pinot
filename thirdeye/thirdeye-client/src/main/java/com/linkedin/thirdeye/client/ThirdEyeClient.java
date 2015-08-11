package com.linkedin.thirdeye.client;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;

import java.util.Map;

public interface ThirdEyeClient {
  Map<DimensionKey, MetricTimeSeries> execute(ThirdEyeRequest request) throws Exception;
  void close() throws Exception;
}
