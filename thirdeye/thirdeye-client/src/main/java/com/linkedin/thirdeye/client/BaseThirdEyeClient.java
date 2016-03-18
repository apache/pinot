package com.linkedin.thirdeye.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.client.util.ThirdEyeClientUtils;

public abstract class BaseThirdEyeClient implements ThirdEyeClient {

  @Override
  public Map<DimensionKey, MetricTimeSeries> execute(ThirdEyeRequest request) throws Exception {
    ThirdEyeRawResponse rawResponse = getRawResponse(request);

    // Figure out the metric types of the projection
    StarTreeConfig starTreeConfig = getStarTreeConfig(request.getCollection());
    Map<String, MetricType> metricTypes = new HashMap<>();
    for (MetricSpec metricSpec : starTreeConfig.getMetrics()) {
      String metricName = metricSpec.getName();
      MetricType metricType = metricSpec.getType();
      metricTypes.put(metricName, metricType);
    }
    List<MetricType> projectionTypes = new ArrayList<>();
    for (String metricName : rawResponse.getMetrics()) {
      MetricType metricType = metricTypes.get(metricName);
      projectionTypes.add(metricType);
    }
    return rawResponse.convert(projectionTypes);
  }

  @Override
  public long getExpectedTimeBuckets(ThirdEyeRequest request) throws Exception {
    StarTreeConfig config = getStarTreeConfig(request.getCollection());
    return ThirdEyeClientUtils.getTimeBucketCount(request, config.getTime().getBucket());
  }

  @Override
  public List<String> getExpectedTimestamps(ThirdEyeRequest request) throws Exception {
    StarTreeConfig config = getStarTreeConfig(request.getCollection());
    return ThirdEyeClientUtils.getTimestamps(request, config.getTime().getBucket());
  }
}
