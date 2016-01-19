package com.linkedin.thirdeye.client;

import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.SegmentDescriptor;
import com.linkedin.thirdeye.api.StarTreeConfig;

public interface ThirdEyeClient {

  Map<DimensionKey, MetricTimeSeries> execute(ThirdEyeRequest request) throws Exception;

  ThirdEyeRawResponse getRawResponse(ThirdEyeRequest request) throws Exception;

  StarTreeConfig getStarTreeConfig(String collection) throws Exception;

  List<String> getCollections() throws Exception;

  List<SegmentDescriptor> getSegmentDescriptors(String collection) throws Exception;

  /** Clear any cached values. */
  void clear() throws Exception;

  void close() throws Exception;

}
