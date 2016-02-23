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

  // TODO Refactor: the client only needs to provide dimensions, metrics, and time field.
  // It's overly complicated to require the other functions provided in the star tree config.
  StarTreeConfig getStarTreeConfig(String collection) throws Exception;

  List<String> getCollections() throws Exception;

  // TODO Refactor: the main purpose of this method is only for showing start + end times for data.
  List<SegmentDescriptor> getSegmentDescriptors(String collection) throws Exception;

  /** Clear any cached values. */
  void clear() throws Exception;

  void close() throws Exception;

}
