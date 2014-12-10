package com.linkedin.thirdeye.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ThirdEyeClient
{
  void connect() throws Exception;

  void disconnect() throws Exception;

  Set<String> getCollections();

  List<ThirdEyeAggregate> getAggregates(String collection) throws IOException;

  List<ThirdEyeAggregate> getAggregates(String collection,
                                        Map<String, String> dimensionValues) throws IOException;

  List<ThirdEyeAggregate> getAggregates(String collection,
                                        Map<String, String> dimensionValues,
                                        Set<Long> timeBuckets) throws IOException;

  List<ThirdEyeAggregate> getAggregates(String collection,
                                        Map<String, String> dimensionValues,
                                        Long start,
                                        Long end) throws IOException;

  List<ThirdEyeTimeSeries> getTimeSeries(String collection,
                                         String metricName,
                                         Long start,
                                         Long end,
                                         Map<String, String> dimensionValues) throws IOException;
}
