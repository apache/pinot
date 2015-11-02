package com.linkedin.thirdeye.bootstrap.aggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MetricSums {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Map<String, Long> metricSum;

  public MetricSums() {
    metricSum = new HashMap<>();
  }

  public MetricSums(Map<String, Long> metricSum) {
    this.metricSum = metricSum;
  }

  public Map<String, Long> getMetricSum() {
    return metricSum;
  }
  public void setMetricSum(Map<String, Long> metricSum) {
    this.metricSum = metricSum;
  }
  public void addMetricSum(String metricName, long metricValue) {
    metricSum.put(metricName, metricValue);
  }

  public byte[] toBytes() throws IOException
  {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(this);
  }

  public static MetricSums fromBytes(byte[] bytes) throws IOException
  {
    return OBJECT_MAPPER.readValue(bytes, MetricSums.class);
  }


}
