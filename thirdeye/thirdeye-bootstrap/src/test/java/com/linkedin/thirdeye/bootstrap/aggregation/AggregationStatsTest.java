package com.linkedin.thirdeye.bootstrap.aggregation;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;

public class AggregationStatsTest {
  @Test
  public void simpleTest() throws JsonGenerationException,
      JsonMappingException, IOException {
    List<String> metricNames = Lists.newArrayList("m1", "m2", "m3", "m4", "m5");
    List<MetricType> metricTypes = Lists.newArrayList(MetricType.INT,
        MetricType.INT, MetricType.INT, MetricType.INT, MetricType.INT);
    MetricSchema schema = new MetricSchema(metricNames, metricTypes);
    AggregationStats holder = new AggregationStats(schema);
    int count = 0;
    Random random = new Random();
    while (count < 10) {
      MetricTimeSeries series = new MetricTimeSeries(schema);
      for (int i = 0; i < 10; i++) {
        for (String name : metricNames) {
          series.set(32000 + random.nextInt(1000), name, random.nextInt(1000));
        }
      }
      holder.record(series);
      count = count + 1;
    }
    System.out.println(holder.toString());

  }
}
