package com.linkedin.thirdeye.bootstrap.rollup;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.api.MetricType;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.linkedin.thirdeye.bootstrap.rollup.phase1.RollupPhaseOneConfig;

public class TestRollupPhaseOneConfig {
  @Test
  public void simple() throws Exception{
    List<String> dimensionNames= Lists.newArrayList("d1","d3","d3","d4","d5");
    List<String> metricNames = Lists.newArrayList("m1","m2","m3","m4","m5");
    List<MetricType> metricTypes = Lists.newArrayList(MetricType.INT,MetricType.INT,MetricType.INT,MetricType.INT,MetricType.INT);
    String thresholdFuncClassName =AverageBasedRollupFunction.class.getCanonicalName();
    Map<String, String> thresholdFuncParams = Maps.newHashMap();
    thresholdFuncParams.put("metricName", "m1");
    thresholdFuncParams.put("averageThreshold", "100");
    thresholdFuncParams.put("timeWindowSize", "100");
    RollupPhaseOneConfig config = new RollupPhaseOneConfig(dimensionNames,
        metricNames, metricTypes, thresholdFuncClassName, thresholdFuncParams);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    objectMapper.writeValue(out, config);

    System.out.println(new String(out.toByteArray()));
  }
}
