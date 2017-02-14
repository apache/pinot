package com.linkedin.thirdeye.detector.functionex.impl;

import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExDataSource;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExResult;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MinMaxThresholdTest {
  static final Logger LOG = LoggerFactory.getLogger(MinMaxThresholdTest.class);

  static class MockDataSource implements AnomalyFunctionExDataSource<String, DataFrame> {
    @Override
    public DataFrame query(String query, AnomalyFunctionExContext context) {
      DataFrame df = new DataFrame(4);
      df.addSeries("good", 3.4, 3.1, 3.7, 3.9);
      df.addSeries("bad", 3.4, 4.2, 2.7, 3.9);
      return df;
    }
  }

  Map<String, String> config;
  MinMaxThreshold func;

  @BeforeMethod
  public void before() {
    config = new HashMap<>();
    config.put("datasource", "mock");
    config.put("query", "select * from my_table");
    config.put("min", "3");
    config.put("max", "4");

    AnomalyFunctionExContext context = new AnomalyFunctionExContext();
    context.setConfig(config);
    context.setDataSources(Collections.singletonMap("mock", new MockDataSource()));

    func = new MinMaxThreshold();
    func.setContext(context);
  }

  @Test
  public void testNoOutlierPass() throws Exception {
    config.put("column", "good");

    AnomalyFunctionExResult result = func.apply();

    LOG.info("all should pass. {}", result.getMessage());
    Assert.assertFalse(result.isAnomaly());
  }

  @Test
  public void testSingleOutlierFail() throws Exception {
    config.put("column", "bad");

    AnomalyFunctionExResult result = func.apply();

    LOG.info("none should pass. {}", result.getMessage());
    Assert.assertTrue(result.isAnomaly());
  }
}
