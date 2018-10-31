package com.linkedin.thirdeye.anomalydetection.datafilter;

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataFilterFactoryTest {

  @Test
  public void testFromSpecNull() throws Exception {
    DataFilter dataFilter = DataFilterFactory.fromSpec(null);
    Assert.assertEquals(dataFilter.getClass(), DummyDataFilter.class);
  }

  @Test
  public void testDataFilterCreation() {
    Map<String, String> spec = new HashMap<>();
    spec.put(AverageThresholdDataFilter.METRIC_NAME_KEY, "metricName");
    spec.put(DataFilterFactory.FILTER_TYPE_KEY, "aVerAge_THrEShOLd");
    DataFilter dataFilter = DataFilterFactory.fromSpec(spec);
    Assert.assertEquals(dataFilter.getClass(), AverageThresholdDataFilter.class);
  }
}
