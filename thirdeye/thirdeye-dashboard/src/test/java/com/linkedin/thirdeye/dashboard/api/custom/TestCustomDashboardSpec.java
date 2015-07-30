package com.linkedin.thirdeye.dashboard.api.custom;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableList;
import junit.framework.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.List;

public class TestCustomDashboardSpec {
  private ObjectMapper objectMapper;
  private List<String> metrics;

  @BeforeClass
  public void beforeClass() throws Exception {
    objectMapper = new ObjectMapper(new YAMLFactory());
    metrics = ImmutableList.of("A", "RATIO(B,A)", "RATIO(C,B)", "C");
  }

  @Test
  public void testParse_full() throws Exception {
    InputStream inputStream = ClassLoader.getSystemResourceAsStream("test-dashboard.yml");
    CustomDashboardSpec spec = objectMapper.readValue(inputStream, CustomDashboardSpec.class);
    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getComponents());
    Assert.assertEquals(spec.getComponents().size(), 2);
    Assert.assertEquals(spec.getCollection(), "c");

    // First (has no dimensions)
    CustomDashboardComponentSpec fst = spec.getComponents().get(0);
    Assert.assertEquals(fst.getMetrics(), metrics);
    Assert.assertNull(fst.getDimensions());

    // Second (has dimensions)
    CustomDashboardComponentSpec sec = spec.getComponents().get(1);
    Assert.assertEquals(sec.getMetrics(), metrics);
    Assert.assertNotNull(sec.getDimensions());
    Assert.assertEquals(sec.getDimensions().get("A").get(0), "A0");
    Assert.assertEquals(sec.getDimensions().get("C").get(0), "C1");
  }
}
