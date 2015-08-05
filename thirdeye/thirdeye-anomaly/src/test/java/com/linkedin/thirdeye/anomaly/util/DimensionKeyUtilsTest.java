package com.linkedin.thirdeye.anomaly.util;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;

/**
 *
 */
public class DimensionKeyUtilsTest {

  @Test
  public void dimensionKeyToJsonStringTest() throws JsonProcessingException {
    String[] values = new String[]{"a","b"};
    List<DimensionSpec> specs = new ArrayList<>();
    specs.add(new DimensionSpec("A"));
    specs.add(new DimensionSpec("B"));
    DimensionKey dk = new DimensionKey(values);

    String json = DimensionKeyUtils.toJsonString(specs, dk);

    Assert.assertEquals(json, "{\"A\":\"a\",\"B\":\"b\"}");
  }

}
