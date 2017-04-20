package com.linkedin.thirdeye.anomalydetection.datafilter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionMap;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AverageThresholdDataFilterTest {
  @Test
  public void testCreate() {
    Map<String, String> dataFilter = new HashMap<>();
    dataFilter.put(DataFilterFactory.FILTER_TYPE_KEY, "aVerAge_THrEShOLd");
    dataFilter.put(AverageThresholdDataFilter.METRIC_NAME_KEY, "metricName");
    dataFilter.put(AverageThresholdDataFilter.THRESHOLD_KEY, "1000");
    dataFilter.put(AverageThresholdDataFilter.MIN_LIVE_ZONE_KEY, "100");

    NavigableMap<DimensionMap, Double> overrideThreshold = new TreeMap<>();

    DimensionMap dimensionMap = new DimensionMap();
    dimensionMap.put("K1", "V1");
    overrideThreshold.put(dimensionMap, 350d);
    DimensionMap dimensionMap2 = new DimensionMap();
    dimensionMap2.put("K1", "V2");
    overrideThreshold.put(dimensionMap2, 350d);

    try {
      ObjectMapper OBJECT_MAPPER = new ObjectMapper();
      String writeValueAsString = OBJECT_MAPPER.writeValueAsString(overrideThreshold);
      dataFilter.put(AverageThresholdDataFilter.OVERRIDE_THRESHOLD_KEY, writeValueAsString);

      AverageThresholdDataFilter averageThresholdDataFilter = new AverageThresholdDataFilter();
      averageThresholdDataFilter.setParameters(dataFilter);

      NavigableMap<DimensionMap, Double> overrideThresholdMap = averageThresholdDataFilter.getOverrideThreshold();
      Assert.assertEquals(overrideThresholdMap.get(dimensionMap), overrideThreshold.get(dimensionMap));
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

}
