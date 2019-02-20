/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.anomalydetection.datafilter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.common.metric.MetricSchema;
import org.apache.pinot.thirdeye.common.metric.MetricTimeSeries;
import org.apache.pinot.thirdeye.common.metric.MetricType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AverageThresholdDataFilterTest {
  private static final double NULL_DOUBLE = Double.NaN;

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

  @DataProvider
  public static Object[][] basicMetricTimeSeries() {
    List<String> metricNames = new ArrayList<String>() {{
      add("metricName");
    }};
    List<MetricType> types = new ArrayList<MetricType>() {{
      add(MetricType.DOUBLE);
    }};

    MetricSchema schema = new MetricSchema(metricNames, types);

    MetricTimeSeries metricTimeSeries = new MetricTimeSeries(schema);

    long[] timestamps = new long[] { 1, 2, 3, 4, 5 };
    double[] doubleValues = new double[] {1.0, 2.0, 3.0, NULL_DOUBLE, 5.0};
    for (int i = 0; i < timestamps.length; i++) {
      double doubleValue = doubleValues[i];
      if (Double.compare(doubleValue, NULL_DOUBLE) != 0) {
        metricTimeSeries.set(timestamps[i], metricNames.get(0), doubleValue);
      }
    }

    return new Object[][] {
        {metricNames, metricTimeSeries}
    };
  }

  @Test(dataProvider = "basicMetricTimeSeries")
  public void testBasicAveragePassThreshold(List<String> metricNames, MetricTimeSeries metricTimeSeries) {
    Map<String, String> dataFilter = new HashMap<>();
    dataFilter.put(DataFilterFactory.FILTER_TYPE_KEY, "aVerAge_THrEShOLd");
    dataFilter.put(AverageThresholdDataFilter.METRIC_NAME_KEY, metricNames.get(0));
    dataFilter.put(AverageThresholdDataFilter.THRESHOLD_KEY, "2.75");
    dataFilter.put(AverageThresholdDataFilter.MIN_LIVE_ZONE_KEY, "0");

    AverageThresholdDataFilter averageThresholdDataFilter = new AverageThresholdDataFilter();
    averageThresholdDataFilter.setParameters(dataFilter);

    Assert.assertTrue(averageThresholdDataFilter.isQualified(metricTimeSeries, new DimensionMap()));
  }

  @Test(dataProvider = "basicMetricTimeSeries")
  public void testBasicAverageFailedThreshold(List<String> metricNames, MetricTimeSeries metricTimeSeries) {
    Map<String, String> dataFilter = new HashMap<>();
    dataFilter.put(DataFilterFactory.FILTER_TYPE_KEY, "aVerAge_THrEShOLd");
    dataFilter.put(AverageThresholdDataFilter.METRIC_NAME_KEY, metricNames.get(0));
    dataFilter.put(AverageThresholdDataFilter.THRESHOLD_KEY, "2.76");
    dataFilter.put(AverageThresholdDataFilter.MIN_LIVE_ZONE_KEY, "0");

    AverageThresholdDataFilter averageThresholdDataFilter = new AverageThresholdDataFilter();
    averageThresholdDataFilter.setParameters(dataFilter);

    Assert.assertFalse(averageThresholdDataFilter.isQualified(metricTimeSeries, new DimensionMap()));
  }

  @Test(dataProvider = "basicMetricTimeSeries")
  public void testMinLiveBucketAveragePassThreshold(List<String> metricNames, MetricTimeSeries metricTimeSeries) {
    Map<String, String> dataFilter = new HashMap<>();
    dataFilter.put(DataFilterFactory.FILTER_TYPE_KEY, "aVerAge_THrEShOLd");
    dataFilter.put(AverageThresholdDataFilter.METRIC_NAME_KEY, metricNames.get(0));
    dataFilter.put(AverageThresholdDataFilter.THRESHOLD_KEY, "3.333");
    dataFilter.put(AverageThresholdDataFilter.MIN_LIVE_ZONE_KEY, "2.0");

    AverageThresholdDataFilter averageThresholdDataFilter = new AverageThresholdDataFilter();
    averageThresholdDataFilter.setParameters(dataFilter);

    Assert.assertTrue(averageThresholdDataFilter.isQualified(metricTimeSeries, new DimensionMap()));
  }

  @Test(dataProvider = "basicMetricTimeSeries")
  public void testLiveBucketAveragePassThreshold(List<String> metricNames, MetricTimeSeries metricTimeSeries) {
    Map<String, String> dataFilter = new HashMap<>();
    dataFilter.put(DataFilterFactory.FILTER_TYPE_KEY, "aVerAge_THrEShOLd");
    dataFilter.put(AverageThresholdDataFilter.METRIC_NAME_KEY, metricNames.get(0));
    dataFilter.put(AverageThresholdDataFilter.THRESHOLD_KEY, "3.33");
    dataFilter.put(AverageThresholdDataFilter.MIN_LIVE_ZONE_KEY, "2.0");
    dataFilter.put(AverageThresholdDataFilter.MAX_LIVE_ZONE_KEY, "5.0");

    AverageThresholdDataFilter averageThresholdDataFilter = new AverageThresholdDataFilter();
    averageThresholdDataFilter.setParameters(dataFilter);

    Assert.assertTrue(averageThresholdDataFilter.isQualified(metricTimeSeries, new DimensionMap()));
  }

  @Test(dataProvider = "basicMetricTimeSeries")
  public void testLiveBucketAverageFailThreshold(List<String> metricNames, MetricTimeSeries metricTimeSeries) {
    Map<String, String> dataFilter = new HashMap<>();
    dataFilter.put(DataFilterFactory.FILTER_TYPE_KEY, "aVerAge_THrEShOLd");
    dataFilter.put(AverageThresholdDataFilter.METRIC_NAME_KEY, metricNames.get(0));
    dataFilter.put(AverageThresholdDataFilter.THRESHOLD_KEY, "3.33");
    dataFilter.put(AverageThresholdDataFilter.MIN_LIVE_ZONE_KEY, "2.0");
    dataFilter.put(AverageThresholdDataFilter.MAX_LIVE_ZONE_KEY, "4.9");

    AverageThresholdDataFilter averageThresholdDataFilter = new AverageThresholdDataFilter();
    averageThresholdDataFilter.setParameters(dataFilter);

    Assert.assertFalse(averageThresholdDataFilter.isQualified(metricTimeSeries, new DimensionMap()));
  }

  @Test(dataProvider = "basicMetricTimeSeries")
  public void testLiveBucketsPctThresholdPassThreshold(List<String> metricNames, MetricTimeSeries metricTimeSeries) {
    Map<String, String> dataFilter = new HashMap<>();
    dataFilter.put(DataFilterFactory.FILTER_TYPE_KEY, "aVerAge_THrEShOLd");
    dataFilter.put(AverageThresholdDataFilter.METRIC_NAME_KEY, metricNames.get(0));
    dataFilter.put(AverageThresholdDataFilter.THRESHOLD_KEY, "1");
    dataFilter.put(AverageThresholdDataFilter.MIN_LIVE_ZONE_KEY, "3.0");
    dataFilter.put(AverageThresholdDataFilter.LIVE_BUCKETS_PERCENTAGE_KEY, "0.50");

    AverageThresholdDataFilter averageThresholdDataFilter = new AverageThresholdDataFilter();
    averageThresholdDataFilter.setParameters(dataFilter);

    Assert.assertTrue(averageThresholdDataFilter.isQualified(metricTimeSeries, new DimensionMap()));
  }

  @Test(dataProvider = "basicMetricTimeSeries")
  public void testLiveBucketsPctThresholdFailThreshold(List<String> metricNames, MetricTimeSeries metricTimeSeries) {
    Map<String, String> dataFilter = new HashMap<>();
    dataFilter.put(DataFilterFactory.FILTER_TYPE_KEY, "aVerAge_THrEShOLd");
    dataFilter.put(AverageThresholdDataFilter.METRIC_NAME_KEY, metricNames.get(0));
    dataFilter.put(AverageThresholdDataFilter.THRESHOLD_KEY, "1");
    dataFilter.put(AverageThresholdDataFilter.MIN_LIVE_ZONE_KEY, "3.0");
    dataFilter.put(AverageThresholdDataFilter.LIVE_BUCKETS_PERCENTAGE_KEY, "0.51");

    AverageThresholdDataFilter averageThresholdDataFilter = new AverageThresholdDataFilter();
    averageThresholdDataFilter.setParameters(dataFilter);

    Assert.assertFalse(averageThresholdDataFilter.isQualified(metricTimeSeries, new DimensionMap()));
  }

  @Test(dataProvider = "basicMetricTimeSeries")
  public void testOverrideThreshold(List<String> metricNames, MetricTimeSeries metricTimeSeries) {
    Map<String, String> dataFilter = new HashMap<>();
    dataFilter.put(DataFilterFactory.FILTER_TYPE_KEY, "aVerAge_THrEShOLd");
    dataFilter.put(AverageThresholdDataFilter.METRIC_NAME_KEY, metricNames.get(0));
    dataFilter.put(AverageThresholdDataFilter.THRESHOLD_KEY, "1000.0");

    NavigableMap<DimensionMap, Double> overrideThreshold = new TreeMap<>();

    DimensionMap dimensionMap = new DimensionMap();
    dimensionMap.put("K1", "V1");
    overrideThreshold.put(dimensionMap, 2.75);

    try {
      ObjectMapper OBJECT_MAPPER = new ObjectMapper();
      String writeValueAsString = OBJECT_MAPPER.writeValueAsString(overrideThreshold);
      dataFilter.put(AverageThresholdDataFilter.OVERRIDE_THRESHOLD_KEY, writeValueAsString);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      Assert.fail();
    }

    AverageThresholdDataFilter averageThresholdDataFilter = new AverageThresholdDataFilter();
    averageThresholdDataFilter.setParameters(dataFilter);

    Assert.assertTrue(averageThresholdDataFilter.isQualified(metricTimeSeries, dimensionMap));
    Assert.assertFalse(averageThresholdDataFilter.isQualified(metricTimeSeries, new DimensionMap()));
  }

}
