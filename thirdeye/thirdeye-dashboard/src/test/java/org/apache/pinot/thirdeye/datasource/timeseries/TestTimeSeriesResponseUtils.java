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

package org.apache.pinot.thirdeye.datasource.timeseries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.pinot.thirdeye.common.dimension.DimensionKey;
import org.apache.pinot.thirdeye.common.metric.MetricSchema;
import org.apache.pinot.thirdeye.common.metric.MetricTimeSeries;
import org.apache.pinot.thirdeye.common.metric.MetricType;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.timeseries.TimeSeriesRow.Builder;
import org.apache.pinot.thirdeye.datasource.timeseries.TimeSeriesRow.TimeSeriesMetric;

public class TestTimeSeriesResponseUtils {

  @Test(dataProvider = "toMapProvider")
  public void toMap(String testName, TimeSeriesResponse response, List<String> schemaDimensions,
      Map<DimensionKey, MetricTimeSeries> expected) {
    Map<DimensionKey, MetricTimeSeries> actual = TimeSeriesResponseConverter.toMap(response, schemaDimensions);
    Assert.assertEquals(actual, expected);
  }

  @DataProvider(name = "toMapProvider")
  public Object[][] toMapProvider() {
    Object[] noGroupByArgsOneMetricPerRow =
        createMapProviderArgs("noGroupByArgsOneMetricPerRow", false, false); // no dimension
    // grouping, each
    // MetricFunction
    // appears on a
    // separate row
    // from others in
    // the same time +
    // dimension key
    Object[] noGroupByArgsAllMetricsInRow =
        createMapProviderArgs("noGroupByArgsAllMetricsInRow", false, true); // no dimension
    // grouping, metric
    // functions for the
    // same time +
    // dimension key
    // will appear in
    // the same row.
    Object[] dimensionGroupByArgsOneMetricPerRow =
        createMapProviderArgs("dimensionGroupByArgsOneMetricPerRow", true, false); // dimension
    // grouping,
    // each
    // metric function appears
    // on a separate row from
    // others in the same time +
    // dimension key.

    Object[] dimensionGroupByWithAllMetricsInRow =
        createMapProviderArgs("dimensionGroupByWithAllMetricsInRow", true, true); // dimension
    // grouping,
    // metric
    // functions
    // for the
    // same time +
    // dimension
    // key appear
    // in one row.
    return new Object[][] {
        noGroupByArgsOneMetricPerRow, noGroupByArgsAllMetricsInRow,
        dimensionGroupByArgsOneMetricPerRow, dimensionGroupByWithAllMetricsInRow
    };
  }

  /**
   * return type: String testName, TimeSeriesResponse, List<String> dimensions, Map<DimensionKey,
   * MetricTimeSeries>.
   */
  public Object[] createMapProviderArgs(String testName, boolean groupByDimension,
      boolean groupTimeSeriesMetricsIntoRow) {
    List<String> dimensions = Arrays.asList("dim1", "dim2", "dim3", "all");
    List<String> dimensionValueSuffixes = Arrays.asList("_a", "_b", "_c"); // appended to each
                                                                           // dimension
    List<MetricFunction> metricFunctions = createSumFunctions("m1", "m2", "m3");
    ConversionDataGenerator dataGenerator =
        new ConversionDataGenerator(dimensions, metricFunctions);
    for (long hoursSinceEpoch = 0; hoursSinceEpoch < 50; hoursSinceEpoch++) {
      DateTime start = new DateTime(hoursSinceEpoch);
      DateTime end = start.plusHours(1);
      for (String dimension : (groupByDimension ? dimensions
          : Collections.<String> singleton("all"))) {
        for (String dimensionValueSuffix : (groupByDimension ? dimensionValueSuffixes
            : Collections.<String> singleton("all"))) {
          String dimensionValue;
          if (groupByDimension) {
            dimensionValue = dimension + dimensionValueSuffix;
          } else {
            dimensionValue = "all";
          }
          List<TimeSeriesMetric> timeSeriesMetrics = new ArrayList<>();
          for (MetricFunction metricFunction : metricFunctions) {
            Double value = (double) (Objects.hash(start, end, dimension, dimensionValue,
                metricFunction.toString()) % 1000); // doesn't matter, the test is that values are
                                                    // consistent between data
                                                    // structures.
            TimeSeriesMetric timeSeriesMetric =
                new TimeSeriesMetric(metricFunction.getMetricName(), value);
            timeSeriesMetrics.add(timeSeriesMetric);
          }
          if (groupTimeSeriesMetricsIntoRow) {
            // add them all at once
            dataGenerator.addEntry(Arrays.asList(dimension), Arrays.asList(dimensionValue), start, end,
                timeSeriesMetrics.toArray(new TimeSeriesMetric[timeSeriesMetrics.size()]));
          } else {
            // add them individually (one metric per row)
            for (TimeSeriesMetric timeSeriesMetric : timeSeriesMetrics) {
              dataGenerator.addEntry(Arrays.asList(dimension), Arrays.asList(dimensionValue), start, end, timeSeriesMetric);
            }
          }
        }
      }
    }
    Object[] dimensionGroupByArgs = new Object[] {
        testName, dataGenerator.getResponse(), dimensions, dataGenerator.getMap()
    };
    return dimensionGroupByArgs;
  }

  private List<MetricFunction> createSumFunctions(String... metricNames) {
    List<MetricFunction> result = new ArrayList<>();
    for (String metricName : metricNames) {
      result.add(new MetricFunction(MetricAggFunction.SUM, metricName, 0L, "dataset", null, null));
    }
    return result;
  }

  /**
   * Helper class to test converting from TimeSeriesResponse to Map<DK,MTS>. This class will
   * simultaneously build corresponding TimeSeriesRow objects and populate a Map<DimensionKey,
   * MetricTimeSeries> with the provided addEntry method.
   */
  private class ConversionDataGenerator {
    private final List<TimeSeriesRow> timeSeriesRows = new ArrayList<>();
    private final Map<DimensionKey, MetricTimeSeries> map = new HashMap<>();
    private final List<String> dimensions;
    private final List<MetricFunction> metricFunctions;
    private final List<String> metricNames;
    private final MetricSchema metricSchema;

    ConversionDataGenerator(List<String> dimensions, List<MetricFunction> metricFunctions) {
      this.dimensions = dimensions;
      this.metricFunctions = metricFunctions;
      List<String> metricNames = new ArrayList<>();
      for (MetricFunction metricFunction : metricFunctions) {
        metricNames.add(metricFunction.getMetricName());
      }
      this.metricNames = metricNames;
      this.metricSchema =
          new MetricSchema(metricNames, Collections.nCopies(metricNames.size(), MetricType.DOUBLE));
    }

    void addEntry(List<String> dimensionNames, List<String> dimensionValues, DateTime start, DateTime end,
        TimeSeriesMetric... timeSeriesMetrics) {
      validateArgs(dimensionNames, dimensionValues, start, end, timeSeriesMetrics);
      timeSeriesRows.add(createRow(dimensionNames, dimensionValues, start, end, timeSeriesMetrics));
      String[] dimensionKeyArr = new String[dimensions.size()];
      Arrays.fill(dimensionKeyArr, "*");
      if (dimensionNames != null && dimensionNames.size() != 0) {
        for (int idx = 0; idx < dimensionNames.size(); ++idx) {
          dimensionKeyArr[dimensions.indexOf(dimensionNames.get(idx))] = dimensionValues.get(idx);
        }
      }
      DimensionKey dimensionKey = new DimensionKey(dimensionKeyArr);
      if (!map.containsKey(dimensionKey)) {
        map.put(dimensionKey, new MetricTimeSeries(metricSchema));
      }
      incrementMetricData(map.get(dimensionKey), start, end, timeSeriesMetrics);
    }

    private void validateArgs(List<String> dimensionNames, List<String> dimensionValue, DateTime start,
        DateTime end, TimeSeriesMetric[] timeSeriesMetrics) {
      if (dimensionNames != null && dimensionNames.size() != 0) {
        for (String dimensionName : dimensionNames) {
          Assert.assertTrue(dimensions.contains(dimensionName));
        }
      }
      for (TimeSeriesMetric metric : timeSeriesMetrics) {
        Assert.assertTrue(metricNames.contains(metric.getMetricName()));
      }
    }

    private TimeSeriesRow createRow(List<String> dimensionNames, List<String> dimensionValues, DateTime start,
        DateTime end, TimeSeriesMetric... timeSeriesMetrics) {
      Builder builder = new TimeSeriesRow.Builder();
      builder.setDimensionNames(dimensionNames);
      builder.setDimensionValues(dimensionValues);
      builder.setStart(start);
      builder.setEnd(end);
      builder.addMetrics(timeSeriesMetrics);
      return builder.build();
    }

    private void incrementMetricData(MetricTimeSeries metricTimeSeries, DateTime start,
        DateTime end, TimeSeriesMetric... timeSeriesMetrics) {
      long timeWindow = start.getMillis();
      for (TimeSeriesMetric metric : timeSeriesMetrics) {
        metricTimeSeries.increment(timeWindow, metric.getMetricName(), metric.getValue());
      }
    }

    private TimeSeriesResponse getResponse() {
      return new TimeSeriesResponse(timeSeriesRows);
    }

    private Map<DimensionKey, MetricTimeSeries> getMap() {
      return map;
    }

  }

}
