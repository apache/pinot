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

package org.apache.pinot.thirdeye.anomalydetection.function;

import org.apache.pinot.thirdeye.anomalydetection.context.TimeSeries;
import org.apache.pinot.thirdeye.common.metric.MetricSchema;
import org.apache.pinot.thirdeye.common.metric.MetricTimeSeries;
import org.apache.pinot.thirdeye.common.metric.MetricType;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BackwardAnomalyFunctionUtilsTest {
  private static final double NULL_DOUBLE = Double.NaN;

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
  public void testSplitSetsOfTimeSeries(List<String> metricNames, MetricTimeSeries metricTimeSeries) throws Exception {
    List<Interval> intervals = new ArrayList<Interval>() {{
      add(new Interval(1L, 4L));
      add(new Interval(3L, 6L));
    }};

    List<TimeSeries> actualTimeSeriesList =
        BackwardAnomalyFunctionUtils.splitSetsOfTimeSeries(metricTimeSeries, metricNames.get(0), intervals);

    List<Long> timestamps1 = new ArrayList<Long>() {{
      add(3L); add(5L);
    }};
    List<Double> values1 = new ArrayList<Double>() {{
      add(3.0); add(5.0);
    }};
    final TimeSeries timeSeries1 = new TimeSeries(timestamps1, values1);
    timeSeries1.setTimeSeriesInterval(new Interval(3L, 6L));

    List<Long> timestamps2 = new ArrayList<Long>() {{
      add(1L); add(2L); add(3L);
    }};
    List<Double> values2 = new ArrayList<Double>() {{
      add(1.0); add(2.0); add(3.0);
    }};
    final TimeSeries timeSeries2 = new TimeSeries(timestamps2, values2);
    timeSeries2.setTimeSeriesInterval(new Interval(1L, 4L));

    List<TimeSeries> expectedTimeSeriesList = new ArrayList<TimeSeries>() {{
      add(timeSeries1);
      add(timeSeries2);
    }};

    Assert.assertEquals(actualTimeSeriesList, expectedTimeSeriesList);
  }

}
