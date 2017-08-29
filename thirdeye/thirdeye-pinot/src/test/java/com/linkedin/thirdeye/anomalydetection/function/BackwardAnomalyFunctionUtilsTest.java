package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
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
