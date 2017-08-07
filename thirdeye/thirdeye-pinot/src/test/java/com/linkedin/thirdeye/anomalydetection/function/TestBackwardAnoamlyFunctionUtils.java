package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.joda.time.Interval;
import org.junit.Test;


public class TestBackwardAnoamlyFunctionUtils {
  private static final String TEST_METRIC = "test";
  @Test
  public void testSplitSetsOfTimeSeries() {
    List<String> metricNames = new ArrayList<>();
    metricNames.add(TEST_METRIC);
    MetricSchema metricSchema =
        new MetricSchema(metricNames, Collections.nCopies(metricNames.size(), MetricType.DOUBLE));
    MetricTimeSeries metricTimeSeries = new MetricTimeSeries(metricSchema);
    metricTimeSeries.set(0, TEST_METRIC, 0);
    metricTimeSeries.set(1, TEST_METRIC, 1);
    metricTimeSeries.set(2, TEST_METRIC, 2);
    metricTimeSeries.set(3, TEST_METRIC, 3);
    metricTimeSeries.set(4, TEST_METRIC, 4);
    metricTimeSeries.set(5, TEST_METRIC, 5);

    List<Interval> intervalList = new ArrayList<>();
    intervalList.add(new Interval(5, 6));
    intervalList.add(new Interval(0, 5));
    List<TimeSeries> timeSeriesList =
        BackwardAnomalyFunctionUtils.splitSetsOfTimeSeries(metricTimeSeries, TEST_METRIC, intervalList);
    assert(timeSeriesList.size() == 2);
    assert(timeSeriesList.get(0).size() == 1);
    assert(timeSeriesList.get(1).size() == 5);
  }
}
