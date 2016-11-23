package com.linkedin.thirdeye.detector.metric.transfer;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.TimeRange;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;
import org.testng.Assert;


public class testMetricTransfer {

  @Test
  public void transfer(){

    // create a mock MetricTimeSeries
    List<String> names = new ArrayList<>(1);
    String mName = "metric0";
    names.add(0, mName);
    List<MetricType> types = Collections.nCopies(names.size(), MetricType.DOUBLE);
    MetricSchema metricSchema = new MetricSchema(names, types);
    MetricTimeSeries metrics = new MetricTimeSeries(metricSchema);
    double [] m0 = {1.0, 1.0, 1.0, 1.0, 1.0, 1.0};
    for (long i=0l; i<=5l; i++) {
      metrics.set(i, mName, 1.0);
    }

    // create a list of mock scaling factors
    ScalingFactor sf0 = new ScalingFactor(2l, 4l, 0.8);
    List<ScalingFactor> sfList0 = new ArrayList<>();
    sfList0.add(sf0);

    MetricTransfer.rescaleMetric(metrics, mName, sfList0);
    double [] m1_expected = {1.0, 1.0, 0.8, 0.8, 1.0, 1.0};
    double [] m_actual = new double[6];
    for (int i=0; i<=5; i++) {
      m_actual[i]= metrics.get(i, mName).doubleValue();
    }
    Assert.assertEquals(m_actual, m1_expected);

    // revert to the original cases
    ScalingFactor _sf0 = new ScalingFactor(2l, 4l, 1.25);
    // no points in time range and no change
    sfList0.remove(0);
    Assert.assertEquals(sfList0.size(), 0);
    sfList0.add(_sf0);
    MetricTransfer.rescaleMetric(metrics, mName, sfList0);
    for (int i=0; i<=5; i++) {
      m_actual[i]= metrics.get(i, mName).doubleValue();
    }
    Assert.assertEquals(m_actual, m0);

    //should not affect
    sfList0.remove(0);
    ScalingFactor sf1 = new ScalingFactor(12l, 14l, 0.8);
    sfList0.add(sf1);
    for (int i=0; i<=5; i++) {
      m_actual[i]= metrics.get(i, mName).doubleValue();
    }
    Assert.assertEquals(m_actual, m0);

  }

}
