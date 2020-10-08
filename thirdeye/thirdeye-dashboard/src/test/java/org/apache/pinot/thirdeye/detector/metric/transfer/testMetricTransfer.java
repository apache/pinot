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

package org.apache.pinot.thirdeye.detector.metric.transfer;

import org.apache.pinot.thirdeye.common.metric.MetricSchema;
import org.apache.pinot.thirdeye.common.metric.MetricTimeSeries;
import org.apache.pinot.thirdeye.common.metric.MetricType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
    // the last three values are current values; the rest values are baseline values
    double [] m0 = {1.0, 1.0, 1.0, 1.0, 1.0, 1.0};
    for (long i=0l; i<=5l; i++) {
      metrics.set(i, mName, 1.0);
    }

    // create a list of mock scaling factors
    ScalingFactor sf0 = new ScalingFactor(2l, 6l, 0.8);
    List<ScalingFactor> sfList0 = new ArrayList<>();
    sfList0.add(sf0);

    Properties properties = new Properties();
    properties.put(MetricTransfer.SEASONAL_SIZE, "3");
    properties.put(MetricTransfer.SEASONAL_UNIT, TimeUnit.MILLISECONDS.toString());
    properties.put(MetricTransfer.BASELINE_SEASONAL_PERIOD, "2"); // mistakenly set 2 on purpose

    MetricTransfer.rescaleMetric(metrics, 3, sfList0, mName, properties);
    double [] m1_expected = {0.8, 0.8, Double.NaN, 1.0, 1.0, 1.0};
    double [] m_actual = new double[6];
    for (int i=0; i<=5; i++) {
      m_actual[i]= metrics.getOrDefault(i, mName, 0).doubleValue();
    }
    Assert.assertEquals(m_actual, m1_expected);

    //should not affect
    sfList0.remove(0);
    ScalingFactor sf1 = new ScalingFactor(12l, 14l, 0.8);
    sfList0.add(sf1);
    MetricTransfer.rescaleMetric(metrics, 3, sfList0, mName, properties);
    for (int i=0; i<=5; i++) {
      m_actual[i]= metrics.getOrDefault(i, mName, 0).doubleValue();
    }
    Assert.assertEquals(m_actual, m1_expected);

  }

}
