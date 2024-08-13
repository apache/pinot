/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.metrics;

import java.util.stream.IntStream;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class AbstractMetricsTest {
  @Test
  public void testAddOrUpdateGauge() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);
    ControllerMetrics controllerMetrics = new ControllerMetrics(new YammerMetricsRegistry());
    String metricName = "test";
    // add gauge
    controllerMetrics.setOrUpdateGauge(metricName, () -> 1L);
    Assert.assertEquals(MetricValueUtils.getGaugeValue(controllerMetrics, metricName), 1);

    // update gauge
    controllerMetrics.setOrUpdateGauge(metricName, () -> 2L);
    Assert.assertEquals(MetricValueUtils.getGaugeValue(controllerMetrics, metricName), 2);

    // remove gauge
    controllerMetrics.removeGauge(metricName);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testMultipleUpdatesToGauge() throws InterruptedException {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);
    ControllerMetrics controllerMetrics = new ControllerMetrics(new YammerMetricsRegistry());
    String metricName = "testMultipleUpdates";

    // update and remove gauge simultaneously
    IntStream.range(0, 1000).forEach(i -> {
      controllerMetrics.setOrUpdateGauge(metricName, () -> (long) i);
    });

    // Verify final value
    Assert.assertEquals(MetricValueUtils.getGaugeValue(controllerMetrics, metricName), 999);
    // remove gauge
    controllerMetrics.removeGauge(metricName);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testRemoveNonExistentGauge() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);
    ControllerMetrics controllerMetrics = new ControllerMetrics(new YammerMetricsRegistry());
    String metricName = "testNonExistent";

    // Attempt to remove a nonexistent gauge
    controllerMetrics.removeGauge(metricName);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testMultipleGauges() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);
    ControllerMetrics controllerMetrics = new ControllerMetrics(new YammerMetricsRegistry());
    String metricName1 = "testMultiple1";
    String metricName2 = "testMultiple2";

    // Add multiple gauges
    controllerMetrics.setOrUpdateGauge(metricName1, () -> 1L);
    controllerMetrics.setOrUpdateGauge(metricName2, () -> 2L);

    // Verify values
    Assert.assertEquals(MetricValueUtils.getGaugeValue(controllerMetrics, metricName1), 1);
    Assert.assertEquals(MetricValueUtils.getGaugeValue(controllerMetrics, metricName2), 2);

    // Remove gauges
    controllerMetrics.removeGauge(metricName1);
    controllerMetrics.removeGauge(metricName2);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }
}
