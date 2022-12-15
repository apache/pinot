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

import com.yammer.metrics.core.MetricName;
import org.apache.pinot.plugin.metrics.yammer.YammerMetric;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricName;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.plugin.metrics.yammer.YammerSettableGauge;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetric;
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
    controllerMetrics.addOrUpdateGauge(metricName, () -> 1L);
    checkGauge(controllerMetrics, metricName, 1);

    // update gauge
    controllerMetrics.addOrUpdateGauge(metricName, () -> 2L);
    checkGauge(controllerMetrics, metricName, 2);

    // remove gauge
    controllerMetrics.removeGauge(metricName);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  private void checkGauge(ControllerMetrics controllerMetrics, String metricName, long value) {
    Assert.assertEquals(controllerMetrics.getMetricsRegistry().allMetrics().size(), 1);
    PinotMetric pinotMetric = controllerMetrics.getMetricsRegistry().allMetrics()
        .get(new YammerMetricName(new MetricName(ControllerMetrics.class, "pinot.controller." + metricName)));
    Assert.assertTrue(pinotMetric instanceof YammerMetric);
    Assert.assertTrue(pinotMetric.getMetric() instanceof YammerSettableGauge);
    Assert.assertEquals(((YammerSettableGauge<Long>) pinotMetric.getMetric()).value(), Long.valueOf(value));
  }
}
