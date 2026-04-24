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
package org.apache.pinot.plugin.metrics.fake;

import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.common.metrics.AbstractMetricsTest;
import org.apache.pinot.common.metrics.MetricsInspector;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotMetric;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;


/**
 * Runs the shared {@link AbstractMetricsTest} against the in-memory {@link FakePinotMetricsRegistry}. This covers
 * {@code AbstractMetrics} logic in pinot-common without pulling in a real metrics plugin.
 */
public class FakeMetricsAbstractMetricsTest extends AbstractMetricsTest {

  @Override
  protected String metricsFactoryClassName() {
    return FakeMetricsFactory.class.getName();
  }

  @Override
  protected PinotMetricsRegistry buildRegistry() {
    return new FakePinotMetricsRegistry();
  }

  @Override
  protected MetricsInspector createInspector(PinotMetricsRegistry registry) {
    return new FakeMetricsInspector(registry);
  }

  @Override
  protected long getGaugeValue(AbstractMetrics<?, ?, ?, ?> metrics, String metricName) {
    PinotMetricName name =
        PinotMetricUtils.makePinotMetricName(metrics.getClass(), metrics.getMetricPrefix() + metricName);
    PinotMetric metric = metrics.getMetricsRegistry().allMetrics().get(name);
    if (!(metric instanceof PinotGauge)) {
      throw new IllegalArgumentException("Not a gauge metric: " + name);
    }
    Object value = ((PinotGauge<?>) metric).value();
    if (!(value instanceof Number)) {
      throw new IllegalStateException("Gauge did not produce a Number: " + name + " -> " + value);
    }
    return ((Number) value).longValue();
  }
}
