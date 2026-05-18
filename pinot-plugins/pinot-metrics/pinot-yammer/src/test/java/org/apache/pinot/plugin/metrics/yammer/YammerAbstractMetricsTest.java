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
package org.apache.pinot.plugin.metrics.yammer;

import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.common.metrics.AbstractMetricsTest;
import org.apache.pinot.common.metrics.MetricsInspector;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;


/**
 * Runs the shared {@link AbstractMetricsTest} against the real {@link YammerMetricsRegistry}, verifying that the
 * yammer plugin respects the generic {@code AbstractMetrics} contract.
 */
public class YammerAbstractMetricsTest extends AbstractMetricsTest {

  @Override
  protected String metricsFactoryClassName() {
    return YammerMetricsFactory.class.getName();
  }

  @Override
  protected PinotMetricsRegistry buildRegistry() {
    return new YammerMetricsRegistry();
  }

  @Override
  protected MetricsInspector createInspector(PinotMetricsRegistry registry) {
    return new YammerMetricsInspector(registry);
  }

  @Override
  protected long getGaugeValue(AbstractMetrics<?, ?, ?, ?> metrics, String metricName) {
    return YammerMetricValueUtils.getGaugeValue(metrics, metricName);
  }
}
