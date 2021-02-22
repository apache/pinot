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
package org.apache.pinot.common.metrics.yammer;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistryListener;
import org.apache.pinot.spi.metrics.PinotMetric;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistryListener;


public class YammerMetricsRegistryListener implements PinotMetricsRegistryListener {
  private final MetricsRegistryListener _metricsRegistryListener;

  public YammerMetricsRegistryListener(MetricsRegistryListener metricsRegistryListener) {
    _metricsRegistryListener = metricsRegistryListener;
  }

  @Override
  public void onMetricAdded(PinotMetricName name, PinotMetric metric) {
    _metricsRegistryListener.onMetricAdded((MetricName) name.getMetricName(), (Metric) metric.getMetric());
  }

  @Override
  public void onMetricRemoved(PinotMetricName name) {
    _metricsRegistryListener.onMetricRemoved((MetricName) name.getMetricName());
  }

  @Override
  public Object getMetricsRegistryListener() {
    return _metricsRegistryListener;
  }
}
