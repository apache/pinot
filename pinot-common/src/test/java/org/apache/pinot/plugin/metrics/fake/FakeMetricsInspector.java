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

import org.apache.pinot.common.metrics.MetricsInspector;
import org.apache.pinot.spi.metrics.PinotMetric;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotMetricsRegistryListener;


public class FakeMetricsInspector extends MetricsInspector {
  private final PinotMetricsRegistry _registry;
  private volatile PinotMetricName _lastMetric;

  public FakeMetricsInspector(PinotMetricsRegistry registry) {
    _registry = registry;
    final FakePinotMetricsRegistry.Listener inner = new FakePinotMetricsRegistry.Listener() {
      @Override
      public void onMetricAdded(PinotMetricName name, PinotMetric metric) {
        _lastMetric = name;
      }

      @Override
      public void onMetricRemoved(PinotMetricName name) {
      }
    };
    registry.addListener(new PinotMetricsRegistryListener() {
      @Override
      public Object getMetricsRegistryListener() {
        return inner;
      }
    });
  }

  @Override
  public PinotMetricName lastMetric() {
    return _lastMetric;
  }

  @Override
  public long getTimerSumMs(PinotMetricName name) {
    PinotMetric metric = _registry.allMetrics().get(name);
    if (!(metric instanceof FakePinotTimer)) {
      throw new IllegalArgumentException("Not a timer metric: " + name);
    }
    return ((FakePinotTimer) metric).sumMs();
  }

  @Override
  public long getMeteredCount(PinotMetricName name) {
    PinotMetric metric = _registry.allMetrics().get(name);
    if (metric instanceof FakePinotMeter) {
      return ((FakePinotMeter) metric).count();
    }
    if (metric instanceof FakePinotTimer) {
      return ((FakePinotTimer) metric).count();
    }
    throw new IllegalArgumentException("Not a metered metric: " + name);
  }
}
