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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.metrics.PinotCounter;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotHistogram;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetric;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotMetricsRegistryListener;
import org.apache.pinot.spi.metrics.PinotTimer;


/**
 * In-memory, no-JMX {@link PinotMetricsRegistry} for unit-testing {@code AbstractMetrics}-derived classes without
 * requiring a real metrics plugin on the classpath.
 */
public class FakePinotMetricsRegistry implements PinotMetricsRegistry {
  private final Map<PinotMetricName, PinotMetric> _metrics = new ConcurrentHashMap<>();
  private final List<Listener> _listeners = new CopyOnWriteArrayList<>();

  @Override
  public <T> PinotGauge<T> newGauge(PinotMetricName name, PinotGauge<T> gauge) {
    @SuppressWarnings("unchecked")
    PinotGauge<T> existing = (PinotGauge<T>) _metrics.get(name);
    if (existing != null) {
      return existing;
    }
    _metrics.put(name, gauge);
    notifyAdded(name, gauge);
    return gauge;
  }

  @Override
  public PinotMeter newMeter(PinotMetricName name, String eventType, TimeUnit unit) {
    return (PinotMeter) _metrics.computeIfAbsent(name, n -> {
      FakePinotMeter meter = new FakePinotMeter(eventType, unit);
      notifyAdded(n, meter);
      return meter;
    });
  }

  @Override
  public PinotCounter newCounter(PinotMetricName name) {
    return (PinotCounter) _metrics.computeIfAbsent(name, n -> {
      FakePinotCounter counter = new FakePinotCounter();
      notifyAdded(n, counter);
      return counter;
    });
  }

  @Override
  public PinotTimer newTimer(PinotMetricName name, TimeUnit durationUnit, TimeUnit rateUnit) {
    return (PinotTimer) _metrics.computeIfAbsent(name, n -> {
      FakePinotTimer timer = new FakePinotTimer(durationUnit, rateUnit);
      notifyAdded(n, timer);
      return timer;
    });
  }

  @Override
  public PinotHistogram newHistogram(PinotMetricName name, boolean biased) {
    throw new UnsupportedOperationException("FakePinotMetricsRegistry does not implement histograms; "
        + "add a FakePinotHistogram if a test needs one");
  }

  @Override
  public void removeMetric(PinotMetricName name) {
    PinotMetric removed = _metrics.remove(name);
    if (removed != null) {
      notifyRemoved(name);
    }
  }

  @Override
  public Map<PinotMetricName, PinotMetric> allMetrics() {
    return Collections.unmodifiableMap(_metrics);
  }

  @Override
  public void addListener(PinotMetricsRegistryListener listener) {
    Object inner = listener.getMetricsRegistryListener();
    if (inner instanceof Listener) {
      Listener fakeListener = (Listener) inner;
      _listeners.add(fakeListener);
      // Replay existing metrics per the PinotMetricsRegistry contract.
      for (Map.Entry<PinotMetricName, PinotMetric> entry : _metrics.entrySet()) {
        fakeListener.onMetricAdded(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public Object getMetricsRegistry() {
    return this;
  }

  @Override
  public void shutdown() {
  }

  private void notifyAdded(PinotMetricName name, PinotMetric metric) {
    for (Listener listener : _listeners) {
      listener.onMetricAdded(name, metric);
    }
  }

  private void notifyRemoved(PinotMetricName name) {
    for (Listener listener : _listeners) {
      listener.onMetricRemoved(name);
    }
  }

  /**
   * Callback surface used by {@link PinotMetricsRegistryListener} subclasses that wrap a {@link Listener}.
   */
  public interface Listener {
    void onMetricAdded(PinotMetricName name, PinotMetric metric);
    void onMetricRemoved(PinotMetricName name);
  }
}
