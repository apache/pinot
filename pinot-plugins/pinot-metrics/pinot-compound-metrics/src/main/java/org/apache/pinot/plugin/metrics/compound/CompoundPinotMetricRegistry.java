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
package org.apache.pinot.plugin.metrics.compound;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.pinot.spi.metrics.PinotCounter;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotHistogram;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetric;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotMetricsRegistryListener;
import org.apache.pinot.spi.metrics.PinotTimer;


public class CompoundPinotMetricRegistry implements PinotMetricsRegistry {
  private final List<PinotMetricsRegistry> _registries;
  private final ConcurrentHashMap<PinotMetricName, PinotMetric> _allMetrics;

  public CompoundPinotMetricRegistry(List<PinotMetricsRegistry> registries) {
    _registries = registries;
    _allMetrics = new ConcurrentHashMap<>();
  }

  @Override
  public void removeMetric(PinotMetricName name) {
    CompoundPinotMetricName castedName = (CompoundPinotMetricName) name;
    _allMetrics.remove(name);
    for (int i = 0; i < _registries.size(); i++) {
      _registries.get(i).removeMetric(castedName.getMetricName().get(i));
    }
  }

  private <T> List<T> map(PinotMetricName name, BiFunction<PinotMetricsRegistry, PinotMetricName, T> mapFun) {
    CompoundPinotMetricName castedName = (CompoundPinotMetricName) name;
    ArrayList<T> result = new ArrayList<>(_registries.size());
    for (int i = 0; i < _registries.size(); i++) {
      PinotMetricName metricName = castedName.getMetricName().get(i);
      T mappedElement = mapFun.apply(_registries.get(i), metricName);
      result.add(mappedElement);
    }
    return result;
  }

  @Override
  public <T> PinotGauge<T> newGauge(PinotMetricName name, PinotGauge<T> gauge) {
    if (gauge == null) {
      return (PinotGauge<T>) _allMetrics.computeIfAbsent(name,
          key -> new CompoundPinotGauge<>(map(key, (reg, subName) -> reg.newGauge(subName, null))));
    } else {
      CompoundPinotGauge<T> compoundGauge =
          (CompoundPinotGauge<T>) PinotMetricUtils.makePinotGauge(avoid -> gauge.value());

      Function<PinotMetricName, CompoundPinotGauge<?>> creator = key -> {
        CompoundPinotMetricName compoundName = (CompoundPinotMetricName) key;
        ArrayList<PinotGauge<T>> gauges = new ArrayList<>(_registries.size());
        for (int i = 0; i < _registries.size(); i++) {
          PinotGauge<?> subGauge = compoundGauge.getMeter(i);
          PinotMetricName subName = compoundName.getMetricName().get(i);
          PinotGauge<T> created = (PinotGauge<T>) _registries.get(i).newGauge(subName, subGauge);
          gauges.add(created);
        }
        return new CompoundPinotGauge<>(gauges);
      };

      return (PinotGauge<T>) _allMetrics.computeIfAbsent(name, creator);
    }
  }

  @Override
  public PinotMeter newMeter(PinotMetricName name, String eventType, TimeUnit unit) {
    return (PinotMeter) _allMetrics.computeIfAbsent(name,
        key -> new CompoundPinotMeter(map(key, (reg, subName) -> reg.newMeter(subName, eventType, unit))));
  }

  @Override
  public PinotCounter newCounter(PinotMetricName name) {
    return (PinotCounter) _allMetrics.computeIfAbsent(name,
        key -> new CompoundPinotCounter(map(key, PinotMetricsRegistry::newCounter)));
  }

  @Override
  public PinotTimer newTimer(PinotMetricName name, TimeUnit durationUnit, TimeUnit rateUnit) {
    return (PinotTimer) _allMetrics.computeIfAbsent(name,
        key -> new CompoundPinotTimer(map(key, (reg, subName) -> reg.newTimer(subName, durationUnit, rateUnit))));
  }

  @Override
  public PinotHistogram newHistogram(PinotMetricName name, boolean biased) {
    return (PinotHistogram) _allMetrics.computeIfAbsent(name,
        key -> new CompoundPinotHistogram(map(key, (reg, subName) -> reg.newHistogram(subName, biased))));
  }

  @Override
  public Map<PinotMetricName, PinotMetric> allMetrics() {
    return _allMetrics;
  }

  @Override
  public void addListener(PinotMetricsRegistryListener listener) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Object getMetricsRegistry() {
    return this;
  }

  @Override
  public void shutdown() {
    for (PinotMetricsRegistry registry : _registries) {
      registry.shutdown();
    }
  }

  List<PinotMetricsRegistry> getRegistries() {
    return _registries;
  }
}
