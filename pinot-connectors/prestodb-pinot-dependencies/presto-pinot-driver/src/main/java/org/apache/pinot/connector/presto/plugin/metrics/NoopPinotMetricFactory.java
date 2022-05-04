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
package org.apache.pinot.connector.presto.plugin.metrics;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.pinot.spi.annotations.metrics.MetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotCounter;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotHistogram;
import org.apache.pinot.spi.metrics.PinotJmxReporter;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetric;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotMetricsRegistryListener;
import org.apache.pinot.spi.metrics.PinotTimer;


/**
 * This package name has to match the regex pattern: ".*\\.plugin\\.metrics\\..*"
 */
@MetricsFactory
public class NoopPinotMetricFactory implements PinotMetricsFactory {
  private PinotMetricsRegistry _pinotMetricsRegistry;

  @Override
  public void init(PinotConfiguration pinotConfiguration) {
  }

  @Override
  public PinotMetricsRegistry getPinotMetricsRegistry() {
    if (_pinotMetricsRegistry == null) {
      _pinotMetricsRegistry = new NoopPinotMetricsRegistry();
    }
    return _pinotMetricsRegistry;
  }

  @Override
  public PinotMetricName makePinotMetricName(Class<?> aClass, String s) {
    return new NoopPinotMetricName();
  }

  @Override
  public <T> PinotGauge<T> makePinotGauge(Function<Void, T> function) {
    return new NoopPinotGauge<T>();
  }

  @Override
  public PinotJmxReporter makePinotJmxReporter(PinotMetricsRegistry pinotMetricsRegistry) {
    return new NoopPinotJmxReporter();
  }

  @Override
  public String getMetricsFactoryName() {
    return "noop";
  }

  public static class NoopPinotMetricsRegistry implements PinotMetricsRegistry {
    @Override
    public void removeMetric(PinotMetricName pinotMetricName) {
    }

    @Override
    public <T> PinotGauge<T> newGauge(PinotMetricName pinotMetricName, PinotGauge<T> pinotGauge) {
      return new NoopPinotGauge<T>();
    }

    @Override
    public PinotMeter newMeter(PinotMetricName pinotMetricName, String s, TimeUnit timeUnit) {
      return new NoopPinotMeter();
    }

    @Override
    public PinotCounter newCounter(PinotMetricName pinotMetricName) {
      return new NoopPinotCounter();
    }

    @Override
    public PinotTimer newTimer(PinotMetricName pinotMetricName, TimeUnit timeUnit, TimeUnit timeUnit1) {
      return new NoopPinotTimer();
    }

    @Override
    public PinotHistogram newHistogram(PinotMetricName pinotMetricName, boolean b) {
      return new NoopPinotHistogram();
    }

    @Override
    public Map<PinotMetricName, PinotMetric> allMetrics() {
      return ImmutableMap.of();
    }

    @Override
    public void addListener(PinotMetricsRegistryListener pinotMetricsRegistryListener) {
    }

    @Override
    public Object getMetricsRegistry() {
      return this;
    }

    @Override
    public void shutdown() {
    }
  }

  private static class NoopPinotJmxReporter implements PinotJmxReporter {
    @Override
    public void start() {
    }
  }

  private static class NoopPinotMeter implements PinotMeter {
    @Override
    public void mark() {
    }

    @Override
    public void mark(long l) {
    }

    @Override
    public Object getMetered() {
      return null;
    }

    @Override
    public TimeUnit rateUnit() {
      return null;
    }

    @Override
    public String eventType() {
      return null;
    }

    @Override
    public long count() {
      return 0;
    }

    @Override
    public double fifteenMinuteRate() {
      return 0;
    }

    @Override
    public double fiveMinuteRate() {
      return 0;
    }

    @Override
    public double meanRate() {
      return 0;
    }

    @Override
    public double oneMinuteRate() {
      return 0;
    }

    @Override
    public Object getMetric() {
      return null;
    }
  }

  private static class NoopPinotMetricName implements PinotMetricName {
    @Override
    public Object getMetricName() {
      return null;
    }
  }

  private static class NoopPinotGauge<T> implements PinotGauge<T> {
    @Override
    public Object getGauge() {
      return null;
    }

    @Override
    public T value() {
      return null;
    }

    @Override
    public Object getMetric() {
      return null;
    }
  }

  private static class NoopPinotCounter implements PinotCounter {
    @Override
    public Object getCounter() {
      return null;
    }

    @Override
    public Object getMetric() {
      return null;
    }
  }

  private static class NoopPinotTimer implements PinotTimer {
    @Override
    public Object getMetered() {
      return null;
    }

    @Override
    public TimeUnit rateUnit() {
      return null;
    }

    @Override
    public String eventType() {
      return null;
    }

    @Override
    public long count() {
      return 0;
    }

    @Override
    public double fifteenMinuteRate() {
      return 0;
    }

    @Override
    public double fiveMinuteRate() {
      return 0;
    }

    @Override
    public double meanRate() {
      return 0;
    }

    @Override
    public double oneMinuteRate() {
      return 0;
    }

    @Override
    public Object getMetric() {
      return null;
    }

    @Override
    public void update(long duration, TimeUnit unit) {
    }

    @Override
    public Object getTimer() {
      return null;
    }
  }

  private static class NoopPinotHistogram implements PinotHistogram {
    @Override
    public Object getHistogram() {
      return null;
    }

    @Override
    public Object getMetric() {
      return null;
    }
  }
}
