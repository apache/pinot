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

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Stoppable;
import com.yammer.metrics.core.Timer;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricsHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsHelper.class);

  private static Map<MetricsRegistry, Object> metricsRegistryMap = new WeakHashMap<MetricsRegistry, Object>();

  private static Map<MetricsRegistryRegistrationListener, Object> metricsRegistryRegistrationListenersMap =
      new WeakHashMap<MetricsRegistryRegistrationListener, Object>();

  /**
   * Initializes the metrics system by initializing the registry registration listeners present in the configuration.
   *
   * @param configuration The subset of the configuration containing the metrics-related keys
   */
  public static void initializeMetrics(PinotConfiguration configuration) {
    synchronized (MetricsHelper.class) {
      List<String> listenerClassNames = configuration.getProperty("metricsRegistryRegistrationListeners",
          Arrays.asList(JmxReporterMetricsRegistryRegistrationListener.class.getName()));

      // Build each listener using their default constructor and add them
      for (String listenerClassName : listenerClassNames) {
        try {
          Class<? extends MetricsRegistryRegistrationListener> clazz =
              (Class<? extends MetricsRegistryRegistrationListener>) Class.forName(listenerClassName);
          Constructor<? extends MetricsRegistryRegistrationListener> defaultConstructor =
              clazz.getDeclaredConstructor();
          MetricsRegistryRegistrationListener listener = defaultConstructor.newInstance();

          addMetricsRegistryRegistrationListener(listener);
        } catch (Exception e) {
          LOGGER
              .warn("Caught exception while initializing MetricsRegistryRegistrationListener " + listenerClassName, e);
        }
      }
    }
  }

  /**
   * Adds a metrics registry registration listener. When adding a metrics registry registration listener, events are
   * fired to add all previously registered metrics registries to the newly added metrics registry registration
   * listener.
   *
   * @param listener The listener to add
   */
  public static void addMetricsRegistryRegistrationListener(MetricsRegistryRegistrationListener listener) {
    synchronized (MetricsHelper.class) {
      metricsRegistryRegistrationListenersMap.put(listener, null);

      // Fire events to register all previously registered metrics registries
      Set<MetricsRegistry> metricsRegistries = metricsRegistryMap.keySet();
      for (MetricsRegistry metricsRegistry : metricsRegistries) {
        listener.onMetricsRegistryRegistered(metricsRegistry);
      }
    }
  }

  /**
   * Registers the metrics registry with the metrics helper.
   *
   * @param registry The registry to register
   */
  public static void registerMetricsRegistry(MetricsRegistry registry) {
    synchronized (MetricsHelper.class) {
      metricsRegistryMap.put(registry, null);

      // Fire event to all registered listeners
      Set<MetricsRegistryRegistrationListener> metricsRegistryRegistrationListeners =
          metricsRegistryRegistrationListenersMap.keySet();
      for (MetricsRegistryRegistrationListener metricsRegistryRegistrationListener : metricsRegistryRegistrationListeners) {
        metricsRegistryRegistrationListener.onMetricsRegistryRegistered(registry);
      }
    }
  }

  /**
   *
   * Return an existing meter if
   *  (a) A meter already exist with the same metric name.
   * Otherwise, creates a new meter and registers
   *
   * @param registry MetricsRegistry
   * @param name metric name
   * @param eventType Event Type
   * @param unit TimeUnit for rate determination
   * @return Meter
   */
  public static Meter newMeter(MetricsRegistry registry, MetricName name, String eventType, TimeUnit unit) {
    if (registry != null) {
      return registry.newMeter(name, eventType, unit);
    } else {
      return Metrics.newMeter(name, eventType, unit);
    }
  }

  /**
   *
   * Return an existing aggregated meter if registry is not null and a aggregated meter already exist
   * with the same metric name. Otherwise, creates a new aggregated meter and registers (if registry not null)
   *
   * @param registry MetricsRegistry
   * @param name metric name
   * @return AggregatedMeter
   */
  public static <T extends Metered & Stoppable> AggregatedMeter<T> newAggregatedMeter(
      AggregatedMetricsRegistry registry, MetricName name) {
    if (registry != null) {
      return registry.newAggregatedMeter(name);
    } else {
      return new AggregatedMeter<T>(); //not registered
    }
  }

  /**
   *
   * Return an existing counter if
   *  (a) A counter already exist with the same metric name.
   * Otherwise, creates a new meter and registers
   *
   * @param registry MetricsRegistry
   * @param name metric name
   * @return Counter
   */
  public static Counter newCounter(MetricsRegistry registry, MetricName name) {
    if (registry != null) {
      return registry.newCounter(name);
    } else {
      return Metrics.newCounter(name);
    }
  }

  /**
   *
   * Return an existing aggregated counter if registry is not null and a aggregated counter already exist
   * with the same metric name. Otherwise, creates a new aggregated counter and registers (if registry not null)
   *
   * @param registry MetricsRegistry
   * @param name metric name
   * @return AggregatedCounter
   */
  public static AggregatedCounter newAggregatedCounter(AggregatedMetricsRegistry registry, MetricName name) {
    if (registry != null) {
      return registry.newAggregatedCounter(name);
    } else {
      return new AggregatedCounter();
    }
  }

  /**
   *
   * Return an existing histogram if
   *  (a) A histogram already exist with the same metric name.
   * Otherwise, creates a new meter and registers
   *
   * @param registry MetricsRegistry
   * @param name metric name
   * @param biased (true if uniform distribution, otherwise exponential weighted)
   * @return histogram
   */
  public static Histogram newHistogram(MetricsRegistry registry, MetricName name, boolean biased) {
    if (registry != null) {
      return registry.newHistogram(name, biased);
    } else {
      return Metrics.newHistogram(name, biased);
    }
  }

  /**
   *
   * Return an existing aggregated histogram if registry is not null and a aggregated histogram already exist
   * with the same metric name. Otherwise, creates a new aggregated histogram and registers (if registry not null)
   *
   * @param registry MetricsRegistry
   * @param name metric name
   * @return AggregatedHistogram
   */
  public static <T extends Sampling> AggregatedHistogram<T> newAggregatedHistogram(AggregatedMetricsRegistry registry,
      MetricName name) {
    if (registry != null) {
      return registry.newAggregatedHistogram(name);
    } else {
      return new AggregatedHistogram<T>();
    }
  }

  /**
   *
   * Return an existing gauge if
   *  (a) A gauge already exist with the same metric name.
   * Otherwise, creates a new meter and registers
   *
   * @param registry MetricsRegistry
   * @param name metric name
   * @param gauge Underlying gauge to be tracked
   * @return gauge
   */
  public static <T> Gauge<T> newGauge(MetricsRegistry registry, MetricName name, Gauge<T> gauge) {
    if (registry != null) {
      return registry.newGauge(name, gauge);
    } else {
      return Metrics.newGauge(name, gauge);
    }
  }

  /**
   * Removes an existing metric
   */
  public static void removeMetric(MetricsRegistry registry, MetricName name) {
    if (registry != null) {
      registry.removeMetric(name);
    } else {
      Metrics.defaultRegistry().removeMetric(name);
    }
  }

  /**
   *
   * Return an existing aggregated long gauge if registry is not null and a aggregated long gauge already exist
   * with the same metric name. Otherwise, creates a new aggregated long gauge and registers (if registry not null)
   *
   * @param registry MetricsRegistry
   * @param name metric name
   * @return AggregatedLongGauge
   */
  public static <T extends Number, V extends Gauge<T>> AggregatedLongGauge<T, V> newAggregatedLongGauge(
      AggregatedMetricsRegistry registry, MetricName name) {
    if (registry != null) {
      return registry.newAggregatedLongGauge(name);
    } else {
      return new AggregatedLongGauge<T, V>();
    }
  }

  /**
   *
   * Return an existing timer if
   *  (a) A timer already exist with the same metric name.
   * Otherwise, creates a new timer and registers
   *
   * @param registry MetricsRegistry
   * @param name metric name
   * @param durationUnit TimeUnit for duration
   * @param rateUnit TimeUnit for rate determination
   * @return Timer
   */
  public static Timer newTimer(MetricsRegistry registry, MetricName name, TimeUnit durationUnit, TimeUnit rateUnit) {
    if (registry != null) {
      return registry.newTimer(name, durationUnit, rateUnit);
    } else {
      return Metrics.newTimer(name, durationUnit, rateUnit);
    }
  }

  /**
   * Useful for measuring elapsed times.
   *
   * Usage :
   * <pre>
   * {@code
   *   TimerContext tc = MtericsHelper.startTimer();
   *   ....
   *   Your code to be measured
   *   ....
   *   tc.stop();
   *   long elapsedTimeMs = tc.getLatencyMs();
   *
   * }
   * </pre>
   * @return
   */
  public static TimerContext startTimer() {
    return new TimerContext();
  }

  /**
   *
   * TimerContext to measure elapsed time
   *
   */
  public static class TimerContext {
    private final long _startTimeNanos;
    private long _stopTimeNanos;
    private boolean _isDone;

    public TimerContext() {
      _startTimeNanos = System.nanoTime();
      _isDone = false;
    }

    public void stop() {
      _isDone = true;
      _stopTimeNanos = System.nanoTime();
    }

    /**
     *
     * @return
     */
    public long getLatencyMs() {
      if (!_isDone) {
        stop();
      }
      return (_stopTimeNanos - _startTimeNanos) / 1000000L;
    }
  }
}
