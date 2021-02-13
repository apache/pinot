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

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.base.PinotCounter;
import org.apache.pinot.common.metrics.base.PinotGauge;
import org.apache.pinot.common.metrics.base.PinotHistogram;
import org.apache.pinot.common.metrics.base.PinotMeter;
import org.apache.pinot.common.metrics.base.PinotMetricName;
import org.apache.pinot.common.metrics.base.PinotMetricsRegistry;
import org.apache.pinot.common.metrics.base.PinotTimer;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricsHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsHelper.class);

  private static Map<PinotMetricsRegistry, Boolean> metricsRegistryMap = new ConcurrentHashMap<>();

  private static Map<MetricsRegistryRegistrationListener, Boolean> metricsRegistryRegistrationListenersMap =
      new ConcurrentHashMap<>();

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

          LOGGER.info("Registering metricsRegistry to listener {}", listenerClassName);
          addMetricsRegistryRegistrationListener(listener);
        } catch (Exception e) {
          LOGGER
              .warn("Caught exception while initializing MetricsRegistryRegistrationListener " + listenerClassName, e);
        }
      }
    }
    LOGGER.info("Number of listeners got registered: {}", metricsRegistryRegistrationListenersMap.size());
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
      metricsRegistryRegistrationListenersMap.put(listener, Boolean.TRUE);

      // Fire events to register all previously registered metrics registries
      Set<PinotMetricsRegistry> metricsRegistries = metricsRegistryMap.keySet();
      LOGGER.info("Number of metrics registry: {}", metricsRegistries.size());
      for (PinotMetricsRegistry metricsRegistry : metricsRegistries) {
        listener.onMetricsRegistryRegistered(metricsRegistry);
      }
    }
  }

  /**
   * Registers the metrics registry with the metrics helper.
   *
   * @param registry The registry to register
   */
  public static void registerMetricsRegistry(PinotMetricsRegistry registry) {
    synchronized (MetricsHelper.class) {
      metricsRegistryMap.put(registry, Boolean.TRUE);

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
  public static PinotMeter newMeter(PinotMetricsRegistry registry, PinotMetricName name, String eventType, TimeUnit unit) {
    return registry.newMeter(name, eventType, unit);
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
  public static PinotCounter newCounter(PinotMetricsRegistry registry, PinotMetricName name) {
    return registry.newCounter(name);
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
  public static PinotHistogram newHistogram(PinotMetricsRegistry registry, PinotMetricName name, boolean biased) {
    return registry.newHistogram(name, biased);
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
  public static <T> PinotGauge<T> newGauge(PinotMetricsRegistry registry, PinotMetricName name, PinotGauge<T> gauge) {
    return registry.newGauge(name, gauge);
  }

  /**
   * Removes an existing metric
   */
  public static void removeMetric(PinotMetricsRegistry registry, PinotMetricName name) {
    registry.removeMetric(name);
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
  public static PinotTimer newTimer(PinotMetricsRegistry registry, PinotMetricName name, TimeUnit durationUnit, TimeUnit rateUnit) {
    return registry.newTimer(name, durationUnit, rateUnit);
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
