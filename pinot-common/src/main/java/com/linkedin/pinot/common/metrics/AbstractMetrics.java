/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.metrics;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;


/**
 * Common code for metrics implementations.
 *
 */
public abstract class AbstractMetrics<QP extends AbstractMetrics.QueryPhase, M extends AbstractMetrics.Meter> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMetrics.class);

  protected final String _metricPrefix;

  protected final MetricsRegistry _metricsRegistry;

  private final Class _clazz;

  public AbstractMetrics(String metricPrefix, MetricsRegistry metricsRegistry, Class clazz) {
    _metricPrefix = metricPrefix;
    _metricsRegistry = metricsRegistry;
    _clazz = clazz;
  }

  public interface QueryPhase {
    String getQueryPhaseName();
  }

  public interface Meter {
    String getMeterName();

    String getUnit();

    boolean isGlobal();
  }

  /**
   * Logs the timing of a query phase.
   *
   * @param request The broker request associated with this query
   * @param phase The query phase for which to log time
   * @param nanos The number of nanoseconds that the phase execution took to complete
   */
  public void addPhaseTiming(final BrokerRequest request, final QP phase, final long nanos) {
    final String fullTimerName = buildMetricName(request, phase.getQueryPhaseName());
    final MetricName metricName = new MetricName(_clazz, fullTimerName);

    MetricsHelper.newTimer(_metricsRegistry, metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS).update(nanos,
        TimeUnit.NANOSECONDS);
  }

  /**
   * Builds a complete metric name, of the form prefix.resource.metric
   *
   * @param request The broker request containing all the information
   * @param metricName The metric name to register
   * @return The complete metric name
   */
  private String buildMetricName(BrokerRequest request, String metricName) {
    if (request != null && request.getQuerySource() != null && request.getQuerySource().getTableName() != null) {
      return _metricPrefix + request.getQuerySource().getTableName() + "." + metricName;
    } else {
      return _metricPrefix + "unknown." + metricName;
    }
  }

  /**
   * Logs the time taken to complete the given callable.
   *
   * @param request The broker request associated with this query
   * @param phase The query phase
   * @param callable The callable to execute
   * @param <T> The return type of the callable
   * @return The return value of the callable passed as a parameter
   * @throws Exception The exception thrown by the callable
   */
  public <T> T timePhase(final BrokerRequest request, final QP phase, final Callable<T> callable) throws Exception {
    long startTime = System.nanoTime();
    T returnValue = callable.call();
    long totalNanos = System.nanoTime() - startTime;

    addPhaseTiming(request, phase, totalNanos);
    LOGGER.info(" Phase:" + phase + " took " + TimeUnit.MILLISECONDS.convert(totalNanos, TimeUnit.NANOSECONDS));
    return returnValue;
  }

  /**
   * Logs a value to a meter.
   *
   * @param request The broker request associated with this query or null if this meter applies globally
   * @param meter The meter to use
   * @param unitCount The number of units to add to the meter
   */
  public void addMeteredValue(final BrokerRequest request, final M meter, final long unitCount) {
    final String fullMeterName;
    String meterName = meter.getMeterName();
    if (request != null) {
      fullMeterName = buildMetricName(request, meterName);
    } else {
      fullMeterName = _metricPrefix + meterName;
    }
    final MetricName metricName = new MetricName(_clazz, fullMeterName);

    MetricsHelper.newMeter(_metricsRegistry, metricName, meter.getUnit(), TimeUnit.SECONDS).mark(unitCount);
  }

  /**
   * Initializes all global meters (such as exceptions count) to zero.
   */
  public void initializeGlobalMeters() {
    M[] meters = getMeters();

    for (M meter : meters) {
      if (meter.isGlobal()) {
        addMeteredValue(null, meter, 0);
      }
    }
  }

  /**
   * Adds a new gauge whose values are retrieved from a callback function.
   *
   * @param metricName The name of the metric
   * @param valueCallback The callback function used to retrieve the value of the gauge
   */
  public void addCallbackGauge(final String metricName, final Callable<Long> valueCallback) {
    MetricsHelper.newGauge(_metricsRegistry, new MetricName(_clazz, _metricPrefix + metricName), new Gauge<Long>() {
      @Override
      public Long value() {
        try {
          return valueCallback.call();
        } catch (Exception e) {
          LOGGER.error("Caught exception", e);
          Utils.rethrowException(e);
          throw new AssertionError("Should not reach this");
        }
      }
    });
  }

  protected abstract QP[] getQueryPhases();

  protected abstract M[] getMeters();
}
