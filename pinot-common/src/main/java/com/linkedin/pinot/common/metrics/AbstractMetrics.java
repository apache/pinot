/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Common code for metrics implementations.
 *
 */
public abstract class AbstractMetrics<QP extends AbstractMetrics.QueryPhase, M extends AbstractMetrics.Meter, G extends
    AbstractMetrics.Gauge, T extends AbstractMetrics.Timer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMetrics.class);

  protected final String _metricPrefix;

  protected final MetricsRegistry _metricsRegistry;

  private final Class _clazz;

  private final Map<String, AtomicLong> _gaugeValues = new ConcurrentHashMap<String, AtomicLong>();

  protected final boolean _global;

  public AbstractMetrics(String metricPrefix, MetricsRegistry metricsRegistry, Class clazz) {
    this(metricPrefix, metricsRegistry, clazz, false);
  }

  public AbstractMetrics(String metricPrefix, MetricsRegistry metricsRegistry, Class clazz, boolean global) {
    _metricPrefix = metricPrefix;
    _metricsRegistry = metricsRegistry;
    _clazz = clazz;
    _global = global;

  }

  public interface QueryPhase {
    String getQueryPhaseName();
  }

  public interface Meter {
    String getMeterName();

    String getUnit();

    boolean isGlobal();
  }

  public interface Gauge {
    String getGaugeName();

    String getUnit();

    boolean isGlobal();
  }

  public interface Timer {
    String getTimerName();

    boolean isGlobal();
  }

  /**
   * Logs the timing of a query phase.
   *
   * @param request The broker request associated with this query
   * @param phase The query phase for which to log time
   * @param duration The duration that the phase execution took to complete
   * @param timeUnit The time unit of the duration
   */
  public void addPhaseTiming(BrokerRequest request, QP phase, long duration, TimeUnit timeUnit) {
    String fullTimerName = buildMetricName(request, phase.getQueryPhaseName());
    addValueToTimer(fullTimerName, duration, timeUnit);
  }

  public void addPhaseTiming(BrokerRequest request, QP phase, long nanos) {
    addPhaseTiming(request, phase, nanos, TimeUnit.NANOSECONDS);
  }

  public void addPhaseTiming(String tableName, QP phase, long duration, TimeUnit timeUnit) {
    String fullTimerName = _metricPrefix + getTableName(tableName) + "." + phase.getQueryPhaseName();
    addValueToTimer(fullTimerName, duration, timeUnit);
  }

  public void addPhaseTiming(String tableName, QP phase, long nanos) {
    addPhaseTiming(tableName, phase, nanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Logs the timing for a table
   *
   * @param tableName The table associated with this timer
   * @param timer The name of timer
   * @param duration The log time duration time value
   * @param timeUnit The log time duration time unit
   */
  public void addTimedTableValue(final String tableName, T timer, final long duration, final TimeUnit timeUnit) {
    final String fullTimerName = _metricPrefix + getTableName(tableName) + "." + timer.getTimerName();
    addValueToTimer(fullTimerName, duration, timeUnit);
  }

  /**
   * Logs the timing for a global timer
   */
  public void addTimedValue(T timer, final long duration, final TimeUnit timeUnit) {
    final String fullTimerName = _metricPrefix + timer.getTimerName();
    addValueToTimer(fullTimerName, duration, timeUnit);
  }

  /**
   * Logs the timing for a metric
   *
   * @param fullTimerName The full name of timer
   * @param duration The log time duration time value
   * @param timeUnit The log time duration time unit
   */
  private void addValueToTimer(String fullTimerName, final long duration, final TimeUnit timeUnit) {
    final MetricName metricName = new MetricName(_clazz, fullTimerName);
    com.yammer.metrics.core.Timer timer =
        MetricsHelper.newTimer(_metricsRegistry, metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    MetricsHelper.newTimer(_metricsRegistry, metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS).update(duration,
        timeUnit);
  }

  /**
   * Builds a complete metric name, of the form prefix.resource.metric
   *
   * @param request The broker request containing all the information
   * @param metricName The metric name to register
   * @return The complete metric name
   */
  private String buildMetricName(@Nullable BrokerRequest request, String metricName) {
    if (request != null && request.getQuerySource() != null && request.getQuerySource().getTableName() != null) {
      return _metricPrefix + getTableName(request.getQuerySource().getTableName()) + "." + metricName;
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
  public <T> T timeQueryPhase(final BrokerRequest request, final QP phase, final Callable<T> callable) throws Exception {
    long startTime = System.nanoTime();
    T returnValue = callable.call();
    long totalNanos = System.nanoTime() - startTime;

    addPhaseTiming(request, phase, totalNanos);
    LOGGER.debug(" Phase: {} took {}ms", phase, TimeUnit.MILLISECONDS.convert(totalNanos, TimeUnit.NANOSECONDS));
    return returnValue;
  }

  /**
   * Logs a value to a meter.
   *
   * @param meter The meter to use
   * @param unitCount The number of units to add to the meter
   */
  public void addMeteredGlobalValue(final M meter, final long unitCount) {
    addMeteredGlobalValue(meter, unitCount, null);
  }

  /**
   * Logs a value to a meter.
   *
   * @param meter The meter to use
   * @param unitCount The number of units to add to the meter
   * @param reusedMeter The meter to reuse
   */
  public com.yammer.metrics.core.Meter addMeteredGlobalValue(final M meter, final long unitCount, com.yammer.metrics.core.Meter reusedMeter) {
    if (reusedMeter != null) {
      reusedMeter.mark(unitCount);
      return reusedMeter;
    } else {
      final String fullMeterName;
      String meterName = meter.getMeterName();
      fullMeterName = _metricPrefix + meterName;
      final MetricName metricName = new MetricName(_clazz, fullMeterName);

      final com.yammer.metrics.core.Meter newMeter =
          MetricsHelper.newMeter(_metricsRegistry, metricName, meter.getUnit(), TimeUnit.SECONDS);
      newMeter.mark(unitCount);
      return newMeter;
    }
  }

  /**
   * Logs a value to a table-level meter.
   *
   * @param tableName The table name
   * @param meter The meter to use
   * @param unitCount The number of units to add to the meter
   */
  public void addMeteredTableValue(final String tableName, final M meter, final long unitCount) {
    addMeteredTableValue(tableName, meter, unitCount, null);
  }

  /**
   * Logs a value to a table-level meter.
   * @param tableName The table name
   * @param meter The meter to use
   * @param unitCount The number of units to add to the meter
   * @param reusedMeter The meter to reuse
   */
  public com.yammer.metrics.core.Meter addMeteredTableValue(final String tableName, final M meter, final long unitCount, com.yammer.metrics.core.Meter reusedMeter) {
    if (reusedMeter != null) {
      reusedMeter.mark(unitCount);
      return reusedMeter;
    } else {
      final String fullMeterName;
      String meterName = meter.getMeterName();
      fullMeterName = _metricPrefix + getTableName(tableName) + "." + meterName;
      final MetricName metricName = new MetricName(_clazz, fullMeterName);

      final com.yammer.metrics.core.Meter newMeter =
          MetricsHelper.newMeter(_metricsRegistry, metricName, meter.getUnit(), TimeUnit.SECONDS);
      newMeter.mark(unitCount);
      return newMeter;
    }
  }

  public com.yammer.metrics.core.Meter getMeteredTableValue(final String tableName, final M meter) {
    final String fullMeterName;
    String meterName = meter.getMeterName();
    fullMeterName = _metricPrefix + getTableName(tableName) + "." + meterName;
    final MetricName metricName = new MetricName(_clazz, fullMeterName);

    return MetricsHelper.newMeter(_metricsRegistry, metricName, meter.getUnit(), TimeUnit.SECONDS);
  }

  /**
   * Logs a value to a meter for a specific query.
   *
   * @param request The broker request associated with this query
   * @param meter The meter to use
   * @param unitCount The number of units to add to the meter
   */
  public void addMeteredQueryValue(final BrokerRequest request, final M meter, final long unitCount) {
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
   * Logs a value to a table gauge.
   *
   * @param tableName The table name
   * @param gauge The gauge to use
   * @param unitCount The number of units to add to the gauge
   */
  public void addValueToTableGauge(final String tableName, final G gauge, final long unitCount) {
    final String fullGaugeName;
    String gaugeName = gauge.getGaugeName();
    fullGaugeName = gaugeName + "." + getTableName(tableName);

    if (!_gaugeValues.containsKey(fullGaugeName)) {
      synchronized (_gaugeValues) {
        if(!_gaugeValues.containsKey(fullGaugeName)) {
          _gaugeValues.put(fullGaugeName, new AtomicLong(unitCount));
          addCallbackGauge(fullGaugeName, new Callable<Long>() {
            @Override
            public Long call() throws Exception {
              return _gaugeValues.get(fullGaugeName).get();
            }
          });
        } else {
          _gaugeValues.get(fullGaugeName).addAndGet(unitCount);
        }
      }
    } else {
      _gaugeValues.get(fullGaugeName).addAndGet(unitCount);
    }
  }

  /**
   * Sets the value of a table gauge.
   *
   * @param tableName The table name
   * @param gauge The gauge to use
   * @param value The value to set the gauge to
   */
  public void setValueOfTableGauge(final String tableName, final G gauge, final long value) {
    final String fullGaugeName;
    String gaugeName = gauge.getGaugeName();
    fullGaugeName = gaugeName + "." + getTableName(tableName);

    if (!_gaugeValues.containsKey(fullGaugeName)) {
      synchronized (_gaugeValues) {
        if(!_gaugeValues.containsKey(fullGaugeName)) {
          _gaugeValues.put(fullGaugeName, new AtomicLong(value));
          addCallbackGauge(fullGaugeName, new Callable<Long>() {
            @Override
            public Long call() throws Exception {
              return _gaugeValues.get(fullGaugeName).get();
            }
          });
        } else {
          _gaugeValues.get(fullGaugeName).set(value);
        }
      }
    } else {
      _gaugeValues.get(fullGaugeName).set(value);
    }
  }

  /**
   * Sets the value of a global  gauge.
   *
   * @param gauge The gauge to use
   * @param value The value to set the gauge to
   */
  public void setValueOfGlobalGauge(final G gauge, final long value) {
    final String fullGaugeName;
    final String gaugeName = gauge.getGaugeName();

    if (!_gaugeValues.containsKey(gaugeName)) {
      synchronized (_gaugeValues) {
        if(!_gaugeValues.containsKey(gaugeName)) {
          _gaugeValues.put(gaugeName, new AtomicLong(value));
          addCallbackGauge(gaugeName, new Callable<Long>() {
            @Override
            public Long call() throws Exception {
              return _gaugeValues.get(gaugeName).get();
            }
          });
        } else {
          _gaugeValues.get(gaugeName).set(value);
        }
      }
    } else {
      _gaugeValues.get(gaugeName).set(value);
    }
  }

  @VisibleForTesting
  public long getValueOfGlobalGauge(final G gauge) {
    String gaugeName = gauge.getGaugeName();
    if (!_gaugeValues.containsKey(gaugeName)) {
      return 0;
    } else {
      return _gaugeValues.get(gaugeName).get();
    }
  }

  /**
   * Gets the value of a table gauge.
   *
   * @param tableName The table name
   * @param gauge The gauge to use
   */
  public long getValueOfTableGauge(final String tableName, final G gauge) {
    final String fullGaugeName;
    String gaugeName = gauge.getGaugeName();
    fullGaugeName = gaugeName + "." + getTableName(tableName);

    if (!_gaugeValues.containsKey(fullGaugeName)) {
      return 0;
    } else {
      return _gaugeValues.get(fullGaugeName).get();
    }
  }

  /**
   * Initializes all global meters (such as exceptions count) to zero.
   */
  public void initializeGlobalMeters() {
    M[] meters = getMeters();

    for (M meter : meters) {
      if (meter.isGlobal()) {
        addMeteredGlobalValue(meter, 0);
      }
    }

    G[] gauges = getGauges();
    for (G gauge : gauges) {
      if (gauge.isGlobal()) {
        setValueOfGlobalGauge(gauge, 0);
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
    MetricsHelper.newGauge(_metricsRegistry, new MetricName(_clazz, _metricPrefix + metricName), new
        com.yammer.metrics.core.Gauge<Long>() {
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

  protected abstract G[] getGauges();

  protected String getTableName(String tableName){
     return (_global ? "allTables" : tableName);
  }
}
