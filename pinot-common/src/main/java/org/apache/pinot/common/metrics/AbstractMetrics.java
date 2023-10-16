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

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.pinot.common.Utils;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotTimer;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Common code for metrics implementations.
 */
public abstract class AbstractMetrics<QP extends AbstractMetrics.QueryPhase, M extends AbstractMetrics.Meter,
    G extends AbstractMetrics.Gauge, T extends AbstractMetrics.Timer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMetrics.class);

  protected final String _metricPrefix;

  protected final PinotMetricsRegistry _metricsRegistry;

  private final Class _clazz;

  // The purpose of having _gaugeValues is to make gauge metric updatable.
  // Since gauge metric itself is updatable now (https://github.com/apache/pinot/pull/9961), we can deprecate it.
  @Deprecated
  private final Map<String, AtomicLong> _gaugeValues = new ConcurrentHashMap<String, AtomicLong>();

  private final boolean _isTableLevelMetricsEnabled;

  // Table level metrics are still emitted for allowed tables even if emitting table level metrics is disabled
  private final Set<String> _allowedTables;

  public AbstractMetrics(String metricPrefix, PinotMetricsRegistry metricsRegistry, Class clazz) {
    this(metricPrefix, metricsRegistry, clazz, true, Collections.emptySet());
  }

  public AbstractMetrics(String metricPrefix, PinotMetricsRegistry metricsRegistry, Class clazz,
      boolean isTableLevelMetricsEnabled, Collection<String> allowedTables) {
    _metricPrefix = metricPrefix;
    _metricsRegistry = metricsRegistry;
    _clazz = clazz;
    _isTableLevelMetricsEnabled = isTableLevelMetricsEnabled;
    _allowedTables = addNameVariations(allowedTables);
  }

  /**
   * Some metrics use raw table name and some use table name with type. This method adds all variations of the table
   * name to the allowed entries to make sure that all metrics are checked against the allowed tables.
   */
  private static Set<String> addNameVariations(Collection<String> allowedTables) {
    return allowedTables.stream().flatMap(tableName -> TableNameBuilder.getTableNameVariations(tableName).stream())
        .collect(Collectors.toCollection(HashSet::new));
  }

  public PinotMetricsRegistry getMetricsRegistry() {
    return _metricsRegistry;
  }

  public interface QueryPhase {
    String getQueryPhaseName();

    default String getDescription() {
      return "";
    }
  }

  public interface Meter {
    String getMeterName();

    String getUnit();

    boolean isGlobal();

    default String getDescription() {
      return "";
    }
  }

  public interface Gauge {
    String getGaugeName();

    String getUnit();

    boolean isGlobal();
    default String getDescription() {
      return "";
    }
  }

  public interface Timer {
    String getTimerName();

    boolean isGlobal();
    default String getDescription() {
      return "";
    }
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
   * Logs the timing for a table with an additional key
   * @param tableName The table associated with this timer
   * @param key The additional key associated with this timer
   * @param timer The name of timer
   * @param duration The log time duration time value
   * @param timeUnit The log time duration time unit
   */
  public void addTimedTableValue(final String tableName, final String key, final T timer, final long duration,
      final TimeUnit timeUnit) {
    final String fullTimerName = _metricPrefix + getTableName(tableName) + "." + key + "." + timer.getTimerName();
    addValueToTimer(fullTimerName, duration, timeUnit);
  }

  /**
   * Logs the timing for a global timer
   * @param timer The name of timer
   * @param duration The log time duration time value
   * @param timeUnit The log time duration time unit
   */
  public void addTimedValue(T timer, final long duration, final TimeUnit timeUnit) {
    final String fullTimerName = _metricPrefix + timer.getTimerName();
    addValueToTimer(fullTimerName, duration, timeUnit);
  }

  /**
   * Logs the timing for a timer with a key
   * @param key The key associated with this timer
   * @param timer The name of timer
   * @param duration The log time duration time value
   * @param timeUnit The log time duration time unit
   */
  public void addTimedValue(final String key, final T timer, final long duration, final TimeUnit timeUnit) {
    final String fullTimerName = _metricPrefix + key + "." + timer.getTimerName();
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
    final PinotMetricName metricName = PinotMetricUtils.makePinotMetricName(_clazz, fullTimerName);
    PinotTimer timer =
        PinotMetricUtils.makePinotTimer(_metricsRegistry, metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    if (timer != null) {
      timer.update(duration, timeUnit);
    }
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
  public PinotMeter addMeteredGlobalValue(final M meter, final long unitCount, PinotMeter reusedMeter) {
    if (reusedMeter != null) {
      reusedMeter.mark(unitCount);
      return reusedMeter;
    } else {
      final String fullMeterName;
      String meterName = meter.getMeterName();
      fullMeterName = _metricPrefix + meterName;
      final PinotMetricName metricName = PinotMetricUtils.makePinotMetricName(_clazz, fullMeterName);

      final PinotMeter newMeter =
          PinotMetricUtils.makePinotMeter(_metricsRegistry, metricName, meter.getUnit(), TimeUnit.SECONDS);
      newMeter.mark(unitCount);
      return newMeter;
    }
  }

  /**
   * Logs a value to a meter with a key.
   *
   * @param key The key associated with this meter
   * @param meter The meter to use
   * @param unitCount The number of units to add to the meter
   */
  public void addMeteredValue(final String key, final M meter, final long unitCount) {
    addMeteredValue(key, meter, unitCount, null);
  }

  /**
   * Logs a value to a meter with a key.
   *
   * @param key The key associated with this meter
   * @param meter The meter to use
   * @param unitCount The number of units to add to the meter
   * @param reusedMeter The meter to reuse
   */
  public PinotMeter addMeteredValue(final String key, final M meter, final long unitCount, PinotMeter reusedMeter) {
    String meterName = meter.getMeterName();
    final String fullMeterName = _metricPrefix + key + "." + meterName;
    return addValueToMeter(fullMeterName, meter.getUnit(), unitCount, reusedMeter);
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
  public PinotMeter addMeteredTableValue(final String tableName, final M meter, final long unitCount,
      PinotMeter reusedMeter) {
    return addValueToMeter(getTableFullMeterName(tableName, meter), meter.getUnit(), unitCount, reusedMeter);
  }

  /**
   * Logs a value to a table-level meter with an additional key
   * @param tableName The table name
   * @param key The additional key associated with this meter
   * @param meter The meter to use
   * @param unitCount The number of units to add to the meter
   */
  public void addMeteredTableValue(final String tableName, final String key, final M meter, final long unitCount) {
    addMeteredTableValue(tableName, key, meter, unitCount, null);
  }

  /**
   * Logs a value to a table-level meter with an additional key
   * @param tableName The table name
   * @param key The additional key associated with this meter
   * @param meter The meter to use
   * @param unitCount The number of units to add to the meter
   * @param reusedMeter The meter to reuse
   */
  public PinotMeter addMeteredTableValue(final String tableName, final String key, final M meter, final long unitCount,
      PinotMeter reusedMeter) {
    String meterName = meter.getMeterName();
    final String fullMeterName = _metricPrefix + getTableName(tableName) + "." + key + "." + meterName;
    return addValueToMeter(fullMeterName, meter.getUnit(), unitCount, reusedMeter);
  }

  private PinotMeter addValueToMeter(final String fullMeterName, final String unit, final long unitCount,
      PinotMeter reusedMeter) {
    if (reusedMeter != null) {
      reusedMeter.mark(unitCount);
      return reusedMeter;
    } else {
      final PinotMetricName metricName = PinotMetricUtils.makePinotMetricName(_clazz, fullMeterName);
      final PinotMeter newMeter =
          PinotMetricUtils.makePinotMeter(_metricsRegistry, metricName, unit, TimeUnit.SECONDS);
      newMeter.mark(unitCount);
      return newMeter;
    }
  }

  public PinotMeter getMeteredTableValue(final String tableName, final M meter) {
    final PinotMetricName metricName = PinotMetricUtils.makePinotMetricName(_clazz,
        getTableFullMeterName(tableName, meter));

    return PinotMetricUtils.makePinotMeter(_metricsRegistry, metricName, meter.getUnit(), TimeUnit.SECONDS);
  }

  private String getTableFullMeterName(final String tableName, final M meter) {
    String meterName = meter.getMeterName();
    return _metricPrefix + getTableName(tableName) + "." + meterName;
  }

  /**
   * @deprecated Please use addMeteredTableValue(final String tableName, final M meter, final long unitCount), which is
   * designed for tracking count and rates.
   *
   * Logs a value to a table gauge.
   *
   * @param tableName The table name
   * @param gauge The gauge to use
   * @param unitCount The number of units to add to the gauge
   */
  @Deprecated
  public void addValueToTableGauge(final String tableName, final G gauge, final long unitCount) {
    final String fullGaugeName = composeTableGaugeName(tableName, gauge);

    AtomicLong gaugeValue = _gaugeValues.get(fullGaugeName);
    if (gaugeValue == null) {
      synchronized (_gaugeValues) {
        if (!_gaugeValues.containsKey(fullGaugeName)) {
          _gaugeValues.put(fullGaugeName, new AtomicLong(unitCount));
          addCallbackGauge(fullGaugeName, new Callable<Long>() {
            @Override
            public Long call()
                throws Exception {
              return _gaugeValues.get(fullGaugeName).get();
            }
          });
        } else {
          _gaugeValues.get(fullGaugeName).addAndGet(unitCount);
        }
      }
    } else {
      gaugeValue.addAndGet(unitCount);
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
    final String fullGaugeName = composeTableGaugeName(tableName, gauge);
    setValueOfGauge(value, fullGaugeName);
  }

  /**
   * Sets the value of a table partition gauge.
   *
   * @param tableName The table name
   * @param partitionId The partition name
   * @param gauge The gauge to use
   * @param value The value to set the gauge to
   */
  public void setValueOfPartitionGauge(final String tableName, final int partitionId, final G gauge, final long value) {
    final String fullGaugeName = composeTableGaugeName(tableName, String.valueOf(partitionId), gauge);
    setValueOfGauge(value, fullGaugeName);
  }

  /**
   * Sets the value of a custom global gauge.
   *
   * @param suffix The suffix to attach to the gauge name
   * @param gauge The gauge to use
   * @param value The value to set the gauge to
   */
  public void setValueOfGlobalGauge(final G gauge, final String suffix, final long value) {
    final String fullGaugeName;
    String gaugeName = gauge.getGaugeName();
    fullGaugeName = gaugeName + "." + suffix;

    setValueOfGauge(value, fullGaugeName);
  }

  /**
   * Sets the value of a global  gauge.
   *
   * @param gauge The gauge to use
   * @param value The value to set the gauge to
   */
  public void setValueOfGlobalGauge(final G gauge, final long value) {
    final String gaugeName = gauge.getGaugeName();

    setValueOfGauge(value, gaugeName);
  }

  private void setValueOfGauge(long value, String gaugeName) {
    AtomicLong gaugeValue = _gaugeValues.get(gaugeName);
    if (gaugeValue == null) {
      synchronized (_gaugeValues) {
        if (!_gaugeValues.containsKey(gaugeName)) {
          _gaugeValues.put(gaugeName, new AtomicLong(value));
          setOrUpdateGauge(gaugeName, () -> _gaugeValues.get(gaugeName).get());
        } else {
          _gaugeValues.get(gaugeName).set(value);
        }
      }
    } else {
      gaugeValue.set(value);
    }
  }

  /**
   * @deprecated Please use addMeteredGlobalValue(final M meter, final long unitCount), which is designed for tracking
   * count and rates.
   *
   * Adds a value to a table gauge.
   *
   * @param gauge The gauge to use
   * @param unitCount The number of units to add to the gauge
   */
  @Deprecated
  public void addValueToGlobalGauge(final G gauge, final long unitCount) {
    String gaugeName = gauge.getGaugeName();

    AtomicLong gaugeValue = _gaugeValues.get(gaugeName);
    if (gaugeValue == null) {
      synchronized (_gaugeValues) {
        if (!_gaugeValues.containsKey(gaugeName)) {
          _gaugeValues.put(gaugeName, new AtomicLong(unitCount));
          setOrUpdateGauge(gaugeName, () -> _gaugeValues.get(gaugeName).get());
        } else {
          _gaugeValues.get(gaugeName).addAndGet(unitCount);
        }
      }
    } else {
      gaugeValue.addAndGet(unitCount);
    }
  }

  /**
   * Initializes all global meters (such as exceptions count) to zero.
   */
  public void initializeGlobalMeters() {
    M[] meters = getMeters();
    LOGGER.info("Initializing global {} meters", meters.length);

    for (M meter : meters) {
      if (meter.isGlobal()) {
        addMeteredGlobalValue(meter, 0);
      }
    }

    G[] gauges = getGauges();
    LOGGER.info("Initializing global {} gauges", gauges.length);
    for (G gauge : gauges) {
      if (gauge.isGlobal()) {
        setValueOfGlobalGauge(gauge, 0);
      }
    }
  }

  /**
   * @deprecated please use setOrUpdateTableGauge(final String tableName, final G gauge,
   *     final Supplier<Long> valueSupplier) instead.
   *
   * Adds a new gauge whose values are retrieved from a callback function.
   * This method may be called multiple times, while it will be registered to callback function only once.
   *
   * @param tableName The table name
   * @param gauge the gauge to use
   * @param valueCallback The callback function used to retrieve the value of the gauge
   */
  @Deprecated
  public void addCallbackTableGaugeIfNeeded(final String tableName, final G gauge, final Callable<Long> valueCallback) {
    final String fullGaugeName = composeTableGaugeName(tableName, gauge);
    addCallbackGaugeIfNeeded(fullGaugeName, valueCallback);
  }

  /**
   * Install a per-partition table gauge.
   *
   * @param tableName The table name
   * @param partitionId The partition id
   * @param gauge The gauge to use
   * @param valueSupplier The supplier function used to retrieve the value of the gauge.
   */
  public void setOrUpdatePartitionGauge(final String tableName, final int partitionId, final G gauge,
      final Supplier<Long> valueSupplier) {
    final String fullGaugeName = composeTableGaugeName(tableName, String.valueOf(partitionId), gauge);
    setOrUpdateGauge(fullGaugeName, valueSupplier);
  }

  /**
   * @deprecated please use setOrUpdateGauge(final String metricName, final Supplier<Long> valueSupplier) instead.
   *
   * Adds a new gauge whose values are retrieved from a callback function.
   * This method may be called multiple times, while it will be registered to callback function only once.
   *
   * It's actually same as addCallbackGauge(final String metricName, final Callable<Long> valueCallback) method.
   *
   * @param metricName The name of the metric
   * @param valueCallback The callback function used to retrieve the value of the gauge
   */
  @Deprecated
  public void addCallbackGaugeIfNeeded(final String metricName, final Callable<Long> valueCallback) {
    if (!_gaugeValues.containsKey(metricName)) {
      synchronized (_gaugeValues) {
        if (!_gaugeValues.containsKey(metricName)) {
          _gaugeValues.put(metricName, new AtomicLong(0L));
          addCallbackGauge(metricName, valueCallback);
        }
      }
    }
  }

  /**
   * @deprecated please use setOrUpdateGauge(final String metricName, final Supplier<Long> valueSupplier) instead.
   *
   * Adds a new gauge whose values are retrieved from a callback function.
   * Once added, the callback function cannot be updated.
   *
   * It's actually same as addCallbackGaugeIfNeeded(final String metricName, final Callable<Long> valueCallback) method
   *
   * @param metricName The name of the metric
   * @param valueCallback The callback function used to retrieve the value of the gauge
   */
  @Deprecated
  public void addCallbackGauge(final String metricName, final Callable<Long> valueCallback) {
    PinotMetricUtils
        .makeGauge(_metricsRegistry, PinotMetricUtils.makePinotMetricName(_clazz, _metricPrefix + metricName),
            PinotMetricUtils.makePinotGauge(avoid -> {
              try {
                return valueCallback.call();
              } catch (Exception e) {
                LOGGER.error("Caught exception", e);
                Utils.rethrowException(e);
                throw new AssertionError("Should not reach this");
              }
            }));
  }

  /**
   * Sets or updates a gauge to the given value.
   * The value can be updated by calling this method again.
   *
   * @param tableName The table name
   * @param key The key associated with this gauge
   * @param gauge The gauge to use
   * @param value The value of the gauge
   */
  public void setOrUpdateTableGauge(final String tableName, final String key, final G gauge, final long value) {
    String fullGaugeName = composeTableGaugeName(tableName, key, gauge);
    setOrUpdateGauge(fullGaugeName, value);
  }

  /**
   * Sets or updates a gauge whose values are retrieved from the given supplier function.
   * The supplier function can be updated by calling this method again.
   *
   * @param tableName The table name
   * @param key The key associated with this gauge
   * @param gauge The gauge to use
   * @param valueSupplier The supplier function used to retrieve the value of the gauge
   */
  public void setOrUpdateTableGauge(final String tableName, final String key, final G gauge,
      final Supplier<Long> valueSupplier) {
    String fullGaugeName = composeTableGaugeName(tableName, key, gauge);
    setOrUpdateGauge(fullGaugeName, valueSupplier);
  }

  /**
   * Sets or updates a gauge to the given value.
   * The value can be updated by calling this method again.
   *
   * @param tableName The table name
   * @param gauge The gauge to use
   * @param value The value of the gauge
   */
  public void setOrUpdateTableGauge(final String tableName, final G gauge, final long value) {
    String fullGaugeName = composeTableGaugeName(tableName, gauge);
    setOrUpdateGauge(fullGaugeName, value);
  }

  /**
   * Sets or updates a gauge whose values are retrieved from the given supplier function.
   * The supplier function can be updated by calling this method again.
   *
   * @param tableName The table name
   * @param gauge The gauge to use
   * @param valueSupplier The supplier function used to retrieve the value of the gauge
   */
  public void setOrUpdateTableGauge(final String tableName, final G gauge,
      final Supplier<Long> valueSupplier) {
    String fullGaugeName = composeTableGaugeName(tableName, gauge);
    setOrUpdateGauge(fullGaugeName, valueSupplier);
  }

  /**
   * Sets or updates a gauge to the given value.
   * The value can be updated by calling this method again.
   *
   * @param metricName The name of the metric
   * @param value The value of the gauge
   */
  public void setOrUpdateGauge(final String metricName, long value) {
    PinotGauge<Long> pinotGauge = PinotMetricUtils.makeGauge(_metricsRegistry,
        PinotMetricUtils.makePinotMetricName(_clazz, _metricPrefix + metricName),
        PinotMetricUtils.makePinotGauge(avoid -> value));
    pinotGauge.setValue(value);
  }

  /**
   * Sets or updates a gauge whose values are retrieved from the given supplier function.
   * The supplier function can be updated by calling this method again.
   *
   * @param metricName The name of the metric
   * @param valueSupplier The supplier function used to retrieve the value of the gauge
   */
  public void setOrUpdateGauge(final String metricName, final Supplier<Long> valueSupplier) {
    PinotGauge<Long> pinotGauge = PinotMetricUtils.makeGauge(_metricsRegistry,
        PinotMetricUtils.makePinotMetricName(_clazz, _metricPrefix + metricName),
        PinotMetricUtils.makePinotGauge(avoid -> valueSupplier.get()));
    pinotGauge.setValueSupplier(valueSupplier);
  }

  /**
   * Like {@link #setOrUpdateGauge(String, Supplier)}
   */
  public void setOrUpdateGauge(final String metricName, final LongSupplier valueSupplier) {
    PinotGauge<Long> pinotGauge = PinotMetricUtils.makeGauge(_metricsRegistry,
        PinotMetricUtils.makePinotMetricName(_clazz, _metricPrefix + metricName),
        PinotMetricUtils.makePinotGauge(avoid -> valueSupplier.getAsLong()));
    pinotGauge.setValueSupplier((Supplier<Long>) () -> (Long) valueSupplier.getAsLong());
  }

  /**
   * Like {@link #setOrUpdateGauge(String, Supplier)} but using a global gauge
   * @throws IllegalArgumentException if the gauge is not global
   */
  public void setOrUpdateGlobalGauge(final G gauge, final Supplier<Long> valueSupplier) {
    Preconditions.checkArgument(gauge.isGlobal(), "Only global gauges should be sent to this method");
    setOrUpdateGauge(gauge.getGaugeName(), valueSupplier);
  }

  /**
   * Like {@link #setOrUpdateGauge(String, LongSupplier)} but using a global gauge
   * @throws IllegalArgumentException if the gauge is not global
   */
  public void setOrUpdateGlobalGauge(final G gauge, final LongSupplier valueSupplier) {
    Preconditions.checkArgument(gauge.isGlobal(), "Only global gauges should be sent to this method");
    setOrUpdateGauge(gauge.getGaugeName(), valueSupplier);
  }

  /**
   * Removes a global gauge given the key and the gauge
   * @param key the key associated with the gauge
   * @param gauge the gauge to be removed
   */
  public void removeGlobalGauge(final String key, final G gauge) {
    final String fullGaugeName = composeGlobalGaugeName(key, gauge);
    removeGauge(fullGaugeName);
  }

  /**
   * Removes a table gauge given the table name and the gauge.
   * The add/remove is expected to work correctly in case of being invoked across multiple threads.
   * @param tableName table name
   * @param gauge the gauge to be removed
   */
  public void removeTableGauge(final String tableName, final G gauge) {
    final String fullGaugeName = composeTableGaugeName(tableName, gauge);
    removeGauge(fullGaugeName);
  }


  /**
   * Removes a table gauge given the table name, the partition id and the gauge.
   * The add/remove is expected to work correctly in case of being invoked across multiple threads.
   * @param tableName table name
   * @param partitionId The partition id
   * @param gauge the gauge to be removed
   */
  public void removePartitionGauge(final String tableName, final int partitionId, final G gauge) {
    final String fullGaugeName = composeTableGaugeName(tableName, String.valueOf(partitionId), gauge);
    removeGauge(fullGaugeName);
  }

  /**
   * Removes a table gauge given the table name, the key and the gauge.
   * The add/remove is expected to work correctly in case of being invoked across multiple threads.
   * @param tableName table name
   * @param key the key associated with the gauge
   * @param gauge the gauge to be removed
   */
  public void removeTableGauge(final String tableName, final String key, final G gauge) {
    final String fullGaugeName = composeTableGaugeName(tableName, key, gauge);
    removeGauge(fullGaugeName);
  }

  private String composeGlobalGaugeName(final String key, final G gauge) {
    return gauge.getGaugeName() + "." + key;
  }

  private String composeTableGaugeName(final String tableName, final G gauge) {
    return gauge.getGaugeName() + "." + getTableName(tableName);
  }

  private String composeTableGaugeName(final String tableName, final String key, final G gauge) {
    return gauge.getGaugeName() + "." + getTableName(tableName) + "." + key;
  }

  /**
   * Remove gauge from Pinot metrics.
   * @param gaugeName gauge name
   */
  public void removeGauge(final String gaugeName) {
    _gaugeValues.remove(gaugeName);
    removeGaugeFromMetricRegistry(gaugeName);
  }

  public void removeTableMeter(final String tableName, final M meter) {
    PinotMetricUtils.removeMetric(_metricsRegistry,
        PinotMetricUtils.makePinotMetricName(_clazz, getTableFullMeterName(tableName, meter)));
  }

  /**
   * Remove callback gauge.
   * @param metricName metric name
   */
  private void removeGaugeFromMetricRegistry(String metricName) {
    PinotMetricUtils
        .removeMetric(_metricsRegistry, PinotMetricUtils.makePinotMetricName(_clazz, _metricPrefix + metricName));
  }

  protected abstract QP[] getQueryPhases();

  protected abstract M[] getMeters();

  protected abstract G[] getGauges();

  protected String getTableName(String tableName) {
    return _isTableLevelMetricsEnabled || _allowedTables.contains(tableName) ? tableName : "allTables";
  }
}
