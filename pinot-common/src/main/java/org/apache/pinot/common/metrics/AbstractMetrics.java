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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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


/// Common code for metrics implementations.
public abstract class AbstractMetrics<QP extends AbstractMetrics.QueryPhase, M extends AbstractMetrics.Meter,
    G extends AbstractMetrics.Gauge, T extends AbstractMetrics.Timer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMetrics.class);

  /// Name every table's series collapses into when table-level metrics are off. Shared by every table, so a
  /// table-scoped sweep must never target it.
  private static final String ALL_TABLES = "allTables";

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
    this(metricPrefix, metricsRegistry, clazz, true, Set.of());
  }

  public AbstractMetrics(String metricPrefix, PinotMetricsRegistry metricsRegistry, Class clazz,
      boolean isTableLevelMetricsEnabled, Collection<String> allowedTables) {
    _metricPrefix = metricPrefix;
    _metricsRegistry = metricsRegistry;
    _clazz = clazz;
    _isTableLevelMetricsEnabled = isTableLevelMetricsEnabled;
    _allowedTables = addNameVariations(allowedTables);
  }

  /// Some metrics use raw table name and some use table name with type. This method adds all variations of the table
  /// name to the allowed entries to make sure that all metrics are checked against the allowed tables.
  private static Set<String> addNameVariations(Collection<String> allowedTables) {
    return allowedTables.stream().flatMap(tableName -> TableNameBuilder.getTableNameVariations(tableName).stream())
        .collect(Collectors.toCollection(HashSet::new));
  }

  public PinotMetricsRegistry getMetricsRegistry() {
    return _metricsRegistry;
  }

  public String getMetricPrefix() {
    return _metricPrefix;
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

  public void removePhaseTiming(String tableName, QP phase) {
    String fullTimerName = _metricPrefix + getTableName(tableName) + "." + phase.getQueryPhaseName();
    removeTimer(fullTimerName);
  }

  public void addPhaseTiming(String tableName, QP phase, long duration, TimeUnit timeUnit) {
    String fullTimerName = _metricPrefix + getTableName(tableName) + "." + phase.getQueryPhaseName();
    addValueToTimer(fullTimerName, duration, timeUnit);
  }

  public void addPhaseTiming(String tableName, QP phase, long nanos) {
    addPhaseTiming(tableName, phase, nanos, TimeUnit.NANOSECONDS);
  }

  /// Logs the timing for a table
  ///
  /// @param tableName The table associated with this timer
  /// @param timer The name of timer
  /// @param duration The log time duration time value
  /// @param timeUnit The log time duration time unit
  public void addTimedTableValue(final String tableName, T timer, final long duration, final TimeUnit timeUnit) {
    final String fullTimerName = _metricPrefix + getTableName(tableName) + "." + timer.getTimerName();
    addValueToTimer(fullTimerName, duration, timeUnit);
  }

  /// Logs the timing for a table with an additional key
  /// @param tableName The table associated with this timer
  /// @param key The additional key associated with this timer
  /// @param timer The name of timer
  /// @param duration The log time duration time value
  /// @param timeUnit The log time duration time unit
  public void addTimedTableValue(final String tableName, final String key, final T timer, final long duration,
      final TimeUnit timeUnit) {
    final String fullTimerName = _metricPrefix + getTableName(tableName) + "." + key + "." + timer.getTimerName();
    addValueToTimer(fullTimerName, duration, timeUnit);
  }

  /// Logs the timing for a global timer
  /// @param timer The name of timer
  /// @param duration The log time duration time value
  /// @param timeUnit The log time duration time unit
  public void addTimedValue(T timer, final long duration, final TimeUnit timeUnit) {
    final String fullTimerName = _metricPrefix + timer.getTimerName();
    addValueToTimer(fullTimerName, duration, timeUnit);
  }

  /// Logs the timing for a timer with a key
  /// @param key The key associated with this timer
  /// @param timer The name of timer
  /// @param duration The log time duration time value
  /// @param timeUnit The log time duration time unit
  public void addTimedValue(final String key, final T timer, final long duration, final TimeUnit timeUnit) {
    final String fullTimerName = _metricPrefix + key + "." + timer.getTimerName();
    addValueToTimer(fullTimerName, duration, timeUnit);
  }

  /// Logs the timing for a metric
  ///
  /// @param fullTimerName The full name of timer
  /// @param duration The log time duration time value
  /// @param timeUnit The log time duration time unit
  private void addValueToTimer(String fullTimerName, final long duration, final TimeUnit timeUnit) {
    final PinotMetricName metricName = PinotMetricUtils.makePinotMetricName(_clazz, fullTimerName);
    PinotTimer timer =
        PinotMetricUtils.makePinotTimer(_metricsRegistry, metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    if (timer != null) {
      timer.update(duration, timeUnit);
    }
  }

  public void removeTimer(final String fullTimerName) {
    PinotMetricUtils
        .removeMetric(_metricsRegistry, PinotMetricUtils.makePinotMetricName(_clazz, fullTimerName));
  }

  public void removeTableTimer(final String tableName, final T timer) {
    final String fullTimerName = _metricPrefix + getTableName(tableName) + "." + timer.getTimerName();
    removeTimer(fullTimerName);
  }

  /// Logs a value to a meter.
  ///
  /// @param meter The meter to use
  /// @param unitCount The number of units to add to the meter
  public void addMeteredGlobalValue(final M meter, final long unitCount) {
    addMeteredGlobalValue(meter, unitCount, null);
  }

  /// Logs a value to a meter.
  ///
  /// @param meter The meter to use
  /// @param unitCount The number of units to add to the meter
  /// @param reusedMeter The meter to reuse
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

  /// Logs a value to a meter with a key.
  ///
  /// @param key The key associated with this meter
  /// @param meter The meter to use
  /// @param unitCount The number of units to add to the meter
  public void addMeteredValue(final String key, final M meter, final long unitCount) {
    addMeteredValue(key, meter, unitCount, null);
  }

  /// Logs a value to a meter with a key.
  ///
  /// @param key The key associated with this meter
  /// @param meter The meter to use
  /// @param unitCount The number of units to add to the meter
  /// @param reusedMeter The meter to reuse
  public PinotMeter addMeteredValue(final String key, final M meter, final long unitCount, PinotMeter reusedMeter) {
    String meterName = meter.getMeterName();
    final String fullMeterName = _metricPrefix + key + "." + meterName;
    return addValueToMeter(fullMeterName, meter.getUnit(), unitCount, reusedMeter);
  }

  /// Logs a value to a table-level meter.
  ///
  /// @param tableName The table name
  /// @param meter The meter to use
  /// @param unitCount The number of units to add to the meter
  public void addMeteredTableValue(final String tableName, final M meter, final long unitCount) {
    addMeteredTableValue(tableName, meter, unitCount, null);
  }

  /// Logs a value to a table-level meter.
  /// @param tableName The table name
  /// @param meter The meter to use
  /// @param unitCount The number of units to add to the meter
  /// @param reusedMeter The meter to reuse
  public PinotMeter addMeteredTableValue(final String tableName, final M meter, final long unitCount,
      PinotMeter reusedMeter) {
    return addValueToMeter(getTableFullMeterName(tableName, meter), meter.getUnit(), unitCount, reusedMeter);
  }

  /// Logs a value to a table-level meter with an additional key
  /// @param tableName The table name
  /// @param key The additional key associated with this meter
  /// @param meter The meter to use
  /// @param unitCount The number of units to add to the meter
  public void addMeteredTableValue(final String tableName, final String key, final M meter, final long unitCount) {
    addMeteredTableValue(tableName, key, meter, unitCount, null);
  }

  /// Logs a value to a table-level meter with an additional key
  /// @param tableName The table name
  /// @param key The additional key associated with this meter
  /// @param meter The meter to use
  /// @param unitCount The number of units to add to the meter
  /// @param reusedMeter The meter to reuse
  public PinotMeter addMeteredTableValue(final String tableName, final String key, final M meter, final long unitCount,
      PinotMeter reusedMeter) {
    String meterName = meter.getMeterName();
    final String fullMeterName = _metricPrefix + getTableName(tableName) + "." + key + "." + meterName;
    return addValueToMeter(fullMeterName, meter.getUnit(), unitCount, reusedMeter);
  }

  public PinotMeter addMeteredValue(final M meter, final long unitCount, final String... tags) {
    String meterName = meter.getMeterName();
    final String fullMeterName = _metricPrefix + meterName + "." + String.join(".", tags);
    return addValueToMeter(fullMeterName, meter.getUnit(), unitCount, null);
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

  public PinotMeter getMeteredValue(final M meter) {
    final PinotMetricName metricName =
        PinotMetricUtils.makePinotMetricName(_clazz, _metricPrefix + meter.getMeterName());
    return PinotMetricUtils.makePinotMeter(_metricsRegistry, metricName, meter.getUnit(), TimeUnit.SECONDS);
  }

  private String getTableFullMeterName(final String tableName, final M meter) {
    String meterName = meter.getMeterName();
    return _metricPrefix + getTableName(tableName) + "." + meterName;
  }

  /// @deprecated Please use addMeteredTableValue(final String tableName, final M meter, final long unitCount), which is
  /// designed for tracking count and rates.
  ///
  /// Logs a value to a table gauge.
  ///
  /// @param tableName The table name
  /// @param gauge The gauge to use
  /// @param unitCount The number of units to add to the gauge
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

  /// Sets the value of a table gauge.
  ///
  /// @param tableName The table name
  /// @param gauge The gauge to use
  /// @param value The value to set the gauge to
  public void setValueOfTableGauge(final String tableName, final G gauge, final long value) {
    final String fullGaugeName = composeTableGaugeName(tableName, gauge);
    setValueOfGauge(value, fullGaugeName);
  }

  /// Sets the value of a table partition gauge.
  ///
  /// @param tableName The table name
  /// @param partitionId The partition name
  /// @param gauge The gauge to use
  /// @param value The value to set the gauge to
  public void setValueOfPartitionGauge(final String tableName, final int partitionId, final G gauge, final long value) {
    final String fullGaugeName = composeTableGaugeName(tableName, String.valueOf(partitionId), gauge);
    setValueOfGauge(value, fullGaugeName);
  }

  /// Sets the value of a custom global gauge.
  ///
  /// @param suffix The suffix to attach to the gauge name
  /// @param gauge The gauge to use
  /// @param value The value to set the gauge to
  public void setValueOfGlobalGauge(final G gauge, final String suffix, final long value) {
    final String fullGaugeName;
    String gaugeName = gauge.getGaugeName();
    fullGaugeName = gaugeName + "." + suffix;

    setValueOfGauge(value, fullGaugeName);
  }

  /// Sets the value of a global  gauge.
  ///
  /// @param gauge The gauge to use
  /// @param value The value to set the gauge to
  public void setValueOfGlobalGauge(final G gauge, final long value) {
    final String gaugeName = gauge.getGaugeName();

    setValueOfGauge(value, gaugeName);
  }

  protected void setValueOfGauge(long value, String gaugeName) {
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

  /// @deprecated Please use addMeteredGlobalValue(final M meter, final long unitCount), which is designed for tracking
  /// count and rates.
  ///
  /// Adds a value to a table gauge.
  ///
  /// @param gauge The gauge to use
  /// @param unitCount The number of units to add to the gauge
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

  /// Get the gauge metric value for the provided gauge name
  /// @param gaugeName gauge name
  /// @return gauge value. If gauge is not present return null.
  @Nullable
  public Long getGaugeValue(final String gaugeName) {
    AtomicLong value = _gaugeValues.get(gaugeName);
    return value != null ? value.get() : null;
  }

  /// Initializes all global meters (such as exceptions count) to zero.
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

  /// @deprecated please use setOrUpdateTableGauge(final String tableName, final G gauge,
  ///     final Supplier<Long> valueSupplier) instead.
  ///
  /// Adds a new gauge whose values are retrieved from a callback function.
  /// This method may be called multiple times, while it will be registered to callback function only once.
  ///
  /// @param tableName The table name
  /// @param gauge the gauge to use
  /// @param valueCallback The callback function used to retrieve the value of the gauge
  @Deprecated
  public void addCallbackTableGaugeIfNeeded(final String tableName, final G gauge, final Callable<Long> valueCallback) {
    final String fullGaugeName = composeTableGaugeName(tableName, gauge);
    addCallbackGaugeIfNeeded(fullGaugeName, valueCallback);
  }

  /// Install a per-partition table gauge.
  ///
  /// @param tableName The table name
  /// @param partitionId The partition id
  /// @param gauge The gauge to use
  /// @param valueSupplier The supplier function used to retrieve the value of the gauge.
  public void setOrUpdatePartitionGauge(final String tableName, final int partitionId, final G gauge,
      final Supplier<Long> valueSupplier) {
    final String fullGaugeName = composeTableGaugeName(tableName, String.valueOf(partitionId), gauge);
    setOrUpdateGauge(fullGaugeName, valueSupplier);
  }

  /// @deprecated please use setOrUpdateGauge(final String metricName, final Supplier<Long> valueSupplier) instead.
  ///
  /// Adds a new gauge whose values are retrieved from a callback function.
  /// This method may be called multiple times, while it will be registered to callback function only once.
  ///
  /// It's actually same as addCallbackGauge(final String metricName, final Callable<Long> valueCallback) method.
  ///
  /// @param metricName The name of the metric
  /// @param valueCallback The callback function used to retrieve the value of the gauge
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

  /// @deprecated please use setOrUpdateGauge(final String metricName, final Supplier<Long> valueSupplier) instead.
  ///
  /// Adds a new gauge whose values are retrieved from a callback function.
  /// Once added, the callback function cannot be updated.
  ///
  /// It's actually same as addCallbackGaugeIfNeeded(final String metricName, final Callable<Long> valueCallback) method
  ///
  /// @param metricName The name of the metric
  /// @param valueCallback The callback function used to retrieve the value of the gauge
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

  /// Sets or updates a gauge to the given value.
  /// The value can be updated by calling this method again.
  ///
  /// @param tableName The table name
  /// @param key The key associated with this gauge
  /// @param gauge The gauge to use
  /// @param value The value of the gauge
  public void setOrUpdateTableGauge(final String tableName, final String key, final G gauge, final long value) {
    String fullGaugeName = composeTableGaugeName(tableName, key, gauge);
    setOrUpdateGauge(fullGaugeName, value);
  }

  /// Sets or updates a gauge whose values are retrieved from the given supplier function.
  /// The supplier function can be updated by calling this method again.
  ///
  /// @param tableName The table name
  /// @param key The key associated with this gauge
  /// @param gauge The gauge to use
  /// @param valueSupplier The supplier function used to retrieve the value of the gauge
  public void setOrUpdateTableGauge(final String tableName, final String key, final G gauge,
      final Supplier<Long> valueSupplier) {
    String fullGaugeName = composeTableGaugeName(tableName, key, gauge);
    setOrUpdateGauge(fullGaugeName, valueSupplier);
  }

  /// Sets or updates a gauge to the given value.
  /// The value can be updated by calling this method again.
  ///
  /// @param tableName The table name
  /// @param gauge The gauge to use
  /// @param value The value of the gauge
  public void setOrUpdateTableGauge(final String tableName, final G gauge, final long value) {
    String fullGaugeName = composeTableGaugeName(tableName, gauge);
    setOrUpdateGauge(fullGaugeName, value);
  }

  /// Sets or updates a gauge whose values are retrieved from the given supplier function.
  /// The supplier function can be updated by calling this method again.
  ///
  /// @param tableName The table name
  /// @param gauge The gauge to use
  /// @param valueSupplier The supplier function used to retrieve the value of the gauge
  public void setOrUpdateTableGauge(final String tableName, final G gauge,
      final Supplier<Long> valueSupplier) {
    String fullGaugeName = composeTableGaugeName(tableName, gauge);
    setOrUpdateGauge(fullGaugeName, valueSupplier);
  }

  /// Sets or updates a gauge to the given value.
  /// The value can be updated by calling this method again.
  ///
  /// @param metricName The name of the metric
  /// @param value The value of the gauge
  public void setOrUpdateGauge(final String metricName, long value) {
    PinotGauge<Long> pinotGauge = PinotMetricUtils.makeGauge(_metricsRegistry,
        PinotMetricUtils.makePinotMetricName(_clazz, _metricPrefix + metricName),
        PinotMetricUtils.makePinotGauge(avoid -> value));
    pinotGauge.setValue(value);
  }

  /// Sets or updates a gauge whose values are retrieved from the given supplier function.
  /// The supplier function can be updated by calling this method again.
  ///
  /// @param metricName The name of the metric
  /// @param valueSupplier The supplier function used to retrieve the value of the gauge
  public void setOrUpdateGauge(final String metricName, final Supplier<Long> valueSupplier) {
    PinotGauge<Long> pinotGauge = PinotMetricUtils.makeGauge(_metricsRegistry,
        PinotMetricUtils.makePinotMetricName(_clazz, _metricPrefix + metricName),
        PinotMetricUtils.makePinotGauge(avoid -> valueSupplier.get()));
    pinotGauge.setValueSupplier(valueSupplier);
  }

  /// Like [#setOrUpdateGauge(String, Supplier)]
  public void setOrUpdateGauge(final String metricName, final LongSupplier valueSupplier) {
    PinotGauge<Long> pinotGauge = PinotMetricUtils.makeGauge(_metricsRegistry,
        PinotMetricUtils.makePinotMetricName(_clazz, _metricPrefix + metricName),
        PinotMetricUtils.makePinotGauge(avoid -> valueSupplier.getAsLong()));
    pinotGauge.setValueSupplier((Supplier<Long>) () -> (Long) valueSupplier.getAsLong());
  }

  /// Like [#setOrUpdateGauge(String, Supplier)] but using a global gauge
  /// @throws IllegalArgumentException if the gauge is not global
  public void setOrUpdateGlobalGauge(final G gauge, final Supplier<Long> valueSupplier) {
    Preconditions.checkArgument(gauge.isGlobal(), "Only global gauges should be sent to this method");
    setOrUpdateGauge(gauge.getGaugeName(), valueSupplier);
  }

  /// Like [#setOrUpdateGauge(String, LongSupplier)] but using a global gauge
  /// @throws IllegalArgumentException if the gauge is not global
  public void setOrUpdateGlobalGauge(final G gauge, final LongSupplier valueSupplier) {
    Preconditions.checkArgument(gauge.isGlobal(), "Only global gauges should be sent to this method");
    setOrUpdateGauge(gauge.getGaugeName(), valueSupplier);
  }

  /// Removes a global gauge given the key and the gauge
  /// @param key the key associated with the gauge
  /// @param gauge the gauge to be removed
  public void removeGlobalGauge(final String key, final G gauge) {
    final String fullGaugeName = composeGlobalGaugeName(key, gauge);
    removeGauge(fullGaugeName);
  }

  /// Removes a table gauge given the table name and the gauge.
  /// The add/remove is expected to work correctly in case of being invoked across multiple threads.
  /// @param tableName table name
  /// @param gauge the gauge to be removed
  public void removeTableGauge(final String tableName, final G gauge) {
    final String fullGaugeName = composeTableGaugeName(tableName, gauge);
    removeGauge(fullGaugeName);
  }


  /// Removes a table gauge given the table name, the partition id and the gauge.
  /// The add/remove is expected to work correctly in case of being invoked across multiple threads.
  /// @param tableName table name
  /// @param partitionId The partition id
  /// @param gauge the gauge to be removed
  public void removePartitionGauge(final String tableName, final int partitionId, final G gauge) {
    final String fullGaugeName = composeTableGaugeName(tableName, String.valueOf(partitionId), gauge);
    removeGauge(fullGaugeName);
  }

  /// Removes a table gauge given the table name, the key and the gauge.
  /// The add/remove is expected to work correctly in case of being invoked across multiple threads.
  /// @param tableName table name
  /// @param key the key associated with the gauge
  /// @param gauge the gauge to be removed
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

  public String composePluginGaugeName(String pluginName, Gauge gauge) {
    return gauge.getGaugeName() + "." + pluginName;
  }

  /// Removes every series this instance registered for the given table.
  ///
  /// Unlike the targeted `removeTable*` methods, this does not rebuild names from the rules used to emit them -- it
  /// scans what is actually registered. That is the whole point. A series emitted with an extra key, or with a
  /// composite table name, embeds a segment no caller can rediscover from the table name alone, so a sweep built on
  /// reconstruction strands exactly those series and keeps stranding each new one that gets added.
  ///
  /// Matching is deliberately narrow:
  ///
  ///   - Only names under this instance's metric prefix are considered, so a table named after a component
  ///     (`broker`) cannot match the prefix itself.
  ///   - The table name must occupy whole `.`-delimited segments, never part of one -- `foo` does not match
  ///     `foobar`, and a database-qualified `db.tbl_OFFLINE` matches only as a unit.
  ///   - A sibling [AbstractMetrics] sharing this registry and prefix keeps its **gauges**. The ownership check
  ///     below -- re-deriving the key under this instance's class -- is exact only where the registry key carries
  ///     the owning class; yammer's does, dropwizard's discards it. What protects the case that actually matters,
  ///     on every implementation, is the vocabulary check above: a sibling's gauge name is absent from this
  ///     instance's [#getGauges()], so `<siblingGauge>.<table>` reads as meter-shaped and the table is not at
  ///     offset 0, so it cannot match. Gauges are the only kind with a re-registration gate ([#_gaugeValues]), so
  ///     dropping one from under its owner would silence it for the life of the process. A sibling's meter or
  ///     timer may be dropped early where the key cannot distinguish owners; that is harmless -- they carry no
  ///     gate and re-register on the next emission. Every instance should still run its own sweep, since that is
  ///     what clears its own [#_gaugeValues].
  ///
  /// A table folded into the shared `allTables` aggregate is safe without a special case: no registered name
  /// contains its name, so nothing matches. Passing `allTables` itself is rejected for the same reason it would be
  /// a disaster -- it would delete the aggregate for every table at once.
  ///
  /// Two residual false positives are accepted: a workload or remote-cluster name exactly equal to a table name
  /// sits in the same slot and would be swept. Both re-register on next use, so the cost is one counter reset.
  ///
  /// Two things this deliberately does **not** reach, both of which need their owner to clean up:
  ///
  ///   - Series a component registers outside any [AbstractMetrics] -- [ValidationMetrics] composes its own
  ///     `pinot.controller.<table>.<gauge>` names against its own class and keeps its own value map, so the
  ///     ownership check above skips it. Dropping its registry entries from here would strand that map and retire
  ///     those gauges for the life of the process.
  ///   - Names where the table is not followed by a path separator, such as the consumer client id form
  ///     `<gauge>.<table>-<topic>-<partition>`. Matching those would mean accepting any prefix match, which is
  ///     what makes `tbl` match `tbl_OFFLINE` and `db.tbl`.
  ///
  /// @param tableName the table to sweep, in whichever name form its emitters used (raw or with type)
  /// @return the number of series removed
  public int removeTableMetrics(String tableName) {
    return removeTableMetrics(List.of(tableName));
  }

  /// Like [#removeTableMetrics(String)], for several tables at once. Prefer this when sweeping a batch: the
  /// registry is scanned once per call, and yammer and dropwizard both materialise a fresh map on every
  /// `allMetrics()`.
  public int removeTableMetrics(Collection<String> tableNames) {
    Set<String> targets = tableNames.stream().filter(t -> !ALL_TABLES.equals(t)).collect(Collectors.toSet());
    if (targets.isEmpty()) {
      return 0;
    }
    Set<String> gaugeNames =
        Arrays.stream(getGauges()).map(Gauge::getGaugeName).collect(Collectors.toCollection(HashSet::new));
    int removed = 0;
    // Snapshot the keys before mutating: the compound registry hands back its live map.
    for (PinotMetricName registeredName : new ArrayList<>(_metricsRegistry.allMetrics().keySet())) {
      String name = registeredName.getName();
      if (!name.startsWith(_metricPrefix)
          || !matchesAnyTable(name.substring(_metricPrefix.length()), targets, gaugeNames)) {
        continue;
      }
      // Re-deriving the key under this class is the ownership test: an identically named series registered by a
      // sibling AbstractMetrics is a different key, so it compares unequal and is left for that instance to sweep.
      if (registeredName.equals(PinotMetricUtils.makePinotMetricName(_clazz, name))) {
        PinotMetricUtils.removeMetric(_metricsRegistry, registeredName);
        removed++;
      }
    }
    // The deprecated gauge paths gate re-registration on _gaugeValues, so an entry left here would stop a removed
    // gauge from ever coming back. Swept from this instance's own map rather than from what matched above, so it
    // stays correct even where the registry cannot tell two instances' series apart.
    synchronized (_gaugeValues) {
      _gaugeValues.keySet().removeIf(gaugeName -> matchesAnyTable(gaugeName, targets, gaugeNames));
    }
    return removed;
  }

  /// Whether the prefix-stripped metric name names one of the given tables.
  ///
  /// The table sits at exactly one offset, decided by the shape: gauges compose `<gauge>.<table>[.<key>]`, while
  /// meters, timers and query phases compose `<table>.<rest>`. Which one applies is settled by asking whether the
  /// leading segment is a known gauge name -- and that question is what keeps a bare `tbl_OFFLINE` from matching
  /// `db.tbl_OFFLINE`, a genuinely different table whose series must survive. A free search for the name anywhere
  /// in the string cannot tell those two apart.
  private static boolean matchesAnyTable(String name, Set<String> tableNames, Set<String> gaugeNames) {
    int firstDot = name.indexOf('.');
    int start = firstDot > 0 && gaugeNames.contains(name.substring(0, firstDot)) ? firstDot + 1 : 0;
    for (String tableName : tableNames) {
      int end = start + tableName.length();
      if (name.startsWith(tableName, start) && (end == name.length() || name.charAt(end) == '.')) {
        return true;
      }
    }
    return false;
  }

  /// Remove gauge from Pinot metrics.
  /// @param gaugeName gauge name
  public void removeGauge(final String gaugeName) {
    synchronized (_gaugeValues) {
      _gaugeValues.remove(gaugeName);
      removeGaugeFromMetricRegistry(gaugeName);
    }
  }

  public void removeTableMeter(final String tableName, final M meter) {
    PinotMetricUtils.removeMetric(_metricsRegistry,
        PinotMetricUtils.makePinotMetricName(_clazz, getTableFullMeterName(tableName, meter)));
  }

  /// Remove callback gauge.
  /// @param metricName metric name
  private void removeGaugeFromMetricRegistry(String metricName) {
    PinotMetricUtils
        .removeMetric(_metricsRegistry, PinotMetricUtils.makePinotMetricName(_clazz, _metricPrefix + metricName));
  }

  protected abstract QP[] getQueryPhases();

  protected abstract M[] getMeters();

  protected abstract G[] getGauges();

  protected String getTableName(String tableName) {
    return _isTableLevelMetricsEnabled || _allowedTables.contains(tableName) ? tableName : ALL_TABLES;
  }
}
