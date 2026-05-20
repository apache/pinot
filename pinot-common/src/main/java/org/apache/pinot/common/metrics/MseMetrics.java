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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.NoopPinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mode-aware metrics shim for the multi-stage engine.
 *
 * <p>All MSE engine call sites route through {@link #get()} instead of {@link ServerMetrics}
 * directly. The active {@link MseMetricsMode} controls where emissions land:
 * <ul>
 *   <li>{@link MseMetricsMode#SERVER} (default): forwarded to {@link ServerMetrics} so existing
 *       {@code pinot.server.*} dashboards continue to work unchanged.</li>
 *   <li>{@link MseMetricsMode#MSE}: emitted to this instance's own {@code pinot.mse.*} registry.</li>
 *   <li>{@link MseMetricsMode#DUAL}: emitted to both, for dashboard migration windows.</li>
 * </ul>
 *
 * <p>The pre-registration {@link #NOOP} is in {@code SERVER} mode, so call sites that emit before
 * any explicit registration behave the same as if they had called {@link ServerMetrics} directly.
 * Because SERVER mode resolves the underlying handle through {@link ServerMetrics#get()},
 * emissions land in the noop registry whenever {@link ServerMetrics#register} has not been called
 * on the local JVM; in that case the {@code MSE} or {@code DUAL} mode must be selected for the
 * series to surface.
 *
 * <p><b>Initialization order:</b> call {@link #registerFromConfig} before constructing components
 * that resolve a {@link PinotMeter} handle once and cache it for the JVM lifetime (notably the MSE
 * {@code MetricsExecutor} cached handles and the inner {@code Metrics} class in
 * {@code OpChainSchedulerService}). The server and broker starters preserve this ordering by
 * registering immediately after {@code ServerMetrics.register} / {@code BrokerMetrics.register}
 * and before any MSE runtime component is built.
 */
public class MseMetrics extends AbstractMetrics<MseQueryPhase, MseMeter, MseGauge, MseTimer> {

  public static final String METRIC_PREFIX = "pinot.mse.";

  private static final Logger LOGGER = LoggerFactory.getLogger(MseMetrics.class);

  private static final MseMetrics NOOP = new MseMetrics(MseMetricsMode.SERVER, new NoopPinotMetricsRegistry());
  private static final AtomicReference<MseMetrics> INSTANCE = new AtomicReference<>(NOOP);

  /**
   * Register {@code mseMetrics} as the JVM-wide instance. Returns {@code true} if installed;
   * {@code false} if another instance was already registered (compare-and-set semantics, matching
   * {@link ServerMetrics#register}).
   */
  public static boolean register(MseMetrics mseMetrics) {
    return INSTANCE.compareAndSet(NOOP, mseMetrics);
  }

  @VisibleForTesting
  public static void deregister() {
    INSTANCE.set(NOOP);
  }

  public static MseMetrics get() {
    return INSTANCE.get();
  }

  /**
   * Reads {@link Helix#CONFIG_OF_MSE_METRICS_MODE} from {@code instanceConfig} (which already
   * contains cluster-config keys merged in via
   * {@code ServiceStartableUtils.applyClusterConfig(...)} at startup) and registers a new
   * {@link MseMetrics} instance with the resolved mode. {@link MseMetricsMode#SERVER} reuses
   * {@link NoopPinotMetricsRegistry} since no {@code pinot.mse.*} series are emitted in that mode.
   * Subsequent calls in the same JVM are no-ops (compare-and-set with the NOOP placeholder).
   */
  public static void registerFromConfig(PinotConfiguration instanceConfig, PinotMetricsRegistry metricsRegistry) {
    String modeStr = instanceConfig.getProperty(Helix.CONFIG_OF_MSE_METRICS_MODE, Helix.DEFAULT_MSE_METRICS_MODE);
    MseMetricsMode mode;
    try {
      mode = MseMetricsMode.valueOf(modeStr.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Invalid value {}={}, falling back to {}", Helix.CONFIG_OF_MSE_METRICS_MODE, modeStr,
          Helix.DEFAULT_MSE_METRICS_MODE);
      mode = MseMetricsMode.valueOf(Helix.DEFAULT_MSE_METRICS_MODE);
    }
    PinotMetricsRegistry effectiveRegistry =
        mode == MseMetricsMode.SERVER ? new NoopPinotMetricsRegistry() : metricsRegistry;
    if (register(new MseMetrics(mode, effectiveRegistry))) {
      LOGGER.info("Registered MseMetrics in {} mode", mode);
    } else {
      LOGGER.info("MseMetrics already registered ({} mode); ignoring duplicate registration with {} mode",
          get().getMode(), mode);
    }
  }

  private final MseMetricsMode _mode;

  /// DUAL-mode wrappers are cached so callers asking for the same {@link MseMeter} repeatedly
  /// (e.g. the MSE engine emission loops in {@code MultiStageOperator}) share one wrapper
  /// instead of allocating a fresh one per increment. {@code null} for non-DUAL modes.
  private final EnumMap<MseMeter, PinotMeter> _dualMeterCache;

  public MseMetrics(MseMetricsMode mode, PinotMetricsRegistry metricsRegistry) {
    super(METRIC_PREFIX, metricsRegistry, MseMetrics.class, false, Collections.emptySet());
    _mode = mode;
    _dualMeterCache = mode == MseMetricsMode.DUAL ? new EnumMap<>(MseMeter.class) : null;
  }

  public MseMetricsMode getMode() {
    return _mode;
  }

  @Override
  protected MseQueryPhase[] getQueryPhases() {
    return MseQueryPhase.values();
  }

  @Override
  protected MseMeter[] getMeters() {
    return MseMeter.values();
  }

  @Override
  protected MseGauge[] getGauges() {
    return MseGauge.values();
  }

  /// Single source of truth for the mode logic; the 2-arg {@code addMeteredGlobalValue} and
  /// {@link #getMeteredValue} delegate here. Reuses {@code reusedMeter} as a fast path so callers
  /// that cache a handle (e.g. {@code MetricsExecutor}) avoid repeated registry lookups.
  @Override
  public PinotMeter addMeteredGlobalValue(MseMeter meter, long unitCount, PinotMeter reusedMeter) {
    if (reusedMeter != null) {
      reusedMeter.mark(unitCount);
      return reusedMeter;
    }
    PinotMeter handle = getMeteredValue(meter);
    handle.mark(unitCount);
    return handle;
  }

  @Override
  public PinotMeter getMeteredValue(MseMeter meter) {
    switch (_mode) {
      case MSE:
        return super.getMeteredValue(meter);
      case DUAL:
        // computeIfAbsent rather than locking — EnumMap supports concurrent reads of distinct
        // keys, and a racing put of the same key just dedupes to one entry on the next read.
        // The DualPinotMeter itself fan-outs to two underlying handles which are themselves
        // dedup'd by their registries, so a duplicate wrapper is harmless if it does happen.
        return _dualMeterCache.computeIfAbsent(meter,
            m -> new DualPinotMeter(super.getMeteredValue(m),
                ServerMetrics.get().getMeteredValue(m.getServerMeter())));
      case SERVER:
      default:
        return ServerMetrics.get().getMeteredValue(meter.getServerMeter());
    }
  }

  @Override
  public void addTimedValue(MseTimer timer, long duration, TimeUnit timeUnit) {
    if (_mode != MseMetricsMode.MSE) {
      ServerMetrics.get().addTimedValue(timer.getServerTimer(), duration, timeUnit);
    }
    if (_mode != MseMetricsMode.SERVER) {
      super.addTimedValue(timer, duration, timeUnit);
    }
  }

  /**
   * Fan-out {@link PinotMeter} returned in {@link MseMetricsMode#DUAL} so callers that cache a
   * handle mark both registries on every increment. Read-side methods delegate to the primary
   * (MSE) handle.
   */
  private static final class DualPinotMeter implements PinotMeter {
    private final PinotMeter _primary;
    private final PinotMeter _secondary;

    private DualPinotMeter(PinotMeter primary, PinotMeter secondary) {
      _primary = primary;
      _secondary = secondary;
    }

    @Override
    public void mark() {
      _primary.mark();
      _secondary.mark();
    }

    @Override
    public void mark(long unitCount) {
      _primary.mark(unitCount);
      _secondary.mark(unitCount);
    }

    @Override
    public long count() {
      return _primary.count();
    }

    @Override
    public Object getMetered() {
      return _primary.getMetered();
    }

    @Override
    public TimeUnit rateUnit() {
      return _primary.rateUnit();
    }

    @Override
    public String eventType() {
      return _primary.eventType();
    }

    @Override
    public double fifteenMinuteRate() {
      return _primary.fifteenMinuteRate();
    }

    @Override
    public double fiveMinuteRate() {
      return _primary.fiveMinuteRate();
    }

    @Override
    public double meanRate() {
      return _primary.meanRate();
    }

    @Override
    public double oneMinuteRate() {
      return _primary.oneMinuteRate();
    }

    @Override
    public Object getMetric() {
      return _primary.getMetric();
    }
  }
}
