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
import javax.annotation.Nullable;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.NoopPinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Mode-aware metrics shim for the multi-stage engine.
///
/// All MSE engine call sites route through [#get()] instead of [ServerMetrics] directly. The
/// active [MseMetricsMode] controls where emissions land:
///
/// - [MseMetricsMode#SERVER] (default): forwarded to [ServerMetrics] so existing `pinot.server.*`
///   dashboards continue to work unchanged.
/// - [MseMetricsMode#MSE]: emitted to this instance's own `pinot.mse.*` registry.
/// - [MseMetricsMode#DUAL]: emitted to both, for dashboard migration windows.
///
/// The pre-registration [#NOOP] is in `SERVER` mode, so call sites that emit before any explicit
/// registration behave the same as if they had called [ServerMetrics] directly. Because SERVER
/// mode resolves the underlying handle through [ServerMetrics#get()], emissions land in the noop
/// registry whenever [ServerMetrics#register] has not been called on the local JVM; in that case
/// the `MSE` or `DUAL` mode must be selected for the series to surface.
///
/// MSE-native metrics — [MseMeter] and [MseTimer] entries with no `ServerMeter` / `ServerTimer`
/// counterpart — are emitted only under `MSE` and `DUAL` modes. In `SERVER` mode they are
/// silently dropped (the legacy namespace has no series to forward to).
///
/// **Initialization order:** call [#registerFromConfig] before constructing components that
/// resolve a [PinotMeter] handle once and cache it for the JVM lifetime (notably the MSE
/// `MetricsExecutor` cached handles and the inner `Metrics` class in `OpChainSchedulerService`).
/// The server and broker starters preserve this ordering by registering immediately after
/// `ServerMetrics.register` / `BrokerMetrics.register` and before any MSE runtime component is
/// built.
public class MseMetrics
    extends AbstractMetrics<AbstractMetrics.QueryPhase, MseMeter, AbstractMetrics.Gauge, MseTimer> {

  public static final String METRIC_PREFIX = "pinot.mse.";

  private static final Logger LOGGER = LoggerFactory.getLogger(MseMetrics.class);

  private static final AbstractMetrics.QueryPhase[] EMPTY_PHASES = new AbstractMetrics.QueryPhase[0];
  private static final AbstractMetrics.Gauge[] EMPTY_GAUGES = new AbstractMetrics.Gauge[0];

  private static final PinotMeter NOOP_PINOT_METER = new NoopPinotMetricsRegistry().newMeter(null, null, null);

  private static final MseMetrics NOOP = new MseMetrics(MseMetricsMode.SERVER, new NoopPinotMetricsRegistry());
  private static final AtomicReference<MseMetrics> INSTANCE = new AtomicReference<>(NOOP);

  /// Registers `mseMetrics` as the JVM-wide instance. Returns `true` if installed; `false` if
  /// another instance was already registered (compare-and-set semantics, matching
  /// [ServerMetrics#register]).
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

  /// Reads [Helix#CONFIG_OF_MSE_METRICS_MODE] from `instanceConfig` (which already contains
  /// cluster-config keys merged in via `ServiceStartableUtils.applyClusterConfig(...)` at
  /// startup) and registers a new [MseMetrics] instance with the resolved mode.
  /// [MseMetricsMode#SERVER] reuses [NoopPinotMetricsRegistry] since no `pinot.mse.*` series are
  /// emitted in that mode. Subsequent calls in the same JVM are no-ops (compare-and-set with the
  /// NOOP placeholder).
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

  /// DUAL-mode wrappers are cached so callers asking for the same [MseMeter] repeatedly (e.g.
  /// the MSE engine emission loops in `MultiStageOperator`) share one wrapper instead of
  /// allocating a fresh one per increment. `null` for non-DUAL modes.
  @Nullable
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
  protected AbstractMetrics.QueryPhase[] getQueryPhases() {
    return EMPTY_PHASES;
  }

  @Override
  protected MseMeter[] getMeters() {
    return MseMeter.values();
  }

  @Override
  protected AbstractMetrics.Gauge[] getGauges() {
    return EMPTY_GAUGES;
  }

  /// Single source of truth for the mode logic; the 2-arg [#addMeteredGlobalValue] and
  /// [#getMeteredValue] delegate here. Reuses `reusedMeter` as a fast path so callers that cache
  /// a handle (e.g. `MetricsExecutor`) avoid repeated registry lookups.
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
    ServerMeter serverMeter = meter.getServerMeter();
    switch (_mode) {
      case MSE:
        return super.getMeteredValue(meter);
      case DUAL:
        if (serverMeter == null) {
          return super.getMeteredValue(meter);
        }
        // computeIfAbsent rather than locking — EnumMap supports concurrent reads of distinct
        // keys, and a racing put of the same key just dedupes to one entry on the next read.
        // The DualPinotMeter itself fan-outs to two underlying handles which are themselves
        // dedup'd by their registries, so a duplicate wrapper is harmless if it does happen.
        return _dualMeterCache.computeIfAbsent(meter,
            m -> new DualPinotMeter(super.getMeteredValue(m), ServerMetrics.get().getMeteredValue(serverMeter)));
      case SERVER:
      default:
        return serverMeter == null ? NOOP_PINOT_METER : ServerMetrics.get().getMeteredValue(serverMeter);
    }
  }

  @Override
  public void addTimedValue(MseTimer timer, long duration, TimeUnit timeUnit) {
    ServerTimer serverTimer = timer.getServerTimer();
    if (_mode != MseMetricsMode.MSE && serverTimer != null) {
      ServerMetrics.get().addTimedValue(serverTimer, duration, timeUnit);
    }
    if (_mode != MseMetricsMode.SERVER) {
      super.addTimedValue(timer, duration, timeUnit);
    }
  }

  /// Fan-out [PinotMeter] returned in [MseMetricsMode#DUAL] so callers that cache a handle mark
  /// both registries on every increment. Read-side methods delegate to the primary (MSE) handle.
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
