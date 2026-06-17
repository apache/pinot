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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.annotations.VisibleForTesting;
import java.util.Locale;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.ResourceUsageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Server-local guard that pauses realtime ingestion when the JVM is under memory pressure, to protect the server from
/// OOM. The dominant driver this targets is the on-heap upsert/dedup primary-key metadata, which grows with ingestion
/// and is not freed by committing a consuming segment.
///
/// A single daemon thread samples the JVM heap every `checkIntervalMs` and maintains a `_heapPressure` flag with
/// hysteresis: it flips to `true` once `usedHeap / maxHeap >= pauseRatio` and back to `false` once
/// `usedHeap / maxHeap <= resumeRatio`. Realtime consumers consult [#shouldPauseConsumption] inside their consume loop
/// and park (stop fetching from the stream) while it returns `true`, so memory stops growing; they resume
/// automatically when heap recovers. This is server-local and self-healing — it does not involve the controller or
/// ZooKeeper.
///
/// SCOPE / RESIDUAL EXPOSURE: the pause is applied only while a segment is in its open-ended `INITIAL_CONSUMING`
/// phase. Segments catching up to a target offset (`CATCHING_UP` / `CONSUMING_TO_ONLINE`, e.g. right after a server
/// restart or during lag recovery) are intentionally NOT paused, because stalling them would wedge the Helix
/// CONSUMING -> ONLINE state transition (`CATCHING_UP` has no time bound). During catch-up the on-heap upsert/dedup
/// metadata can still grow, so this guard dampens and delays OOM rather than guaranteeing prevention; it buys time for
/// GC, segment commits, and TTL eviction. See [RealtimeSegmentDataManager#consumeLoop].
///
/// The [Mode] controls which tables are guarded ([Mode#ALL], [Mode#UPSERT_DEDUP_ONLY], or [Mode#DISABLED]). To avoid
/// wedging ingestion if the sampler thread ever dies, the trigger fails open: if the last sample is older than a
/// staleness threshold, [#shouldPauseConsumption] ignores the (possibly stuck) `_heapPressure` flag.
///
/// This class is a process-wide singleton (mirroring [RealtimeConsumptionRateManager]); it is initialized once from
/// server config at startup via [#init] and read directly by [RealtimeSegmentDataManager]. It is thread-safe.
public class RealtimeIngestionMemoryGuard {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeIngestionMemoryGuard.class);

  /// Which realtime tables the guard applies to.
  public enum Mode {
    /// Pause consumption for any realtime table under memory pressure.
    ALL,
    /// Pause consumption only for upsert/dedup tables (whose on-heap metadata is the main OOM driver).
    UPSERT_DEDUP_ONLY,
    /// Guard is turned off; consumption is never paused.
    DISABLED
  }

  private static final RealtimeIngestionMemoryGuard INSTANCE = new RealtimeIngestionMemoryGuard();

  public static RealtimeIngestionMemoryGuard getInstance() {
    return INSTANCE;
  }

  private final LongSupplier _usedHeapSupplier;
  private final LongSupplier _maxHeapSupplier;
  private final LongSupplier _clockMs;

  // Configuration, set on init() and treated as effectively immutable afterwards.
  private volatile Mode _mode = Mode.DISABLED;
  private volatile double _pauseRatio = Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_PAUSE_HEAP_RATIO;
  private volatile double _resumeRatio = Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_RESUME_HEAP_RATIO;
  private volatile long _checkIntervalMs = Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_CHECK_INTERVAL_MS;
  private volatile long _staleThresholdMs = 5 * Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_CHECK_INTERVAL_MS;

  // Runtime state.
  private volatile boolean _heapPressure = false;
  private volatile long _lastSampleTimeMs = 0;
  @Nullable
  private volatile ServerMetrics _serverMetrics;
  private boolean _initialized = false;

  private RealtimeIngestionMemoryGuard() {
    this(ResourceUsageUtils::getUsedHeapSize, ResourceUsageUtils::getMaxHeapSize, System::currentTimeMillis);
  }

  @VisibleForTesting
  RealtimeIngestionMemoryGuard(LongSupplier usedHeapSupplier, LongSupplier maxHeapSupplier, LongSupplier clockMs) {
    _usedHeapSupplier = usedHeapSupplier;
    _maxHeapSupplier = maxHeapSupplier;
    _clockMs = clockMs;
  }

  /// Initializes the guard from server config and, unless the mode is [Mode#DISABLED], starts the heap sampler thread.
  /// Safe to call once at server startup; subsequent calls are ignored (the guard is configured once per process).
  public synchronized void init(PinotConfiguration serverConfig, ServerMetrics serverMetrics) {
    if (_initialized) {
      LOGGER.warn("RealtimeIngestionMemoryGuard already initialized, ignoring re-init");
      return;
    }
    _initialized = true;
    _serverMetrics = serverMetrics;
    applyConfig(serverConfig);

    if (_mode == Mode.DISABLED) {
      LOGGER.info("RealtimeIngestionMemoryGuard is disabled");
      return;
    }
    LOGGER.info("Starting RealtimeIngestionMemoryGuard: mode={}, pauseHeapRatio={}, resumeHeapRatio={}, "
            + "checkIntervalMs={}, maxHeapBytes={}", _mode, _pauseRatio, _resumeRatio, _checkIntervalMs,
        _maxHeapSupplier.getAsLong());
    // Daemon sampler: dies on JVM exit, so no explicit shutdown is needed (mirrors RealtimeConsumptionRateManager).
    Thread samplerThread = new Thread(this::samplerLoop, "RealtimeIngestionMemoryGuard");
    samplerThread.setDaemon(true);
    samplerThread.start();
  }

  /// Parses and validates the guard configuration into the in-memory fields, without starting the sampler thread.
  /// Visible for testing so the decision logic can be configured deterministically.
  @VisibleForTesting
  void applyConfig(PinotConfiguration serverConfig) {
    _mode = parseMode(serverConfig.getProperty(Server.CONFIG_OF_SERVER_CONSUMPTION_MEMORY_PAUSE_MODE,
        Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_PAUSE_MODE));

    double pauseRatio = serverConfig.getProperty(Server.CONFIG_OF_SERVER_CONSUMPTION_MEMORY_PAUSE_HEAP_RATIO,
        Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_PAUSE_HEAP_RATIO);
    double resumeRatio = serverConfig.getProperty(Server.CONFIG_OF_SERVER_CONSUMPTION_MEMORY_RESUME_HEAP_RATIO,
        Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_RESUME_HEAP_RATIO);
    // Require 0 < resumeRatio < pauseRatio <= 1.0 so the hysteresis band is well-formed; otherwise reject both and
    // fall back to the defaults rather than silently consuming bad thresholds.
    if (!(pauseRatio > 0.0 && pauseRatio <= 1.0 && resumeRatio > 0.0 && resumeRatio < pauseRatio)) {
      LOGGER.warn("Invalid heap usage ratios (pause={}, resume={}); falling back to defaults (pause={}, resume={})",
          pauseRatio, resumeRatio, Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_PAUSE_HEAP_RATIO,
          Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_RESUME_HEAP_RATIO);
      pauseRatio = Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_PAUSE_HEAP_RATIO;
      resumeRatio = Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_RESUME_HEAP_RATIO;
    }
    _pauseRatio = pauseRatio;
    _resumeRatio = resumeRatio;

    long checkIntervalMs = serverConfig.getProperty(Server.CONFIG_OF_SERVER_CONSUMPTION_MEMORY_CHECK_INTERVAL_MS,
        Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_CHECK_INTERVAL_MS);
    if (checkIntervalMs <= 0) {
      LOGGER.warn("Invalid check interval {}ms; falling back to default {}ms", checkIntervalMs,
          Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_CHECK_INTERVAL_MS);
      checkIntervalMs = Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_CHECK_INTERVAL_MS;
    }
    _checkIntervalMs = checkIntervalMs;
    // Treat the heap-pressure flag as stale (and so fail open) if the sampler has not refreshed it for several check
    // intervals, which indicates the sampler thread has died.
    _staleThresholdMs = 5 * checkIntervalMs;
  }

  private static Mode parseMode(String modeStr) {
    try {
      return Mode.valueOf(modeStr.trim().toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Invalid {} value '{}'; falling back to default '{}'",
          Server.CONFIG_OF_SERVER_CONSUMPTION_MEMORY_PAUSE_MODE, modeStr,
          Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_PAUSE_MODE);
      return Mode.valueOf(Server.DEFAULT_SERVER_CONSUMPTION_MEMORY_PAUSE_MODE);
    }
  }

  private void samplerLoop() {
    while (true) {
      try {
        sampleOnce();
      } catch (Throwable t) {
        LOGGER.warn("Error while sampling heap usage in RealtimeIngestionMemoryGuard", t);
      }
      try {
        Thread.sleep(_checkIntervalMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /// Samples heap usage once and updates the [#_heapPressure] flag using hysteresis. Visible for testing so the
  /// decision logic can be driven deterministically without the sampler thread.
  @VisibleForTesting
  void sampleOnce() {
    long maxHeap = _maxHeapSupplier.getAsLong();
    long usedHeap = _usedHeapSupplier.getAsLong();
    if (maxHeap <= 0) {
      // Cannot compute a ratio without a heap max; leave the pressure state and heartbeat untouched so that, if this
      // persists, the staleness check in shouldPauseConsumption() fails open rather than honoring a stuck flag.
      return;
    }
    // Refresh the heartbeat only after a usable sample so a live-but-blind sampler cannot keep a stuck flag "fresh".
    _lastSampleTimeMs = _clockMs.getAsLong();
    double ratio = (double) usedHeap / maxHeap;
    boolean previous = _heapPressure;
    boolean next = previous;
    if (!previous && ratio >= _pauseRatio) {
      next = true;
    } else if (previous && ratio <= _resumeRatio) {
      next = false;
    }
    if (next != previous) {
      _heapPressure = next;
      ServerMetrics serverMetrics = _serverMetrics;
      if (serverMetrics != null) {
        serverMetrics.setValueOfGlobalGauge(ServerGauge.REALTIME_INGESTION_MEMORY_PAUSED, next ? 1L : 0L);
      }
      if (next) {
        LOGGER.warn("Pausing realtime ingestion due to heap pressure: usedHeap={} bytes, maxHeap={} bytes, "
            + "ratio={} >= pauseRatio={}", usedHeap, maxHeap, ratio, _pauseRatio);
      } else {
        LOGGER.info("Resuming realtime ingestion: usedHeap={} bytes, maxHeap={} bytes, ratio={} <= resumeRatio={}",
            usedHeap, maxHeap, ratio, _resumeRatio);
      }
    }
  }

  /// Returns `true` if the consumer for a partition with the given (nullable) upsert/dedup metadata managers should
  /// currently pause consumption. The caller passes its own metadata managers (either may be `null` for non-upsert,
  /// non-dedup tables); the guard derives table eligibility from them.
  ///
  /// This is called from the consume loop and must be cheap: it reads volatile flags only.
  public boolean shouldPauseConsumption(@Nullable PartitionUpsertMetadataManager upsertMetadataManager,
      @Nullable PartitionDedupMetadataManager dedupMetadataManager) {
    Mode mode = _mode;
    if (mode == Mode.DISABLED) {
      return false;
    }
    if (mode == Mode.UPSERT_DEDUP_ONLY && upsertMetadataManager == null && dedupMetadataManager == null) {
      return false;
    }
    // Fail open if the sampler heartbeat is stale (sampler thread presumed dead) so a stuck flag can never wedge
    // ingestion.
    return _heapPressure && (_clockMs.getAsLong() - _lastSampleTimeMs) <= _staleThresholdMs;
  }

  /// Returns the configured sampling / pause re-check interval in milliseconds. Used by a parked consumer as its sleep
  /// quantum between pause re-checks.
  long getCheckIntervalMs() {
    return _checkIntervalMs;
  }

  @VisibleForTesting
  Mode getMode() {
    return _mode;
  }

  @VisibleForTesting
  boolean isHeapPressure() {
    return _heapPressure;
  }
}
