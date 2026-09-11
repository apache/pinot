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
package org.apache.pinot.broker.broker;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Coordinates broker query drain.
///
/// Drain has three effects:
///
/// 1. New query admissions are rejected locally.
/// 2. The broker marks its Helix instance config with `shutdownInProgress=true`
///    and removes itself from `brokerResource`, so clients that discover brokers
///    through Helix stop selecting it.
/// 3. A caller can wait for in-flight queries to finish and optionally trigger
///    broker shutdown.
///
/// The in-flight count tracks accepted broker query requests, not engine-internal
/// subqueries. That makes the wait semantics independent of single-stage,
/// multi-stage, and time-series query execution details.
///
/// Query admission and release use a single atomic state containing the drain gate
/// and in-flight count. Drain initiation atomically closes the gate, so queries
/// admitted before that transition are counted and queries arriving after it are
/// rejected without contending on a global monitor.
public class BrokerDrainManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerDrainManager.class);
  private static final long DRAINING_MASK = Long.MIN_VALUE;
  private static final long IN_FLIGHT_QUERY_MASK = Long.MAX_VALUE;

  private final String _instanceId;
  private final Supplier<HelixManager> _helixManagerSupplier;
  private final Runnable _drainStartedCallback;
  private final Runnable _shutdownCallback;
  private final long _defaultDrainTimeoutMs;
  private final boolean _enableHelixUpdates;
  private final boolean _drainSupported;

  private final Object _lifecycleLock = new Object();
  private final Object _drainWaitLock = new Object();
  private final AtomicLong _queryState = new AtomicLong();
  private final AtomicBoolean _shutdownTriggered = new AtomicBoolean(false);
  private volatile long _drainStartTimeMs = -1L;
  private volatile List<String> _tablesRemovedFromBrokerResource = List.of();
  private boolean _startupReady;
  private volatile boolean _shutdownMarkerReconciled;
  private volatile boolean _brokerResourceReconciled;

  public BrokerDrainManager(String instanceId, Supplier<HelixManager> helixManagerSupplier,
      Runnable drainStartedCallback, Runnable shutdownCallback, long defaultDrainTimeoutMs) {
    this(instanceId, helixManagerSupplier, drainStartedCallback, shutdownCallback, defaultDrainTimeoutMs, true, false,
        true);
  }

  private BrokerDrainManager(String instanceId, Supplier<HelixManager> helixManagerSupplier,
      Runnable drainStartedCallback, Runnable shutdownCallback, long defaultDrainTimeoutMs,
      boolean enableHelixUpdates, boolean startupReady, boolean drainSupported) {
    _instanceId = instanceId;
    _helixManagerSupplier = helixManagerSupplier;
    _drainStartedCallback = drainStartedCallback;
    _shutdownCallback = shutdownCallback;
    _defaultDrainTimeoutMs = defaultDrainTimeoutMs;
    _enableHelixUpdates = enableHelixUpdates;
    _drainSupported = drainSupported;
    _startupReady = startupReady;
    _shutdownMarkerReconciled = !enableHelixUpdates;
    _brokerResourceReconciled = !enableHelixUpdates;
  }

  public static BrokerDrainManager noop(String instanceId) {
    return new BrokerDrainManager(instanceId, () -> null, () -> {
    }, () -> {
    }, 0L, false, true, true);
  }

  /// Returns a manager that keeps query admission enabled but explicitly rejects drain operations.
  public static BrokerDrainManager unsupported(String instanceId) {
    return new BrokerDrainManager(instanceId, () -> null, () -> {
    }, () -> {
    }, 0L, false, true, false);
  }

  @VisibleForTesting
  public static BrokerDrainManager localOnly(String instanceId, Runnable drainStartedCallback,
      Runnable shutdownCallback, long defaultDrainTimeoutMs) {
    return new BrokerDrainManager(instanceId, () -> null, drainStartedCallback, shutdownCallback, defaultDrainTimeoutMs,
        false, true, true);
  }

  /// Opens the production drain gate after startup reconciliation and service-status setup have completed.
  public void markStartupReady() {
    synchronized (_lifecycleLock) {
      _startupReady = true;
    }
  }

  @Nullable
  public QueryPermit tryAcquireQuery() {
    while (true) {
      long queryState = _queryState.get();
      if (isDraining(queryState)) {
        return null;
      }
      if (getInFlightQueries(queryState) == Integer.MAX_VALUE) {
        throw new IllegalStateException("Too many in-flight queries on broker " + _instanceId);
      }
      if (_queryState.compareAndSet(queryState, queryState + 1)) {
        return new QueryPermit(this);
      }
    }
  }

  public DrainStatus drain(long timeoutMs, boolean shutdown)
      throws InterruptedException {
    startDrain();
    boolean drained = awaitNoInflightQueries(resolveTimeoutMs(timeoutMs));
    if (drained && shutdown) {
      triggerShutdown();
    }
    return getStatus(drained ? "Broker drained" : "Timed out waiting for in-flight queries to finish");
  }

  public DrainStatus getStatus() {
    return getStatus(isDraining() ? "Broker is draining" : "Broker is accepting queries");
  }

  public boolean isDraining() {
    return isDraining(_queryState.get());
  }

  public boolean isDrainComplete() {
    long queryState = _queryState.get();
    return isDraining(queryState) && getInFlightQueries(queryState) == 0 && isCoordinationReconciled();
  }

  public String getRejectMessage() {
    return "Broker " + _instanceId + " is draining and not accepting new queries";
  }

  private void startDrain() {
    synchronized (_lifecycleLock) {
      if (!_drainSupported) {
        throw new BrokerDrainUnsupportedException();
      }
      if (!_startupReady) {
        throw new BrokerStartupInProgressException(_instanceId);
      }
      HelixManager helixManager = null;
      if (_enableHelixUpdates && (!_shutdownMarkerReconciled || !_brokerResourceReconciled)) {
        helixManager = getConnectedHelixManager();
        if (!_shutdownMarkerReconciled) {
          markShutdownInProgress(helixManager);
          _shutdownMarkerReconciled = true;
        }
      }
      if (_enableHelixUpdates && !_brokerResourceReconciled) {
        List<String> tablesRemovedFromBrokerResource = removeBrokerFromBrokerResource(helixManager);
        _tablesRemovedFromBrokerResource = tablesRemovedFromBrokerResource;
        _brokerResourceReconciled = true;
      }
      // Keep accepting queries unless every required distributed write is confirmed. If an ambiguous write actually
      // committed, a later drain call safely reconciles it again before closing the local admission gate.
      if (!isDraining()) {
        activateLocalDrain();
      }
    }
  }

  private void activateLocalDrain() {
    _drainStartTimeMs = System.currentTimeMillis();
    long queryState;
    do {
      queryState = _queryState.get();
      if (isDraining(queryState)) {
        return;
      }
    } while (!_queryState.compareAndSet(queryState, queryState | DRAINING_MASK));
    if (getInFlightQueries(queryState) == 0) {
      synchronized (_drainWaitLock) {
        _drainWaitLock.notifyAll();
      }
    }
    try {
      _drainStartedCallback.run();
    } catch (RuntimeException e) {
      // Helix and local admission state are already committed, so rolling back here would be unsafe. Continue the
      // drain and let the caller observe the committed state instead of stranding it behind a failed callback.
      LOGGER.error("Caught exception from drain-start callback for broker {}", _instanceId, e);
    }
  }

  private HelixManager getConnectedHelixManager() {
    HelixManager helixManager = _helixManagerSupplier.get();
    if (helixManager == null || !helixManager.isConnected()) {
      throw new IllegalStateException("Broker participant Helix manager is not connected");
    }
    return helixManager;
  }

  private void markShutdownInProgress(HelixManager helixManager) {
    boolean updated = HelixHelper.updateInstanceConfig(helixManager.getHelixDataAccessor(), _instanceId,
        instanceConfig -> {
          List<String> previousTags =
              instanceConfig.getRecord().getListField(CommonConstants.Helix.PREVIOUS_TAGS);
          if (previousTags == null) {
            instanceConfig.getRecord()
                .setListField(CommonConstants.Helix.PREVIOUS_TAGS, new ArrayList<>(instanceConfig.getTags()));
          }
          List<String> activeTags = new ArrayList<>(instanceConfig.getTags());
          activeTags.removeIf(TagNameUtils::isBrokerTag);
          instanceConfig.getRecord()
              .setListField(InstanceConfig.InstanceConfigProperty.TAG_LIST.name(), activeTags);
          instanceConfig.getRecord()
              .setSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, Boolean.TRUE.toString());
        });
    if (!updated) {
      throw new IllegalStateException("Failed to mark broker instance " + _instanceId + " as shutting down");
    }
    LOGGER.info("Marked broker instance {} with {}=true and temporarily removed its broker tags", _instanceId,
        CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS);
  }

  private List<String> removeBrokerFromBrokerResource(HelixManager helixManager) {
    List<String> tablesRemoved = new ArrayList<>();
    if (HelixHelper.updateIdealState(helixManager, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, idealState -> {
      tablesRemoved.clear();
      for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
        if (entry.getValue().remove(_instanceId) != null) {
          tablesRemoved.add(entry.getKey());
        }
      }
      return idealState;
    }) == null) {
      // IdealStateGroupCommit returns null when this waiter is interrupted. The write outcome is ambiguous, so keep
      // local admission open and require a later drain call to reconcile the BrokerResource idempotently.
      throw new IllegalStateException("Interrupted while removing broker instance " + _instanceId
          + " from brokerResource");
    }
    LOGGER.info("Removed broker instance {} from brokerResource for {} table(s): {}", _instanceId,
        tablesRemoved.size(), tablesRemoved);
    return List.copyOf(tablesRemoved);
  }

  private boolean awaitNoInflightQueries(long timeoutMs)
      throws InterruptedException {
    long timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    long startTimeNs = System.nanoTime();
    synchronized (_drainWaitLock) {
      while (getInFlightQueries(_queryState.get()) > 0) {
        long elapsedNs = System.nanoTime() - startTimeNs;
        if (elapsedNs >= timeoutNs) {
          return false;
        }
        TimeUnit.NANOSECONDS.timedWait(_drainWaitLock, timeoutNs - elapsedNs);
      }
      return true;
    }
  }

  private long resolveTimeoutMs(long timeoutMs) {
    if (timeoutMs >= 0) {
      return timeoutMs;
    }
    return _defaultDrainTimeoutMs;
  }

  /// Triggers shutdown after a completed drain. HTTP callers should invoke this from their response completion
  /// callback so the admin server is not stopped before the success response has been written.
  public void triggerShutdown() {
    if (!isDrainComplete()) {
      throw new IllegalStateException("Cannot shut down broker before drain reconciliation completes");
    }
    if (!_shutdownTriggered.compareAndSet(false, true)) {
      return;
    }
    Thread shutdownThread = new Thread(() -> {
      try {
        _shutdownCallback.run();
      } catch (RuntimeException | Error e) {
        _shutdownTriggered.set(false);
        LOGGER.error("Caught exception while shutting down drained broker {}", _instanceId, e);
      }
    }, "broker-drain-shutdown");
    shutdownThread.setDaemon(false);
    shutdownThread.start();
  }

  private void releaseQuery() {
    long queryState;
    long updatedQueryState;
    do {
      queryState = _queryState.get();
      if (getInFlightQueries(queryState) == 0) {
        throw new IllegalStateException("Cannot release query permit when no query is in flight");
      }
      updatedQueryState = queryState - 1;
    } while (!_queryState.compareAndSet(queryState, updatedQueryState));
    if (isDraining(updatedQueryState) && getInFlightQueries(updatedQueryState) == 0) {
      synchronized (_drainWaitLock) {
        _drainWaitLock.notifyAll();
      }
    }
  }

  private DrainStatus getStatus(String message) {
    long queryState = _queryState.get();
    boolean draining = isDraining(queryState);
    int inFlightQueries = getInFlightQueries(queryState);
    long drainStartTimeMs = draining ? _drainStartTimeMs : -1L;
    long drainDurationMs = drainStartTimeMs >= 0 ? Math.max(0L, System.currentTimeMillis() - drainStartTimeMs) : 0L;
    return new DrainStatus(_instanceId, draining, !draining, inFlightQueries, drainStartTimeMs, drainDurationMs,
        draining && inFlightQueries == 0 && isCoordinationReconciled(), _shutdownMarkerReconciled,
        _brokerResourceReconciled, _shutdownTriggered.get(), _tablesRemovedFromBrokerResource, message);
  }

  private static boolean isDraining(long queryState) {
    return (queryState & DRAINING_MASK) != 0;
  }

  private static int getInFlightQueries(long queryState) {
    return (int) (queryState & IN_FLIGHT_QUERY_MASK);
  }

  private boolean isCoordinationReconciled() {
    return !_enableHelixUpdates || (_shutdownMarkerReconciled && _brokerResourceReconciled);
  }

  /// Represents one query admitted before the drain gate closes. Closing the permit releases that query from the
  /// in-flight count. Close is idempotent and safe to invoke concurrently.
  public static final class QueryPermit implements AutoCloseable {
    private final BrokerDrainManager _brokerDrainManager;
    private boolean _closed;

    private QueryPermit(BrokerDrainManager brokerDrainManager) {
      _brokerDrainManager = brokerDrainManager;
    }

    @Override
    public synchronized void close() {
      if (!_closed) {
        _closed = true;
        _brokerDrainManager.releaseQuery();
      }
    }
  }

  /// Indicates that the production broker has not completed startup reconciliation and cannot begin draining yet.
  public static final class BrokerStartupInProgressException extends IllegalStateException {
    private BrokerStartupInProgressException(String instanceId) {
      super("Broker " + instanceId + " is still starting and cannot drain yet");
    }
  }

  /// Indicates that an embedding constructed the broker admin surface without a shared drain manager.
  public static final class BrokerDrainUnsupportedException extends UnsupportedOperationException {
    private BrokerDrainUnsupportedException() {
      super("Broker drain is unavailable because this broker was started without drain coordination");
    }
  }

  /// Immutable snapshot of local query-admission, drain, shutdown, and Helix-reconciliation state.
  public static final class DrainStatus {
    private final String _instanceId;
    private final boolean _draining;
    private final boolean _acceptingQueries;
    private final int _inFlightQueries;
    private final long _drainStartTimeMs;
    private final long _drainDurationMs;
    private final boolean _drained;
    private final boolean _shutdownMarkerReconciled;
    private final boolean _brokerResourceReconciled;
    private final boolean _shutdownTriggered;
    private final List<String> _tablesRemovedFromBrokerResource;
    private final String _message;

    private DrainStatus(String instanceId, boolean draining, boolean acceptingQueries, int inFlightQueries,
        long drainStartTimeMs, long drainDurationMs, boolean drained, boolean shutdownMarkerReconciled,
        boolean brokerResourceReconciled, boolean shutdownTriggered, List<String> tablesRemovedFromBrokerResource,
        String message) {
      _instanceId = instanceId;
      _draining = draining;
      _acceptingQueries = acceptingQueries;
      _inFlightQueries = inFlightQueries;
      _drainStartTimeMs = drainStartTimeMs;
      _drainDurationMs = drainDurationMs;
      _drained = drained;
      _shutdownMarkerReconciled = shutdownMarkerReconciled;
      _brokerResourceReconciled = brokerResourceReconciled;
      _shutdownTriggered = shutdownTriggered;
      _tablesRemovedFromBrokerResource = List.copyOf(tablesRemovedFromBrokerResource);
      _message = message;
    }

    public String getInstanceId() {
      return _instanceId;
    }

    public boolean isDraining() {
      return _draining;
    }

    public boolean isAcceptingQueries() {
      return _acceptingQueries;
    }

    public int getInFlightQueries() {
      return _inFlightQueries;
    }

    public long getDrainStartTimeMs() {
      return _drainStartTimeMs;
    }

    public long getDrainDurationMs() {
      return _drainDurationMs;
    }

    public boolean isDrained() {
      return _drained;
    }

    public boolean isShutdownMarkerReconciled() {
      return _shutdownMarkerReconciled;
    }

    public boolean isBrokerResourceReconciled() {
      return _brokerResourceReconciled;
    }

    public boolean isShutdownTriggered() {
      return _shutdownTriggered;
    }

    public List<String> getTablesRemovedFromBrokerResource() {
      return _tablesRemovedFromBrokerResource;
    }

    public String getMessage() {
      return _message;
    }
  }
}
