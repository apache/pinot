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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
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
public class BrokerDrainManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerDrainManager.class);
  private static final long SHUTDOWN_CALLBACK_DELAY_MS = 1_000L;

  private final String _instanceId;
  private final Supplier<HelixManager> _helixManagerSupplier;
  private final Runnable _drainStartedCallback;
  private final Runnable _shutdownCallback;
  private final long _defaultDrainTimeoutMs;
  private final boolean _enableHelixUpdates;

  private final Object _lock = new Object();
  private final AtomicBoolean _draining = new AtomicBoolean(false);
  private final AtomicBoolean _shutdownTriggered = new AtomicBoolean(false);
  private volatile long _drainStartTimeMs = -1L;
  private volatile List<String> _tablesRemovedFromBrokerResource = Collections.emptyList();
  private int _inFlightQueries;

  public BrokerDrainManager(String instanceId, Supplier<HelixManager> helixManagerSupplier,
      Runnable drainStartedCallback, Runnable shutdownCallback, long defaultDrainTimeoutMs) {
    this(instanceId, helixManagerSupplier, drainStartedCallback, shutdownCallback, defaultDrainTimeoutMs, true);
  }

  private BrokerDrainManager(String instanceId, Supplier<HelixManager> helixManagerSupplier,
      Runnable drainStartedCallback, Runnable shutdownCallback, long defaultDrainTimeoutMs,
      boolean enableHelixUpdates) {
    _instanceId = instanceId;
    _helixManagerSupplier = helixManagerSupplier;
    _drainStartedCallback = drainStartedCallback;
    _shutdownCallback = shutdownCallback;
    _defaultDrainTimeoutMs = defaultDrainTimeoutMs;
    _enableHelixUpdates = enableHelixUpdates;
  }

  public static BrokerDrainManager noop(String instanceId) {
    return new BrokerDrainManager(instanceId, () -> null, () -> {
    }, () -> {
    }, 0L, false);
  }

  @VisibleForTesting
  public static BrokerDrainManager localOnly(String instanceId, Runnable drainStartedCallback,
      Runnable shutdownCallback, long defaultDrainTimeoutMs) {
    return new BrokerDrainManager(instanceId, () -> null, drainStartedCallback, shutdownCallback, defaultDrainTimeoutMs,
        false);
  }

  @Nullable
  public QueryPermit tryAcquireQuery() {
    synchronized (_lock) {
      if (_draining.get()) {
        return null;
      }
      _inFlightQueries++;
      return new QueryPermit(this);
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
    return getStatus(_draining.get() ? "Broker is draining" : "Broker is accepting queries");
  }

  public boolean isDraining() {
    return _draining.get();
  }

  public boolean isDrainComplete() {
    synchronized (_lock) {
      return _draining.get() && _inFlightQueries == 0;
    }
  }

  public String getRejectMessage() {
    return "Broker " + _instanceId + " is draining and not accepting new queries";
  }

  private void startDrain() {
    if (!_draining.compareAndSet(false, true)) {
      return;
    }
    _drainStartTimeMs = System.currentTimeMillis();
    _drainStartedCallback.run();
    if (_enableHelixUpdates) {
      HelixManager helixManager = getConnectedHelixManager();
      markShutdownInProgress(helixManager);
      _tablesRemovedFromBrokerResource = removeBrokerFromBrokerResource(helixManager);
    }
    synchronized (_lock) {
      if (_inFlightQueries == 0) {
        _lock.notifyAll();
      }
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
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT, helixManager.getClusterName())
            .forParticipant(_instanceId).build();
    helixManager.getConfigAccessor()
        .set(scope, Collections.singletonMap(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, Boolean.TRUE.toString()));
    LOGGER.info("Marked broker instance {} with {}=true", _instanceId,
        CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS);
  }

  private List<String> removeBrokerFromBrokerResource(HelixManager helixManager) {
    List<String> tablesRemoved = new ArrayList<>();
    HelixHelper.updateIdealState(helixManager, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, idealState -> {
      for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
        if (entry.getValue().remove(_instanceId) != null) {
          tablesRemoved.add(entry.getKey());
        }
      }
      return idealState;
    });
    LOGGER.info("Removed broker instance {} from brokerResource for {} table(s): {}", _instanceId,
        tablesRemoved.size(), tablesRemoved);
    return List.copyOf(tablesRemoved);
  }

  private boolean awaitNoInflightQueries(long timeoutMs)
      throws InterruptedException {
    long deadlineMs = System.currentTimeMillis() + timeoutMs;
    synchronized (_lock) {
      while (_inFlightQueries > 0) {
        long waitMs = deadlineMs - System.currentTimeMillis();
        if (waitMs <= 0) {
          return false;
        }
        _lock.wait(waitMs);
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

  private void triggerShutdown() {
    if (!_shutdownTriggered.compareAndSet(false, true)) {
      return;
    }
    Thread shutdownThread = new Thread(() -> {
      try {
        Thread.sleep(SHUTDOWN_CALLBACK_DELAY_MS);
        _shutdownCallback.run();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted before shutting down drained broker {}", _instanceId, e);
      } catch (Exception e) {
        LOGGER.error("Caught exception while shutting down drained broker {}", _instanceId, e);
      }
    }, "broker-drain-shutdown");
    shutdownThread.setDaemon(false);
    shutdownThread.start();
  }

  private void releaseQuery() {
    synchronized (_lock) {
      _inFlightQueries--;
      if (_inFlightQueries == 0) {
        _lock.notifyAll();
      }
    }
  }

  private DrainStatus getStatus(String message) {
    synchronized (_lock) {
      long nowMs = System.currentTimeMillis();
      long drainDurationMs = _drainStartTimeMs >= 0 ? nowMs - _drainStartTimeMs : 0;
      return new DrainStatus(_instanceId, _draining.get(), !_draining.get(), _inFlightQueries, _drainStartTimeMs,
          drainDurationMs, _draining.get() && _inFlightQueries == 0, _shutdownTriggered.get(),
          _tablesRemovedFromBrokerResource, message);
    }
  }

  public static final class QueryPermit implements AutoCloseable {
    private final BrokerDrainManager _brokerDrainManager;
    private boolean _closed;

    private QueryPermit(BrokerDrainManager brokerDrainManager) {
      _brokerDrainManager = brokerDrainManager;
    }

    @Override
    public void close() {
      if (!_closed) {
        _closed = true;
        _brokerDrainManager.releaseQuery();
      }
    }
  }

  public static final class DrainStatus {
    private final String _instanceId;
    private final boolean _draining;
    private final boolean _acceptingQueries;
    private final int _inFlightQueries;
    private final long _drainStartTimeMs;
    private final long _drainDurationMs;
    private final boolean _drained;
    private final boolean _shutdownTriggered;
    private final List<String> _tablesRemovedFromBrokerResource;
    private final String _message;

    private DrainStatus(String instanceId, boolean draining, boolean acceptingQueries, int inFlightQueries,
        long drainStartTimeMs, long drainDurationMs, boolean drained, boolean shutdownTriggered,
        List<String> tablesRemovedFromBrokerResource, String message) {
      _instanceId = instanceId;
      _draining = draining;
      _acceptingQueries = acceptingQueries;
      _inFlightQueries = inFlightQueries;
      _drainStartTimeMs = drainStartTimeMs;
      _drainDurationMs = drainDurationMs;
      _drained = drained;
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
