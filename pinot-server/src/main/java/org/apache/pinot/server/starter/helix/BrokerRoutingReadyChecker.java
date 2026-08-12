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
package org.apache.pinot.server.starter.helix;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Checks broker routing state in the background while a server starts. The checker remains false until every online
/// broker reports the server as routable, or until the configured timeout when fail-open behavior is enabled. Health
/// endpoints only read the cached result. Brokers must return the routing-specific response, so an older broker's
/// normal health response cannot be mistaken for an acknowledgement. All mutable state is confined to the scheduler
/// thread except for the volatile ready flag read by health-check threads.
public class BrokerRoutingReadyChecker implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerRoutingReadyChecker.class);
  private static final long CHECK_INTERVAL_MS = 1_000L;
  private static final long CHECK_TIMEOUT_MS = 5_000L;

  private final String _serverInstanceId;
  private final Supplier<Set<String>> _onlineBrokersSupplier;
  private final Predicate<Set<String>> _allBrokersReady;
  private final ScheduledExecutorService _scheduler;
  private final ExecutorService _requestExecutor;
  private final LongSupplier _currentTimeMs;
  private final long _deadlineMs;
  private final boolean _failOpen;
  private volatile boolean _ready;
  private boolean _timeoutLogged;

  public BrokerRoutingReadyChecker(HelixManager helixManager, String serverInstanceId, long timeoutMs,
      boolean failOpen) {
    _serverInstanceId = serverInstanceId;
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    String clusterName = helixManager.getClusterName();
    _onlineBrokersSupplier = () -> {
      ExternalView brokerResource = helixAdmin.getResourceExternalView(clusterName,
          CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      return Set.copyOf(HelixHelper.getOnlineInstanceFromExternalView(brokerResource));
    };
    _requestExecutor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("broker-routing-ready-request-%d").setDaemon(true).build());
    _allBrokersReady = brokers -> checkAllBrokers(helixAdmin, clusterName, brokers);
    _scheduler = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("broker-routing-ready-checker").setDaemon(true).build());
    _currentTimeMs = System::currentTimeMillis;
    _deadlineMs = _currentTimeMs.getAsLong() + timeoutMs;
    _failOpen = failOpen;
  }

  @VisibleForTesting
  BrokerRoutingReadyChecker(String serverInstanceId, Supplier<Set<String>> onlineBrokersSupplier,
      Predicate<Set<String>> allBrokersReady) {
    this(serverInstanceId, onlineBrokersSupplier, allBrokersReady, Long.MAX_VALUE, false, () -> 0L);
  }

  @VisibleForTesting
  BrokerRoutingReadyChecker(String serverInstanceId, Supplier<Set<String>> onlineBrokersSupplier,
      Predicate<Set<String>> allBrokersReady, long timeoutMs, boolean failOpen, LongSupplier currentTimeMs) {
    _serverInstanceId = serverInstanceId;
    _onlineBrokersSupplier = onlineBrokersSupplier;
    _allBrokersReady = allBrokersReady;
    _requestExecutor = null;
    _scheduler = null;
    _currentTimeMs = currentTimeMs;
    _deadlineMs = currentTimeMs.getAsLong() + timeoutMs;
    _failOpen = failOpen;
  }

  public void start() {
    _scheduler.scheduleWithFixedDelay(this::check, 0L, CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  public boolean isReady() {
    return _ready;
  }

  @VisibleForTesting
  void check() {
    if (_ready) {
      return;
    }
    try {
      Set<String> onlineBrokers = _onlineBrokersSupplier.get();
      if (!onlineBrokers.isEmpty() && _allBrokersReady.test(onlineBrokers)
          // Do not mark the server ready if broker membership changed while acknowledgements were collected.
          && onlineBrokers.equals(_onlineBrokersSupplier.get())) {
        _ready = true;
        LOGGER.info("All online brokers report server {} as routable: {}", _serverInstanceId, onlineBrokers);
        return;
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to check broker routing readiness for server {}", _serverInstanceId, e);
    }

    if (_currentTimeMs.getAsLong() >= _deadlineMs) {
      if (!_timeoutLogged) {
        LOGGER.warn("Timed out waiting for all online brokers to report server {} as routable; failOpen={}",
            _serverInstanceId, _failOpen);
        _timeoutLogged = true;
      }
      if (_failOpen) {
        _ready = true;
      }
    }
  }

  private boolean checkAllBrokers(HelixAdmin helixAdmin, String clusterName, Set<String> brokers) {
    List<CompletableFuture<Boolean>> futures = new ArrayList<>(brokers.size());
    for (String broker : brokers) {
      futures.add(CompletableFuture.supplyAsync(() -> checkBroker(helixAdmin, clusterName, broker), _requestExecutor));
    }
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .get(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return futures.stream().allMatch(CompletableFuture::join);
    } catch (Exception e) {
      futures.forEach(future -> future.cancel(true));
      LOGGER.debug("Failed to collect routing readiness from all online brokers for server {}", _serverInstanceId, e);
      return false;
    }
  }

  private boolean checkBroker(HelixAdmin helixAdmin, String clusterName, String broker) {
    try {
      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, broker);
      if (instanceConfig == null) {
        return false;
      }
      String serverInstance = URLEncoder.encode(_serverInstanceId, StandardCharsets.UTF_8);
      URI uri = URI.create(InstanceUtils.getInstanceBaseUri(instanceConfig) + "/health?serverInstance="
          + serverInstance);
      SimpleHttpResponse response = HttpClient.getInstance().sendGetRequest(uri);
      return response.getStatusCode() == 200
          && CommonConstants.Broker.SERVER_ROUTING_READY_RESPONSE.equals(response.getResponse());
    } catch (Exception e) {
      LOGGER.debug("Broker {} has not confirmed routing readiness for server {}", broker, _serverInstanceId, e);
      return false;
    }
  }

  @Override
  public void close() {
    if (_scheduler != null) {
      _scheduler.shutdownNow();
    }
    if (_requestExecutor != null) {
      _requestExecutor.shutdownNow();
    }
  }
}
