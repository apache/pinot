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
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.auth.NullAuthProvider;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Checks broker routing state while a server starts. Readiness requests perform a synchronized check until every
/// online broker reports the server as routable, or until the configured timeout when fail-open behavior is enabled.
/// Successful readiness is cached. Brokers must return the routing-specific response, so an older broker's normal
/// health response cannot be mistaken for an acknowledgement.
public class BrokerRoutingReadyChecker implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerRoutingReadyChecker.class);
  private static final long CHECK_TIMEOUT_MS = 5_000L;

  private final String _serverInstanceId;
  private final Supplier<Set<String>> _onlineBrokersSupplier;
  private final Predicate<Set<String>> _allBrokersReady;
  @Nullable
  private final ExecutorService _requestExecutor;
  private final LongSupplier _currentTimeMs;
  private final long _deadlineMs;
  private final boolean _failOpen;
  private final AuthProvider _authProvider;
  private boolean _ready;
  private boolean _timeoutLogged;

  public BrokerRoutingReadyChecker(HelixManager helixManager, long timeoutMs, boolean failOpen,
      AuthProvider authProvider) {
    this(helixManager, timeoutMs, failOpen, authProvider,
        (uri, provider) -> HttpClient.getInstance().sendGetRequest(uri, null, provider));
  }

  @VisibleForTesting
  BrokerRoutingReadyChecker(HelixManager helixManager, long timeoutMs, boolean failOpen, AuthProvider authProvider,
      RoutingStatusClient routingStatusClient) {
    this(createProductionContext(helixManager, authProvider, routingStatusClient), timeoutMs, failOpen, authProvider);
  }

  private BrokerRoutingReadyChecker(ProductionContext context, long timeoutMs, boolean failOpen,
      AuthProvider authProvider) {
    this(context._serverInstanceId, context._onlineBrokersSupplier, context._allBrokersReady,
        context._requestExecutor, timeoutMs, failOpen, System::currentTimeMillis, authProvider);
  }

  private static ProductionContext createProductionContext(HelixManager helixManager, AuthProvider authProvider,
      RoutingStatusClient routingStatusClient) {
    String serverInstanceId = helixManager.getInstanceName();
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    String clusterName = helixManager.getClusterName();
    Supplier<Set<String>> onlineBrokersSupplier = () -> {
      ExternalView brokerResource = helixAdmin.getResourceExternalView(clusterName,
          CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      return HelixHelper.getOnlineInstanceFromExternalView(brokerResource);
    };
    ExecutorService requestExecutor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("broker-routing-ready-request-%d").setDaemon(true).build());
    Predicate<Set<String>> allBrokersReady = brokers -> checkAllBrokers(serverInstanceId, helixAdmin, clusterName,
        brokers, requestExecutor, authProvider, routingStatusClient);
    return new ProductionContext(serverInstanceId, onlineBrokersSupplier, allBrokersReady, requestExecutor);
  }

  @VisibleForTesting
  BrokerRoutingReadyChecker(String serverInstanceId, Supplier<Set<String>> onlineBrokersSupplier,
      Predicate<Set<String>> allBrokersReady) {
    this(serverInstanceId, onlineBrokersSupplier, allBrokersReady, null, Long.MAX_VALUE, false, () -> 0L,
        new NullAuthProvider());
  }

  @VisibleForTesting
  BrokerRoutingReadyChecker(String serverInstanceId, Supplier<Set<String>> onlineBrokersSupplier,
      Predicate<Set<String>> allBrokersReady, long timeoutMs, boolean failOpen, LongSupplier currentTimeMs) {
    this(serverInstanceId, onlineBrokersSupplier, allBrokersReady, null, timeoutMs, failOpen, currentTimeMs,
        new NullAuthProvider());
  }

  private BrokerRoutingReadyChecker(String serverInstanceId, Supplier<Set<String>> onlineBrokersSupplier,
      Predicate<Set<String>> allBrokersReady, @Nullable ExecutorService requestExecutor, long timeoutMs,
      boolean failOpen, LongSupplier currentTimeMs, AuthProvider authProvider) {
    _serverInstanceId = serverInstanceId;
    _onlineBrokersSupplier = onlineBrokersSupplier;
    _allBrokersReady = allBrokersReady;
    _requestExecutor = requestExecutor;
    _currentTimeMs = currentTimeMs;
    _deadlineMs = currentTimeMs.getAsLong() + timeoutMs;
    _failOpen = failOpen;
    _authProvider = authProvider;
  }

  public synchronized boolean isReady() {
    check();
    return _ready;
  }

  @VisibleForTesting
  AuthProvider getAuthProvider() {
    return _authProvider;
  }

  private void check() {
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

  private static boolean checkAllBrokers(String serverInstanceId, HelixAdmin helixAdmin, String clusterName,
      Set<String> brokers, ExecutorService requestExecutor, AuthProvider authProvider,
      RoutingStatusClient routingStatusClient) {
    List<CompletableFuture<Boolean>> futures = new ArrayList<>(brokers.size());
    for (String broker : brokers) {
      futures.add(CompletableFuture.supplyAsync(
          () -> checkBroker(serverInstanceId, helixAdmin, clusterName, broker, authProvider, routingStatusClient),
          requestExecutor));
    }
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .get(CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return futures.stream().allMatch(CompletableFuture::join);
    } catch (Exception e) {
      futures.forEach(future -> future.cancel(true));
      LOGGER.debug("Failed to collect routing readiness from all online brokers for server {}", serverInstanceId, e);
      return false;
    }
  }

  private static boolean checkBroker(String serverInstanceId, HelixAdmin helixAdmin, String clusterName, String broker,
      AuthProvider authProvider, RoutingStatusClient routingStatusClient) {
    try {
      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, broker);
      if (instanceConfig == null) {
        return false;
      }
      URI uri = URI.create(InstanceUtils.getInstanceBaseUri(instanceConfig) + "/routing/server/" + serverInstanceId);
      SimpleHttpResponse response = routingStatusClient.get(uri, authProvider);
      return response.getStatusCode() == 200
          && CommonConstants.Broker.SERVER_ROUTING_READY_RESPONSE.equals(response.getResponse());
    } catch (Exception e) {
      LOGGER.debug("Broker {} has not confirmed routing readiness for server {}", broker, serverInstanceId, e);
      return false;
    }
  }

  @Override
  public synchronized void close() {
    if (_requestExecutor != null) {
      _requestExecutor.shutdownNow();
    }
  }

  private static class ProductionContext {
    private final String _serverInstanceId;
    private final Supplier<Set<String>> _onlineBrokersSupplier;
    private final Predicate<Set<String>> _allBrokersReady;
    private final ExecutorService _requestExecutor;

    private ProductionContext(String serverInstanceId, Supplier<Set<String>> onlineBrokersSupplier,
        Predicate<Set<String>> allBrokersReady, ExecutorService requestExecutor) {
      _serverInstanceId = serverInstanceId;
      _onlineBrokersSupplier = onlineBrokersSupplier;
      _allBrokersReady = allBrokersReady;
      _requestExecutor = requestExecutor;
    }
  }

  @VisibleForTesting
  interface RoutingStatusClient {
    SimpleHttpResponse get(URI uri, AuthProvider authProvider) throws IOException;
  }
}
