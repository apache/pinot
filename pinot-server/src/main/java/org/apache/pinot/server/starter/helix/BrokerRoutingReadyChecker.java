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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.hc.core5.http.Header;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.auth.NullAuthProvider;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.utils.http.HttpClientConfig;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Checks broker routing state while a server starts. A single background thread polls every online broker until all
/// of them report the server as routable, then caches success. Readiness requests only read the cached state and never
/// perform network I/O. Broker requests are sequential and have bounded connect, pool-checkout and response timeouts,
/// so an unavailable broker cannot create unbounded tasks or threads.
public class BrokerRoutingReadyChecker implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerRoutingReadyChecker.class);
  private static final long CHECK_INTERVAL_MS = 1_000L;
  private static final long REQUEST_TIMEOUT_MS = 5_000L;
  private static final int MAX_RESPONSE_LENGTH = 1_024;

  private final String _serverInstanceId;
  private final Supplier<Set<String>> _onlineBrokersSupplier;
  private final BrokersReadyEvaluator _allBrokersReady;
  @Nullable
  private final ScheduledExecutorService _checkExecutor;
  private final RoutingStatusClient _routingStatusClient;
  private final LongSupplier _currentTimeMs;
  private final long _deadlineMs;
  private final boolean _failOpen;
  private final AuthProvider _authProvider;
  private final AtomicReference<State> _state;
  private boolean _timeoutLogged;

  public BrokerRoutingReadyChecker(HelixManager helixManager, long timeoutMs, boolean failOpen,
      AuthProvider authProvider) {
    this(createProductionContext(helixManager, authProvider), timeoutMs, failOpen, authProvider);
  }

  @VisibleForTesting
  BrokerRoutingReadyChecker(HelixManager helixManager, long timeoutMs, boolean failOpen, AuthProvider authProvider,
      RoutingStatusClient routingStatusClient) {
    this(helixManager, timeoutMs, failOpen, authProvider, routingStatusClient, System::currentTimeMillis);
  }

  @VisibleForTesting
  BrokerRoutingReadyChecker(HelixManager helixManager, long timeoutMs, boolean failOpen, AuthProvider authProvider,
      RoutingStatusClient routingStatusClient, LongSupplier currentTimeMs) {
    this(createContext(helixManager, routingStatusClient, null, authProvider, new AtomicReference<>(State.CHECKING)),
        timeoutMs, failOpen, currentTimeMs, authProvider);
  }

  private BrokerRoutingReadyChecker(ProductionContext context, long timeoutMs, boolean failOpen,
      AuthProvider authProvider) {
    this(context, timeoutMs, failOpen, System::currentTimeMillis, authProvider);
  }

  private BrokerRoutingReadyChecker(ProductionContext context, long timeoutMs, boolean failOpen,
      LongSupplier currentTimeMs, AuthProvider authProvider) {
    this(context._serverInstanceId, context._onlineBrokersSupplier, context._allBrokersReady,
        context._checkExecutor, context._routingStatusClient, timeoutMs, failOpen, currentTimeMs,
        authProvider, context._state);
  }

  @VisibleForTesting
  BrokerRoutingReadyChecker(String serverInstanceId, Supplier<Set<String>> onlineBrokersSupplier,
      Predicate<Set<String>> allBrokersReady) {
    this(serverInstanceId, onlineBrokersSupplier, allBrokersReady, Long.MAX_VALUE, false, () -> 0L);
  }

  @VisibleForTesting
  BrokerRoutingReadyChecker(String serverInstanceId, Supplier<Set<String>> onlineBrokersSupplier,
      Predicate<Set<String>> allBrokersReady, long timeoutMs, boolean failOpen, LongSupplier currentTimeMs) {
    this(serverInstanceId, onlineBrokersSupplier, (brokers, shouldStop) -> allBrokersReady.test(brokers), null,
        RoutingStatusClient.NOOP, timeoutMs, failOpen, currentTimeMs, new NullAuthProvider(),
        new AtomicReference<>(State.CHECKING));
  }

  private BrokerRoutingReadyChecker(String serverInstanceId, Supplier<Set<String>> onlineBrokersSupplier,
      BrokersReadyEvaluator allBrokersReady, @Nullable ScheduledExecutorService checkExecutor,
      RoutingStatusClient routingStatusClient, long timeoutMs, boolean failOpen, LongSupplier currentTimeMs,
      AuthProvider authProvider, AtomicReference<State> state) {
    _serverInstanceId = serverInstanceId;
    _onlineBrokersSupplier = onlineBrokersSupplier;
    _allBrokersReady = allBrokersReady;
    _checkExecutor = checkExecutor;
    _routingStatusClient = routingStatusClient;
    _currentTimeMs = currentTimeMs;
    long nowMs = currentTimeMs.getAsLong();
    _deadlineMs = timeoutMs >= Long.MAX_VALUE - nowMs ? Long.MAX_VALUE : nowMs + Math.max(timeoutMs, 0L);
    _failOpen = failOpen;
    _authProvider = authProvider;
    _state = state;
    if (_checkExecutor != null) {
      _checkExecutor.scheduleWithFixedDelay(this::check, 0L, CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
  }

  public boolean isReady() {
    transitionToReadyOnFailOpenTimeout();
    return _state.get() == State.READY;
  }

  @VisibleForTesting
  AuthProvider getAuthProvider() {
    return _authProvider;
  }

  @VisibleForTesting
  synchronized void check() {
    if (_state.get() != State.CHECKING || transitionToReadyOnFailOpenTimeout()) {
      return;
    }
    try {
      Set<String> onlineBrokers = _onlineBrokersSupplier.get();
      if (!onlineBrokers.isEmpty() && _allBrokersReady.test(onlineBrokers, this::shouldStopChecking)
          && _state.get() == State.CHECKING
          // Do not mark the server ready if broker membership changed while acknowledgements were collected.
          && onlineBrokers.equals(_onlineBrokersSupplier.get())) {
        if (_state.compareAndSet(State.CHECKING, State.READY)) {
          LOGGER.info("All online brokers report server {} as routable: {}", _serverInstanceId, onlineBrokers);
          stopChecking();
        }
        return;
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to check broker routing readiness for server {}", _serverInstanceId, e);
    }

    if (transitionToReadyOnFailOpenTimeout()) {
      return;
    }
    if (_currentTimeMs.getAsLong() >= _deadlineMs) {
      if (!_timeoutLogged) {
        LOGGER.warn("Timed out waiting for all online brokers to report server {} as routable; failOpen={}",
            _serverInstanceId, _failOpen);
        _timeoutLogged = true;
      }
    }
  }

  private boolean shouldStopChecking() {
    return _state.get() != State.CHECKING || Thread.currentThread().isInterrupted()
        || transitionToReadyOnFailOpenTimeout();
  }

  private boolean transitionToReadyOnFailOpenTimeout() {
    if (!_failOpen || _currentTimeMs.getAsLong() < _deadlineMs) {
      return false;
    }
    if (_state.compareAndSet(State.CHECKING, State.READY)) {
      LOGGER.warn("Timed out waiting for all online brokers to report server {} as routable; failOpen=true",
          _serverInstanceId);
      stopChecking();
    }
    return _state.get() != State.CHECKING;
  }

  private void stopChecking() {
    if (_checkExecutor != null) {
      _checkExecutor.shutdown();
    }
  }

  private static ProductionContext createProductionContext(HelixManager helixManager, AuthProvider authProvider) {
    HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder()
        .withMaxConns(1)
        .withMaxConnsPerRoute(1)
        .withConnectionTimeoutMs((int) REQUEST_TIMEOUT_MS)
        .withFollowRedirects(false)
        .build();
    RoutingStatusClient routingStatusClient = new SecureRoutingStatusClient(
        new HttpRoutingStatusClient(new HttpClient(httpClientConfig, TlsUtils.getSslContext(), true)));
    ScheduledExecutorService checkExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("broker-routing-ready-check-%d").setDaemon(true).build());
    return createContext(helixManager, routingStatusClient, checkExecutor, authProvider,
        new AtomicReference<>(State.CHECKING));
  }

  private static ProductionContext createContext(HelixManager helixManager, RoutingStatusClient routingStatusClient,
      @Nullable ScheduledExecutorService checkExecutor, AuthProvider authProvider, AtomicReference<State> state) {
    String serverInstanceId = helixManager.getInstanceName();
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    String clusterName = helixManager.getClusterName();
    Supplier<Set<String>> onlineBrokersSupplier = () -> {
      ExternalView brokerResource = helixAdmin.getResourceExternalView(clusterName,
          CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      return HelixHelper.getOnlineInstanceFromExternalView(brokerResource);
    };
    RoutingStatusClient secureRoutingStatusClient =
        routingStatusClient instanceof SecureRoutingStatusClient ? routingStatusClient
            : new SecureRoutingStatusClient(routingStatusClient);
    BrokersReadyEvaluator allBrokersReady = (brokers, shouldStop) -> {
      for (String broker : brokers) {
        if (shouldStop.getAsBoolean() || state.get() != State.CHECKING || Thread.currentThread().isInterrupted()) {
          return false;
        }
        if (!checkBroker(serverInstanceId, helixAdmin, clusterName, broker, authProvider,
            secureRoutingStatusClient)) {
          return false;
        }
      }
      return true;
    };
    return new ProductionContext(serverInstanceId, onlineBrokersSupplier, allBrokersReady, checkExecutor,
        secureRoutingStatusClient, state);
  }

  private static boolean checkBroker(String serverInstanceId, HelixAdmin helixAdmin, String clusterName, String broker,
      AuthProvider authProvider, RoutingStatusClient routingStatusClient) {
    try {
      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, broker);
      if (instanceConfig == null) {
        return false;
      }
      URI uri = URI.create(InstanceUtils.getInstanceBaseUri(instanceConfig) + "/routing/server/" + serverInstanceId);
      List<Header> authHeaders = List.copyOf(AuthProviderUtils.toRequestHeaders(authProvider));
      SimpleHttpResponse response = routingStatusClient.get(uri, authHeaders);
      return response.getStatusCode() == 200
          && CommonConstants.Broker.SERVER_ROUTING_READY_RESPONSE.equals(response.getResponse());
    } catch (Exception e) {
      LOGGER.debug("Broker {} has not confirmed routing readiness for server {}", broker, serverInstanceId, e);
      return false;
    }
  }

  @Override
  public void close() {
    if (_state.getAndSet(State.CLOSED) == State.CLOSED) {
      return;
    }
    if (_checkExecutor != null) {
      _checkExecutor.shutdownNow();
    }
    try {
      _routingStatusClient.close();
    } catch (IOException e) {
      LOGGER.warn("Failed to close broker routing readiness client", e);
    }
  }

  private static class ProductionContext {
    private final String _serverInstanceId;
    private final Supplier<Set<String>> _onlineBrokersSupplier;
    private final BrokersReadyEvaluator _allBrokersReady;
    @Nullable
    private final ScheduledExecutorService _checkExecutor;
    private final RoutingStatusClient _routingStatusClient;
    private final AtomicReference<State> _state;

    private ProductionContext(String serverInstanceId, Supplier<Set<String>> onlineBrokersSupplier,
        BrokersReadyEvaluator allBrokersReady, @Nullable ScheduledExecutorService checkExecutor,
        RoutingStatusClient routingStatusClient, AtomicReference<State> state) {
      _serverInstanceId = serverInstanceId;
      _onlineBrokersSupplier = onlineBrokersSupplier;
      _allBrokersReady = allBrokersReady;
      _checkExecutor = checkExecutor;
      _routingStatusClient = routingStatusClient;
      _state = state;
    }
  }

  @FunctionalInterface
  private interface BrokersReadyEvaluator {
    boolean test(Set<String> brokers, BooleanSupplier shouldStop);
  }

  private static class HttpRoutingStatusClient implements RoutingStatusClient {
    private final HttpClient _httpClient;

    private HttpRoutingStatusClient(HttpClient httpClient) {
      _httpClient = httpClient;
    }

    @Override
    public SimpleHttpResponse get(URI uri, List<Header> authHeaders) throws IOException {
      return _httpClient.sendGetRequest(uri, authHeaders, REQUEST_TIMEOUT_MS, REQUEST_TIMEOUT_MS,
          MAX_RESPONSE_LENGTH);
    }

    @Override
    public void close() throws IOException {
      _httpClient.close();
    }
  }

  private static class SecureRoutingStatusClient implements RoutingStatusClient {
    private final RoutingStatusClient _delegate;
    private final Set<String> _warnedInsecureAuthorities = new HashSet<>();

    private SecureRoutingStatusClient(RoutingStatusClient delegate) {
      _delegate = delegate;
    }

    @Override
    public SimpleHttpResponse get(URI uri, List<Header> authHeaders) throws IOException {
      if (!authHeaders.isEmpty() && !CommonConstants.HTTPS_PROTOCOL.equalsIgnoreCase(uri.getScheme())) {
        if (_warnedInsecureAuthorities.add(uri.getAuthority())) {
          LOGGER.error("Refusing to send broker routing readiness credentials over non-HTTPS transport to {}",
              uri.getAuthority());
        }
        throw new IOException("Broker routing readiness authentication requires HTTPS");
      }
      return _delegate.get(uri, authHeaders);
    }

    @Override
    public void close() throws IOException {
      _delegate.close();
    }
  }

  @VisibleForTesting
  interface RoutingStatusClient extends AutoCloseable {
    RoutingStatusClient NOOP = (uri, authHeaders) -> {
      throw new UnsupportedOperationException("No routing status client configured");
    };

    SimpleHttpResponse get(URI uri, List<Header> authHeaders) throws IOException;

    @Override
    default void close() throws IOException {
    }
  }

  private enum State {
    CHECKING,
    READY,
    CLOSED
  }
}
