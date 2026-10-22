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
package org.apache.pinot.controller.coordination;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.audit.AuditServiceBinder;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.ControllerAdminApiApplication;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.resources.LandingPageHandler;
import org.apache.pinot.controller.api.resources.PinotControllerAppConfigs;
import org.apache.pinot.controller.api.resources.PinotControllerAuthResource;
import org.apache.pinot.controller.api.resources.PinotControllerHealthCheck;
import org.apache.pinot.controller.api.resources.PinotVersionRestletResource;
import org.apache.pinot.controller.api.resources.WebApplicationExceptionMapper;
import org.apache.pinot.coordination.ratis.RatisMetadataAccess;
import org.apache.pinot.coordination.ratis.RatisMetadataStore;
import org.apache.pinot.coordination.ratis.RatisMetadataStore.MembershipClockSettings;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ConfigurePolicy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.GetControl;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Response;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipQuery;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.transport.ControllerAccessPolicy;
import org.apache.pinot.coordination.ratis.transport.MetadataAudit;
import org.apache.pinot.coordination.ratis.transport.MetadataGrpcServer;
import org.apache.pinot.coordination.ratis.transport.MetadataGrpcService.Limits;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.services.ServiceRole;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;


/// Owns one embedded voter, its qualified membership clock and authenticated metadata endpoint. Lifecycle methods
/// are serialized; the normal HTTP/UI shell exposes committed metadata management, without scheduling or query
/// readiness.
/// The current voter transport remains restricted to three loopback peers. No Helix resources are constructed.
public final class RatisControllerRuntime implements AutoCloseable, ServiceStatus.ServiceStatusCallback {
  private final ControllerConf _config;
  private final List<ListenerConfig> _listeners;
  private final UUID _history;
  private final String _instanceId;
  private final String _providerClass;
  private final InetSocketAddress _address;
  private final Path _certificate;
  private final Path _privateKey;
  private final Path _trust;
  private final long _startupMillis;
  @Nullable
  private RatisControllerProvider _provider;
  @Nullable
  private MetadataGrpcServer _endpoint;
  @Nullable
  private volatile RatisMetadataStore _store;
  @Nullable
  private volatile RatisMetadataAccess _access;
  @Nullable
  private ControllerAdminApiApplication _adminApp;
  @Nullable
  private NativeControllerMetadata _metadata;
  private volatile boolean _started;
  private volatile boolean _closed;
  private volatile String _status = "Native controller has not started";

  public RatisControllerRuntime(ControllerConf config) {
    _config = new ControllerConf(config.toMap());
    _history = UUID.fromString(NativeMetadataConfig.required(config, "historyId"));
    String voter = NativeMetadataConfig.required(config, "id");
    _instanceId = config.getInstanceId() == null ? "Controller_ratis_" + voter : config.getInstanceId();
    checkArgument(!_instanceId.isBlank(), "Native controller instance ID must not be blank");
    _providerClass = NativeMetadataConfig.required(config, "controller.provider.class");
    String host = NativeMetadataConfig.required(config, "metadata.host");
    int port = Integer.parseInt(NativeMetadataConfig.required(config, "metadata.port"));
    checkArgument(port > 0 && port <= 65535, "Metadata port must be between 1 and 65535");
    _address = new InetSocketAddress(host, port);
    _certificate = NativeMetadataConfig.readableFile(config, "tls.certChain");
    _privateKey = NativeMetadataConfig.readableFile(config, "tls.privateKey");
    _trust = NativeMetadataConfig.readableFile(config, "tls.trustCerts");
    _listeners = ListenerConfigUtil.buildControllerConfigs(_config);
    _startupMillis = config.getProperty(NativeMetadataConfig.PREFIX + "controller.startup.timeoutMs", 30_000L);
    checkArgument(_startupMillis > 0 && _startupMillis <= 300_000,
        "Native controller startup timeout must be between 1 and 300000 milliseconds");
  }

  public String instanceId() {
    return _instanceId;
  }

  public synchronized void start() throws Exception {
    checkState(!_started && !_closed && _provider == null, "Native controller is already started or closed");
    try {
      AccessControlFactory accessControlFactory =
          PluginManager.get().createInstance(_config.getAccessControlFactoryClass());
      accessControlFactory.initWithoutHelix(_config);
      _provider = PluginManager.get().createInstance(_providerClass);
      _provider.init(_config);
      MembershipClockSettings clock = requireNonNull(_provider.membershipClockSettings(), "membershipClockSettings");
      ControllerAccessPolicy controllers = requireNonNull(_provider.controllerAccessPolicy(), "controllerAccessPolicy");
      ParticipantAccessPolicy participants = requireNonNull(_provider.participantAccessPolicy(),
          "participantAccessPolicy");
      MetadataAudit audit = requireNonNull(_provider.audit(), "audit");
      Policy policy = requireNonNull(_provider.bootstrapPolicy(), "bootstrapPolicy");
      validateBootstrapPolicy(policy);
      _store = new RatisMetadataStore(_config);
      _access = _store.openMetadataAccess();
      _store.startMembershipClock(clock);
      initializePolicy(policy);
      _endpoint = new MetadataGrpcServer(_address, _certificate, _privateKey, _trust, _access, controllers, _access,
          fingerprint -> {
            ParticipantIdentity identity = participants.authorize(fingerprint);
            return identity != null && identity.capabilities().isEmpty() ? identity : null;
          }, audit, Limits.defaults());
      startHttp(accessControlFactory);
      checkClock();
      _started = true;
      _status = "Native metadata and controller HTTP initialized; query serving is disabled";
    } catch (Exception | Error e) {
      _status = "Native controller startup failed: " + e.getMessage();
      try {
        close();
      } catch (Exception cleanup) {
        e.addSuppressed(cleanup);
      }
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw e;
    }
  }

  private void startHttp(AccessControlFactory accessControlFactory) throws IOException {
    NativeControllerMetadata metadata = new NativeControllerMetadata(requireNonNull(_access), _history, _config);
    _metadata = metadata;
    metadata.start();
    // The metrics factory owns its process-wide registry; a controller must not shut it down on restart.
    PinotMetricsRegistry registry =
        PinotMetricUtils.getPinotMetricsRegistry(_config.subset("pinot.controller.metrics"));
    ControllerMetrics metrics = new ControllerMetrics(_config.getMetricsPrefix(), registry);
    metrics.initializeGlobalMeters();
    metrics.setValueOfGlobalGauge(ControllerGauge.VERSION, PinotVersion.VERSION_METRIC_NAME, 1);
    _adminApp = new ControllerAdminApiApplication(_config, Set.of(NativeControllerResource.class,
        LandingPageHandler.class, PinotControllerAuthResource.class, PinotVersionRestletResource.class,
        PinotControllerAppConfigs.class, PinotControllerHealthCheck.class, WebApplicationExceptionMapper.class));
    Instant startTime = Instant.now();
    _adminApp.registerBinder(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(_config).to(ControllerConf.class);
        bind(_instanceId).named(BaseControllerStarter.CONTROLLER_INSTANCE_ID);
        bind(startTime).named(ControllerAdminApiApplication.START_TIME);
        bind(metrics).to(ControllerMetrics.class);
        bind(accessControlFactory).to(AccessControlFactory.class);
        bind(metadata).to(NativeControllerMetadata.class);
      }
    });
    _adminApp.registerBinder(new AuditServiceBinder(metadata, ServiceRole.CONTROLLER, metrics));
    _adminApp.start(_listeners, metrics);
  }

  private static void validateBootstrapPolicy(Policy policy) {
    long maxMillis = Long.MAX_VALUE / 1_000_000;
    checkArgument(policy.version() == 1 && policy.minTickIntervalMillis() > 0
        && policy.minTickIntervalMillis() <= maxMillis && policy.leaseRounds() >= 2
        && policy.permitLifetimeMillis() > 0 && policy.permitLifetimeMillis() <= maxMillis
        && policy.maxQueryLifetimeMillis() > 0 && policy.maxQueryLifetimeMillis() <= maxMillis
        && policy.maxRevisionLag() > 0
        && (policy.permitLifetimeMillis() - 1) / policy.minTickIntervalMillis() < policy.leaseRounds() - 1,
        "Bootstrap membership policy requires explicit safe version-one timing bounds");
  }

  private void initializePolicy(Policy policy) throws IOException, InterruptedException {
    long started = System.nanoTime();
    long budget = TimeUnit.MILLISECONDS.toNanos(_startupMillis);
    IOException lastFailure = null;
    while (System.nanoTime() - started < budget) {
      checkClock();
      try {
        Response current = requireNonNull(_access).query(new MembershipQuery(_history, new GetControl()),
            remainingMillis(started, budget));
        checkState(current.status() == Status.OK && current.control() != null,
            "Cannot read membership bootstrap state: %s", current.status());
        if (current.control().policy() != null) {
          return;
        }
        if (current.control().controlEpoch() != null) {
          // A timeout is an unknown result. Every retry still expects version zero, so an existing policy cannot
          // be reset even after leadership changes or competing controllers bootstrap concurrently.
          Response configured = requireNonNull(_access).execute(new MembershipCommand(_history, _instanceId,
              new ConfigurePolicy(current.control().controlEpoch(), 0, policy)), remainingMillis(started, budget));
          if (configured.status() == Status.OK) {
            return;
          }
          checkState(configured.status() == Status.CONFLICT || configured.status() == Status.FENCED
              || configured.status() == Status.NOT_READY, "Membership bootstrap rejected: %s", configured.status());
        }
      } catch (IOException e) {
        lastFailure = e;
      }
      long remaining = budget - (System.nanoTime() - started);
      if (remaining > 0) {
        TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(50)));
      }
    }
    throw new IOException("Timed out initializing native membership through quorum", lastFailure);
  }

  private static long remainingMillis(long started, long budget) throws IOException {
    long remaining = budget - (System.nanoTime() - started);
    if (remaining <= 0) {
      throw new IOException("Native controller startup deadline expired");
    }
    return Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining));
  }

  private void checkClock() throws IOException {
    String failure = requireNonNull(_store).membershipClockFailure();
    if (failure != null) {
      throw new IOException("Membership clock qualification failed: " + failure);
    }
  }

  @Override
  public ServiceStatus.Status getServiceStatus() {
    if (!_started || _closed) {
      return ServiceStatus.Status.NOT_STARTED;
    }
    try {
      checkClock();
      Response response = requireNonNull(_access).query(new MembershipQuery(_history, new GetControl()), 1000);
      if (response.status() != Status.OK || response.control() == null || response.control().policy() == null) {
        throw new IOException("Committed membership policy is unavailable");
      }
      String configFailure = requireNonNull(_metadata).clusterConfigFailure();
      if (configFailure != null) {
        throw new IOException("Native cluster configuration refresh failed: " + configFailure);
      }
      if (_adminApp == null || _adminApp.getHttpServer() == null || !_adminApp.getHttpServer().isStarted()) {
        throw new IOException("Native controller HTTP is not running");
      }
      _status = "Native metadata and controller HTTP initialized; query serving is disabled";
      return ServiceStatus.Status.GOOD;
    } catch (Exception e) {
      _status = "Native metadata is unavailable: " + e.getMessage();
      return ServiceStatus.Status.BAD;
    }
  }

  @Override
  public String getStatusDescription() {
    return _status;
  }

  @Override
  public synchronized void close() throws Exception {
    if (_closed) {
      return;
    }
    _closed = true;
    _started = false;
    Exception failure = null;
    for (AutoCloseable resource : new AutoCloseable[]{_adminApp == null ? null : _adminApp::stop, _metadata,
        _endpoint, _access, _store, _provider}) {
      if (resource != null) {
        try {
          resource.close();
        } catch (Exception e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }
}
