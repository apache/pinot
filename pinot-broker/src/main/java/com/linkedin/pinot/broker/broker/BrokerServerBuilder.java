/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.broker.broker;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.broker.broker.helix.LiveInstancesChangeListenerImpl;
import com.linkedin.pinot.broker.queryquota.TableQueryQuotaManager;
import com.linkedin.pinot.broker.requesthandler.BrokerRequestHandler;
import com.linkedin.pinot.broker.requesthandler.ConnectionPoolBrokerRequestHandler;
import com.linkedin.pinot.broker.requesthandler.SingleConnectionBrokerRequestHandler;
import com.linkedin.pinot.broker.routing.RoutingTable;
import com.linkedin.pinot.broker.routing.TimeBoundaryService;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BrokerServerBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServerBuilder.class);

  public static final String DELAY_SHUTDOWN_TIME_MS_CONFIG = "pinot.broker.delayShutdownTimeMs";
  public static final long DEFAULT_DELAY_SHUTDOWN_TIME_MS = 10_000;
  public static final String ACCESS_CONTROL_PREFIX = "pinot.broker.access.control";
  public static final String METRICS_CONFIG_PREFIX = "pinot.broker.metrics";
  public static final String TABLE_LEVEL_METRICS_CONFIG = "pinot.broker.enableTableLevelMetrics";
  public static final String REQUEST_HANDLER_TYPE_CONFIG = "pinot.broker.requestHandlerType";
  public static final String DEFAULT_REQUEST_HANDLER_TYPE = "connectionPool";
  public static final String SINGLE_CONNECTION_REQUEST_HANDLER_TYPE = "singleConnection";

  public enum State {
    INIT,
    STARTING,
    RUNNING,
    SHUTTING_DOWN,
    SHUTDOWN
  }

  // Running State Of broker
  private final AtomicReference<State> _state = new AtomicReference<>();

  private final Configuration _config;
  private final long _delayedShutdownTimeMs;
  private final RoutingTable _routingTable;
  private final TimeBoundaryService _timeBoundaryService;
  private final LiveInstancesChangeListenerImpl _liveInstanceChangeListener;
  private final TableQueryQuotaManager _tableQueryQuotaManager;
  private final AccessControlFactory _accessControlFactory;
  private final MetricsRegistry _metricsRegistry;
  private final BrokerMetrics _brokerMetrics;
  private final BrokerRequestHandler _brokerRequestHandler;
  private final BrokerAdminApiApplication _brokerAdminApplication;

  public BrokerServerBuilder(Configuration config, RoutingTable routingTable, TimeBoundaryService timeBoundaryService,
      LiveInstancesChangeListenerImpl liveInstanceChangeListener, TableQueryQuotaManager tableQueryQuotaManager) {
    _state.set(State.INIT);
    _config = config;
    _delayedShutdownTimeMs = config.getLong(DELAY_SHUTDOWN_TIME_MS_CONFIG, DEFAULT_DELAY_SHUTDOWN_TIME_MS);
    _routingTable = routingTable;
    _timeBoundaryService = timeBoundaryService;
    _liveInstanceChangeListener = liveInstanceChangeListener;
    _tableQueryQuotaManager = tableQueryQuotaManager;
    _accessControlFactory = AccessControlFactory.loadFactory(_config.subset(ACCESS_CONTROL_PREFIX));
    _metricsRegistry = new MetricsRegistry();
    MetricsHelper.initializeMetrics(config.subset(METRICS_CONFIG_PREFIX));
    MetricsHelper.registerMetricsRegistry(_metricsRegistry);
    _brokerMetrics = new BrokerMetrics(_metricsRegistry, !_config.getBoolean(TABLE_LEVEL_METRICS_CONFIG, true));
    _brokerMetrics.initializeGlobalMeters();
    _brokerRequestHandler = buildRequestHandler();
    _brokerAdminApplication = new BrokerAdminApiApplication(this);
  }

  private BrokerRequestHandler buildRequestHandler() {
    String requestHandlerType = _config.getString(REQUEST_HANDLER_TYPE_CONFIG, DEFAULT_REQUEST_HANDLER_TYPE);
    if (requestHandlerType.equalsIgnoreCase(SINGLE_CONNECTION_REQUEST_HANDLER_TYPE)) {
      LOGGER.info("Using SingleConnectionBrokerRequestHandler");
      return new SingleConnectionBrokerRequestHandler(_config, _routingTable, _timeBoundaryService,
          _accessControlFactory, _tableQueryQuotaManager, _brokerMetrics);
    } else {
      LOGGER.info("Using ConnectionPoolBrokerRequestHandler");
      return new ConnectionPoolBrokerRequestHandler(_config, _routingTable, _timeBoundaryService, _accessControlFactory,
          _tableQueryQuotaManager, _brokerMetrics, _liveInstanceChangeListener, _metricsRegistry);
    }
  }

  public void start() {
    LOGGER.info("Starting Pinot Broker");
    Utils.logVersions();

    Preconditions.checkState(_state.get() == State.INIT);
    _state.set(State.STARTING);

    _brokerRequestHandler.start();
    _brokerAdminApplication.start(_config.getInt(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT,
        CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT));

    _state.set(State.RUNNING);
    LOGGER.info("Pinot Broker started");
  }

  public void stop() {
    LOGGER.info("Shutting down Pinot Broker");

    try {
      Thread.sleep(_delayedShutdownTimeMs);
    } catch (Exception e) {
      LOGGER.error("Caught exception while waiting for shutdown delay period of {}ms", _delayedShutdownTimeMs, e);
    }

    Preconditions.checkState(_state.get() == State.RUNNING);
    _state.set(State.SHUTTING_DOWN);

    _brokerRequestHandler.shutDown();
    _brokerAdminApplication.stop();

    _state.set(State.SHUTDOWN);
    LOGGER.info("Finish shutting down Pinot Broker");
  }

  public State getCurrentState() {
    return _state.get();
  }

  public RoutingTable getRoutingTable() {
    return _routingTable;
  }

  public TimeBoundaryService getTimeBoundaryService() {
    return _timeBoundaryService;
  }

  public AccessControlFactory getAccessControlFactory() {
    return _accessControlFactory;
  }

  public MetricsRegistry getMetricsRegistry() {
    return _metricsRegistry;
  }

  public BrokerMetrics getBrokerMetrics() {
    return _brokerMetrics;
  }

  public BrokerRequestHandler getBrokerRequestHandler() {
    return _brokerRequestHandler;
  }
}
