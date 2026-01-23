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
package org.apache.pinot.broker.routing.manager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code RemoteClusterBrokerRoutingManager} manages the routing for a single remote Pinot cluster available for
 * federation. It periodically checks for changes in the set of tables available in the remote cluster and updates the
 * routing accordingly.
 */
public class RemoteClusterBrokerRoutingManager extends BaseBrokerRoutingManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteClusterBrokerRoutingManager.class);
  private static final long ROUTING_CHANGE_DETECTION_INTERVAL_MS = 10_000L;

  private final String _remoteClusterName;
  private final AtomicBoolean _processChangeInRouting = new AtomicBoolean(false);
  private final ScheduledExecutorService _routingChangeExecutor;

  public RemoteClusterBrokerRoutingManager(String remoteClusterName, BrokerMetrics brokerMetrics,
      ServerRoutingStatsManager serverRoutingStatsManager, PinotConfiguration pinotConfig) {
    super(brokerMetrics, serverRoutingStatsManager, pinotConfig);
    _remoteClusterName = remoteClusterName;
    _routingChangeExecutor = Executors.newSingleThreadScheduledExecutor();
    _routingChangeExecutor.scheduleAtFixedRate(this::determineRoutingChangeForTables, 0,
        ROUTING_CHANGE_DETECTION_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    _routingChangeExecutor.shutdownNow();
  }

  public void determineRoutingChangeForTables() {
    if (!_processChangeInRouting.getAndSet(false)) {
      return;
    }
    LOGGER.info("Processing routing changes for remote cluster: {}", _remoteClusterName);
    long startTimeMs = System.currentTimeMillis();

    try {
      Preconditions.checkArgument(_externalViewPathPrefix.endsWith("/"),
          "External view path prefix: %s should end with a '/'", _externalViewPathPrefix);
      String externalViewPath = _externalViewPathPrefix.substring(0, _externalViewPathPrefix.length() - 1);
      Set<String> currentTables = _zkDataAccessor.getChildNames(externalViewPath, 0).stream()
          .filter(t -> TableNameBuilder.getTableTypeFromTableName(t) != null)
          .collect(Collectors.toSet());
      Set<String> knownTables = new HashSet<>(_routingEntryMap.keySet());

      Set<String> toAdd = new HashSet<>(currentTables);
      toAdd.removeAll(knownTables);
      Set<String> toRemove = new HashSet<>(knownTables);
      toRemove.removeAll(currentTables);

      toAdd.forEach(this::addRouting);
      toRemove.forEach(this::dropRouting);
    } catch (Exception e) {
      LOGGER.error("Caught exception while determining routing changes for remote cluster: {}", _remoteClusterName, e);
    } finally {
      _brokerMetrics.addTimedValue(_remoteClusterName, BrokerTimer.REMOTE_CLUSTER_BROKER_ROUTING_CALCULATION_TIME_MS,
          System.currentTimeMillis() - startTimeMs, TimeUnit.MILLISECONDS);
    }
  }

  private void addRouting(String table) {
    LOGGER.info("Adding routing for table: {} in cluster: {}", table, _remoteClusterName);
    try {
      if (ZKMetadataProvider.isLogicalTableExists(_propertyStore, table)) {
        buildRoutingForLogicalTable(table);
      } else {
        buildRouting(table);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while adding routing for table: {} in cluster: {}", table, _remoteClusterName, e);
    }
  }

  private void dropRouting(String table) {
    LOGGER.info("Dropping routing for table: {} in cluster: {}", table, _remoteClusterName);
    try {
      if (ZKMetadataProvider.isLogicalTableExists(_propertyStore, table)) {
        removeRoutingForLogicalTable(table);
      } else {
        removeRouting(table);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while dropping routing for table: {} in cluster: {}", table, _remoteClusterName, e);
    }
  }

  @Override
  protected void processSegmentAssignmentChangeInternal() {
    super.processSegmentAssignmentChangeInternal();
    _processChangeInRouting.set(true);
  }

  @VisibleForTesting
  boolean hasRoutingChangeScheduled() {
    return _processChangeInRouting.get();
  }

  @VisibleForTesting
  boolean isExecutorShutdown() {
    return _routingChangeExecutor.isShutdown() || _routingChangeExecutor.isTerminated();
  }
}
