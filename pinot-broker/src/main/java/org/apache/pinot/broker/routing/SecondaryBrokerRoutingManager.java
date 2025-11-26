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
package org.apache.pinot.broker.routing;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SecondaryBrokerRoutingManager extends BrokerRoutingManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecondaryBrokerRoutingManager.class);
  private static final long ROUTING_CHANGE_DETECTION_INTERVAL_MS = 10_000L;

  private final AtomicBoolean _processChangeInRouting = new AtomicBoolean(false);
  private final ScheduledExecutorService _routingChangeExecutor;

  public SecondaryBrokerRoutingManager(BrokerMetrics brokerMetrics,
      ServerRoutingStatsManager serverRoutingStatsManager, PinotConfiguration pinotConfig) {
    super(brokerMetrics, serverRoutingStatsManager, pinotConfig);
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
    LOGGER.info("Processing routing changes for cluster: {}", _parentClusterName);
    long startTimeMs = System.currentTimeMillis();
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.SECONDARY_BROKER_ROUTING_CALCULATION_COUNT, 1L);

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

    _brokerMetrics.addTimedValue(BrokerTimer.SECONDARY_BROKER_ROUTING_CALCULATION_TIME_MS,
        System.currentTimeMillis() - startTimeMs, TimeUnit.MILLISECONDS);
  }

  private void addRouting(String table) {
    LOGGER.info("Adding routing for table: {} in cluster: {}", table, _parentClusterName);
    if (ZKMetadataProvider.isLogicalTableExists(_propertyStore, table)) {
      buildRoutingForLogicalTable(table);
    } else {
      buildRouting(table);
    }
  }

  private void dropRouting(String table) {
    LOGGER.info("Dropping routing for table: {} in cluster: {}", table, _parentClusterName);
    if (ZKMetadataProvider.isLogicalTableExists(_propertyStore, table)) {
      removeRoutingForLogicalTable(table);
    } else {
      removeRouting(table);
    }
  }

  @Override
  protected void processSegmentAssignmentChangeInternal() {
    super.processSegmentAssignmentChangeInternal();
    _processChangeInRouting.set(true);
  }
}
