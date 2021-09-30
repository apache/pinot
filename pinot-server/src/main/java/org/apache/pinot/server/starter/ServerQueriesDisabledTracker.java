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
package org.apache.pinot.server.starter;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.HelixManager;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class to track if the server is queries disabled.
 */
public class ServerQueriesDisabledTracker {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueriesDisabledTracker.class);
  private static final long FETCH_INTERVAL_MINS = 10L;

  private final ScheduledExecutorService _executorService = Executors.newSingleThreadScheduledExecutor();
  private final HelixManager _helixManager;
  private final ServerMetrics _serverMetrics;
  private final String _clusterName;
  private final String _instanceId;
  private AtomicBoolean _queriesDisabled = new AtomicBoolean();

  public ServerQueriesDisabledTracker(String helixClusterName, String instanceId, HelixManager helixManager,
      ServerMetrics serverMetrics) {
    _helixManager = helixManager;
    _serverMetrics = serverMetrics;
    _clusterName = helixClusterName;
    _instanceId = instanceId;
  }

  public void start() {
    _serverMetrics.addCallbackGauge(CommonConstants.Helix.QUERIES_DISABLED, () -> _queriesDisabled.get() ? 1L : 0L);
    LOGGER.info("Tracking server queries disabled.");
    _executorService.scheduleWithFixedDelay(() -> {
      ZNRecord instanceConfigZNRecord =
          _helixManager.getConfigAccessor().getInstanceConfig(_clusterName, _instanceId).getRecord();
      if (instanceConfigZNRecord == null) {
        LOGGER.error("Failed to get instance config: {} in {} from zookeeper", _instanceId, _clusterName);
      } else {
        _queriesDisabled.set(instanceConfigZNRecord.getBooleanField(CommonConstants.Helix.QUERIES_DISABLED, false));
      }
    }, 0L, FETCH_INTERVAL_MINS, TimeUnit.MINUTES);
  }

  public void stop() {
    LOGGER.info("Stopped tracking server queries disabled.");
    _executorService.shutdown();
  }
}
