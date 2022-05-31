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

package org.apache.pinot.core.transport.server.routing.stats;

import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerRoutingStatsManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerRoutingStatsManager.class);

  private volatile boolean _isEnabled;
  private Map<String, ServerRoutingStatsEntry> _serverQueryStatsMap;
  private ExecutorService _executorService;

  public void init() {
    _isEnabled = true;
    _executorService = Executors.newFixedThreadPool(2);
    _serverQueryStatsMap = new ConcurrentHashMap<>();
  }

  public boolean isEnabled() {
    return _isEnabled;
  }

  public void shutDown() {
    // As the stats are not persistent, shutdown need not wait for task termination.
    _isEnabled = false;
    _executorService.shutdownNow();
  }

  // TODO(Vivek): Expand with other parameters.
  public void recordStatsForQuerySubmit(String serverInstanceId) {
    Preconditions.checkState(_isEnabled, "ServerRoutingStatsManager is not enabled.");

    // Increment numOutstandingRequests
    // TODO(Vivek): Add getters and setters for ServerRoutingStatsEntry.
    _executorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          updateQuerySubmitStats(serverInstanceId);
        } catch (Exception e) {
          LOGGER.error("Exception caught while updating stats", e);
          e.printStackTrace();
        }
      }
    });
  }

  // TODO(Vivek): Expand with other parameters.
  public void recordStatsForQueryCompletion(String serverInstanceId, int latency) {
    Preconditions.checkState(_isEnabled, "ServerRoutingStatsManager is not enabled.");

    _executorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          updateQueryCompletionStats(serverInstanceId, latency);
        } catch (Exception e) {
          LOGGER.error("Exception caught while updating stats", e);
          e.printStackTrace();
        }
      }
    });
  }

  public ServerRoutingStatsEntry getServerStats(String serverInstanceId) {
    return _serverQueryStatsMap.get(serverInstanceId);
  }

  public Iterator<Map.Entry<String, ServerRoutingStatsEntry>> getServerRoutingStatsItr() {
    return _serverQueryStatsMap.entrySet().iterator();
  }

  private void updateQuerySubmitStats(String serverInstanceId) {
    ServerRoutingStatsEntry stats =
        _serverQueryStatsMap.computeIfAbsent(serverInstanceId, k -> new ServerRoutingStatsEntry(serverInstanceId));

    // Number of in-flight requests
    stats.incrementNumInFlightRequests();
  }

  private void updateQueryCompletionStats(String serverInstanceId, int latencyMs) {
    ServerRoutingStatsEntry stats =
        _serverQueryStatsMap.computeIfAbsent(serverInstanceId, k -> new ServerRoutingStatsEntry(serverInstanceId));

    // Number of in-flight requests
    stats.decrementNumInFlightRequests();

    // Latency
    stats.recordLatency(latencyMs);
  }
}
