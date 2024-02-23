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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker.AdaptiveServerSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 *  {@code ServerRoutingStatsManager} manages the query routing stats for each server and used by the Adaptive
 *  Server Selection feature (when enabled). The stats are maintained at the broker and are updated when a query is
 *  submitted to a server and when a server responds after processing a query.
 */
public class ServerRoutingStatsManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerRoutingStatsManager.class);

  private final PinotConfiguration _config;
  private final BrokerMetrics _brokerMetrics;
  private volatile boolean _isEnabled;
  private ConcurrentHashMap<String, ServerRoutingStatsEntry> _serverQueryStatsMap;

  // Main executor service for collecting and aggregating stats for all servers.
  private ExecutorService _executorService;

  // ScheduledExecutorServer for processing periodic tasks like decay.
  private ScheduledExecutorService _periodicTaskExecutor;

  // Warn threshold for Main executor service queue size
  private int _executorQueueSizeWarnThreshold;

  private double _alpha;
  private long _autoDecayWindowMs;
  private long _warmupDurationMs;
  private double _avgInitializationVal;
  private int _hybridScoreExponent;

  public ServerRoutingStatsManager(PinotConfiguration pinotConfig, BrokerMetrics brokerMetrics) {
    _config = pinotConfig;
    _brokerMetrics = brokerMetrics;
  }

  public void init() {
    _isEnabled = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_ENABLE_STATS_COLLECTION,
        AdaptiveServerSelector.DEFAULT_ENABLE_STATS_COLLECTION);
    if (!_isEnabled) {
      LOGGER.info("Server stats collection for Adaptive Server Selection is not enabled.");
      return;
    }

    LOGGER.info("Initializing ServerRoutingStatsManager for Adaptive Server Selection.");

    _alpha = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_EWMA_ALPHA,
        AdaptiveServerSelector.DEFAULT_EWMA_ALPHA);
    _autoDecayWindowMs = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_AUTODECAY_WINDOW_MS,
        AdaptiveServerSelector.DEFAULT_AUTODECAY_WINDOW_MS);
    _warmupDurationMs = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_WARMUP_DURATION_MS,
        AdaptiveServerSelector.DEFAULT_WARMUP_DURATION_MS);
    _avgInitializationVal = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_AVG_INITIALIZATION_VAL,
        AdaptiveServerSelector.DEFAULT_AVG_INITIALIZATION_VAL);
    _hybridScoreExponent = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_HYBRID_SCORE_EXPONENT,
        AdaptiveServerSelector.DEFAULT_HYBRID_SCORE_EXPONENT);

    int threadPoolSize = _config.getProperty(AdaptiveServerSelector.CONFIG_OF_STATS_MANAGER_THREADPOOL_SIZE,
        AdaptiveServerSelector.DEFAULT_STATS_MANAGER_THREADPOOL_SIZE);
    _executorService = Executors.newFixedThreadPool(threadPoolSize);

    _executorQueueSizeWarnThreshold = _config.getProperty(
        AdaptiveServerSelector.CONFIG_OF_STATS_MANAGER_QUEUE_SIZE_WARN_THRESHOLD,
        AdaptiveServerSelector.DEFAULT_STATS_MANAGER_QUEUE_SIZE_WARN_THRESHOLD);

    _periodicTaskExecutor = Executors.newSingleThreadScheduledExecutor();

    // Entries in this map are never deleted unless the broker process restarts. This is okay for now because the
    // number of servers will be finite and should not cause memory bloat.
    _serverQueryStatsMap = new ConcurrentHashMap<>();
  }

  public boolean isEnabled() {
    return _isEnabled;
  }

  public void shutDown() {
    // As the stats are not persistent, shutdown need not wait for task termination.
    if (!_isEnabled) {
      return;
    }

    LOGGER.info("Shutting down ServerRoutingStatsManager.");
    _isEnabled = false;
    _executorService.shutdownNow();
  }

  public int getQueueSize() {
    if (!_isEnabled) {
      return 0;
    }

    ThreadPoolExecutor tpe = (ThreadPoolExecutor) _executorService;
    return tpe.getQueue().size();
  }

  public long getCompletedTaskCount() {
    if (!_isEnabled) {
      return 0;
    }

    ThreadPoolExecutor tpe = (ThreadPoolExecutor) _executorService;
    return tpe.getCompletedTaskCount();
  }

  /**
   * Called when a query is submitted to a server. Updates stats corresponding to query submission.
   */
  public void recordStatsAfterQuerySubmission(long requestId, String serverInstanceId) {
    if (!_isEnabled) {
      return;
    }

    _executorService.execute(() -> {
      try {
        recordQueueSizeMetrics();
        updateStatsAfterQuerySubmission(serverInstanceId);
      } catch (Exception e) {
        LOGGER.error("Exception caught while updating stats. requestId={}, exception={}", requestId, e);
      }
    });
  }

  private void updateStatsAfterQuerySubmission(String serverInstanceId) {
    ServerRoutingStatsEntry stats = _serverQueryStatsMap.computeIfAbsent(serverInstanceId,
        k -> new ServerRoutingStatsEntry(serverInstanceId, _alpha, _autoDecayWindowMs, _warmupDurationMs,
            _avgInitializationVal, _hybridScoreExponent, _periodicTaskExecutor));

    try {
      stats.getServerWriteLock().lock();
      stats.updateNumInFlightRequestsForQuerySubmission();
    } finally {
      stats.getServerWriteLock().unlock();
    }
  }

  /**
   * Called when a query response is received from the server. Updates stats related to query completion.
   */
  public void recordStatsUponResponseArrival(long requestId, String serverInstanceId, long latency) {
    if (!_isEnabled) {
      return;
    }

    _executorService.execute(() -> {
      try {
        updateStatsUponResponseArrival(serverInstanceId, latency);
      } catch (Exception e) {
        LOGGER.error("Exception caught while updating stats. requestId={}, exception={}", requestId, e);
      }
    });
  }

  private void updateStatsUponResponseArrival(String serverInstanceId, long latencyMs) {
    ServerRoutingStatsEntry stats = _serverQueryStatsMap.computeIfAbsent(serverInstanceId,
        k -> new ServerRoutingStatsEntry(serverInstanceId, _alpha, _autoDecayWindowMs, _warmupDurationMs,
            _avgInitializationVal, _hybridScoreExponent, _periodicTaskExecutor));

    try {
      stats.getServerWriteLock().lock();
      stats.updateNumInFlightRequestsForResponseArrival();
      if (latencyMs >= 0.0) {
        stats.updateLatency(latencyMs);
      }
    } finally {
      stats.getServerWriteLock().unlock();
    }
  }

  /**
   * Returns ServerRoutingStatsStr for debugging/logging.
   */
  public String getServerRoutingStatsStr() {
    if (!_isEnabled) {
      return "";
    }

    StringBuilder stringBuilder =
        new StringBuilder("(Server=NumInFlightRequests,NumInFlightRequestsEMA,LatencyEMA," + "Score)");

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      Integer numInFlightRequests = stats.getNumInFlightRequests();
      Double numInFlightRequestsEMA = stats.getInFlightRequestsEMA();
      Double latencyEMA = stats.getLatencyEMA();
      Double score = stats.computeHybridScore();
      stats.getServerReadLock().unlock();

      stringBuilder.append(";").append(server).append("=").append(numInFlightRequests.toString()).append(",")
          .append(numInFlightRequestsEMA.toString()).append(",").append(latencyEMA.toString()).append(",")
          .append(score.toString());
    }

    return stringBuilder.toString();
  }

  /*
   * ===================================================================================================================
   * The helper functions required by various AdaptiveSelectors are defined below.
   *
   * 1. NumInFlightReqSelector - fetchNumInFlightRequestsForAllServers(), fetchNumInFlightRequestsForServer()
   * 2. LatencySelector - fetchEMALatencyForAllServers(), fetchEMALatencyForServer()
   * 3. HybridSelector - fetchScoreForAllServers(), fetchScoreForServer()
   *
   * We avoid returning all the stats to each selector to keep the critical section (under locks) as small as
   * possible). ServerRoutingStatsManager does not sort the servers in any particular order while accumulating stats
   * as it is not aware of what sorting strategy to use. This logic is contained in the various
   * AdaptiveServerSelectors.
   *
   * TODO: Explore if reads to the _serverQueryStatsMap can be done without locking.
   * ===================================================================================================================
   */

  /**
   * Returns a list containing each server and the corresponding number of in-flight requests active on the server.
   */
  public List<Pair<String, Integer>> fetchNumInFlightRequestsForAllServers() {
    List<Pair<String, Integer>> response = new ArrayList<>();
    if (!_isEnabled) {
      return response;
    }

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      int numInFlightRequests = stats.getNumInFlightRequests();
      stats.getServerReadLock().unlock();

      response.add(new ImmutablePair<>(server, numInFlightRequests));
    }

    return response;
  }

  /**
   * Same as above but returns the number of inflight requests for the input server.
   */
  public Integer fetchNumInFlightRequestsForServer(String server) {
    if (!_isEnabled) {
      return null;
    }

    ServerRoutingStatsEntry stats = _serverQueryStatsMap.get(server);
    if (stats == null) {
      return null;
    }

    try {
      stats.getServerReadLock().lock();
      return stats.getNumInFlightRequests();
    } finally {
      stats.getServerReadLock().unlock();
    }
  }

  /**
   * Returns a list containing each server and the corresponding EMA latency seen for queries on the server.
   */
  public List<Pair<String, Double>> fetchEMALatencyForAllServers() {
    List<Pair<String, Double>> response = new ArrayList<>();
    if (!_isEnabled) {
      return response;
    }

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      double latency = stats.getLatencyEMA();
      stats.getServerReadLock().unlock();

      response.add(new ImmutablePair<>(server, latency));
    }

    return response;
  }

  /**
   * Same as above but returns the EMA latency for the input server.
   */
  public Double fetchEMALatencyForServer(String server) {
    if (!_isEnabled) {
      return null;
    }

    ServerRoutingStatsEntry stats = _serverQueryStatsMap.get(server);
    if (stats == null) {
      return null;
    }

    try {
      stats.getServerReadLock().lock();
      return stats.getLatencyEMA();
    } finally {
      stats.getServerReadLock().unlock();
    }
  }

  /**
   * Returns a list containing each server and the corresponding Hybrid score for each server. The Hybrid score is
   * calculated based on https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf.
   */
  public List<Pair<String, Double>> fetchHybridScoreForAllServers() {
    List<Pair<String, Double>> response = new ArrayList<>();
    if (!_isEnabled) {
      return response;
    }

    for (Map.Entry<String, ServerRoutingStatsEntry> entry : _serverQueryStatsMap.entrySet()) {
      String server = entry.getKey();
      Preconditions.checkState(entry.getValue() != null, "Server stats is null");
      ServerRoutingStatsEntry stats = entry.getValue();

      stats.getServerReadLock().lock();
      double score = stats.computeHybridScore();
      stats.getServerReadLock().unlock();

      response.add(new ImmutablePair<>(server, score));
    }

    return response;
  }

  /**
   * Same as above but returns the score for a single server.
   */
  public Double fetchHybridScoreForServer(String server) {
    if (!_isEnabled) {
      return null;
    }

    ServerRoutingStatsEntry stats = _serverQueryStatsMap.get(server);
    if (stats == null) {
      return null;
    }

    try {
      stats.getServerReadLock().lock();
      return stats.computeHybridScore();
    } finally {
      stats.getServerReadLock().unlock();
    }
  }

  private void recordQueueSizeMetrics() {
    int queueSize = getQueueSize();
    _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.ROUTING_STATS_MANAGER_QUEUE_SIZE, queueSize);
    if (queueSize > _executorQueueSizeWarnThreshold) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.ROUTING_STATS_MANAGER_Q_LIMIT_REACHED, 1L);
    }
  }
}
