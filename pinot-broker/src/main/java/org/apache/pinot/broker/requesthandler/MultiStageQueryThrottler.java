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
package org.apache.pinot.broker.requesthandler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.broker.broker.helix.ClusterChangeHandler;
import org.apache.pinot.common.concurrency.AdjustableSemaphore;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadExceedStrategy;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class helps limit the number of multi-stage queries being executed concurrently. Note that the cluster
 * configuration is a "per server" value and the broker currently computes the max server query threads as
 * <em>CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS * numServers / numBrokers</em>. Note that the config value,
 * number of servers, and number of brokers are all dynamically updated here.
 * <p>
 * Another assumption made here is that queries are evenly distributed across brokers.
 * <p>
 * This is designed to limit the number of multi-stage queries being concurrently executed across a cluster and is not
 * intended to prevent individual large queries from being executed.
 */
public class MultiStageQueryThrottler implements ClusterChangeHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiStageQueryThrottler.class);

  private final int _maxServerQueryThreadsFromBrokerConfig;
  private final QueryThreadExceedStrategy _exceedStrategy;
  private final AtomicInteger _currentQueryServerThreads = new AtomicInteger();
  /**
   * If _maxServerQueryThreads is <= 0, it means that the cluster is not configured to limit the number of multi-stage
   * queries that can be executed concurrently. In this case, we should not block the query.
   */
  private int _maxServerQueryThreads;
  private int _numBrokers;
  private int _numServers;
  private AdjustableSemaphore _semaphore;

  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private HelixConfigScope _helixConfigScope;

  public MultiStageQueryThrottler(PinotConfiguration brokerConf) {
    _maxServerQueryThreadsFromBrokerConfig = brokerConf.getProperty(
        CommonConstants.Broker.CONFIG_OF_MSE_MAX_SERVER_QUERY_THREADS,
        CommonConstants.Broker.DEFAULT_MSE_MAX_SERVER_QUERY_THREADS);

    String strategyStr = brokerConf.getProperty(
        CommonConstants.Broker.CONFIG_OF_MSE_MAX_SERVER_QUERY_THREADS_EXCEED_STRATEGY,
        CommonConstants.Broker.DEFAULT_MSE_MAX_SERVER_QUERY_THREADS_EXCEED_STRATEGY);
    QueryThreadExceedStrategy strategy;
    try {
      strategy = QueryThreadExceedStrategy.valueOf(strategyStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid exceed strategy: {}, using default: {}", strategyStr,
          CommonConstants.Broker.DEFAULT_MSE_MAX_SERVER_QUERY_THREADS_EXCEED_STRATEGY);
      strategy = QueryThreadExceedStrategy.valueOf(
          CommonConstants.Broker.DEFAULT_MSE_MAX_SERVER_QUERY_THREADS_EXCEED_STRATEGY);
    }
    _exceedStrategy = strategy;
  }

  @Override
  public void init(HelixManager helixManager) {
    _helixManager = helixManager;
    _helixAdmin = _helixManager.getClusterManagmentTool();
    _helixConfigScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(
        _helixManager.getClusterName()).build();
    _maxServerQueryThreads = calculateMaxServerQueryThreads();

    List<String> clusterInstances = _helixAdmin.getInstancesInCluster(_helixManager.getClusterName());
    _numBrokers = Math.max(1, (int) clusterInstances.stream()
        .filter(instance -> instance.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE))
        .count());
    _numServers = Math.max(1, (int) clusterInstances.stream()
        .filter(instance -> instance.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE))
        .count());

    if (_maxServerQueryThreads > 0) {
      int semaphoreLimit = calculateSemaphoreLimit();
      _semaphore = new AdjustableSemaphore(semaphoreLimit, true);
    }
  }

  /**
   * Returns true if the query can be executed (waiting until it can be executed if necessary), false otherwise.
   * <p>
   * {@link #release(int)} should be called after the query is done executing. It is the responsibility of the caller to
   * ensure that {@link #release(int)} is called exactly once for each call to this method.
   *
   * @param numQueryThreads the estimated number of query server threads
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   *
   * @throws InterruptedException if the current thread is interrupted
   * @throws RuntimeException if the query can never be dispatched due to the number of estimated query server threads
   * being greater than the maximum number of server query threads calculated on the basis of
   * <em>CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS * numServers / numBrokers</em>
   */
  public boolean tryAcquire(int numQueryThreads, long timeout, TimeUnit unit)
      throws InterruptedException {
    if (_maxServerQueryThreads <= 0) {
      _currentQueryServerThreads.addAndGet(numQueryThreads);
      return true;
    }

    if (numQueryThreads > _semaphore.getTotalPermits() && _exceedStrategy != QueryThreadExceedStrategy.LOG) {
      throw new RuntimeException(
          String.format("Can't dispatch query because the estimated number of server threads for this query is too "
              + "large for the configured value of '%s' or '%s'. estimatedThreads=%d configuredLimit=%d",
                  CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS,
                  CommonConstants.Broker.CONFIG_OF_MSE_MAX_SERVER_QUERY_THREADS,
                  numQueryThreads, _semaphore.getTotalPermits()));
    }

    if (_exceedStrategy == QueryThreadExceedStrategy.LOG) {
      boolean wouldThrottle = _currentQueryServerThreads.get() + numQueryThreads > _maxServerQueryThreads;
      if (wouldThrottle) {
        LOGGER.warn(
            "Exceed strategy LOG: Query would have been throttled. estimatedThreads: {} availableThreads: {}",
            numQueryThreads, _maxServerQueryThreads - _currentQueryServerThreads.get());
      }
      _currentQueryServerThreads.addAndGet(numQueryThreads);
      return true;
    } else if (_exceedStrategy == QueryThreadExceedStrategy.WAIT) {
      boolean result = _semaphore.tryAcquire(numQueryThreads, timeout, unit);
      if (result) {
        _currentQueryServerThreads.addAndGet(numQueryThreads);
      }
      return result;
    } else {
      throw new IllegalStateException(String.format(
          "%s is configured to an unsupported strategy.", this.getClass().getName()));
    }
  }

  /**
   * Should be called after the query is done executing. It is the responsibility of the caller to ensure that this
   * method is called exactly once for each call to {@link #tryAcquire(int, long, TimeUnit)}.
   */
  public void release(int numQueryThreads) {
    _currentQueryServerThreads.addAndGet(-1 * numQueryThreads);
    if (_maxServerQueryThreads > 0 && _exceedStrategy == QueryThreadExceedStrategy.WAIT) {
      _semaphore.release(numQueryThreads);
    }
  }

  @Override
  public void processClusterChange(HelixConstants.ChangeType changeType) {
    Preconditions.checkArgument(
        changeType == HelixConstants.ChangeType.EXTERNAL_VIEW || changeType == HelixConstants.ChangeType.CLUSTER_CONFIG,
        "MultiStageQuerySemaphore can only handle EXTERNAL_VIEW and CLUSTER_CONFIG changes");

    if (changeType == HelixConstants.ChangeType.EXTERNAL_VIEW) {
      List<String> clusterInstances = _helixAdmin.getInstancesInCluster(_helixManager.getClusterName());
      int numBrokers = Math.max(1, (int) clusterInstances.stream()
          .filter(instance -> instance.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE))
          .count());
      int numServers = Math.max(1, (int) clusterInstances.stream()
          .filter(instance -> instance.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE))
          .count());

      if (numBrokers != _numBrokers || numServers != _numServers) {
        _numBrokers = numBrokers;
        _numServers = numServers;
        if (_maxServerQueryThreads > 0) {
          int semaphoreLimit = calculateSemaphoreLimit();
          _semaphore.setPermits(semaphoreLimit);
        }
      }
    } else {
      int newMaxServerQueryThreads = calculateMaxServerQueryThreads();

      if (_maxServerQueryThreads == newMaxServerQueryThreads) {
        return;
      }

      if (_maxServerQueryThreads <= 0 && newMaxServerQueryThreads > 0
          || _maxServerQueryThreads > 0 && newMaxServerQueryThreads <= 0) {
        // This operation isn't safe to do while queries are running so we require a restart of the broker for this
        // change to take effect.
        LOGGER.warn("Enabling or disabling limitation of the maximum number of multi-stage queries running "
            + "concurrently requires a restart of the broker to take effect");
        return;
      }

      if (newMaxServerQueryThreads > 0) {
        _maxServerQueryThreads = newMaxServerQueryThreads;
        int semaphoreLimit = calculateSemaphoreLimit();
        _semaphore.setPermits(semaphoreLimit);
      }
    }
  }

  public int currentQueryServerThreads() {
    return _currentQueryServerThreads.get();
  }

  @VisibleForTesting
  int availablePermits() {
    return _semaphore.availablePermits();
  }

  @VisibleForTesting
  int calculateMaxServerQueryThreads() {
    if (_maxServerQueryThreadsFromBrokerConfig > 0) {
      return _maxServerQueryThreadsFromBrokerConfig;
    }
    return Integer.parseInt(_helixAdmin.getConfig(_helixConfigScope,
        Collections.singletonList(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS))
        .getOrDefault(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS,
            CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS));
  }

  private int calculateSemaphoreLimit() {
    int semaphoreLimit = Math.max(1, _maxServerQueryThreads * _numServers / _numBrokers);
    LOGGER.info("Calculating estimated server query threads limit: {} for maxServerQueryThreads: {}, "
            + "numBrokers: {}, numServers: {}, exceedStrategy: {}",
        semaphoreLimit, _maxServerQueryThreads, _numBrokers, _numServers, _exceedStrategy);
    return semaphoreLimit;
  }
}
