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
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.broker.broker.helix.ClusterChangeHandler;
import org.apache.pinot.common.concurrency.AdjustableSemaphore;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class helps limit the number of multi-stage queries being executed concurrently. Currently, this limit is
 * applied at the broker level, but could be moved to the server level in the future. Note that the cluster
 * configuration is a "per server" value and the broker currently simply assumes that a query will be across all
 * servers. Another assumption here is that queries are evenly distributed across brokers. Ideally, we want to move to a
 * model where the broker asks each server whether it can execute a query stage before dispatching the query stage to
 * the server. This would allow for more fine-grained control over the number of queries being executed concurrently
 * (but there are some limitations around ordering and blocking that need to be solved first).
 */
public class MultiStageQueryThrottler implements ClusterChangeHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiStageQueryThrottler.class);

  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private HelixConfigScope _helixConfigScope;
  private int _numBrokers;
  private int _numServers;
  // If _maxConcurrentQueries is <= 0, it means that the cluster is not configured to limit the number of multi-stage
  // queries that can be executed concurrently. In this case, we should not block the query.
  private int _maxConcurrentQueries;
  private AdjustableSemaphore _semaphore;

  @Override
  public void init(HelixManager helixManager) {
    _helixManager = helixManager;
    _helixAdmin = _helixManager.getClusterManagmentTool();
    _helixConfigScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(
        _helixManager.getClusterName()).build();

    _maxConcurrentQueries = Integer.parseInt(
        _helixAdmin.getConfig(_helixConfigScope,
                Collections.singletonList(CommonConstants.Helix.CONFIG_OF_MAX_CONCURRENT_MULTI_STAGE_QUERIES))
            .getOrDefault(CommonConstants.Helix.CONFIG_OF_MAX_CONCURRENT_MULTI_STAGE_QUERIES,
                CommonConstants.Helix.DEFAULT_MAX_CONCURRENT_MULTI_STAGE_QUERIES));

    List<String> clusterInstances = _helixAdmin.getInstancesInCluster(_helixManager.getClusterName());
    _numBrokers = Math.max(1, (int) clusterInstances.stream()
        .filter(instance -> instance.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE))
        .count());
    _numServers = Math.max(1, (int) clusterInstances.stream()
        .filter(instance -> instance.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE))
        .count());

    if (_maxConcurrentQueries > 0) {
      _semaphore = new AdjustableSemaphore(Math.max(1, _maxConcurrentQueries * _numServers / _numBrokers), true);
    }
  }

  /**
   * Returns true if the query can be executed (waiting until it can be executed if necessary), false otherwise.
   * <p>
   * {@link #release()} should be called after the query is done executing. It is the responsibility of the caller to
   * ensure that {@link #release()} is called exactly once for each call to this method.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @throws InterruptedException if the current thread is interrupted
   */
  public boolean tryAcquire(long timeout, TimeUnit unit)
      throws InterruptedException {
    if (_maxConcurrentQueries <= 0) {
      return true;
    }
    return _semaphore.tryAcquire(timeout, unit);
  }

  /**
   * Should be called after the query is done executing. It is the responsibility of the caller to ensure that this
   * method is called exactly once for each call to {@link #tryAcquire(long, TimeUnit)}.
   */
  public void release() {
    if (_maxConcurrentQueries > 0) {
      _semaphore.release();
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
        if (_maxConcurrentQueries > 0) {
          _semaphore.setPermits(Math.max(1, _maxConcurrentQueries * _numServers / _numBrokers));
        }
      }
    } else {
      int maxConcurrentQueries = Integer.parseInt(
          _helixAdmin.getConfig(_helixConfigScope,
                  Collections.singletonList(CommonConstants.Helix.CONFIG_OF_MAX_CONCURRENT_MULTI_STAGE_QUERIES))
              .getOrDefault(CommonConstants.Helix.CONFIG_OF_MAX_CONCURRENT_MULTI_STAGE_QUERIES,
                  CommonConstants.Helix.DEFAULT_MAX_CONCURRENT_MULTI_STAGE_QUERIES));

      if (_maxConcurrentQueries == maxConcurrentQueries) {
        return;
      }

      if (_maxConcurrentQueries <= 0 && maxConcurrentQueries > 0
          || _maxConcurrentQueries > 0 && maxConcurrentQueries <= 0) {
        // This operation isn't safe to do while queries are running so we require a restart of the broker for this
        // change to take effect.
        LOGGER.warn("Enabling or disabling limitation of the maximum number of multi-stage queries running "
            + "concurrently requires a restart of the broker to take effect");
        return;
      }

      if (maxConcurrentQueries > 0) {
        _semaphore.setPermits(Math.max(1, maxConcurrentQueries * _numServers / _numBrokers));
      }
      _maxConcurrentQueries = maxConcurrentQueries;
    }
  }

  @VisibleForTesting
  int availablePermits() {
    return _semaphore.availablePermits();
  }
}
