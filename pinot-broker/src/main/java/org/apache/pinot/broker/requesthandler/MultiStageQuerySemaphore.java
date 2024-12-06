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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.broker.broker.helix.ClusterChangeHandler;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class helps limit the number of multi-stage queries being executed concurrently. Currently, this limit is
 * applied at the broker level, but could be moved to the server level in the future.
 */
public class MultiStageQuerySemaphore extends Semaphore implements ClusterChangeHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiStageQuerySemaphore.class);

  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private HelixConfigScope _helixConfigScope;
  private int _numBrokers;
  private int _maxConcurrentQueries;
  private int _totalPermits;

  public MultiStageQuerySemaphore(int numBrokers, int maxConcurrentQueries) {
    super(Math.max(1, maxConcurrentQueries / Math.max(numBrokers, 1)), true);
    _maxConcurrentQueries = maxConcurrentQueries;
    _numBrokers = Math.max(1, numBrokers);
    _totalPermits =
        maxConcurrentQueries > 0 ? Math.max(1, maxConcurrentQueries / Math.max(numBrokers, 1)) : maxConcurrentQueries;
  }

  @Override
  public void acquire()
      throws InterruptedException {
    // If _totalPermits is <= 0, it means that the cluster is not configured to limit the number of multi-stage queries
    // that can be executed concurrently. In this case, we should not block the query.
    if (_totalPermits > 0) {
      super.acquire();
    }
  }

  @Override
  public void acquireUninterruptibly() {
    // If _totalPermits is <= 0, it means that the cluster is not configured to limit the number of multi-stage queries
    // that can be executed concurrently. In this case, we should not block the query.
    if (_totalPermits > 0) {
      super.acquireUninterruptibly();
    }
  }

  @Override
  public boolean tryAcquire() {
    // If _totalPermits is <= 0, it means that the cluster is not configured to limit the number of multi-stage queries
    // that can be executed concurrently. In this case, we should not block the query.
    if (_totalPermits > 0) {
      return super.tryAcquire();
    } else {
      return true;
    }
  }

  @Override
  public boolean tryAcquire(long timeout, TimeUnit unit)
      throws InterruptedException {
    // If _totalPermits is <= 0, it means that the cluster is not configured to limit the number of multi-stage queries
    // that can be executed concurrently. In this case, we should not block the query.
    if (_totalPermits > 0) {
      return super.tryAcquire(timeout, unit);
    } else {
      return true;
    }
  }

  @Override
  public void release() {
    if (_totalPermits > 0) {
      super.release();
    }
  }

  @Override
  public void init(HelixManager helixManager) {
    _helixManager = helixManager;
    _helixAdmin = _helixManager.getClusterManagmentTool();
    _helixConfigScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(
        _helixManager.getClusterName()).build();
  }

  @Override
  public void processClusterChange(HelixConstants.ChangeType changeType) {
    Preconditions.checkArgument(
        changeType == HelixConstants.ChangeType.EXTERNAL_VIEW || changeType == HelixConstants.ChangeType.CLUSTER_CONFIG,
        "MultiStageQuerySemaphore can only handle EXTERNAL_VIEW and CLUSTER_CONFIG changes");

    if (changeType == HelixConstants.ChangeType.EXTERNAL_VIEW) {
      int numBrokers =
          Math.max(1, (int) _helixAdmin
              .getInstancesInCluster(_helixManager.getClusterName())
              .stream()
              .filter(instance -> instance.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE))
              .count());

      if (numBrokers != _numBrokers) {
        _numBrokers = numBrokers;
        if (_maxConcurrentQueries > 0) {
          int newTotalPermits = Math.max(1, _maxConcurrentQueries / _numBrokers);
          if (newTotalPermits > _totalPermits) {
            release(newTotalPermits - _totalPermits);
          } else if (newTotalPermits < _totalPermits) {
            reducePermits(_totalPermits - newTotalPermits);
          }
          _totalPermits = newTotalPermits;
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
        int newTotalPermits = Math.max(1, maxConcurrentQueries / _numBrokers);
        if (newTotalPermits > _totalPermits) {
          release(newTotalPermits - _totalPermits);
        } else if (newTotalPermits < _totalPermits) {
          reducePermits(_totalPermits - newTotalPermits);
        }
        _totalPermits = newTotalPermits;
      }
      _maxConcurrentQueries = maxConcurrentQueries;
    }
  }
}
