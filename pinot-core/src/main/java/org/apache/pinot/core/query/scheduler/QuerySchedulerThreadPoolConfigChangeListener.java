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
package org.apache.pinot.core.query.scheduler;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Listens for cluster config changes to dynamically resize the query scheduler thread pools
 * ({@code query_runner_threads} and {@code query_worker_threads}).
 */
public class QuerySchedulerThreadPoolConfigChangeListener implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuerySchedulerThreadPoolConfigChangeListener.class);

  static final String QUERY_RUNNER_THREADS_KEY =
      CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + ResourceManager.QUERY_RUNNER_CONFIG_KEY;
  static final String QUERY_WORKER_THREADS_KEY =
      CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + ResourceManager.QUERY_WORKER_CONFIG_KEY;

  private final ResourceManager _resourceManager;

  public QuerySchedulerThreadPoolConfigChangeListener(ResourceManager resourceManager) {
    _resourceManager = resourceManager;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(QUERY_RUNNER_THREADS_KEY)
        && !changedConfigs.contains(QUERY_WORKER_THREADS_KEY)) {
      return;
    }

    int newRunnerThreads = _resourceManager.getNumQueryRunnerThreads();
    int newWorkerThreads = _resourceManager.getNumQueryWorkerThreads();

    if (changedConfigs.contains(QUERY_RUNNER_THREADS_KEY)) {
      if (clusterConfigs.containsKey(QUERY_RUNNER_THREADS_KEY)) {
        try {
          newRunnerThreads = Integer.parseInt(clusterConfigs.get(QUERY_RUNNER_THREADS_KEY));
        } catch (NumberFormatException e) {
          LOGGER.error("Invalid value for {}: '{}'. Keeping current value: {}",
              QUERY_RUNNER_THREADS_KEY, clusterConfigs.get(QUERY_RUNNER_THREADS_KEY),
              _resourceManager.getNumQueryRunnerThreads(), e);
          return;
        }
      } else {
        newRunnerThreads = ResourceManager.DEFAULT_QUERY_RUNNER_THREADS;
        LOGGER.info("Key '{}' was removed from cluster config. Reverting to system default: {}",
            QUERY_RUNNER_THREADS_KEY, newRunnerThreads);
      }
    }

    if (changedConfigs.contains(QUERY_WORKER_THREADS_KEY)) {
      if (clusterConfigs.containsKey(QUERY_WORKER_THREADS_KEY)) {
        try {
          newWorkerThreads = Integer.parseInt(clusterConfigs.get(QUERY_WORKER_THREADS_KEY));
        } catch (NumberFormatException e) {
          LOGGER.error("Invalid value for {}: '{}'. Keeping current value: {}",
              QUERY_WORKER_THREADS_KEY, clusterConfigs.get(QUERY_WORKER_THREADS_KEY),
              _resourceManager.getNumQueryWorkerThreads(), e);
          return;
        }
      } else {
        newWorkerThreads = ResourceManager.DEFAULT_QUERY_WORKER_THREADS;
        LOGGER.info("Key '{}' was removed from cluster config. Reverting to system default: {}",
            QUERY_WORKER_THREADS_KEY, newWorkerThreads);
      }
    }

    LOGGER.info("Cluster config change detected. Resizing query scheduler thread pools: runner={}, worker={}",
        newRunnerThreads, newWorkerThreads);
    _resourceManager.resizeThreadPools(newRunnerThreads, newWorkerThreads);
  }
}
