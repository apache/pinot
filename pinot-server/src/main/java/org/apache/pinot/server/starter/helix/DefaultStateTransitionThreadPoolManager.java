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
package org.apache.pinot.server.starter.helix;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.messaging.handling.MessageTask;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.pinot.core.data.manager.SegmentOperationsTaskContext;
import org.apache.pinot.core.data.manager.SegmentOperationsTaskType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.executor.DecoratorExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Default state transition thread pool manager backed by a fixed-size pool.
 *
 * <p>This replaces the Helix-managed state transition thread pool to provide explicit context tracking
 * for segment operations. The pool size is configurable via
 * {@link org.apache.pinot.spi.utils.CommonConstants.Server#CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE}.</p>
 *
 * <p><b>Migration from Helix configuration:</b> Previously, the thread pool size was configured in ZooKeeper
 * using the key "STATE_TRANSITION.maxThreads" at either the participant or cluster config level.
 * For backward compatibility, this implementation will read from the legacy Helix config if the new
 * Pinot config is not set.</p>
 *
 * <p><b>Configuration precedence:</b></p>
 * <ol>
 *   <li>Pinot server config: pinot.server.instance.stateTransitionThreadPoolSize</li>
 *   <li>Helix instance config: STATE_TRANSITION.maxThreads (from CONFIGS/PARTICIPANT/&lt;instance&gt;)</li>
 *   <li>Helix cluster config: STATE_TRANSITION.maxThreads (from CONFIGS/CLUSTER/&lt;cluster&gt;)</li>
 *   <li>Default value: 40</li>
 * </ol>
 */
public class DefaultStateTransitionThreadPoolManager implements StateTransitionThreadPoolManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStateTransitionThreadPoolManager.class);
  private static final String HELIX_STATE_TRANSITION_KEY = "STATE_TRANSITION.maxThreads";

  private final PinotConfiguration _serverConf;
  private final HelixManager _helixManager;
  private ExecutorService _executorService;

  /**
   * Creates a state transition thread pool manager with the default pool size configuration.
   * The pool size is read from the server configuration.
   *
   * @param serverConf the server configuration
   */
  public DefaultStateTransitionThreadPoolManager(PinotConfiguration serverConf) {
    this(serverConf, null);
  }

  /**
   * Creates a state transition thread pool manager with backward compatibility for legacy Helix config.
   * The executor service is created lazily in {@link #onHelixManagerConnected()} after Helix connects,
   * allowing it to read legacy Helix config if needed.
   *
   * @param serverConf the server configuration
   * @param helixManager the Helix manager to read legacy config from later (can be null)
   */
  public DefaultStateTransitionThreadPoolManager(PinotConfiguration serverConf,
      @Nullable HelixManager helixManager) {
    _serverConf = serverConf;
    _helixManager = helixManager;
    // Executor service will be created in onHelixManagerConnected()
  }

  @Override
  public void onHelixManagerConnected() {
    // Determine pool size with full precedence: Pinot config > Helix config > default
    int poolSize = determinePoolSize();
    _executorService =
        new ContextualStateTransitionExecutor(new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat("state-transition-%d").build()));
    LOGGER.info("Created state transition thread pool with size: {}", poolSize);
  }

  /**
   * Determines the thread pool size with full precedence checking.
   * This is called after Helix connects so we can read legacy Helix config.
   */
  private int determinePoolSize() {
    // 1. Check Pinot config first (highest precedence)
    if (_serverConf.containsKey(
        org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE)) {
      int poolSize = _serverConf.getProperty(
          org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE,
          org.apache.pinot.spi.utils.CommonConstants.Server.DEFAULT_STATE_TRANSITION_THREAD_POOL_SIZE);
      LOGGER.info("Using state transition thread pool size from Pinot config: {}", poolSize);
      return poolSize;
    }

    // 2. Check legacy Helix config if HelixManager is available and connected
    if (_helixManager != null && _helixManager.isConnected()) {
      Integer legacyPoolSize = readLegacyHelixConfig(_helixManager);
      if (legacyPoolSize != null) {
        LOGGER.info("Using state transition thread pool size from legacy Helix config: {}. "
                + "Consider migrating to Pinot config: {}", legacyPoolSize,
            org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE);
        return legacyPoolSize;
      }
    }

    // 3. Use default
    int defaultSize = org.apache.pinot.spi.utils.CommonConstants.Server.DEFAULT_STATE_TRANSITION_THREAD_POOL_SIZE;
    LOGGER.info("Using default state transition thread pool size: {}", defaultSize);
    return defaultSize;
  }

  /**
   * Reads the legacy Helix STATE_TRANSITION.maxThreads config from instance or cluster config.
   * Returns null if not found.
   */
  @Nullable
  private Integer readLegacyHelixConfig(HelixManager helixManager) {
    try {
      String clusterName = helixManager.getClusterName();
      String instanceName = helixManager.getInstanceName();

      // Try instance-level config first
      HelixConfigScope instanceScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT)
          .forCluster(clusterName)
          .forParticipant(instanceName)
          .build();
      String instanceValue = helixManager.getConfigAccessor().get(instanceScope, HELIX_STATE_TRANSITION_KEY);
      if (instanceValue != null) {
        try {
          int poolSize = Integer.parseInt(instanceValue);
          LOGGER.info("Found legacy Helix instance config {}={}", HELIX_STATE_TRANSITION_KEY, poolSize);
          return poolSize;
        } catch (NumberFormatException e) {
          LOGGER.warn("Invalid value for legacy Helix instance config {}: {}", HELIX_STATE_TRANSITION_KEY,
              instanceValue);
        }
      }

      // Try cluster-level config
      HelixConfigScope clusterScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
          .forCluster(clusterName)
          .build();
      String clusterValue = helixManager.getConfigAccessor().get(clusterScope, HELIX_STATE_TRANSITION_KEY);
      if (clusterValue != null) {
        try {
          int poolSize = Integer.parseInt(clusterValue);
          LOGGER.info("Found legacy Helix cluster config {}={}", HELIX_STATE_TRANSITION_KEY, poolSize);
          return poolSize;
        } catch (NumberFormatException e) {
          LOGGER.warn("Invalid value for legacy Helix cluster config {}: {}", HELIX_STATE_TRANSITION_KEY,
              clusterValue);
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to read legacy Helix config", e);
    }

    return null;
  }

  @Override
  @Nullable
  public StateModelFactory.CustomizedExecutorService getExecutorService(Message.MessageInfo messageInfo) {
    return null;
  }

  @Override
  @Nullable
  public ExecutorService getExecutorService(String resourceName, String fromState, String toState) {
    return null;
  }

  @Override
  @Nullable
  public ExecutorService getExecutorService(String resourceName) {
    if (_executorService == null) {
      LOGGER.warn("Executor service not initialized yet. State transition for resource {} will use Helix default pool.",
          resourceName);
    }
    return _executorService;
  }

  @Override
  public void shutdown() {
    if (_executorService != null) {
      _executorService.shutdown();
    }
  }

  private static class ContextualStateTransitionExecutor extends DecoratorExecutorService {
    public ContextualStateTransitionExecutor(ExecutorService delegate) {
      super(delegate);
    }

    @Override
    protected <T> Callable<T> decorate(Callable<T> task) {
      Message message = task instanceof MessageTask ? ((MessageTask) task).getMessage() : null;
      String tableNameWithType = message != null ? message.getResourceName() : null;
      if (!(task instanceof MessageTask)) {
        LOGGER.warn(
            "Submitting a Callable task that is not a MessageTask. State transition task will be wrapped with null "
                + "table name.");
      }
      return SegmentOperationsTaskContext.wrap(task, SegmentOperationsTaskType.STATE_TRANSITION, tableNameWithType);
    }

    @Override
    protected Runnable decorate(Runnable task) {
      Message message = task instanceof MessageTask ? ((MessageTask) task).getMessage() : null;
      String tableNameWithType = message != null ? message.getResourceName() : null;
      if (!(task instanceof MessageTask)) {
        LOGGER.warn(
            "Submitting a Runnable task that is not a MessageTask. State transition task will be wrapped with null "
                + "table name.");
      }
      return SegmentOperationsTaskContext.wrap(task, SegmentOperationsTaskType.STATE_TRANSITION, tableNameWithType);
    }
  }
}
