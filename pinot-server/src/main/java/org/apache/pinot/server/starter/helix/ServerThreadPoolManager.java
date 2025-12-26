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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import javax.annotation.Nullable;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_HELIX_CONSUMING_TRANSITION_THREAD_POOL_SIZE;
import static org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_HELIX_TRANSITION_THREAD_POOL_SIZE;


/**
 * Manages the custom Helix state transition thread pools for Pinot server.
 * Helix state transition will use their default thread pool if the thread pool provided by this class is null. Also see
 * {@link SegmentOnlineOfflineStateModelFactory#getExecutorService(String, String, String)}
 */
public class ServerThreadPoolManager implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerThreadPoolManager.class);
  @Nullable
  private ThreadPoolExecutor _helixTransitionExecutor;
  @Nullable
  private ThreadPoolExecutor _helixConsumingTransitionExecutor;

  public ServerThreadPoolManager(PinotConfiguration serverConf) {
    String helixTransitionThreadPoolSizeStr =
        serverConf.getProperty(CONFIG_OF_HELIX_TRANSITION_THREAD_POOL_SIZE);
    String helixConsumingTransitionThreadPoolSizeStr =
        serverConf.getProperty(CONFIG_OF_HELIX_CONSUMING_TRANSITION_THREAD_POOL_SIZE);
    if (helixTransitionThreadPoolSizeStr != null) {
      int helixTransitionThreadPoolSize = Integer.parseInt(helixTransitionThreadPoolSizeStr);
      _helixTransitionExecutor = new ThreadPoolExecutor(helixTransitionThreadPoolSize, helixTransitionThreadPoolSize,
          0L, java.util.concurrent.TimeUnit.SECONDS, new java.util.concurrent.LinkedBlockingQueue<>(),
          new ThreadFactoryBuilder().setNameFormat("HelixTransitionExecutor-%d").build());
      LOGGER.info("Created HelixTransitionExecutor with pool size: {}", helixTransitionThreadPoolSize);
    }
    if (helixConsumingTransitionThreadPoolSizeStr != null) {
      int helixConsumingTransitionThreadPoolSize = Integer.parseInt(helixConsumingTransitionThreadPoolSizeStr);
      _helixConsumingTransitionExecutor =
          new ThreadPoolExecutor(helixConsumingTransitionThreadPoolSize, helixConsumingTransitionThreadPoolSize,
              0L, java.util.concurrent.TimeUnit.SECONDS, new java.util.concurrent.LinkedBlockingQueue<>(),
              new ThreadFactoryBuilder().setNameFormat("HelixConsumingTransitionExecutor-%d").build());
      LOGGER.info("Created HelixConsumingTransitionExecutor with pool size: {}",
          helixConsumingTransitionThreadPoolSize);
    }
  }

  /**
   * There will be no effect on the change to attempt to remove or add in the custom helix transition thread pool.
   * For example, from null to 40 or from 40 to null. Because the thread pool would be registered via
   * {@link StateModelFactory#getExecutorService(String, String, String)} upon the first time a transition type is
   * seen, and will not be changed after that.
   * But the change from 40 to 10, for example, is effective because it just changes the core size and max size of
   * the thread pool, not to reassign the thread pool object.
   */
  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (changedConfigs.contains(CONFIG_OF_HELIX_TRANSITION_THREAD_POOL_SIZE)) {
      if (clusterConfigs.get(CONFIG_OF_HELIX_TRANSITION_THREAD_POOL_SIZE) != null) {
        if (_helixTransitionExecutor == null) {
          LOGGER.warn("Custom thread pool HelixTransitionExecutor cannot be created on the fly from the config change. "
              + "Please restart the server to take effect.");
        } else {
          int newPoolSize = Integer.parseInt(clusterConfigs.get(CONFIG_OF_HELIX_TRANSITION_THREAD_POOL_SIZE));
          _helixTransitionExecutor.setCorePoolSize(newPoolSize);
          _helixTransitionExecutor.setMaximumPoolSize(newPoolSize);
          LOGGER.info("Updated HelixTransitionExecutor pool size to: {}", newPoolSize);
        }
      } else if (_helixTransitionExecutor != null) {
        LOGGER.warn(
            "Custom thread pool HelixTransitionExecutor would still be used for Helix state transitions even though "
                + "the config {} is removed from cluster config, using the last known size: {}",
            CONFIG_OF_HELIX_TRANSITION_THREAD_POOL_SIZE, _helixTransitionExecutor.getCorePoolSize());
      }
    }
    if (changedConfigs.contains(CONFIG_OF_HELIX_CONSUMING_TRANSITION_THREAD_POOL_SIZE)) {
      if (clusterConfigs.get(CONFIG_OF_HELIX_CONSUMING_TRANSITION_THREAD_POOL_SIZE) != null) {
        if (_helixConsumingTransitionExecutor == null) {
          LOGGER.warn(
              "Custom thread pool HelixConsumingTransitionExecutor cannot be created on the fly from the config "
                  + "change. Please restart the server to take effect.");
        } else {
          int newPoolSize = Integer.parseInt(clusterConfigs.get(CONFIG_OF_HELIX_CONSUMING_TRANSITION_THREAD_POOL_SIZE));
          _helixConsumingTransitionExecutor.setCorePoolSize(newPoolSize);
          _helixConsumingTransitionExecutor.setMaximumPoolSize(newPoolSize);
          LOGGER.info("Updated HelixConsumingTransitionExecutor pool size to: {}", newPoolSize);
        }
      } else if (_helixConsumingTransitionExecutor != null) {
        LOGGER.warn("Custom thread pool HelixConsumingTransitionExecutor would still be used for Helix consuming state "
                + "transitions even though the config {} is removed from cluster config, using the last known size: {}",
            CONFIG_OF_HELIX_CONSUMING_TRANSITION_THREAD_POOL_SIZE, _helixConsumingTransitionExecutor.getCorePoolSize());
      }
    }
  }

  @Nullable
  public ThreadPoolExecutor getHelixTransitionExecutor() {
    return _helixTransitionExecutor;
  }

  @Nullable
  public ThreadPoolExecutor getHelixConsumingTransitionExecutor() {
    return _helixConsumingTransitionExecutor;
  }

  public void shutdown() {
    if (_helixTransitionExecutor != null) {
      _helixTransitionExecutor.shutdownNow();
    }
    if (_helixConsumingTransitionExecutor != null) {
      _helixConsumingTransitionExecutor.shutdownNow();
    }
  }
}
