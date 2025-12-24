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
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;

import static org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_HELIX_CONSUMING_TRANSITION_THREAD_POOL_SIZE;
import static org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_HELIX_TRANSITION_THREAD_POOL_SIZE;
import static org.apache.pinot.spi.utils.CommonConstants.Server.DEFAULT_HELIX_CONSUMING_TRANSITION_THREAD_POOL_SIZE;
import static org.apache.pinot.spi.utils.CommonConstants.Server.DEFAULT_HELIX_TRANSITION_THREAD_POOL_SIZE;


public class ServerThreadPoolManager implements PinotClusterConfigChangeListener {
  private final ThreadPoolExecutor _helixTransitionExecutor;
  private final ThreadPoolExecutor _helixConsumingTransitionExecutor;

  public ServerThreadPoolManager(PinotConfiguration serverConf) {
    int helixTransitionThreadPoolSize =
        serverConf.getProperty(CONFIG_OF_HELIX_TRANSITION_THREAD_POOL_SIZE, DEFAULT_HELIX_TRANSITION_THREAD_POOL_SIZE);
    int helixConsumingTransitionThreadPoolSize =
        serverConf.getProperty(CONFIG_OF_HELIX_CONSUMING_TRANSITION_THREAD_POOL_SIZE,
            DEFAULT_HELIX_CONSUMING_TRANSITION_THREAD_POOL_SIZE);
    _helixTransitionExecutor = new ThreadPoolExecutor(helixTransitionThreadPoolSize, helixTransitionThreadPoolSize,
        0L, java.util.concurrent.TimeUnit.SECONDS, new java.util.concurrent.LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder().setNameFormat("HelixTransitionExecutor-%d").build());
    _helixConsumingTransitionExecutor =
        new ThreadPoolExecutor(helixConsumingTransitionThreadPoolSize, helixConsumingTransitionThreadPoolSize,
            0L, java.util.concurrent.TimeUnit.SECONDS, new java.util.concurrent.LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("HelixConsumingTransitionExecutor-%d").build());
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (changedConfigs.contains(CONFIG_OF_HELIX_TRANSITION_THREAD_POOL_SIZE)) {
      int newPoolSize = Integer.parseInt(clusterConfigs.get(CONFIG_OF_HELIX_TRANSITION_THREAD_POOL_SIZE));
      _helixTransitionExecutor.setCorePoolSize(newPoolSize);
      _helixTransitionExecutor.setMaximumPoolSize(newPoolSize);
    }
    if (changedConfigs.contains(CONFIG_OF_HELIX_CONSUMING_TRANSITION_THREAD_POOL_SIZE)) {
      int newPoolSize = Integer.parseInt(clusterConfigs.get(CONFIG_OF_HELIX_CONSUMING_TRANSITION_THREAD_POOL_SIZE));
      _helixConsumingTransitionExecutor.setCorePoolSize(newPoolSize);
      _helixConsumingTransitionExecutor.setMaximumPoolSize(newPoolSize);
    }
  }

  public ThreadPoolExecutor getHelixTransitionExecutor() {
    return _helixTransitionExecutor;
  }

  public ThreadPoolExecutor getHelixConsumingTransitionExecutor() {
    return _helixConsumingTransitionExecutor;
  }

  public void shutdown() {
    _helixTransitionExecutor.shutdownNow();
    _helixConsumingTransitionExecutor.shutdownNow();
  }
}
