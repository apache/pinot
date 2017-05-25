/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.broker.broker.helix;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.transport.netty.PooledNettyClientResourceManager;
import com.linkedin.pinot.transport.pool.KeyedPool;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LiveInstancesChangeListenerImpl implements LiveInstanceChangeListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(LiveInstancesChangeListenerImpl.class);

  private static final boolean DO_NOT_RECREATE = false;

  private long timeout;
  private final Map<String, String> liveInstanceToSessionIdMap;
  private KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> connectionPool;

  public LiveInstancesChangeListenerImpl(String clusterName) {
    this.liveInstanceToSessionIdMap = new HashMap<String, String>();
  }

  public void init(final KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> connectionPool, final long timeout) {
    this.connectionPool = connectionPool;
    this.timeout = timeout;
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
    if (connectionPool == null) {
      LOGGER.warn("init has not been called on the live instances listener, ignoring live instance change.");
      return;
    }

    for (LiveInstance instance : liveInstances) {

      String instanceId = instance.getInstanceName();
      String sessionId = instance.getSessionId();

      if (!instanceId.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)) {
        LOGGER.debug("Skipping non-server instance {}", instanceId);
        continue;
      }

      String namePortStr = instanceId.split(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)[1];
      String hostName = namePortStr.split("_")[0];
      int port;
      try {
        port = Integer.parseInt(namePortStr.split("_")[1]);
      } catch (Exception e) {
        port = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
        LOGGER.warn("Port for server instance {} does not appear to be numeric, defaulting to {}.", instanceId, port, e);
      }

      if (liveInstanceToSessionIdMap.containsKey(instanceId)) {
        // sessionId has changed
        if (!sessionId.equals(liveInstanceToSessionIdMap.get(instanceId))) {
          try {
            LOGGER.info("Instance {} has changed session id {} -> {}, validating connection pool for this instance.", instanceId, sessionId,
                liveInstanceToSessionIdMap.get(instanceId));
            ServerInstance ins = ServerInstance.forHostPort(hostName, port);
            connectionPool.validatePool(ins, DO_NOT_RECREATE);
            liveInstanceToSessionIdMap.put(instanceId, sessionId);
          } catch (Exception e) {
            LOGGER.error("Error trying to validate & destroy dead connections for {}", instanceId, e);
          }
        }
      } else {
        LOGGER.info("Found new instance {} with session id {}, adding to session id map.", instanceId, sessionId);
        // we don't have this instanceId
        // lets first check if the connection is valid or not
        try {
          ServerInstance ins = ServerInstance.forHostPort(hostName, port);
          connectionPool.validatePool(ins, DO_NOT_RECREATE);
          liveInstanceToSessionIdMap.put(instanceId, sessionId);
        } catch (Exception e) {
          LOGGER.error("Error trying to destroy dead connections for {}", instanceId, e);
        }
      }
    }
  }
}
