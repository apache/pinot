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
package org.apache.pinot.broker.broker.helix;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.common.response.ServerInstance;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.transport.netty.PooledNettyClientResourceManager;
import org.apache.pinot.transport.pool.KeyedPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Cluster change handler for live instance changes.
 */
public class LiveInstanceChangeHandler implements ClusterChangeHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(LiveInstanceChangeHandler.class);

  private static final boolean DO_NOT_RECREATE = false;

  private final HelixDataAccessor _helixDataAccessor;
  private final PropertyKey _liveInstancesKey;

  private KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> _connectionPool;
  private Map<String, String> _liveInstanceToSessionIdMap;

  public LiveInstanceChangeHandler(HelixManager helixManager) {
    _helixDataAccessor = helixManager.getHelixDataAccessor();
    _liveInstancesKey = new PropertyKey.Builder(helixManager.getClusterName()).liveInstances();
  }

  public void init(KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> connectionPool) {
    _connectionPool = connectionPool;
    _liveInstanceToSessionIdMap = new HashMap<>();
  }

  @Override
  public void processClusterChange(HelixConstants.ChangeType changeType) {
    Preconditions
        .checkState(changeType == HelixConstants.ChangeType.LIVE_INSTANCE, "Illegal change type: " + changeType);

    // Skip processing live instance change for single-connection routing
    if (_connectionPool == null) {
      return;
    }

    List<LiveInstance> liveInstances = _helixDataAccessor.getChildValues(_liveInstancesKey);

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
        LOGGER
            .warn("Port for server instance {} does not appear to be numeric, defaulting to {}.", instanceId, port, e);
      }

      if (_liveInstanceToSessionIdMap.containsKey(instanceId)) {
        // sessionId has changed
        if (!sessionId.equals(_liveInstanceToSessionIdMap.get(instanceId))) {
          try {
            LOGGER.info("Instance {} has changed session id {} -> {}, validating connection pool for this instance.",
                instanceId, sessionId, _liveInstanceToSessionIdMap.get(instanceId));
            ServerInstance ins = ServerInstance.forHostPort(hostName, port);
            _connectionPool.validatePool(ins, DO_NOT_RECREATE);
            _liveInstanceToSessionIdMap.put(instanceId, sessionId);
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
          _connectionPool.validatePool(ins, DO_NOT_RECREATE);
          _liveInstanceToSessionIdMap.put(instanceId, sessionId);
        } catch (Exception e) {
          LOGGER.error("Error trying to destroy dead connections for {}", instanceId, e);
        }
      }
    }
  }
}
