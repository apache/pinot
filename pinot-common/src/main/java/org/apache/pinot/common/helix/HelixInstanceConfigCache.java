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
package org.apache.pinot.common.helix;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.helix.ClusterChangeHandler;
import org.apache.pinot.common.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to maintain a cache of all the {@code INSTANCE_CONFIG} managed by
 * the Pinot Helix cluster. It provides a singleton object to lookup the host
 * and port information based on the specified instance name.
 *
 * This will be helpful in decoupling instance name from the physical host and port
 * information (https://github.com/apache/incubator-pinot/issues/4525)
 */
public class HelixInstanceConfigCache implements ClusterChangeHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixInstanceConfigCache.class);
  private HelixManager _helixManager;
  private Map<String, InstanceConfig> _instanceNameToConfigMap;
  private static HelixInstanceConfigCache _instance;
  private volatile boolean _initialized;

  private HelixInstanceConfigCache() {
    _initialized = false;
  }

  public static synchronized HelixInstanceConfigCache getInstance() {
    if (_instance == null) {
      _instance = new HelixInstanceConfigCache();
    }
    return _instance;
  }

  public boolean isInitialized() {
    return _initialized;
  }

  @Override
  public void init(HelixManager helixManager) {
    Preconditions.checkState(_helixManager == null, "HelixInstanceConfigCache is already initialized");
    _helixManager = helixManager;
    _instanceNameToConfigMap = new HashMap<>();
    _initialized = true;
  }

  @Override
  public void processClusterChange(HelixConstants.ChangeType changeType) {
    Preconditions
        .checkState(changeType == HelixConstants.ChangeType.INSTANCE_CONFIG, "Illegal change type: " + changeType);
    HelixDataAccessor helixDataAccessor = _helixManager.getHelixDataAccessor();
    PropertyKey.Builder propertyKeyBuilder = helixDataAccessor.keyBuilder();
    List<InstanceConfig> instanceConfigs = helixDataAccessor.getChildValues(propertyKeyBuilder.instanceConfigs());
    for (InstanceConfig instanceConfig : instanceConfigs) {
      _instanceNameToConfigMap.put(instanceConfig.getInstanceName(), instanceConfig);
    }
  }

  public String getHostname(String instanceName) {
    Preconditions.checkNotNull(_instanceNameToConfigMap.get(instanceName));
    return _instanceNameToConfigMap.get(instanceName).getHostName();
  }

  public int getPort(String instanceName) {
    Preconditions.checkNotNull(_instanceNameToConfigMap.get(instanceName));
    int port;

    try {
      port = Integer.parseInt(_instanceNameToConfigMap.get(instanceName).getPort());
    } catch (Exception e) {
      port = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
      LOGGER
          .warn("Port for server instance {} does not appear to be numeric, defaulting to {}.", instanceName, port, e);
    }
    return port;
  }
}
