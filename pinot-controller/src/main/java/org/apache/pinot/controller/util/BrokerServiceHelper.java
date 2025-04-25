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
package org.apache.pinot.controller.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class for interacting with broker APIs.
 */
public class BrokerServiceHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServiceHelper.class);
  private static final String TIME_BOUNDARY_INFO_API_PATH = "/debug/timeBoundary/%s";
  private static final int DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ControllerConf _controllerConf;
  private final Executor _executorService;
  private final PoolingHttpClientConnectionManager _connectionManager;
  private final AuthProvider _authProvider;
  private CompletionServiceHelper _completionServiceHelper;

  public BrokerServiceHelper(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf controllerConf,
      Executor executor, PoolingHttpClientConnectionManager connectionManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _controllerConf = controllerConf;
    _executorService = executor;
    _connectionManager = connectionManager;
    _authProvider = AuthProviderUtils.extractAuthProvider(controllerConf, ControllerConf.CONTROLLER_BROKER_AUTH_PREFIX);
  }

  @VisibleForTesting
  // Set the CompletionServiceHelper for testing purposes.
  public void setCompletionServiceHelper(CompletionServiceHelper completionServiceHelper) {
    _completionServiceHelper = completionServiceHelper;
  }

  public BiMap<String, String> getBrokerEndpointsForInstance(List<InstanceConfig> instanceConfigs) {
    String protocol = _controllerConf.getControllerBrokerProtocol();
    BiMap<String, String> endpointsToInstances = HashBiMap.create(instanceConfigs.size());
    for (InstanceConfig instanceConfig : instanceConfigs) {
      String hostName = instanceConfig.getHostName();
      if (hostName.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
        hostName = hostName.substring(CommonConstants.Helix.BROKER_INSTANCE_PREFIX_LENGTH);
      }
      int port =
          _controllerConf.getControllerBrokerPortOverride() > 0 ? _controllerConf.getControllerBrokerPortOverride()
              : Integer.parseInt(instanceConfig.getPort());
      String brokerEndpoint = String.format("%s://%s:%d", protocol, hostName, port);
      endpointsToInstances.put(brokerEndpoint, instanceConfig.getInstanceName());
    }
    return endpointsToInstances;
  }

  public TimeBoundaryInfo getTimeBoundaryInfo(TableConfig offlineTableConfig) {
    String offlineTableName = offlineTableConfig.getTableName();

    List<InstanceConfig> instanceConfigs = _pinotHelixResourceManager.getBrokerInstancesConfigsFor(offlineTableName);
    BiMap<String, String> endpointsToInstances = getBrokerEndpointsForInstance(instanceConfigs);

    CompletionServiceHelper completionServiceHelper;
    if (_completionServiceHelper == null) {
      completionServiceHelper = new CompletionServiceHelper(_executorService, _connectionManager, endpointsToInstances);
    } else {
      completionServiceHelper = _completionServiceHelper;
    }

    List<String> timeBoundaryInfoUris = new ArrayList<>(endpointsToInstances.size());
    for (String endpoint : endpointsToInstances.keySet()) {
      String timeBoundaryInfoUri = endpoint + String.format(TIME_BOUNDARY_INFO_API_PATH, offlineTableName);
      timeBoundaryInfoUris.add(timeBoundaryInfoUri);
    }
    Map<String, String> reqHeaders = AuthProviderUtils.makeAuthHeadersMap(_authProvider);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(timeBoundaryInfoUris, offlineTableName, false, reqHeaders,
            DEFAULT_REQUEST_TIMEOUT_MS, "get time boundary information for table from broker instances");
    String responseKey = null;
    String responseValue = null;
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        responseKey = streamResponse.getKey();
        responseValue = streamResponse.getValue();
        // return the first valid response
        return JsonUtils.stringToObject(responseValue, TimeBoundaryInfo.class);
      } catch (JsonProcessingException e) {
        LOGGER.debug("Error parsing response into TimeBoundaryInfo object. key: {}, value: {}",
            responseKey, responseValue, e);
      }
    }
    throw new RuntimeException(String.format("Error parsing response into TimeBoundaryInfo object. key: %s, value: %s",
        responseKey, responseValue));
  }
}
