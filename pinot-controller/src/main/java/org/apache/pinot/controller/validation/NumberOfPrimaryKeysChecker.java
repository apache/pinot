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
package org.apache.pinot.controller.validation;

import com.google.common.collect.BiMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.restlet.resources.PrimaryKeyCountInfo;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NumberOfPrimaryKeysChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(NumberOfPrimaryKeysChecker.class);
  private static final long DISABLE_NUMBER_OF_PRIMARY_KEYS_CHECK = 0;
  public static final String NUMBER_OF_PRIMARY_KEYS_API_PATH = "/instance/numberOfPrimaryKeys";

  private final PinotHelixResourceManager _helixResourceManager;
  private final int _resourceUtilizationCheckTimeoutMs;
  private final long _resourceUtilizationCheckerFrequencyMs;
  private final double _numberOfPrimaryKeysThreshold;

  public NumberOfPrimaryKeysChecker(PinotHelixResourceManager helixResourceManager, ControllerConf controllerConf) {
    _helixResourceManager = helixResourceManager;
    _numberOfPrimaryKeysThreshold = controllerConf.getNumberOfPrimaryKeysThreshold();
    _resourceUtilizationCheckTimeoutMs = controllerConf.getNumberOfPrimaryKeysCheckTimeoutMs();
    _resourceUtilizationCheckerFrequencyMs = controllerConf.getResourceUtilizationCheckerFrequency() * 1000;
  }

  /**
   * Check if the number of primary keys for the requested table is within the configured limits.
   */
  public boolean isNumberOfPrimaryKeysWithinLimits(String tableNameWithType) {
    if (_numberOfPrimaryKeysThreshold <= DISABLE_NUMBER_OF_PRIMARY_KEYS_CHECK) {
      // The primary key count check is disabled
      LOGGER.debug("Primary key count threshold <= 0, which means it is disabled, returning true");
      return true;
    }
    if (StringUtils.isEmpty(tableNameWithType)) {
      throw new IllegalArgumentException("Table name found to be null or empty while computing number of primary "
          + "keys.");
    }
    TableConfig tableConfig = _helixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      // table does not exist
      LOGGER.warn("Table config for table: {} is null", tableNameWithType);
      return true;
    }
    if (TableNameBuilder.isOfflineTableResource(tableNameWithType)
        || (!tableConfig.isDedupEnabled() && !tableConfig.isUpsertEnabled())) {
      // table isn't of a type that can have primary keys
      LOGGER.warn("Table: {} is either an OFFLINE table or doesn't have upserts or dedup enabled",
          tableNameWithType);
      return true;
    }
    List<String> instances = _helixResourceManager.getServerInstancesForTable(tableNameWithType, TableType.REALTIME);
    return isNumberOfPrimaryKeysWithinLimits(instances);
  }

  private boolean isNumberOfPrimaryKeysWithinLimits(List<String> instances) {
    for (String instance : instances) {
      assert _numberOfPrimaryKeysThreshold > DISABLE_NUMBER_OF_PRIMARY_KEYS_CHECK;
      PrimaryKeyCountInfo primaryKeyCountInfo = ResourceUtilizationInfo.getPrimaryKeyCountInfo(instance);
      if (primaryKeyCountInfo == null) {
        LOGGER.warn("Primary key count info for server: {} is null", instance);
        continue;
      }
      // Ignore if the primary key count info is stale. The info is considered stale if it is older than the
      // ResourceUtilizationChecker tasks frequency.
      if (primaryKeyCountInfo.getLastUpdatedTimeInEpochMs()
          < System.currentTimeMillis() - _resourceUtilizationCheckerFrequencyMs) {
        LOGGER.warn("Primary key count info for server: {} is stale", instance);
        continue;
      }
      if (primaryKeyCountInfo.getNumPrimaryKeys() > _numberOfPrimaryKeysThreshold) {
        LOGGER.warn("Primary key count {} for server: {} is above threshold: {}",
            primaryKeyCountInfo.getNumPrimaryKeys(), instance, _numberOfPrimaryKeysThreshold);
        return false;
      }
    }
    return true;
  }

  /**
   * Compute disk utilization for the requested instances using the <code>CompletionServiceHelper</code>.
   */
  public void computeNumberOfPrimaryKeys(BiMap<String, String> endpointsToInstances,
      CompletionServiceHelper completionServiceHelper) {
    if (_numberOfPrimaryKeysThreshold <= DISABLE_NUMBER_OF_PRIMARY_KEYS_CHECK) {
      // The primary key count check is disabled
      LOGGER.debug("Primary key count threshold <= 0, which means it is disabled, returning true");
      ResourceUtilizationInfo.setPrimaryKeyCountInfo(Collections.emptyMap());
      return;
    }
    List<String> numberOfPrimaryKeysUris = new ArrayList<>(endpointsToInstances.size());
    for (String endpoint : endpointsToInstances.keySet()) {
      String numberOfPrimaryKeysUri = endpoint + NUMBER_OF_PRIMARY_KEYS_API_PATH;
      numberOfPrimaryKeysUris.add(numberOfPrimaryKeysUri);
    }
    Map<String, String> reqHeaders = new HashMap<>();
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(numberOfPrimaryKeysUris, "no-op", false, reqHeaders,
            _resourceUtilizationCheckTimeoutMs, "get primary key count info from servers");
    LOGGER.info("Service response: {}", serviceResponse._httpResponses);
    Map<String, PrimaryKeyCountInfo> primaryKeyCountInfoMap = new HashMap<>();
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        PrimaryKeyCountInfo primaryKeyCountInfo = JsonUtils.stringToObject(streamResponse.getValue(),
            PrimaryKeyCountInfo.class);
        if (primaryKeyCountInfo != null && StringUtils.isNotEmpty(primaryKeyCountInfo.getInstanceId())) {
          LOGGER.debug("Primary key count for instance: {} is {}", primaryKeyCountInfo.getInstanceId(),
              primaryKeyCountInfo);
          primaryKeyCountInfoMap.put(primaryKeyCountInfo.getInstanceId(), primaryKeyCountInfo);
        } else {
          LOGGER.warn("Primary key count for info for server {} is null or empty", streamResponse.getKey());
        }
      } catch (Exception e) {
        LOGGER.warn("Unable to parse server {} response due to an error: ", streamResponse.getKey(), e);
      }
    }
    if (!primaryKeyCountInfoMap.isEmpty()) {
      ResourceUtilizationInfo.setPrimaryKeyCountInfo(primaryKeyCountInfoMap);
    }
  }
}
