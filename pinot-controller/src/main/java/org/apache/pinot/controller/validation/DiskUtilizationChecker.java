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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.restlet.resources.DiskUsageInfo;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DiskUtilizationChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(DiskUtilizationChecker.class);
  private final int _resourceUtilizationCheckTimeoutMs;
  private final long _resourceUtilizationCheckerFrequencyMs;
  private final double _diskUtilizationThreshold;
  private final String _diskUtilizationPath;
  public static final String DISK_UTILIZATION_API_PATH = "/instance/diskUtilization";

  private final PinotHelixResourceManager _helixResourceManager;

  public DiskUtilizationChecker(PinotHelixResourceManager helixResourceManager, ControllerConf controllerConf) {
    _helixResourceManager = helixResourceManager;
    _diskUtilizationPath = controllerConf.getDiskUtilizationPath();
    _diskUtilizationThreshold = controllerConf.getDiskUtilizationThreshold();
    _resourceUtilizationCheckTimeoutMs = controllerConf.getDiskUtilizationCheckTimeoutMs();
    _resourceUtilizationCheckerFrequencyMs = controllerConf.getResourceUtilizationCheckerFrequency() * 1000;
  }

  /**
   * Check if disk utilization for the requested table is within the configured limits.
   */
  public boolean isDiskUtilizationWithinLimits(String tableNameWithType) {
    if (StringUtils.isEmpty(tableNameWithType)) {
      throw new IllegalArgumentException("Table name found to be null or empty while computing disk utilization.");
    }
    TableConfig tableConfig = _helixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      LOGGER.warn("Table config for table: {} is null", tableNameWithType);
      return true; // table does not exist
    }
    List<String> instances;
    if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
      instances = _helixResourceManager.getServerInstancesForTable(tableNameWithType, TableType.OFFLINE);
    } else {
      instances = _helixResourceManager.getServerInstancesForTable(tableNameWithType, TableType.REALTIME);
    }
    return isDiskUtilizationWithinLimits(instances);
  }

  private boolean isDiskUtilizationWithinLimits(List<String> instances) {
    for (String instance : instances) {
      DiskUsageInfo diskUsageInfo = ResourceUtilizationInfo.getDiskUsageInfo(instance);
      if (diskUsageInfo == null) {
        LOGGER.warn("Disk utilization info for server: {} is null", instance);
        continue;
      }
      // Ignore if the disk utilization info is stale. The info is considered stale if it is older than the
      // ResourceUtilizationChecker tasks frequency.
      if (diskUsageInfo.getLastUpdatedTimeInEpochMs()
          < System.currentTimeMillis() - _resourceUtilizationCheckerFrequencyMs) {
        LOGGER.warn("Disk utilization info for server: {} is stale", instance);
        continue;
      }
      if (diskUsageInfo.getUsedSpaceBytes() > diskUsageInfo.getTotalSpaceBytes() * _diskUtilizationThreshold) {
        LOGGER.warn("Disk utilization for server: {} is above threshold: {}%. UsedBytes: {}, TotalBytes: {}",
            instance, diskUsageInfo.getUsedSpaceBytes() * 100 / diskUsageInfo.getTotalSpaceBytes(), diskUsageInfo
                .getUsedSpaceBytes(), diskUsageInfo.getTotalSpaceBytes());
        return false;
      }
    }
    return true;
  }

  /**
   * Compute disk utilization for the requested instances using the <code>CompletionServiceHelper</code>.
   */
  public void computeDiskUtilization(BiMap<String, String> endpointsToInstances,
      CompletionServiceHelper completionServiceHelper) {
    List<String> diskUtilizationUris = new ArrayList<>(endpointsToInstances.size());
    for (String endpoint : endpointsToInstances.keySet()) {
      String diskUtilizationUri = endpoint + DISK_UTILIZATION_API_PATH;
      diskUtilizationUris.add(diskUtilizationUri);
    }
    Map<String, String> reqHeaders = new HashMap<>();
    reqHeaders.put("diskUtilizationPath", _diskUtilizationPath);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(diskUtilizationUris, "no-op", false, reqHeaders,
            _resourceUtilizationCheckTimeoutMs, "get disk utilization info from servers");
    LOGGER.info("Service response: {}", serviceResponse._httpResponses);
    Map<String, DiskUsageInfo> diskUsageInfoMap = new HashMap<>();
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        DiskUsageInfo diskUsageInfo = JsonUtils.stringToObject(streamResponse.getValue(), DiskUsageInfo.class);
        if (diskUsageInfo != null && StringUtils.isNotEmpty(diskUsageInfo.getInstanceId())) {
          LOGGER.debug("Disk utilization for instance: {} is {}", diskUsageInfo.getInstanceId(), diskUsageInfo);
          diskUsageInfoMap.put(diskUsageInfo.getInstanceId(), diskUsageInfo);
        } else {
          LOGGER.warn("Disk utilization info for server {} is null or empty", streamResponse.getKey());
        }
      } catch (Exception e) {
        LOGGER.warn("Unable to parse server {} response due to an error: ", streamResponse.getKey(), e);
      }
    }
    if (!diskUsageInfoMap.isEmpty()) {
      ResourceUtilizationInfo.setDiskUsageInfo(diskUsageInfoMap);
    }
  }
}
