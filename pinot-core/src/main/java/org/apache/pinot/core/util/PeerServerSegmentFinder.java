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
package org.apache.pinot.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PeerServerSegmentFinder discovers the server having the input segment in a ONLINE state through external view of a
 * Pinot table.
 */
public class PeerServerSegmentFinder {
  private static final Logger _logger = LoggerFactory.getLogger(PeerServerSegmentFinder.class);
  /**
   *
   * @param segmentName
   * @param downloadScheme Can be either http or https.
   * @param helixManager
   * @return the download uri string of format http(s)://hostname:port/segments/tablenameWithType/segmentName; null if
   * no such server found.
   */
  public static String getPeerServerURI(String segmentName, String downloadScheme, HelixManager helixManager) {
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    String tableNameWithType =
        TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(llcSegmentName.getTableName());

    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    String clusterName = helixManager.getClusterName();
    if (clusterName == null) {
      _logger.error("ClusterName not found");
      return null;
    }
    ExternalView externalViewForResource =
        HelixHelper.getExternalViewForResource(helixAdmin, clusterName, tableNameWithType);
    if (externalViewForResource == null) {
      _logger.warn("External View not found for table {}", tableNameWithType);
      return null;
    }
    // Find out the ONLINE server serving the segment.
    for (String segment : externalViewForResource.getPartitionSet()) {
      if (!segmentName.equals(segment)) {
        continue;
      }

      Map<String, String> instanceToStateMap = externalViewForResource.getStateMap(segmentName);

      // Randomly pick a server from the list of on-line server hosting the given segment.
      List<String> availableServers = new ArrayList<>();
      for (Map.Entry<String, String> instanceState : instanceToStateMap.entrySet()) {
        if ("ONLINE".equals(instanceState.getValue())) {
          _logger.info("Found ONLINE server {} for segment {}.", instanceState.getKey(), segmentName);
          String instanceId = instanceState.getKey();
          availableServers.add(instanceId);
        }
      }
      if (availableServers.isEmpty()) {
        return null;
      }
      Random r = new Random();
      String instanceId = availableServers.get(r.nextInt(availableServers.size()));

      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceId);
      String hostName = instanceConfig.getHostName();

      int port;
      try {
        HelixHelper.getInstanceConfigsMapFor(instanceId, clusterName, helixAdmin);
        port = Integer.parseInt(HelixHelper.getInstanceConfigsMapFor(instanceId, clusterName, helixAdmin)
            .get(CommonConstants.Helix.Instance.ADMIN_PORT_KEY));
      } catch (Exception e) {
        port = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
      }

      return StringUtil
          .join("/",  downloadScheme + "://" + hostName + ":" + port, "segments", tableNameWithType, segmentName);
    }
    return null;
  }
}
