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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.ListUtils;
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
 * PeerServerSegmentFinder discovers all the servers having the input segment in a ONLINE state through external view of
 * a Pinot table.
 */
public class PeerServerSegmentFinder {
  private static final Logger _logger = LoggerFactory.getLogger(PeerServerSegmentFinder.class);
  /**
   *
   * @param segmentName
   * @param downloadScheme Can be either http or https.
   * @param helixManager
   * @return a list of uri strings of the form http(s)://hostname:port/segments/tablenameWithType/segmentName
   * for the servers hosting ONLINE segments; empty list if no such server found.
   */
  public static List<URI> getPeerServerURIs(String segmentName, String downloadScheme, HelixManager helixManager) {
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    String tableNameWithType =
        TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(llcSegmentName.getTableName());

    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    String clusterName = helixManager.getClusterName();
    if (clusterName == null) {
      _logger.error("ClusterName not found");
      return ListUtils.EMPTY_LIST;
    }
    ExternalView externalViewForResource =
        HelixHelper.getExternalViewForResource(helixAdmin, clusterName, tableNameWithType);
    if (externalViewForResource == null) {
      _logger.warn("External View not found for table {}", tableNameWithType);
      return ListUtils.EMPTY_LIST;
    }
    List<URI> onlineServerURIs = new ArrayList<>();
    // Find out the ONLINE server serving the segment.
    for (String segment : externalViewForResource.getPartitionSet()) {
      if (!segmentName.equals(segment)) {
        continue;
      }

      Map<String, String> instanceToStateMap = externalViewForResource.getStateMap(segmentName);

      // Randomly pick a server from the list of on-line server hosting the given segment.
      for (Map.Entry<String, String> instanceState : instanceToStateMap.entrySet()) {
        if ("ONLINE".equals(instanceState.getValue())) {
          String instanceId = instanceState.getKey();
          _logger.info("Found ONLINE server {} for segment {}.", instanceId, segmentName);
          InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceId);
          String hostName = instanceConfig.getHostName();
          int port = getServerAdminPort(helixAdmin, clusterName, instanceId);
          try {
            onlineServerURIs.add(new URI(StringUtil
                .join("/",  downloadScheme + "://" + hostName + ":" + port, "segments", tableNameWithType, segmentName)));
          } catch (URISyntaxException e) {
            _logger.warn("Error in uri syntax: ", e);
          }
        }
      }
    }
    return onlineServerURIs;
  }

  private static int getServerAdminPort(HelixAdmin helixAdmin, String clusterName, String instanceId) {
    try {
      return Integer.parseInt(HelixHelper.getInstanceConfigsMapFor(instanceId, clusterName, helixAdmin)
          .get(CommonConstants.Helix.Instance.ADMIN_PORT_KEY));
    } catch (Exception e) {
      _logger.warn("Failed to retrieve ADMIN PORT for instanceId {} in the cluster {} ", instanceId, clusterName, e);
      return CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
    }
  }
}
