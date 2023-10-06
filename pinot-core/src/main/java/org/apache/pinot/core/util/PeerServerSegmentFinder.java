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
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PeerServerSegmentFinder discovers all the servers having the input segment in an ONLINE state through external view
 * of a Pinot table. It performs retries during the discovery to minimize the chance of Helix state propagation delay.
 */
public class PeerServerSegmentFinder {
  private PeerServerSegmentFinder() {
  }

  private static final Logger _logger = LoggerFactory.getLogger(PeerServerSegmentFinder.class);
  private static final int MAX_NUM_ATTEMPTS = 5;
  private static final int INITIAL_DELAY_MS = 500;
  private static final double DELAY_SCALE_FACTOR = 2;

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
    return getPeerServerURIs(segmentName, downloadScheme, helixManager, tableNameWithType);
  }

  public static List<URI> getPeerServerURIs(String segmentName, String downloadScheme,
      HelixManager helixManager, String tableNameWithType) {
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    String clusterName = helixManager.getClusterName();
    if (clusterName == null) {
      _logger.error("ClusterName not found");
      return ListUtils.EMPTY_LIST;
    }
    final List<URI> onlineServerURIs = new ArrayList<>();
    try {
      RetryPolicies.exponentialBackoffRetryPolicy(MAX_NUM_ATTEMPTS, INITIAL_DELAY_MS, DELAY_SCALE_FACTOR)
          .attempt(() -> {
            getOnlineServersFromExternalView(segmentName, downloadScheme, tableNameWithType, helixAdmin, clusterName,
                onlineServerURIs);
            return !onlineServerURIs.isEmpty();
          });
    } catch (Exception e) {
      _logger.error("Failure in getting online servers for segment {}", segmentName, e);
    }
    return onlineServerURIs;
  }

  private static void getOnlineServersFromExternalView(String segmentName, String downloadScheme,
      String tableNameWithType, HelixAdmin helixAdmin, String clusterName, List<URI> onlineServerURIs) {
    ExternalView externalViewForResource =
        HelixHelper.getExternalViewForResource(helixAdmin, clusterName, tableNameWithType);
    if (externalViewForResource == null) {
      _logger.warn("External View not found for table {}", tableNameWithType);
      return;
    }
    // Find out the ONLINE servers serving the segment.
    Map<String, String> instanceToStateMap = externalViewForResource.getStateMap(segmentName);
    for (Map.Entry<String, String> instanceState : instanceToStateMap.entrySet()) {
      if ("ONLINE".equals(instanceState.getValue())) {
        String instanceId = instanceState.getKey();
        _logger.info("Found ONLINE server {} for segment {}.", instanceId, segmentName);
        InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceId);
        String hostName = instanceConfig.getHostName();
        int port = getServerAdminPort(helixAdmin, clusterName, instanceId, downloadScheme);
        try {
          onlineServerURIs.add(new URI(StringUtil
              .join("/", downloadScheme + "://" + hostName + ":" + port, "segments", tableNameWithType, segmentName)));
        } catch (URISyntaxException e) {
          _logger.warn("Error in uri syntax: ", e);
        }
      }
    }
  }

  private static int getServerAdminPort(HelixAdmin helixAdmin, String clusterName, String instanceId, String downloadScheme) {
    try {
      return Integer.parseInt(HelixHelper.getInstanceConfigsMapFor(instanceId, clusterName, helixAdmin)
          .get(getServerAdminPortKey(downloadScheme)));
    } catch (Exception e) {
      _logger.warn("Failed to retrieve ADMIN PORT for instanceId {} in the cluster {} ", instanceId, clusterName, e);
      return CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
    }
  }

  private static String getServerAdminPortKey(String downloadScheme) {
    switch (downloadScheme) {
      case CommonConstants.HTTPS_PROTOCOL:
        return CommonConstants.Helix.Instance.ADMIN_HTTPS_PORT_KEY;
      case CommonConstants.HTTP_PROTOCOL:
      default:
        return CommonConstants.Helix.Instance.ADMIN_PORT_KEY;
    }
  }
}
