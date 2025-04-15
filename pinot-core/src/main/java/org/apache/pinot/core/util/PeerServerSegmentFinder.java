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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.Instance;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(PeerServerSegmentFinder.class);
  private static final int MAX_NUM_ATTEMPTS = 5;
  private static final int INITIAL_DELAY_MS = 500;
  private static final double DELAY_SCALE_FACTOR = 2;

  /**
   * Returns a list of URIs of the form 'http(s)://hostname:port/segments/tableNameWithType/segmentName' for the servers
   * hosting ONLINE segments; empty list if no such server found. The download scheme can be either 'http' or 'https'.
   */
  public static List<URI> getPeerServerURIs(HelixManager helixManager, String tableNameWithType, String segmentName,
      String downloadScheme) {
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    String clusterName = helixManager.getClusterName();
    List<URI> onlineServerURIs = new ArrayList<>();
    try {
      RetryPolicies.exponentialBackoffRetryPolicy(MAX_NUM_ATTEMPTS, INITIAL_DELAY_MS, DELAY_SCALE_FACTOR)
          .attempt(() -> {
            getOnlineServersFromExternalView(helixAdmin, clusterName, tableNameWithType, segmentName, downloadScheme,
                onlineServerURIs);
            return !onlineServerURIs.isEmpty();
          });
    } catch (AttemptsExceededException e) {
      LOGGER.error("Failed to find ONLINE servers for segment: {} in table: {} after {} attempts", segmentName,
          tableNameWithType, MAX_NUM_ATTEMPTS);
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting peer server URIs for segment: {} in table: {}", segmentName,
          tableNameWithType, e);
    }
    return onlineServerURIs;
  }

  public static void getOnlineServersFromExternalView(HelixAdmin helixAdmin, String clusterName,
      String tableNameWithType, String segmentName, String downloadScheme, List<URI> onlineServerURIs)
      throws Exception {
    ExternalView externalView = helixAdmin.getResourceExternalView(clusterName, tableNameWithType);
    if (externalView == null) {
      LOGGER.warn("Failed to find external view for table: {}", tableNameWithType);
      return;
    }
    // Find out the ONLINE servers serving the segment.
    Map<String, String> instanceStateMap = externalView.getStateMap(segmentName);
    if (instanceStateMap == null) {
      LOGGER.warn("Failed to find segment: {} in external view for the table: {}", segmentName, tableNameWithType);
      return;
    }
    for (Map.Entry<String, String> instanceState : instanceStateMap.entrySet()) {
      if (SegmentStateModel.ONLINE.equals(instanceState.getValue())) {
        String instanceId = instanceState.getKey();
        LOGGER.info("Found ONLINE server: {} for segment: {} in table: {}", instanceId, segmentName, tableNameWithType);
        InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceId);
        String hostName = instanceConfig.getHostName();
        String adminPortKey = getAdminPortKey(downloadScheme);
        int port = instanceConfig.getRecord().getIntField(adminPortKey, Server.DEFAULT_ADMIN_API_PORT);
        onlineServerURIs.add(new URI(
            StringUtil.join("/", downloadScheme + "://" + hostName + ":" + port, "segments", tableNameWithType,
                segmentName)));
      }
    }
  }

  private static String getAdminPortKey(String downloadScheme) {
    switch (downloadScheme) {
      case CommonConstants.HTTP_PROTOCOL:
        return Instance.ADMIN_PORT_KEY;
      case CommonConstants.HTTPS_PROTOCOL:
        return Instance.ADMIN_HTTPS_PORT_KEY;
      default:
        throw new IllegalArgumentException("Unsupported download scheme: " + downloadScheme);
    }
  }
}
