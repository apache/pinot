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
package org.apache.pinot.common.utils.fetcher;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.helix.HelixHelper;

// This segment fetcher downloads the segment from peer Pinot servers discovered through external view of a pinot
// table. The format fo expected segment address uris is
//   server:///segment_name
// Thus the host component is empty.
// To use this segment fetcher, servers need to put "server" in their segment fetcher protocol.
public class PeerServerSegmentFetcher extends BaseSegmentFetcher {
  public static final String SCHEME = "server";
  private HelixManager _helixManager;
  private String _helixClusterName;

  public PeerServerSegmentFetcher(HelixManager helixManager, String helixClusterName) {
    _helixManager = helixManager;
    _helixClusterName = helixClusterName;
  }

  @Override
  public void fetchSegmentToLocalWithoutRetry(URI uri, File dest)
      throws Exception {
    if (!SCHEME.equals(uri.getScheme())) {
      throw new IllegalArgumentException(uri.toString());
    }
    if (uri.getPath() == null || !uri.getPath().startsWith("/")) {
      throw new IllegalArgumentException(uri.toString());
    }
    String peerServerUri = getPeerServerURI(uri.getPath().substring(1));
    if (peerServerUri == null) {
      throw new Exception("Unable to found peer server for segment " + uri);
    }

    SegmentFetcherFactory.fetchSegmentToLocal(new URI(peerServerUri), dest);
  }

  // Return the address of an ONLINE server hosting a segment. The returned address is of format
  //  http://hostname:port/segments/tablenameWithType/segmentName
  private String getPeerServerURI(String segmentName) {
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    String tableNameWithType = TableNameBuilder.forType(CommonConstants.Helix.TableType.REALTIME).
        tableNameWithType(llcSegmentName.getTableName());

    ExternalView externalViewForResource =
        HelixHelper.getExternalViewForResource(_helixManager.getClusterManagmentTool(), _helixClusterName, tableNameWithType);
    if (externalViewForResource == null) {
      _logger.warn("External View not found for segment {}", segmentName);
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

      InstanceConfig instanceConfig = _helixManager.getClusterManagmentTool().getInstanceConfig(_helixClusterName, instanceId);
      String hostName = instanceConfig.getHostName();

      int port;
      try {
        port = Integer.parseInt(instanceConfig.getPort());
      } catch (Exception e) {
        port = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
      }

      return StringUtil.join("/", "http://" + hostName + ":" + port, "segments", tableNameWithType, segmentName);
    }
    return null;
  }
}
