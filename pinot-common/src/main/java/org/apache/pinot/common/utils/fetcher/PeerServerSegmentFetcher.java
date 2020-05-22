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

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.configuration.Configuration;
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

/**
 * This segment fetcher downloads the segment by first discovering the server having the segment through external view
 * of a Pinot table and then downloading the segment from the peer server using a configured http or https fetcher. By
 * default, HttpSegmentFetcher is used.
 * The format fo expected segment address uri is
 *    peer:///segment_name
 * Note the host component is empty.
 */
public class PeerServerSegmentFetcher extends BaseSegmentFetcher {
  private static final String PEER_2_PEER_PROTOCOL = "peer";
  private static final String PEER_SEGMENT_DOWNLOAD_SCHEME = "peerSegmentDownloadScheme";
  private HelixManager _helixManager;
  private String _helixClusterName;
  private HttpSegmentFetcher _httpSegmentFetcher;
  // The value is either https or http
  private final String _httpScheme;

  public PeerServerSegmentFetcher(Configuration config, HelixManager helixManager, String helixClusterName) {
    _helixManager = helixManager;
    _helixClusterName = helixClusterName;
    switch (config.getString(PEER_SEGMENT_DOWNLOAD_SCHEME)) {
      case "https":
        _httpSegmentFetcher = new HttpsSegmentFetcher();
        _httpScheme = "https";
        break;
      default:
        _httpSegmentFetcher = new HttpSegmentFetcher();
        _httpScheme = "http";
    }
    _httpSegmentFetcher.init(config);
  }

  public PeerServerSegmentFetcher(HttpSegmentFetcher httpSegmentFetcher, String httpScheme, HelixManager helixManager,
      String helixClusterName) {
    _helixManager = helixManager;
    _helixClusterName = helixClusterName;
    _httpSegmentFetcher = httpSegmentFetcher;
    _httpScheme = httpScheme;
  }

  @Override
  public void fetchSegmentToLocalWithoutRetry(URI uri, File dest)
      throws Exception {
    if (!PEER_2_PEER_PROTOCOL.equals(uri.getScheme())) {
      throw new IllegalArgumentException(uri.toString());
    }
    if (uri.getPath() == null || !uri.getPath().startsWith("/")) {
      throw new IllegalArgumentException(uri.toString());
    }
    String peerServerUri = getPeerServerURI(uri.getPath().substring(1));
    if (peerServerUri == null) {
      throw new Exception("Unable to find peer server for segment " + uri);
    }

    _logger.info("Use peer server uri {} for segment download with class {}.", peerServerUri,
        SegmentFetcherFactory.getSegmentFetcher(new URI(peerServerUri).getScheme()).getClass().getName());
    _httpSegmentFetcher.fetchSegmentToLocal(new URI(peerServerUri), dest);
    _logger.info("Download succeed with {}",
        SegmentFetcherFactory.getSegmentFetcher(new URI(peerServerUri).getScheme()).getClass().getName());
  }

  // Return the address of an ONLINE server hosting a segment. The returned address is of format
  //  http(s)://hostname:port/segments/tablenameWithType/segmentName
  private String getPeerServerURI(String segmentName) {
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    String tableNameWithType =
        TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(llcSegmentName.getTableName());

    HelixAdmin helixAdmin = _helixManager.getClusterManagmentTool();
    ExternalView externalViewForResource =
        HelixHelper.getExternalViewForResource(helixAdmin, _helixClusterName, tableNameWithType);
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

      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(_helixClusterName, instanceId);
      String hostName = instanceConfig.getHostName();

      int port;
      try {
        HelixHelper.getInstanceConfigsMapFor(instanceId, _helixClusterName, helixAdmin);
        port = Integer.parseInt(HelixHelper.getInstanceConfigsMapFor(instanceId, _helixClusterName, helixAdmin)
            .get(CommonConstants.Helix.Instance.ADMIN_PORT_KEY));
      } catch (Exception e) {
        port = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
      }

      return StringUtil
          .join("/", _httpScheme + "://" + hostName + ":" + port, "segments", tableNameWithType, segmentName);
    }
    return null;
  }
}
