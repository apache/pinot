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

import com.google.common.collect.BiMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.TableTierInfo;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;


/**
 * Reads segment storage tiers from servers for the given table.
 */
public class TableTierReader {
  private final Executor _executor;
  private final HttpConnectionManager _connectionManager;
  private final PinotHelixResourceManager _helixResourceManager;

  public TableTierReader(Executor executor, HttpConnectionManager connectionManager,
      PinotHelixResourceManager helixResourceManager) {
    _executor = executor;
    _connectionManager = connectionManager;
    _helixResourceManager = helixResourceManager;
  }

  /**
   * Get the segment storage tiers for the given table. The servers or segments not responding the request are
   * recorded in the result to be checked by caller.
   *
   * @param tableNameWithType table name with type
   * @param timeoutMs timeout for reading segment tiers from servers
   * @return details of segment storage tiers for the given table
   */
  public TableTierDetails getTableTierDetails(String tableNameWithType, @Nullable String segmentName, int timeoutMs)
      throws InvalidConfigException {
    Map<String, List<String>> serverToSegmentsMap = new HashMap<>();
    if (segmentName == null) {
      serverToSegmentsMap.putAll(_helixResourceManager.getServerToSegmentsMap(tableNameWithType));
    } else {
      List<String> segmentInList = Collections.singletonList(segmentName);
      for (String server : _helixResourceManager.getServers(tableNameWithType, segmentName)) {
        serverToSegmentsMap.put(server, segmentInList);
      }
    }
    BiMap<String, String> endpoints = _helixResourceManager.getDataInstanceAdminEndpoints(serverToSegmentsMap.keySet());
    ServerTableTierReader serverTableTierReader = new ServerTableTierReader(_executor, _connectionManager);
    Map<String, TableTierInfo> serverToTableTierInfoMap =
        serverTableTierReader.getTableTierInfoFromServers(endpoints, tableNameWithType, timeoutMs);

    TableTierDetails tableTierDetails = new TableTierDetails(tableNameWithType);
    for (Map.Entry<String, List<String>> entry : serverToSegmentsMap.entrySet()) {
      String server = entry.getKey();
      TableTierInfo tableTierInfo = serverToTableTierInfoMap.get(server);
      if (tableTierInfo == null) {
        tableTierDetails._missingServers.add(server);
        continue;
      }
      Map<String, String> segmentTiers = tableTierInfo.getSegmentTiers();
      for (String expectedSegment : entry.getValue()) {
        if (!segmentTiers.containsKey(expectedSegment)) {
          tableTierDetails._missingSegments.computeIfAbsent(server, (k) -> new HashSet<>()).add(expectedSegment);
        } else {
          tableTierDetails._segmentTiers.computeIfAbsent(expectedSegment, (k) -> new HashMap<>())
              .put(server, segmentTiers.get(expectedSegment));
        }
      }
    }
    return tableTierDetails;
  }

  // This class aggregates the TableTierInfo returned from multi servers.
  public static class TableTierDetails {
    private final String _tableName;
    private final Set<String> _missingServers = new HashSet<>();
    private final Map<String/*server*/, Set<String>/*segments*/> _missingSegments = new HashMap<>();
    private final Map<String/*segment*/, Map<String/*server*/, String/*tier*/>> _segmentTiers = new HashMap<>();

    TableTierDetails(String tableName) {
      _tableName = tableName;
    }

    public String getTableName() {
      return _tableName;
    }

    public Map<String, Map<String, String>> getSegmentTiers() {
      return _segmentTiers;
    }

    public Map<String, Set<String>> getMissingSegments() {
      return _missingSegments;
    }

    public Set<String> getMissingServers() {
      return _missingServers;
    }
  }
}
