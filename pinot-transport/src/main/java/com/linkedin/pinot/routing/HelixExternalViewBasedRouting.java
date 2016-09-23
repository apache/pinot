/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.routing;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.routing.builder.BalancedRandomRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.KafkaHighLevelConsumerBasedRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.KafkaLowLevelConsumerRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.transport.common.SegmentIdSet;

/*
 * TODO
 * Would be better to not have the external view based routing not aware of the fact that there are HLC and LLC
 * implementations. A better way to do it would be to have a RoutingTable implementation that merges the output
 * of an offline routing table and a realtime routing table, with the realtime routing table being aware of the
 * fact that there is both an hlc and llc one.
 */
public class HelixExternalViewBasedRouting implements RoutingTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixExternalViewBasedRouting.class);
  private final RoutingTableBuilder _offlineRoutingTableBuilder;
  private final RoutingTableBuilder _realtimeHLCRoutingTableBuilder;
  private final RoutingTableBuilder _realtimeLLCRoutingTableBuilder;

  /*
   * _brokerRoutingTable has entries for offline as well as realtime tables. For the
   * realtime tables it has entries consisting of high-level kafka consumer segments only.
   *
   * _llcBrokerRoutingTable has entries for realtime tables only, and has entries for low-level
   * kafka consumer segments only.
   */
  private final Map<String, List<ServerToSegmentSetMap>> _brokerRoutingTable =
      new ConcurrentHashMap<String, List<ServerToSegmentSetMap>>();
  private final Map<String, List<ServerToSegmentSetMap>> _llcBrokerRoutingTable =
      new ConcurrentHashMap<String, List<ServerToSegmentSetMap>>();
  private final Map<String, Integer> _routingTableLastKnownZkVersionMap = new ConcurrentHashMap<>();
  private final Random _random = new Random(System.currentTimeMillis());
  private final HelixExternalViewBasedTimeBoundaryService _timeBoundaryService;
  private final RoutingTableSelector _routingTableSelector;

  private BrokerMetrics _brokerMetrics;

  public HelixExternalViewBasedRouting(ZkHelixPropertyStore<ZNRecord> propertyStore,
      RoutingTableSelector routingTableSelector) {
    _timeBoundaryService = new HelixExternalViewBasedTimeBoundaryService(propertyStore);
    _offlineRoutingTableBuilder = new BalancedRandomRoutingTableBuilder();
    _realtimeHLCRoutingTableBuilder = new KafkaHighLevelConsumerBasedRoutingTableBuilder();
    _realtimeLLCRoutingTableBuilder = new KafkaLowLevelConsumerRoutingTableBuilder();
    _routingTableSelector = routingTableSelector;
  }

  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {
    String tableName = request.getTableName();
    List<ServerToSegmentSetMap> serverToSegmentSetMaps;

    if (CommonConstants.Helix.TableType.REALTIME.equals(TableNameBuilder.getTableTypeFromTableName(tableName))) {
      if (_brokerRoutingTable.containsKey(tableName) && _brokerRoutingTable.get(tableName).size() != 0) {
        if (_llcBrokerRoutingTable.containsKey(tableName) && _llcBrokerRoutingTable.get(tableName).size() != 0) {
          // Has both high and low-level segments. Follow what the routing table selector says.
          if (_routingTableSelector.shouldUseLLCRouting(tableName)) {
            serverToSegmentSetMaps = routeToLLC(tableName);
          } else {
            serverToSegmentSetMaps = routeToHLC(tableName);
          }
        } else {
          // Has only hi-level consumer segments.
          serverToSegmentSetMaps = routeToHLC(tableName);
        }
      } else {
        // May have only low-level consumer segments
        serverToSegmentSetMaps = routeToLLC(tableName);
      }
    } else {  // Offline table, use the conventional routing table
      serverToSegmentSetMaps = _brokerRoutingTable.get(tableName);
    }

    // This map can be potentially empty, for example for realtime table with no segments.
    if ( serverToSegmentSetMaps == null || serverToSegmentSetMaps.isEmpty()) {
      return Collections.emptyMap();
    }
    return serverToSegmentSetMaps.get(_random.nextInt(serverToSegmentSetMaps.size())).getRouting();
  }

  private List<ServerToSegmentSetMap> routeToLLC(String tableName) {
    if (_brokerMetrics != null) {
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.LLC_QUERY_COUNT, 1);
    }
    return _llcBrokerRoutingTable.get(tableName);
  }

  private List<ServerToSegmentSetMap> routeToHLC(String tableName) {
    if (_brokerMetrics != null) {
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.HLC_QUERY_COUNT, 1);
    }
    return _brokerRoutingTable.get(tableName);
  }

  public void setBrokerMetrics(BrokerMetrics brokerMetrics) {
    _brokerMetrics = brokerMetrics;
  }

  @Override
  public void start() {
    LOGGER.info("Starting HelixExternalViewBasedRouting!");
  }

  @Override
  public void shutdown() {
    LOGGER.info("Shutting down HelixExternalViewBasedRouting!");
  }

  public void markDataResourceOnline(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigList) {
    if (externalView == null) {
      return;
    }
    int externalViewRecordVersion = externalView.getRecord().getVersion();
    if (_routingTableLastKnownZkVersionMap.containsKey(tableName)) {
      long lastKnownZkVersion = _routingTableLastKnownZkVersionMap.get(tableName);
      if (externalViewRecordVersion == lastKnownZkVersion) {
        LOGGER.info(
            "No change on routing table version (current version {}, last known version {}), do nothing for table {}",
            externalViewRecordVersion, lastKnownZkVersion, tableName);
        return;
      }

      LOGGER.info(
          "Updating routing table for table {} due to ZK change (current version {}, last known version {})",
          tableName, externalViewRecordVersion, lastKnownZkVersion);
    }

    _routingTableLastKnownZkVersionMap.put(tableName, externalViewRecordVersion);
    RoutingTableBuilder routingTableBuilder;
    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);

    if (CommonConstants.Helix.TableType.REALTIME.equals(tableType)) {
      routingTableBuilder = _realtimeHLCRoutingTableBuilder;
    } else {
      routingTableBuilder = _offlineRoutingTableBuilder;
    }

    LOGGER.info("Trying to compute routing table for table {} using {}", tableName, routingTableBuilder);

    try {
      List<ServerToSegmentSetMap> serverToSegmentSetMap =
          routingTableBuilder.computeRoutingTableFromExternalView(tableName, externalView, instanceConfigList);

      _brokerRoutingTable.put(tableName, serverToSegmentSetMap);
      if (CommonConstants.Helix.TableType.REALTIME.equals(tableType)) {
        try {
          List<ServerToSegmentSetMap> llcserverToSegmentSetMap = _realtimeLLCRoutingTableBuilder.computeRoutingTableFromExternalView(tableName, externalView, instanceConfigList);

          _llcBrokerRoutingTable.put(tableName, llcserverToSegmentSetMap);
        } catch (Exception e) {
          LOGGER.error("Failed to compute LLC routing table for {}. Ignoring", tableName, e);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to compute/update the routing table", e);
    }

    try {
      LOGGER.info("Trying to compute time boundary service for table {}", tableName);
      _timeBoundaryService.updateTimeBoundaryService(externalView);
    } catch (Exception e) {
      LOGGER.error("Failed to update the TimeBoundaryService", e);
    }
  }

  public void markDataResourceOffline(String tableName) {
    LOGGER.info("Trying to remove data table from broker for {}", tableName);
    _brokerRoutingTable.remove(tableName);
    _routingTableLastKnownZkVersionMap.remove(tableName);
    _timeBoundaryService.remove(tableName);
  }

  public TimeBoundaryService getTimeBoundaryService() {
    return _timeBoundaryService;
  }

  @Override
  public String dumpSnapshot(String tableName)
      throws Exception {
    JSONObject ret = new JSONObject();
    JSONArray routingTableSnapshot = new JSONArray();

    for (String currentTable : _brokerRoutingTable.keySet()) {
      if (tableName == null || currentTable.startsWith(tableName)) {
        JSONObject tableEntry = new JSONObject();
        tableEntry.put("tableName", currentTable);

        JSONArray entries = new JSONArray();
        List<ServerToSegmentSetMap> routableTable = _brokerRoutingTable.get(currentTable);
        for (ServerToSegmentSetMap serverToInstaceMap : routableTable) {
          entries.put(new JSONObject(serverToInstaceMap.toString()));
        }
        tableEntry.put("routingTableEntries", entries);

        routingTableSnapshot.put(tableEntry);
      }
    }

    ret.put("routingTableSnapshot", routingTableSnapshot);

    routingTableSnapshot = new JSONArray();
    for (String currentTable : _llcBrokerRoutingTable.keySet()) {
      if (tableName == null || currentTable.startsWith(tableName)) {
        JSONObject tableEntry = new JSONObject();
        tableEntry.put("tableName", currentTable);

        JSONArray entries = new JSONArray();
        List<ServerToSegmentSetMap> routableTable = _llcBrokerRoutingTable.get(currentTable);
        for (ServerToSegmentSetMap serverToInstaceMap : routableTable) {
          entries.put(new JSONObject(serverToInstaceMap.toString()));
        }
        tableEntry.put("routingTableEntries", entries);

        routingTableSnapshot.put(tableEntry);
      }
    }
    ret.put("llcRoutingTableSnapshot", routingTableSnapshot);

    ret.put("host", NetUtil.getHostnameOrAddress());

    return ret.toString(2);
  }
}
