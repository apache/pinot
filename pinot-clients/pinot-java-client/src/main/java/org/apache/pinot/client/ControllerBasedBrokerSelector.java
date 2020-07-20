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
package org.apache.pinot.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ControllerBasedBrokerSelector implements BrokerSelector {
  public static final int DEFAULT_CONTROLLER_REQUEST_RETRIES = 3;
  public static final long DEFAULT_CONTROLLER_REQUEST_RETRIES_INTERVAL_IN_MILLS = 5000; // 5 seconds
  public static final long DEFAULT_CONTROLLER_REQUEST_SCHEDULE_INTERVAL_IN_MILLS = 300000; // 5 minutes
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerBasedBrokerSelector.class);
  private static final String CONTROLLER_ONLINE_TABLE_BROKERS_MAP_URL_TEMPLATE = "%s/brokers/tables?state=online";
  private static final Random RANDOM = new Random();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final String[] _controllerUrls;
  private final AtomicReference<Map<String, List<String>>> _tableToBrokerListMapRef =
      new AtomicReference<Map<String, List<String>>>();
  private final AtomicReference<List<String>> _allBrokerListRef = new AtomicReference<List<String>>();
  private final int _controllerFetchRetries;
  private final long _controllerFetchRetriesIntervalMills;
  private final long _controllerFetchScheduleIntervalMills;

  public ControllerBasedBrokerSelector(String controllerUrl) {
    this(controllerUrl, DEFAULT_CONTROLLER_REQUEST_RETRIES, DEFAULT_CONTROLLER_REQUEST_RETRIES_INTERVAL_IN_MILLS,
        DEFAULT_CONTROLLER_REQUEST_SCHEDULE_INTERVAL_IN_MILLS);
  }

  public ControllerBasedBrokerSelector(String controllerUrl, int controllerFetchRetries,
      long controllerFetchRetriesIntervalMills, long controllerFetchScheduleIntervalMills) {
    _controllerUrls = controllerUrl.split(",");
    _controllerFetchRetries = controllerFetchRetries;
    _controllerFetchRetriesIntervalMills = controllerFetchRetriesIntervalMills;
    _controllerFetchScheduleIntervalMills = controllerFetchScheduleIntervalMills;
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(() -> refreshBroker(), _controllerFetchScheduleIntervalMills,
        _controllerFetchScheduleIntervalMills, TimeUnit.MILLISECONDS);
    refreshBroker();
  }

  private void refreshBroker() {
    Map<String, List<String>> tableToBrokerListMap = getTableToBrokersMap();
    if (tableToBrokerListMap == null) {
      throw new RuntimeException("Unable to fetch broker information from controller");
    }
    _tableToBrokerListMapRef.set(ImmutableMap.copyOf(tableToBrokerListMap));
    Set<String> brokerSet = new HashSet<>();
    for (List<String> brokerList : tableToBrokerListMap.values()) {
      brokerSet.addAll(brokerList);
    }
    _allBrokerListRef.set(ImmutableList.copyOf(brokerSet));
  }

  private Map<String, List<String>> getTableToBrokersMap() {
    int controllerIdx = RANDOM.nextInt(_controllerUrls.length);
    for (int i = 0; i < _controllerFetchRetries; i++) {
      try {
        String brokerTableQueryResp = IOUtils.toString(
            new URL(String.format(CONTROLLER_ONLINE_TABLE_BROKERS_MAP_URL_TEMPLATE, _controllerUrls[controllerIdx])));
        Map<String, List<String>> tablesToBrokersMap = OBJECT_MAPPER.readValue(brokerTableQueryResp, Map.class);
        for (String table : tablesToBrokersMap.keySet()) {
          List<String> brokerHostPorts = new ArrayList<>();
          for (String broker : tablesToBrokersMap.get(table)) {
            brokerHostPorts.add(Utils.generateBrokerHostPort(broker));
          }
          tablesToBrokersMap.put(table, brokerHostPorts);
        }
        return tablesToBrokersMap;
      } catch (Exception e) {
        LOGGER.warn("Unable to fetch controller broker information from '{}', retry in {} millseconds",
            _controllerUrls[controllerIdx], _controllerFetchRetriesIntervalMills, e);
        controllerIdx = (controllerIdx + 1) % _controllerUrls.length;
        try {
          Thread.sleep(_controllerFetchRetriesIntervalMills);
        } catch (InterruptedException interruptedException) {
          // Swallow
        }
      }
    }
    LOGGER.warn("Failed to fetch controller broker information with {} retries", _controllerFetchRetries);
    return null;
  }

  @Override
  public String selectBroker(String table) {
    if (table == null) {
      List<String> list = _allBrokerListRef.get();
      if (list != null && !list.isEmpty()) {
        return list.get(RANDOM.nextInt(list.size()));
      } else {
        return null;
      }
    }
    String tableName =
        table.replace(ExternalViewReader.OFFLINE_SUFFIX, "").replace(ExternalViewReader.REALTIME_SUFFIX, "");
    List<String> list = _tableToBrokerListMapRef.get().get(tableName);
    if (list != null && !list.isEmpty()) {
      return list.get(RANDOM.nextInt(list.size()));
    }
    return null;
  }
}
