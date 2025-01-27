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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.pinot.client.utils.BrokerSelectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Maintains a mapping between table name and list of brokers
 */
public class DynamicBrokerSelector implements BrokerSelector, IZkDataListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicBrokerSelector.class);

  protected final AtomicReference<Map<String, List<String>>> _tableToBrokerListMapRef = new AtomicReference<>();
  protected final AtomicReference<List<String>> _allBrokerListRef = new AtomicReference<>();
  protected final ZkClient _zkClient;
  protected final ExternalViewReader _evReader;
  //The preferTlsPort will be mapped to client config in the future, when we support full TLS
  public DynamicBrokerSelector(String zkServers, boolean preferTlsPort) {
    _zkClient = getZkClient(zkServers);
    _zkClient.setZkSerializer(new BytesPushThroughSerializer());
    _zkClient.waitUntilConnected(60, TimeUnit.SECONDS);
    _zkClient.subscribeDataChanges(ExternalViewReader.BROKER_EXTERNAL_VIEW_PATH, this);
    _evReader = getEvReader(_zkClient, preferTlsPort);
    refresh();
  }
  public DynamicBrokerSelector(String zkServers) {
    this(zkServers, false);
  }

  @VisibleForTesting
  protected ZkClient getZkClient(String zkServers) {
    return new ZkClient(zkServers);
  }

  @VisibleForTesting
  protected ExternalViewReader getEvReader(ZkClient zkClient) {
    return getEvReader(zkClient, false);
  }

  @VisibleForTesting
  protected ExternalViewReader getEvReader(ZkClient zkClient, boolean preferTlsPort) {
    return new ExternalViewReader(zkClient, preferTlsPort);
  }

  private void refresh() {
    Map<String, List<String>> tableToBrokerListMap = _evReader.getTableToBrokersMap();
    _tableToBrokerListMapRef.set(tableToBrokerListMap);
    Set<String> brokerSet = new HashSet<>();
    for (List<String> brokerList : tableToBrokerListMap.values()) {
      brokerSet.addAll(brokerList);
    }
    _allBrokerListRef.set(new ArrayList<>(brokerSet));
    LOGGER.info("Refreshed table to broker list map: {}", _tableToBrokerListMapRef.get());
  }

  @Nullable
  @Override
  public String selectBroker(String... tableNames) {
    if (!(tableNames == null || tableNames.length == 0 || tableNames[0] == null)) {
      // getting list of brokers hosting all the tables.
      String randomBroker = BrokerSelectorUtils.getRandomBroker(Arrays.asList(tableNames),
          _tableToBrokerListMapRef.get());
      if (randomBroker != null) {
        return randomBroker;
      }
    }

    // Return a broker randomly if table is null or no broker is found for the specified table.
    List<String> list = _allBrokerListRef.get();
    if (list != null && !list.isEmpty()) {
      return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }
    return null;
  }

  @Override
  public List<String> getBrokers() {
    return _allBrokerListRef.get();
  }

  @Override
  public void close() {
    _zkClient.close();
  }

  @Override
  public void handleDataChange(String dataPath, Object data) {
    refresh();
  }

  @Override
  public void handleDataDeleted(String dataPath) {
    refresh();
  }
}
