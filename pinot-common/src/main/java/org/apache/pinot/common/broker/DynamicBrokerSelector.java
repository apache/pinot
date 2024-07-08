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
package org.apache.pinot.common.broker;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;


/**
 * Maintains a mapping between table name and list of brokers
 */
public class DynamicBrokerSelector extends BrokerInfoSelector implements IZkDataListener {
  protected static final Random RANDOM = new Random();

  protected final AtomicReference<Map<String, List<BrokerInfo>>> _tableToBrokerListMapRef = new AtomicReference<>();
  protected final AtomicReference<List<BrokerInfo>> _allBrokerListRef = new AtomicReference<>();
  protected final ZkClient _zkClient;
  protected final ExternalViewReader _evReader;

  //The preferTlsPort will be mapped to client config in the future, when we support full TLS
  public DynamicBrokerSelector(String zkServers, boolean preferTlsPort) {
    super(preferTlsPort);
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
    Map<String, List<BrokerInfo>> tableToBrokerListMap = _evReader.getTableToBrokerInfosMap();
    _tableToBrokerListMapRef.set(tableToBrokerListMap);
    Set<BrokerInfo> brokerInfoSet = new HashSet<>();
    for (List<BrokerInfo> brokerInfoList : tableToBrokerListMap.values()) {
      brokerInfoSet.addAll(brokerInfoList);
    }
    _allBrokerListRef.set(new ArrayList<>(brokerInfoSet));
  }

  @Nullable
  @Override
  public BrokerInfo selectBrokerInfo(String... tableNames) {
    List<BrokerInfo> brokerInfoList = _allBrokerListRef.get();
    if (!(tableNames == null || tableNames.length == 0 || tableNames[0] == null)) {
      // getting list of brokers hosting all the tables.
      List<BrokerInfo> commonBrokers = BrokerSelectorUtils.getTablesCommonBrokers(Arrays.asList(tableNames),
          _tableToBrokerListMapRef.get());
      if (commonBrokers != null && !commonBrokers.isEmpty()) {
        brokerInfoList = commonBrokers;
      }
    }

    // Return a broker randomly if table is null or no broker is found for the specified table.
    if (!brokerInfoList.isEmpty()) {
      return brokerInfoList.get(RANDOM.nextInt(brokerInfoList.size()));
    }
    return null;
  }

  @Override
  public List<BrokerInfo> getBrokerInfoList() {
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
