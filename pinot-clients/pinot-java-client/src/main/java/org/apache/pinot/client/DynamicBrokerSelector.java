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
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
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
public class DynamicBrokerSelector implements BrokerSelector, IZkDataListener {
  private static final Random RANDOM = new Random();

  private final AtomicReference<Map<String, List<String>>> _tableToBrokerListMapRef = new AtomicReference<>();
  private final AtomicReference<List<String>> _allBrokerListRef = new AtomicReference<>();
  private final ZkClient _zkClient;
  private final ExternalViewReader _evReader;
  private final List<String> _brokerList;

  public DynamicBrokerSelector(String zkServers) {
    _zkClient = getZkClient(zkServers);
    _zkClient.setZkSerializer(new BytesPushThroughSerializer());
    _zkClient.waitUntilConnected(60, TimeUnit.SECONDS);
    _zkClient.subscribeDataChanges(ExternalViewReader.BROKER_EXTERNAL_VIEW_PATH, this);
    _evReader = getEvReader(_zkClient);
    _brokerList = ImmutableList.of(zkServers);
    refresh();
  }

  @VisibleForTesting
  protected ZkClient getZkClient(String zkServers) {
    return new ZkClient(zkServers);
  }

  @VisibleForTesting
  protected ExternalViewReader getEvReader(ZkClient zkClient) {
    return new ExternalViewReader(zkClient);
  }

  private void refresh() {
    Map<String, List<String>> tableToBrokerListMap = _evReader.getTableToBrokersMap();
    _tableToBrokerListMapRef.set(tableToBrokerListMap);
    Set<String> brokerSet = new HashSet<>();
    for (List<String> brokerList : tableToBrokerListMap.values()) {
      brokerSet.addAll(brokerList);
    }
    _allBrokerListRef.set(new ArrayList<>(brokerSet));
  }

  @Nullable
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
    String tableName = table.replace(ExternalViewReader.OFFLINE_SUFFIX, "")
        .replace(ExternalViewReader.REALTIME_SUFFIX, "");
    List<String> list = _tableToBrokerListMapRef.get().get(tableName);
    if (list != null && !list.isEmpty()) {
      return list.get(RANDOM.nextInt(list.size()));
    }
    return null;
  }

  @Override
  public List<String> getBrokers() {
    return _brokerList;
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
