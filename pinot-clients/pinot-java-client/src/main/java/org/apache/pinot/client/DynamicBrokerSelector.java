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

import static org.apache.pinot.client.ExternalViewReader.OFFLINE_SUFFIX;
import static org.apache.pinot.client.ExternalViewReader.REALTIME_SUFFIX;


/**
 * Maintains a mapping between table name and list of brokers
 */
public class DynamicBrokerSelector implements BrokerSelector, IZkDataListener {
  AtomicReference<Map<String, List<String>>> tableToBrokerListMapRef = new AtomicReference<Map<String, List<String>>>();
  AtomicReference<List<String>> allBrokerListRef = new AtomicReference<List<String>>();
  private final Random _random = new Random();
  private ExternalViewReader evReader;

  public DynamicBrokerSelector(String zkServers) {
    ZkClient zkClient = getZkClient(zkServers);
    zkClient.setZkSerializer(new BytesPushThroughSerializer());
    zkClient.waitUntilConnected(60, TimeUnit.SECONDS);
    zkClient.subscribeDataChanges(ExternalViewReader.BROKER_EXTERNAL_VIEW_PATH, this);
    evReader = getEvReader(zkClient);
    refresh();
  }

  protected ZkClient getZkClient(String zkServers) {
    return new ZkClient(zkServers);
  }

  protected ExternalViewReader getEvReader(ZkClient zkClient) {
    return new ExternalViewReader(zkClient);
  }

  private void refresh() {
    Map<String, List<String>> tableToBrokerListMap = evReader.getTableToBrokersMap();
    tableToBrokerListMapRef.set(tableToBrokerListMap);
    Set<String> brokerSet = new HashSet<>();
    for (List<String> brokerList : tableToBrokerListMap.values()) {
      brokerSet.addAll(brokerList);
    }
    allBrokerListRef.set(new ArrayList<>(brokerSet));
  }

  @Nullable
  @Override
  public String selectBroker(String table) {
    if (table == null) {
      List<String> list = allBrokerListRef.get();
      if (list != null && !list.isEmpty()) {
        return list.get(_random.nextInt(list.size()));
      } else {
        return null;
      }
    }
    String tableName = table.replace(OFFLINE_SUFFIX, "").replace(REALTIME_SUFFIX, "");
    List<String> list = tableToBrokerListMapRef.get().get(tableName);
    if (list != null && !list.isEmpty()) {
      return list.get(_random.nextInt(list.size()));
    }
    return null;
  }

  @Override
  public void handleDataChange(String dataPath, Object data)
      throws Exception {
    refresh();
  }


  @Override
  public void handleDataDeleted(String dataPath)
      throws Exception {
    refresh();
  }
}
