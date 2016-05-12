/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;

/**
 * Maintains a mapping between table name and list of brokers
 */
public class DynamicBrokerSelector implements BrokerSelector, IZkDataListener {
  Map<String, List<String>> tableToBrokerListMap = new HashMap<>();
  private final Random _random = new Random();

  public DynamicBrokerSelector(String zkServers) {
    ZkClient zkClient = new ZkClient(zkServers);
    zkClient.setZkSerializer(new BytesPushThroughSerializer());
    ExternalViewReader evReader = new ExternalViewReader(zkClient);
    tableToBrokerListMap = evReader.getTableToBrokersMap();
    System.out.println(tableToBrokerListMap);
  }

  @Override
  public String selectBroker(String table) {
    List<String> list = tableToBrokerListMap.get(table);
    if (list != null && !list.isEmpty()) {
      return list.get(_random.nextInt(list.size()));
    }
    return null;
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception {
    // TODO Auto-generated method stub
    
  }
}
