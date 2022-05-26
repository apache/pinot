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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;


/**
 * Maintains table -> list of brokers, supports update
 * TODO can we introduce a SSE based controller endpoint to make the update reactive in the client?
 */
public class BrokerCache {

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class BrokerInstance {
    private String _host;
    private Integer _port;

    public String getHost() {
      return _host;
    }

    public void setHost(String host) {
      _host = host;
    }

    public Integer getPort() {
      return _port;
    }

    public void setPort(Integer port) {
      _port = port;
    }
  }

  private static final TypeReference<Map<String, List<BrokerInstance>>> RESPONSE_TYPE_REF =
      new TypeReference<Map<String, List<BrokerInstance>>>() {
      };
  private final Random _random = new Random();
  private final AsyncHttpClient _client;
  private final String _address;
  private volatile BrokerData _brokerData;

  public BrokerCache(String scheme, String controllerHost, int controllerPort) {
    _client = Dsl.asyncHttpClient();
    ControllerRequestURLBuilder controllerRequestURLBuilder =
        ControllerRequestURLBuilder.baseUrl(scheme + "://" + controllerHost + ":" + controllerPort);
    _address = controllerRequestURLBuilder.forLiveBrokerTablesGet();
  }

  private Map<String, List<BrokerInstance>> getTableToBrokersData() throws Exception {
    BoundRequestBuilder getRequest = _client.prepareGet(_address);
    Future<Response> responseFuture = getRequest.addHeader("accept", "application/json").execute();
    Response response = responseFuture.get();
    String responseBody = response.getResponseBody(StandardCharsets.UTF_8);
    return JsonUtils.stringToObject(responseBody, RESPONSE_TYPE_REF);
  }

  private BrokerData getBrokerData(Map<String, List<BrokerInstance>> responses) {
    Set<String> brokers = new HashSet<>();
    Map<String, List<String>> tableToBrokersMap = new HashMap<>();
    Set<String> uniqueTableNames = new HashSet<>();

    for (Map.Entry<String, List<BrokerInstance>> tableToBrokers : responses.entrySet()) {
      List<String> brokersForTable = new ArrayList<>();
      tableToBrokers.getValue().forEach(br -> {
        String brokerHostPort = br.getHost() + ":" + br.getPort();
        brokersForTable.add(brokerHostPort);
        brokers.add(brokerHostPort);
      });
      String tableName = tableToBrokers.getKey();
      tableToBrokersMap.put(tableName, brokersForTable);

      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      uniqueTableNames.add(rawTableName);
    }

    // Add table names without suffixes
    uniqueTableNames.forEach(tableName -> {
      if (!tableToBrokersMap.containsKey(tableName)) {
        // 2 possible scenarios:
        // 1) Both OFFLINE_SUFFIX and REALTIME_SUFFIX tables present -> use intersection of both the lists
        // 2) Either OFFLINE_SUFFIX or REALTIME_SUFFIX (and npt both) raw table present -> use the list as it is

        String offlineTable = tableName + ExternalViewReader.OFFLINE_SUFFIX;
        String realtimeTable = tableName + ExternalViewReader.REALTIME_SUFFIX;
        if (tableToBrokersMap.containsKey(offlineTable) && tableToBrokersMap.containsKey(realtimeTable)) {
          List<String> realtimeBrokers = tableToBrokersMap.get(realtimeTable);
          List<String> offlineBrokers = tableToBrokersMap.get(offlineTable);
          List<String> tableBrokers =
              realtimeBrokers.stream().filter(offlineBrokers::contains).collect(Collectors.toList());
          tableToBrokersMap.put(tableName, tableBrokers);
        } else {
          tableToBrokersMap.put(tableName, tableToBrokersMap.getOrDefault(offlineTable,
              tableToBrokersMap.getOrDefault(realtimeTable, new ArrayList<>())));
        }
      }
    });

    return new BrokerData(tableToBrokersMap, new ArrayList<>(brokers));
  }

  protected void updateBrokerData()
      throws Exception {
    Map<String, List<BrokerInstance>> responses = getTableToBrokersData();
    _brokerData = getBrokerData(responses);
  }

  public String getBroker(String tableName) {
    List<String> brokers =
        (tableName == null) ? _brokerData.getBrokers() : _brokerData.getTableToBrokerMap().get(tableName);
    return brokers.get(_random.nextInt(brokers.size()));
  }

  public List<String> getBrokers() {
    return _brokerData.getBrokers();
  }
}
