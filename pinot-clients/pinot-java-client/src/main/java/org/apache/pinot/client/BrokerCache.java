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
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.apache.pinot.client.utils.BrokerSelectorUtils;
import org.apache.pinot.client.utils.ConnectionUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
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
  private static final String DEFAULT_CONTROLLER_READ_TIMEOUT_MS = "60000";
  private static final String DEFAULT_CONTROLLER_CONNECT_TIMEOUT_MS = "2000";
  private static final String DEFAULT_CONTROLLER_HANDSHAKE_TIMEOUT_MS = "2000";
  private static final String DEFAULT_CONTROLLER_TLS_V10_ENABLED = "false";
  private static final String SCHEME = "scheme";

  private final Random _random = new Random();
  private final AsyncHttpClient _client;
  private final String _address;
  private final Map<String, String> _headers;
  private final Properties _properties;
  private volatile BrokerData _brokerData;

  public BrokerCache(Properties properties, String controllerUrl) {
    String scheme = properties.getProperty(SCHEME, CommonConstants.HTTP_PROTOCOL);
    DefaultAsyncHttpClientConfig.Builder builder = Dsl.config();
    if (scheme.contentEquals(CommonConstants.HTTPS_PROTOCOL)) {
      SSLContext sslContext = ConnectionUtils.getSSLContextFromProperties(properties);
      builder.setSslContext(new JdkSslContext(sslContext, true, ClientAuth.OPTIONAL));
    }

    int readTimeoutMs = Integer.parseInt(properties.getProperty("controllerReadTimeoutMs",
        DEFAULT_CONTROLLER_READ_TIMEOUT_MS));
    int connectTimeoutMs = Integer.parseInt(properties.getProperty("controllerConnectTimeoutMs",
        DEFAULT_CONTROLLER_CONNECT_TIMEOUT_MS));
    int handshakeTimeoutMs = Integer.parseInt(properties.getProperty("controllerHandshakeTimeoutMs",
        DEFAULT_CONTROLLER_HANDSHAKE_TIMEOUT_MS));
    String appId = properties.getProperty("appId");
    boolean tlsV10Enabled = Boolean.parseBoolean(properties.getProperty("controllerTlsV10Enabled",
        DEFAULT_CONTROLLER_TLS_V10_ENABLED))
        || Boolean.parseBoolean(System.getProperties().getProperty("controller.tlsV10Enabled",
        DEFAULT_CONTROLLER_TLS_V10_ENABLED));

    TlsProtocols tlsProtocols = TlsProtocols.defaultProtocols(tlsV10Enabled);
    builder.setReadTimeout(readTimeoutMs)
        .setConnectTimeout(connectTimeoutMs)
        .setHandshakeTimeout(handshakeTimeoutMs)
        .setUserAgent(ConnectionUtils.getUserAgentVersionFromClassPath("ua_broker_cache", appId))
        .setEnabledProtocols(tlsProtocols.getEnabledProtocols().toArray(new String[0]));

    _client = Dsl.asyncHttpClient(builder.build());
    ControllerRequestURLBuilder controllerRequestURLBuilder =
        ControllerRequestURLBuilder.baseUrl(scheme + "://" + controllerUrl);
    _address = controllerRequestURLBuilder.forLiveBrokerTablesGet();
    _headers = ConnectionUtils.getHeadersFromProperties(properties);
    _properties = properties;
  }

  private Map<String, List<BrokerInstance>> getTableToBrokersData() throws Exception {
    BoundRequestBuilder getRequest = _client.prepareGet(_address);

    if (_headers != null) {
      _headers.forEach((k, v) -> getRequest.addHeader(k, v));
    }

    Future<Response> responseFuture = getRequest.addHeader("accept", "application/json").execute();
    Response response = responseFuture.get();
    return JsonUtils.inputStreamToObject(response.getResponseBodyAsStream(), RESPONSE_TYPE_REF);
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

  public String getBroker(String... tableNames) {
    List<String> brokers = null;
    if (!(tableNames == null || tableNames.length == 0 || tableNames[0] == null)) {
       // returning list of common brokers hosting all the tables.
       brokers = BrokerSelectorUtils.getTablesCommonBrokers(Arrays.asList(tableNames),
           _brokerData.getTableToBrokerMap());
    }

    if (brokers == null || brokers.isEmpty()) {
      brokers = _brokerData.getBrokers();
    }
    return brokers.get(_random.nextInt(brokers.size()));
  }

  public List<String> getBrokers() {
    return _brokerData.getBrokers();
  }
}
