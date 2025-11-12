/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.pinot.broker.querylog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.QueryLogResponseAggregator;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper that fans out system.query_log requests to peer brokers and aggregates their responses.
 */
public class QueryLogFanoutService {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryLogFanoutService.class);
  private static final String QUERY_ENDPOINT = "/query/sql";
  private static final String FANOUT_HEADER = "X-Pinot-QueryLog-Fanout";

  private final HelixManager _helixManager;
  private final String _clusterName;
  private final String _brokerId;
  private final long _timeoutMs;
  private final String _protocol;
  private final HttpClient _httpClient = HttpClient.getInstance();

  public QueryLogFanoutService(HelixManager helixManager, String brokerId, long timeoutMs, String protocol) {
    _helixManager = helixManager;
    _clusterName = helixManager != null ? helixManager.getClusterName() : null;
    _brokerId = brokerId;
    _timeoutMs = timeoutMs;
    _protocol = StringUtils.defaultIfEmpty(protocol, CommonConstants.HTTP_PROTOCOL);
  }

  public BrokerResponseNative fanout(String sql, JsonNode originalRequest, BrokerResponseNative localResponse,
      RequestContext requestContext, @Nullable HttpHeaders httpHeaders) {
    long startTimeMs = System.currentTimeMillis();
    List<BrokerResponseNative> responses = new ArrayList<>();
    responses.add(localResponse);
    List<QueryProcessingException> fetchExceptions = new ArrayList<>();

    if (_helixManager == null || _clusterName == null) {
      LOGGER.warn("Helix manager not available, skipping query_log fanout");
      return localResponse;
    }

    List<InstanceConfig> brokerInstances = getOnlineBrokerInstances();
    if (brokerInstances.isEmpty()) {
      return localResponse;
    }
    Map<String, String> forwardHeaders = extractHeaders(httpHeaders);
    forwardHeaders.putIfAbsent("Content-Type", "application/json");
    forwardHeaders.putIfAbsent("Accept", "application/json");
    forwardHeaders.put(FANOUT_HEADER, _brokerId);

    ObjectNode requestPayload = buildRequestPayload(originalRequest, sql);
    String payload = requestPayload.toString();

    for (InstanceConfig instanceConfig : brokerInstances) {
      String instanceId = instanceConfig.getInstanceName();
      if (instanceId.equals(_brokerId)) {
        continue;
      }
      if (!instanceConfig.getInstanceEnabled()) {
        continue;
      }
      String host = getHost(instanceConfig);
      String port = instanceConfig.getPort();
      if (host == null || port == null) {
        continue;
      }
      String url = String.format("%s://%s:%s%s", _protocol, host, port, QUERY_ENDPOINT);
      try {
        HttpPost post = new HttpPost(new URI(url));
        post.setEntity(new StringEntity(payload, ContentType.APPLICATION_JSON));
        forwardHeaders.forEach(post::setHeader);
        SimpleHttpResponse response =
            HttpClient.wrapAndThrowHttpException(_httpClient.sendRequest(post, _timeoutMs));
        BrokerResponseNative remoteResponse =
            JsonUtils.stringToObject(response.getResponse(), BrokerResponseNative.class);
        responses.add(remoteResponse);
      } catch (Exception e) {
        LOGGER.warn("Failed to fetch query log from broker {} at {}: {}", instanceId, url, e.getMessage());
        fetchExceptions.add(new QueryProcessingException(QueryErrorCode.BROKER_REQUEST_SEND,
            String.format("Failed to fetch query log from broker %s: %s", instanceId, e.getMessage())));
      }
    }

    BrokerResponseNative aggregated = QueryLogResponseAggregator.aggregate(responses);
    long elapsed = System.currentTimeMillis() - startTimeMs;
    aggregated.setNumServersQueried(brokerInstances.size());
    aggregated.setNumServersResponded(responses.size());
    aggregated.setTimeUsedMs(Math.max(aggregated.getTimeUsedMs(), elapsed));

    List<QueryProcessingException> exceptions = new ArrayList<>(aggregated.getExceptions());
    exceptions.addAll(fetchExceptions);
    aggregated.setExceptions(exceptions);
    aggregated.setBrokerId(_brokerId);
    aggregated.setRequestId(Long.toString(requestContext.getRequestId()));
    aggregated.setTraceInfo(localResponse.getTraceInfo());
    aggregated.setTablesQueried(localResponse.getTablesQueried() != null ? localResponse.getTablesQueried()
        : aggregated.getTablesQueried());
    return aggregated;
  }

  private List<InstanceConfig> getOnlineBrokerInstances() {
    try {
      HelixAdmin helixAdmin = _helixManager.getClusterManagmentTool();
      List<String> instanceNames = helixAdmin.getInstancesInCluster(_clusterName);
      List<InstanceConfig> instances = new ArrayList<>(instanceNames.size());
      for (String instance : instanceNames) {
        if (!instance.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
          continue;
        }
        InstanceConfig config = helixAdmin.getInstanceConfig(_clusterName, instance);
        if (config != null) {
          instances.add(config);
        }
      }
      return instances;
    } catch (Exception e) {
      LOGGER.warn("Failed to fetch broker instances from Helix: {}", e.getMessage());
      return List.of();
    }
  }

  private static String getHost(InstanceConfig instanceConfig) {
    String host = instanceConfig.getHostName();
    if (host == null) {
      host = instanceConfig.getInstanceName();
      if (host != null && host.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
        host = host.substring(CommonConstants.Helix.BROKER_INSTANCE_PREFIX_LENGTH);
      }
    }
    return host;
  }

  private static Map<String, String> extractHeaders(@Nullable HttpHeaders httpHeaders) {
    if (httpHeaders == null) {
      return new HashMap<>();
    }
    Map<String, String> headers = new HashMap<>();
    httpHeaders.getRequestHeaders().forEach((key, values) -> {
      if (values != null && !values.isEmpty()) {
        headers.put(key, values.get(0));
      }
    });
    return headers;
  }

  private static ObjectNode buildRequestPayload(JsonNode originalRequest, String sql) {
    ObjectNode requestPayload = originalRequest != null && originalRequest.isObject()
        ? ((ObjectNode) originalRequest).deepCopy()
        : JsonUtils.newObjectNode();
    requestPayload.put(Broker.Request.SQL, sql);
    Map<String, String> options = new HashMap<>();
    JsonNode optionsNode = requestPayload.get(Broker.Request.QUERY_OPTIONS);
    if (optionsNode != null && optionsNode.isTextual()) {
      options.putAll(RequestUtils.getOptionsFromString(optionsNode.asText()));
    }
    options.put(CommonConstants.Broker.Request.QueryOptionKey.QUERY_LOG_FANOUT, "false");
    requestPayload.put(Broker.Request.QUERY_OPTIONS, toOptionString(options));
    return requestPayload;
  }

  private static String toOptionString(Map<String, String> options) {
    if (options.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, String> entry : options.entrySet()) {
      if (!first) {
        builder.append(';');
      }
      builder.append(entry.getKey()).append('=').append(entry.getValue());
      first = false;
    }
    return builder.toString();
  }
}
