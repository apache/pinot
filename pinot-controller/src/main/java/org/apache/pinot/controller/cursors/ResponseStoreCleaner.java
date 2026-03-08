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
package org.apache.pinot.controller.cursors;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.http.MultiHttpRequest;
import org.apache.pinot.common.http.MultiHttpRequestResponse;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.resources.InstanceInfo;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ResponseStoreCleaner periodically gets all responses stored in a response store and deletes the ones that have
 * expired. From each broker, tt gets the list of responses. Each of the response has an expiration unix timestamp.
 * If the current timestamp is greater, it calls a DELETE API for every response that has expired.
 */
public class ResponseStoreCleaner extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResponseStoreCleaner.class);
  // Increased timeout to handle large response stores
  private static final int GET_TIMEOUT_MS = 60_000;
  private static final int DELETE_TIMEOUT_MS = 10_000;
  private static final String QUERY_RESULT_STORE = "%s://%s:%d/responseStore";
  private static final String DELETE_QUERY_RESULT = "%s://%s:%d/responseStore/%s";
  // Used in tests to trigger the delete instead of waiting for the wall clock to move to an appropriate time.
  public static final String CLEAN_AT_TIME = "response.store.cleaner.clean.at.ms";
  private final ControllerConf _controllerConf;
  private final Executor _executor;
  private final PoolingHttpClientConnectionManager _connectionManager;
  private final AuthProvider _authProvider;

  public ResponseStoreCleaner(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerMetrics controllerMetrics, Executor executor,
      PoolingHttpClientConnectionManager connectionManager) {
    super("ResponseStoreCleaner", getFrequencyInSeconds(config), getInitialDelayInSeconds(config),
        pinotHelixResourceManager, leadControllerManager, controllerMetrics);
    _controllerConf = config;
    _executor = executor;
    _connectionManager = connectionManager;
    _authProvider =
        AuthProviderUtils.extractAuthProvider(config, ControllerConf.CONTROLLER_BROKER_AUTH_PREFIX);
  }

  private static long getInitialDelayInSeconds(ControllerConf config) {
    long initialDelay = config.getPeriodicTaskInitialDelayInSeconds();
    String responseStoreCleanerTaskInitialDelay =
        config.getProperty(CommonConstants.CursorConfigs.RESPONSE_STORE_CLEANER_INITIAL_DELAY);
    if (responseStoreCleanerTaskInitialDelay != null) {
      initialDelay = TimeUnit.SECONDS.convert(TimeUtils.convertPeriodToMillis(responseStoreCleanerTaskInitialDelay),
          TimeUnit.MILLISECONDS);
    }
    return initialDelay;
  }

  private static long getFrequencyInSeconds(ControllerConf config) {
    long frequencyInSeconds = TimeUnit.SECONDS.convert(
        TimeUtils.convertPeriodToMillis(CommonConstants.CursorConfigs.DEFAULT_RESPONSE_STORE_CLEANER_FREQUENCY_PERIOD),
        TimeUnit.MILLISECONDS);
    String responseStoreCleanerTaskPeriod =
        config.getProperty(CommonConstants.CursorConfigs.RESPONSE_STORE_CLEANER_FREQUENCY_PERIOD);
    if (responseStoreCleanerTaskPeriod != null) {
      frequencyInSeconds = TimeUnit.SECONDS.convert(TimeUtils.convertPeriodToMillis(responseStoreCleanerTaskPeriod),
          TimeUnit.MILLISECONDS);
    }

    return frequencyInSeconds;
  }

  @Override
  protected void processTables(List<String> tableNamesWithType, Properties periodicTaskProperties) {
    long cleanAtMs = System.currentTimeMillis();
    String cleanAtMsStr = periodicTaskProperties.getProperty(CLEAN_AT_TIME);
    if (cleanAtMsStr != null) {
      cleanAtMs = Long.parseLong(cleanAtMsStr);
    }
    doClean(cleanAtMs);
  }

  public void doClean(long currentTime) {
    List<InstanceConfig> brokerList = _pinotHelixResourceManager.getAllBrokerInstanceConfigs();
    Map<String, InstanceInfo> brokers = new HashMap<>();
    for (InstanceConfig broker : brokerList) {
      brokers.put(getInstanceKey(broker.getHostName(), broker.getPort()),
          new InstanceInfo(broker.getInstanceName(), broker.getHostName(), Integer.parseInt(broker.getPort()),
              Integer.parseInt(HelixHelper.getGrpcPort(broker))));
    }

    Map<String, String> requestHeaders;
    try {
      requestHeaders = AuthProviderUtils.makeAuthHeadersMap(_authProvider);
    } catch (Exception e) {
      LOGGER.error("Failed to create auth headers for response store cleanup", e);
      return;
    }

    Map<String, List<CursorResponseNative>> brokerCursorsMap;
    try {
      brokerCursorsMap = getAllQueryResults(brokers, requestHeaders);
    } catch (Exception e) {
      LOGGER.error("Failed to get query results from brokers for cleanup", e);
      return;
    }

    String protocol = _controllerConf.getControllerBrokerProtocol();
    int portOverride = _controllerConf.getControllerBrokerPortOverride();

    // Process each broker independently to ensure partial failures don't block cleanup of other brokers
    for (Map.Entry<String, List<CursorResponseNative>> entry : brokerCursorsMap.entrySet()) {
      String brokerKey = entry.getKey();
      InstanceInfo broker = brokers.get(brokerKey);

      // Collect URLs for expired responses for THIS broker only
      List<String> brokerUrls = new ArrayList<>();
      for (CursorResponse response : entry.getValue()) {
        if (response.getExpirationTimeMs() <= currentTime) {
          int port = portOverride > 0 ? portOverride : broker.getPort();
          brokerUrls.add(
              String.format(DELETE_QUERY_RESULT, protocol, broker.getHost(), port, response.getRequestId()));
        }
      }

      if (brokerUrls.isEmpty()) {
        LOGGER.debug("No expired responses to clean up for broker: {}", brokerKey);
        continue;
      }

      LOGGER.info("Cleaning up {} expired responses from broker: {}", brokerUrls.size(), brokerKey);

      try {
        Map<String, String> deleteStatus =
            deleteExpiredResponses(requestHeaders, brokerUrls);
        deleteStatus.forEach(
            (key, value) -> LOGGER.info("ResponseStore delete response - Broker: {}. Response: {}", key, value));
      } catch (Exception e) {
        // Log error but continue with other brokers - don't let one broker failure block cleanup of others
        LOGGER.error("Failed to delete expired responses from broker: {}. Will retry on next cleanup cycle.",
            brokerKey, e);
      }
    }
  }

  /**
   * Delete expired responses from brokers. Treats 404 responses as success since the goal
   * is to ensure the response doesn't exist (idempotent delete).
   */
  private Map<String, String> deleteExpiredResponses(Map<String, String> requestHeaders, List<String> brokerUrls)
      throws Exception {
    List<Pair<String, String>> urlsAndRequestBodies = new ArrayList<>(brokerUrls.size());
    brokerUrls.forEach((url) -> urlsAndRequestBodies.add(Pair.of(url, "")));

    CompletionService<MultiHttpRequestResponse> completionService =
        new MultiHttpRequest(_executor, _connectionManager).execute(urlsAndRequestBodies, requestHeaders,
            DELETE_TIMEOUT_MS, "DELETE", HttpDelete::new);

    Map<String, String> responseMap = new HashMap<>();
    List<String> errMessages = new ArrayList<>();

    for (int i = 0; i < brokerUrls.size(); i++) {
      try (MultiHttpRequestResponse httpRequestResponse = completionService.take().get()) {
        URI uri = httpRequestResponse.getURI();
        int status = httpRequestResponse.getResponse().getCode();
        String responseString = EntityUtils.toString(httpRequestResponse.getResponse().getEntity());

        if (status == 200) {
          responseMap.put(getInstanceKey(uri.getHost(), Integer.toString(uri.getPort())), responseString);
        } else if (status == 404) {
          // 404 means the response is already deleted - this is acceptable for idempotent cleanup
          LOGGER.debug("Response already deleted (404) for uri: {}", uri);
          responseMap.put(getInstanceKey(uri.getHost(), Integer.toString(uri.getPort())),
              "Already deleted (was 404)");
        } else {
          // Other errors are unexpected and should be logged
          LOGGER.warn("Unexpected status={} from uri='{}', response='{}'", status, uri, responseString);
          errMessages.add(String.format("Unexpected status=%d from uri='%s'", status, uri));
        }
      } catch (Exception e) {
        LOGGER.error("Failed to execute DELETE op", e);
        errMessages.add(e.getMessage());
      }
    }

    if (!errMessages.isEmpty()) {
      throw new RuntimeException("Some delete operations failed: " + StringUtils.join(errMessages, ", "));
    }
    return responseMap;
  }

  private Map<String, List<CursorResponseNative>> getAllQueryResults(Map<String, InstanceInfo> brokers,
      Map<String, String> requestHeaders)
      throws Exception {
    String protocol = _controllerConf.getControllerBrokerProtocol();
    int portOverride = _controllerConf.getControllerBrokerPortOverride();
    List<String> brokerUrls = new ArrayList<>();
    for (InstanceInfo broker : brokers.values()) {
      int port = portOverride > 0 ? portOverride : broker.getPort();
      brokerUrls.add(String.format(QUERY_RESULT_STORE, protocol, broker.getHost(), port));
    }
    LOGGER.debug("Getting stored responses via broker urls: {}", brokerUrls);

    List<Pair<String, String>> urlsAndRequestBodies = new ArrayList<>(brokerUrls.size());
    brokerUrls.forEach((url) -> urlsAndRequestBodies.add(Pair.of(url, "")));

    CompletionService<MultiHttpRequestResponse> completionService =
        new MultiHttpRequest(_executor, _connectionManager).execute(urlsAndRequestBodies, requestHeaders,
            GET_TIMEOUT_MS, "GET", HttpGet::new);

    Map<String, List<CursorResponseNative>> responseMap = new HashMap<>();
    List<String> errMessages = new ArrayList<>();

    for (int i = 0; i < brokerUrls.size(); i++) {
      try (MultiHttpRequestResponse httpRequestResponse = completionService.take().get()) {
        URI uri = httpRequestResponse.getURI();
        int status = httpRequestResponse.getResponse().getCode();
        String responseString = EntityUtils.toString(httpRequestResponse.getResponse().getEntity());

        if (status != 200) {
          errMessages.add(String.format("Unexpected status=%d from uri='%s', response='%s'",
              status, uri, responseString));
          continue;
        }

        String brokerKey = getInstanceKey(uri.getHost(), Integer.toString(uri.getPort()));
        try {
          List<CursorResponseNative> responses = JsonUtils.stringToObject(responseString, new TypeReference<>() {
          });
          responseMap.put(brokerKey, responses);
          LOGGER.debug("Got {} stored responses from broker: {}", responses.size(), brokerKey);
        } catch (IOException ex) {
          LOGGER.error("Failed to parse response from broker: {}", brokerKey, ex);
          errMessages.add(String.format("Failed to parse response from broker '%s': %s", brokerKey, ex.getMessage()));
        }
      } catch (Exception e) {
        LOGGER.error("Failed to execute GET op", e);
        errMessages.add(e.getMessage());
      }
    }

    if (!errMessages.isEmpty()) {
      LOGGER.warn("Some brokers failed to respond: {}", errMessages);
      // Only throw if ALL brokers failed - allow partial success
      if (responseMap.isEmpty()) {
        throw new RuntimeException("All brokers failed to respond: " + StringUtils.join(errMessages, ", "));
      }
    }

    return responseMap;
  }

  private static String getInstanceKey(String hostname, String port) {
    return hostname + ":" + port;
  }
}
