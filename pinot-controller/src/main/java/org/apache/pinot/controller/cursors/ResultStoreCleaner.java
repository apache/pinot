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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.http.MultiHttpRequest;
import org.apache.pinot.common.http.MultiHttpRequestResponse;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.resources.InstanceInfo;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResultStoreCleaner extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultStoreCleaner.class);
  private static final int TIMEOUT_MS = 3000;
  private static final String QUERY_RESULT_STORE = "%s://%s:%d/resultStore";
  private static final String DELETE_QUERY_RESULT = "%s://%s:%d/resultStore/%s";
  public static final String CLEAN_AT_TIME = "result.store.cleaner.clean.at.ms";
  private final ControllerConf _controllerConf;
  private final Executor _executor;
  private final PoolingHttpClientConnectionManager _connectionManager;

  public ResultStoreCleaner(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerMetrics controllerMetrics, Executor executor,
      PoolingHttpClientConnectionManager connectionManager) {
    super("ResultStoreCleaner", getFrequencyInSeconds(config), getInitialDelayInSeconds(config),
        pinotHelixResourceManager, leadControllerManager, controllerMetrics);
    _controllerConf = config;
    _executor = executor;
    _connectionManager = connectionManager;
  }

  private static long getInitialDelayInSeconds(ControllerConf config) {
    long initialDelay = config.getPeriodicTaskInitialDelayInSeconds();
    String resultStoreCleanerTaskInitialDelay =
        config.getProperty(CommonConstants.CursorConfigs.RESULT_STORE_CLEANER_INITIAL_DELAY);
    if (resultStoreCleanerTaskInitialDelay != null) {
      initialDelay = TimeUnit.SECONDS.convert(TimeUtils.convertPeriodToMillis(resultStoreCleanerTaskInitialDelay),
          TimeUnit.MILLISECONDS);
    }
    return initialDelay;
  }

  private static long getFrequencyInSeconds(ControllerConf config) {
    long frequencyInSeconds = 0;
    String resultStoreCleanerTaskPeriod =
        config.getProperty(CommonConstants.CursorConfigs.RESULT_STORE_CLEANER_FREQUENCY_PERIOD);
    if (resultStoreCleanerTaskPeriod != null) {
      frequencyInSeconds = TimeUnit.SECONDS.convert(TimeUtils.convertPeriodToMillis(resultStoreCleanerTaskPeriod),
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
    Map<String, InstanceInfo> brokers = brokerList.stream().collect(
        Collectors.toMap(x -> getInstanceKey(x.getHostName(), x.getPort()),
            x -> new InstanceInfo(x.getInstanceName(), x.getHostName(), Integer.parseInt(x.getPort()))));

    try {
      Map<String, List<CursorResponseNative>> brokerResponses = getAllQueryResults(brokers, Collections.emptyMap());

      String protocol = _controllerConf.getControllerBrokerProtocol();
      int portOverride = _controllerConf.getControllerBrokerPortOverride();

      List<String> brokerUrls = new ArrayList<>();
      for (Map.Entry<String, List<CursorResponseNative>> entry : brokerResponses.entrySet()) {
        for (CursorResponse response : entry.getValue()) {
          if (response.getExpirationTimeMs() <= currentTime) {
            InstanceInfo broker = brokers.get(entry.getKey());
            int port = portOverride > 0 ? portOverride : broker.getPort();
            brokerUrls.add(
                String.format(DELETE_QUERY_RESULT, protocol, broker.getHost(), port, response.getRequestId()));
          }
        }
        Map<String, String> responses = getResponseMap(Collections.emptyMap(), brokerUrls, "DELETE", HttpDelete::new);

        responses.forEach((key, value) -> LOGGER.info(
            String.format("ResultStore delete response - Broker: %s. Response: %s", key, value)));
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
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
    LOGGER.debug("Getting running queries via broker urls: {}", brokerUrls);
    Map<String, String> strResponseMap = getResponseMap(requestHeaders, brokerUrls, "GET", HttpGet::new);
    return strResponseMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
      try {
        return JsonUtils.stringToObject(e.getValue(), new TypeReference<>() {
        });
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }));
  }

  private <T extends HttpUriRequestBase> Map<String, String> getResponseMap(Map<String, String> requestHeaders,
      List<String> brokerUrls, String methodName, Function<String, T> httpRequestBaseSupplier)
      throws Exception {
    List<Pair<String, String>> urlsAndRequestBodies = new ArrayList<>(brokerUrls.size());
    brokerUrls.forEach((url) -> urlsAndRequestBodies.add(Pair.of(url, "")));

    CompletionService<MultiHttpRequestResponse> completionService =
        new MultiHttpRequest(_executor, _connectionManager).execute(urlsAndRequestBodies, requestHeaders,
            ResultStoreCleaner.TIMEOUT_MS, methodName, httpRequestBaseSupplier);
    Map<String, String> responseMap = new HashMap<>();
    List<String> errMessages = new ArrayList<>(brokerUrls.size());
    for (int i = 0; i < brokerUrls.size(); i++) {
      try (MultiHttpRequestResponse httpRequestResponse = completionService.take().get()) {
        // The completion order is different from brokerUrls, thus use uri in the response.
        URI uri = httpRequestResponse.getURI();
        int status = httpRequestResponse.getResponse().getCode();
        String responseString = EntityUtils.toString(httpRequestResponse.getResponse().getEntity());
        // Unexpected server responses are collected and returned as exception.
        if (status != 200) {
          throw new Exception(
              String.format("Unexpected status=%d and response='%s' from uri='%s'", status, responseString, uri));
        }
        responseMap.put((getInstanceKey(uri.getHost(), Integer.toString(uri.getPort()))), responseString);
      } catch (Exception e) {
        LOGGER.error("Failed to get queries", e);
        // Can't just throw exception from here as there is a need to release the other connections.
        // So just collect the error msg to throw them together after the for-loop.
        errMessages.add(e.getMessage());
      }
    }
    if (!errMessages.isEmpty()) {
      throw new Exception("Unexpected responses from brokers: " + StringUtils.join(errMessages, ","));
    }
    return responseMap;
  }

  private static String getInstanceKey(String hostname, String port) {
    return hostname + ":" + port;
  }
}
