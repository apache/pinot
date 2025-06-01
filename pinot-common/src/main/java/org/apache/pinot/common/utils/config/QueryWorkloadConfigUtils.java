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
package org.apache.pinot.common.utils.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.HttpVersion;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.messages.QueryWorkloadRefreshMessage;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.utils.http.HttpClientConfig;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;


public class QueryWorkloadConfigUtils {
  private QueryWorkloadConfigUtils() {
  }

  private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(QueryWorkloadConfigUtils.class);
  private static final HttpClient HTTP_CLIENT = new HttpClient(HttpClientConfig.DEFAULT_HTTP_CLIENT_CONFIG,
          TlsUtils.getSslContext());

  /**
   * Converts a ZNRecord into a QueryWorkloadConfig object by extracting mapFields.
   *
   * @param znRecord The ZNRecord containing workload config data.
   * @return A QueryWorkloadConfig object.
   */
  public static QueryWorkloadConfig fromZNRecord(ZNRecord znRecord) {
    Preconditions.checkNotNull(znRecord, "ZNRecord cannot be null");
    String queryWorkloadName = znRecord.getSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME);
    Preconditions.checkNotNull(queryWorkloadName, "queryWorkloadName cannot be null");
    String nodeConfigsJson = znRecord.getSimpleField(QueryWorkloadConfig.NODE_CONFIGS);
    Preconditions.checkNotNull(nodeConfigsJson, "nodeConfigs cannot be null");
    try {
      List<NodeConfig> nodeConfigs = JsonUtils.stringToObject(nodeConfigsJson, new TypeReference<>() { });
      return new QueryWorkloadConfig(queryWorkloadName, nodeConfigs);
    } catch (Exception e) {
      String errorMessage = String.format("Failed to convert ZNRecord : %s to QueryWorkloadConfig", znRecord);
      throw new RuntimeException(errorMessage, e);
    }
  }

  /**
   * Updates a ZNRecord with the fields from a WorkloadConfig object.
   *
   * @param queryWorkloadConfig The QueryWorkloadConfig object to convert.
   * @param znRecord The ZNRecord to update.
   */
  public static void updateZNRecordWithWorkloadConfig(ZNRecord znRecord, QueryWorkloadConfig queryWorkloadConfig) {
    znRecord.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, queryWorkloadConfig.getQueryWorkloadName());
    try {
      znRecord.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS,
          JsonUtils.objectToString(queryWorkloadConfig.getNodeConfigs()));
    } catch (Exception e) {
      String errorMessage = String.format("Failed to convert QueryWorkloadConfig : %s to ZNRecord",
          queryWorkloadConfig);
      throw new RuntimeException(errorMessage, e);
    }
  }

  public static void updateZNRecordWithInstanceCost(ZNRecord znRecord, String queryWorkloadName,
      InstanceCost instanceCost) {
    Preconditions.checkNotNull(znRecord, "ZNRecord cannot be null");
    Preconditions.checkNotNull(instanceCost, "InstanceCost cannot be null");
    try {
      znRecord.setSimpleField(QueryWorkloadRefreshMessage.QUERY_WORKLOAD_NAME, queryWorkloadName);
      znRecord.setSimpleField(QueryWorkloadRefreshMessage.INSTANCE_COST, JsonUtils.objectToString(instanceCost));
    } catch (Exception e) {
      String errorMessage = String.format("Failed to convert InstanceCost : %s to ZNRecord",
          instanceCost);
      throw new RuntimeException(errorMessage, e);
    }
  }

  public static InstanceCost getInstanceCostFromZNRecord(ZNRecord znRecord) {
    Preconditions.checkNotNull(znRecord, "ZNRecord cannot be null");
    String instanceCostJson = znRecord.getSimpleField(QueryWorkloadRefreshMessage.INSTANCE_COST);
    Preconditions.checkNotNull(instanceCostJson, "InstanceCost cannot be null");
    try {
      return JsonUtils.stringToObject(instanceCostJson, InstanceCost.class);
    } catch (Exception e) {
      String errorMessage = String.format("Failed to convert ZNRecord : %s to InstanceCost", znRecord);
      throw new RuntimeException(errorMessage, e);
    }
  }
  /**
   * Fetches query workload configs for a specific instance from the controller.
   *
   * @param controllerUrl The URL of the controller.
   * @param instanceId The ID of the instance to fetch configs for.
   * @param nodeType The type of node (e.g., BROKER, SERVER).
   * @return A map of workload names to their corresponding InstanceCost objects.
   */
  public static Map<String, InstanceCost> getQueryWorkloadConfigsFromController(String controllerUrl, String instanceId,
                                                                                NodeConfig.Type nodeType) {
    try {
      if (controllerUrl == null || controllerUrl.isEmpty()) {
        LOGGER.warn("Controller URL is empty, cannot fetch query workload configs for instance: {}", instanceId);
        return Collections.emptyMap();
      }
      URI queryWorkloadURI = new URI(controllerUrl + "/queryWorkloadConfigs/instance/" + instanceId + "?nodeType="
              + nodeType);
      ClassicHttpRequest request = ClassicRequestBuilder.get(queryWorkloadURI)
              .setVersion(HttpVersion.HTTP_1_1)
              .setHeader(HttpHeaders.CONTENT_TYPE, HttpClient.JSON_CONTENT_TYPE)
              .build();
      AtomicReference<Map<String, InstanceCost>> workloadToInstanceCost = new AtomicReference<>(null);
      RetryPolicy retryPolicy = RetryPolicies.exponentialBackoffRetryPolicy(3, 3000L, 1.2f);
      retryPolicy.attempt(() -> {
        try {
          SimpleHttpResponse response = HttpClient.wrapAndThrowHttpException(
                  HTTP_CLIENT.sendRequest(request, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS)
          );
          if (response.getStatusCode() == HttpStatus.SC_OK) {
            workloadToInstanceCost.set(JsonUtils.stringToObject(response.getResponse(), new TypeReference<>() { }));
            LOGGER.info("Successfully fetched query workload configs from controller: {}, Instance: {}",
                    controllerUrl, instanceId);
            return true;
          } else if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
            LOGGER.info("No query workload configs found for controller: {}, Instance: {}", controllerUrl, instanceId);
            workloadToInstanceCost.set(Collections.emptyMap());
            return true;
          } else {
            LOGGER.warn("Failed to fetch query workload configs from controller: {}, Instance: {}, Status: {}",
                    controllerUrl, instanceId, response.getStatusCode());
            return false;
          }
        } catch (Exception e) {
          LOGGER.warn("Failed to fetch query workload configs from controller: {}, Instance: {}",
                  controllerUrl, instanceId, e);
          return false;
        }
      });
      return workloadToInstanceCost.get();
    } catch (Exception e) {
      LOGGER.warn("Failed to fetch query workload configs from controller: {}, Instance: {}",
              controllerUrl, instanceId, e);
      return Collections.emptyMap();
    }
  }

  /**
   * Validates the given QueryWorkloadConfig and returns a list of validation error messages.
   *
   * @param config the QueryWorkloadConfig to validate
   * @return a list of validation errors; empty if config is valid
   */
  public static List<String> validateQueryWorkloadConfig(QueryWorkloadConfig config) {
    List<String> errors = new ArrayList<>();
    if (config == null) {
      errors.add("QueryWorkloadConfig cannot be null");
      return errors;
    }
    String name = config.getQueryWorkloadName();
    if (name == null || name.trim().isEmpty()) {
      errors.add("queryWorkloadName cannot be null or empty");
    }
    List<NodeConfig> nodeConfigs = config.getNodeConfigs();
    if (nodeConfigs == null || nodeConfigs.isEmpty()) {
      errors.add("nodeConfigs cannot be null or empty");
    } else {
      for (int i = 0; i < nodeConfigs.size(); i++) {
        NodeConfig nodeConfig = nodeConfigs.get(i);
        String prefix = "nodeConfigs[" + i + "]";
        if (nodeConfig == null) {
          errors.add(prefix + " cannot be null");
          continue;
        }
        if (nodeConfig.getNodeType() == null) {
          errors.add(prefix + ".type cannot be null");
        }
        // Validate EnforcementProfile
        EnforcementProfile enforcementProfile = nodeConfig.getEnforcementProfile();
        if (enforcementProfile == null) {
          errors.add(prefix + "enforcementProfile cannot be null");
        } else {
           if (enforcementProfile.getCpuCostNs() < 0) {
             errors.add(prefix + ".enforcementProfile.cpuCostNs cannot be negative");
           }
           if (enforcementProfile.getMemoryCostBytes() < 0) {
               errors.add(prefix + ".enforcementProfile.memoryCostBytes cannot be negative");
           }
        }
        // Validate PropagationScheme
        PropagationScheme propagationScheme = nodeConfig.getPropagationScheme();
        if (propagationScheme == null) {
          errors.add(prefix + ".propagationScheme cannot be null");
        } else {
          if (propagationScheme.getPropagationType() == null) {
            errors.add(prefix + ".propagationScheme.type cannot be null");
          }
        }
      }
    }
    return errors;
  }
}
