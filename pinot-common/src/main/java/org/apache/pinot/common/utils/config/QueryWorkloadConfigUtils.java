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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.pinot.spi.config.workload.PropagationEntity;
import org.apache.pinot.spi.config.workload.PropagationEntityOverrides;
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
          long enforcementCpu = enforcementProfile.getCpuCostNs();
          long enforcementMem = enforcementProfile.getMemoryCostBytes();
           if (enforcementCpu < 0) {
             errors.add(prefix + ".enforcementProfile.cpuCostNs cannot be negative");
           }
           if (enforcementMem < 0) {
             errors.add(prefix + ".enforcementProfile.memoryCostBytes cannot be negative");
           }
          // Validate PropagationScheme
          PropagationScheme propagationScheme = nodeConfig.getPropagationScheme();
          if (propagationScheme == null) {
            errors.add(prefix + ".propagationScheme cannot be null");
          } else {
            PropagationScheme.Type propagationType = propagationScheme.getPropagationType();
            if (propagationType == null) {
              errors.add(prefix + ".propagationScheme.type cannot be null");
            }
            // Validate PropagationEntities
            validateEntityList(propagationScheme.getPropagationEntities(),
                prefix + ".propagationScheme.propagationEntities", errors,
                enforcementProfile.getCpuCostNs(), enforcementProfile.getMemoryCostBytes());
          }
        }
      }
    }
    return errors;
  }

  /**
   * Validates a list of PropagationEntity objects.
   * <p>
   * This method performs comprehensive validation including:
   * <ul>
   *   <li>Ensures the list is non-null and non-empty</li>
   *   <li>Checks for duplicate propagationEntity IDs</li>
   *   <li>Validates cpuCostNs and memoryCostBytes for non-null/non-negative values</li>
   *   <li>Ensures consistency in cost definitions across all entities (either all or none define costs)</li>
   *   <li>Validates that total costs do not exceed provided limits (if any)</li>
   *   <li>Validates any overrides within each entity for the same cost rules</li>
   *   <li>Rewrites empty costs to evenly distribute parent limits if all entities have empty costs</li>
   * </ul>
   *
   */
  private static void validateEntityList(List<PropagationEntity> entities, String prefix,
                                         List<String> errors, Long limitCpu, Long limitMem) {
    if (entities == null || entities.isEmpty()) {
      errors.add(prefix + " cannot be null or empty");
      return;
    }
    Set<String> seenIds = new HashSet<>();
    // Accumulate total CPU/memory costs to ensure they don't exceed enforcementProfile limits
    long totalCpu = 0;
    long totalMem = 0;
    // Track whether costs are defined or empty to ensure consistency across all entities
    int definedCount = 0;
    int emptyCount = 0;
    for (int i = 0; i < entities.size(); i++) {
      PropagationEntity entity = entities.get(i);
      String entityPrefix = prefix + "[" + i + "]";
      if (entity == null) {
        errors.add(entityPrefix + " cannot be null");
        continue;
      }
      validateDuplicateEntity(entity.getEntity(), entityPrefix, seenIds, errors);
      Long currentCpu = entity.getCpuCostNs();
      Long currentMem = entity.getMemoryCostBytes();
      // Both costs must be defined or both null
      // If both are defined, add to totalCpu and totalMem for limit validation
      // If both are null, do nothing
      if (currentCpu != null && currentMem != null) {
        totalCpu += costOrZero(entityPrefix, "cpuCostNs", currentCpu, errors);
        totalMem += costOrZero(entityPrefix, "memoryCostBytes", currentMem, errors);
        definedCount++;
      } else if (currentCpu == null && currentMem == null) {
        emptyCount++;
      } else {
        errors.add(entityPrefix + " must have both cpuCostNs and memoryCostBytes defined or both null");
        break;
      }
      if (definedCount > 0 && emptyCount > 0) {
        errors.add(prefix + " must have either all or none of the propagationEntities define costs");
        break;
      }
      List<PropagationEntityOverrides> overrides = entity.getOverrides();
      if (overrides != null && !overrides.isEmpty()) {
        validateOverrides(overrides, entityPrefix, errors, currentCpu, currentMem);
      }
    }
    validateLimits(totalCpu, totalMem, limitCpu, limitMem, prefix, errors);
    // If no errors and all entities have empty costs, rewrite to evenly distribute enforcementProfile costs
    if (errors.isEmpty() && definedCount == 0 && emptyCount == entities.size()) {
      rewriteEmptyCosts(entities, limitCpu, limitMem);
    }
  }

  private static void validateOverrides(List<PropagationEntityOverrides> overrides, String prefix,
                                        List<String> errors, Long limitCpu, Long limitMem) {
    long totalCpu = 0;
    long totalMem = 0;
    for (int i = 0; i < overrides.size(); i++) {
      PropagationEntityOverrides override = overrides.get(i);
      String overridePrefix = prefix + ".overrides[" + i + "]";
      if (override == null) {
        errors.add(overridePrefix + " cannot be null");
        continue;
      }
      Set<String> seenIds = new HashSet<>();
      validateDuplicateEntity(override.getEntity(), overridePrefix, seenIds, errors);
      // For overrides, costs must be defined for each entry
      totalCpu += costOrZero(overridePrefix, "cpuCostNs", override.getCpuCostNs(), errors);
      totalMem += costOrZero(overridePrefix, "memoryCostBytes", override.getMemoryCostBytes(), errors);
    }
    validateLimits(totalCpu, totalMem, limitCpu, limitMem, prefix, errors);
  }

  private static void validateLimits(Long totalCpu, Long totalMem, Long limitCpu, Long limitMem,
      String prefix, List<String> errors) {
    if (limitCpu != null && totalCpu > limitCpu) {
      errors.add(prefix + " total CPU cost (" + totalCpu + " ns) exceeds parent/limit (" + limitCpu + " ns)");
    }
    if (limitMem != null && totalMem > limitMem) {
      errors.add(prefix + " total memory cost (" + totalMem + " bytes) exceeds parent/limit (" + limitMem + " bytes)");
    }
  }

  private static void validateDuplicateEntity(String entityId, String prefix, Set<String> entityIds,
      List<String> errors) {
    if (entityId == null || entityId.trim().isEmpty()) {
      errors.add(prefix + ".propagationEntity cannot be null or empty");
    } else {
      // Check for duplicate propagationEntity IDs
      if (entityIds.contains(entityId)) {
        errors.add(prefix + ".propagationEntity '" + entityId + "' is duplicated");
      } else {
        entityIds.add(entityId);
      }
    }
  }

  private static void rewriteEmptyCosts(List<PropagationEntity> entities, long totalCpu, long totalMem) {
    int numEntities = entities.size();
    long shareCpuCostNs = totalCpu / numEntities;
    long shareMemoryCostBytes = totalMem / numEntities;
    for (PropagationEntity entity : entities) {
      if (entity.getCpuCostNs() == null && entity.getMemoryCostBytes() == null) {
        entity.setCpuCostNs(shareCpuCostNs);
        entity.setMemoryCostBytes(shareMemoryCostBytes);
      }
    }
  }

  /** Validate non-null/non-negative and return the positive value (else 0) for accumulation. */
  private static long costOrZero(String prefix, String field, Long value, List<String> errors) {
    if (value == null) {
      errors.add(prefix + "." + field + " cannot be null");
      return 0L;
    }
    if (value < 0) {
      errors.add(prefix + "." + field + " cannot be negative, got: " + value);
      return 0L;
    }
    return value > 0 ? value : 0L;
  }
}
