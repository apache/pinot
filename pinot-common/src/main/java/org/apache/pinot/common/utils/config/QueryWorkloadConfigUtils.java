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
import org.apache.pinot.spi.config.workload.CostSplit;
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
          // Validate CostSplits
          validateCostSplits(propagationScheme.getCostSplits(), prefix + ".propagationScheme.costSplits", errors);
        }
      }
    }
    return errors;
  }

  /**
   * Validates a list of CostSplit objects and their nested sub-allocations.
   *
   * @param costSplits the list of CostSplit objects to validate
   * @param prefix the prefix for error messages
   * @param errors the list to add validation errors to
   */
  private static void validateCostSplits(List<CostSplit> costSplits, String prefix, List<String> errors) {
    if (costSplits == null) {
      errors.add(prefix + " cannot be null");
      return;
    }

    if (costSplits.isEmpty()) {
      errors.add(prefix + " cannot be empty");
      return;
    }

    Set<String> costIds = new HashSet<>();
    long totalCpuCost = 0;
    long totalMemoryCost = 0;

    for (int i = 0; i < costSplits.size(); i++) {
      CostSplit costSplit = costSplits.get(i);
      String costSplitPrefix = prefix + "[" + i + "]";

      if (costSplit == null) {
        errors.add(costSplitPrefix + " cannot be null");
        continue;
      }

      // Validate costId
      String costId = costSplit.getCostId();
      if (costId == null || costId.trim().isEmpty()) {
        errors.add(costSplitPrefix + ".costId cannot be null or empty");
      } else {
        // Check for duplicate costIds
        if (costIds.contains(costId)) {
          errors.add(costSplitPrefix + ".costId '" + costId + "' is duplicated");
        } else {
          costIds.add(costId);
        }

        // Validate costId format (basic validation)
        if (!isValidCostId(costId)) {
          errors.add(costSplitPrefix + ".costId '" + costId + "' contains invalid characters");
        }
      }

      // Validate CPU cost
      long cpuCostNs = costSplit.getCpuCostNs();
      if (cpuCostNs < 0) {
        errors.add(costSplitPrefix + ".cpuCostNs cannot be negative, got: " + cpuCostNs);
      } else if (cpuCostNs == 0) {
        errors.add(costSplitPrefix + ".cpuCostNs should be positive, got: " + cpuCostNs);
      } else {
        // Check for potential overflow when summing
        if (totalCpuCost > Long.MAX_VALUE - cpuCostNs) {
          errors.add(prefix + " total CPU cost would overflow");
        } else {
          totalCpuCost += cpuCostNs;
        }
      }

      // Validate memory cost
      long memoryCostBytes = costSplit.getMemoryCostBytes();
      if (memoryCostBytes < 0) {
        errors.add(costSplitPrefix + ".memoryCostBytes cannot be negative, got: " + memoryCostBytes);
      } else if (memoryCostBytes == 0) {
        errors.add(costSplitPrefix + ".memoryCostBytes should be positive, got: " + memoryCostBytes);
      } else {
        // Check for potential overflow when summing
        if (totalMemoryCost > Long.MAX_VALUE - memoryCostBytes) {
          errors.add(prefix + " total memory cost would overflow");
        } else {
          totalMemoryCost += memoryCostBytes;
        }
      }

      // Validate sub-allocations (recursive validation)
      List<CostSplit> subAllocations = costSplit.getSubAllocations();
      if (subAllocations != null && !subAllocations.isEmpty()) {
        validateCostSplitSubAllocations(subAllocations, costSplitPrefix + ".subAllocations",
                                      cpuCostNs, memoryCostBytes, errors);
      }
    }
  }

  /**
   * Validates sub-allocations within a CostSplit to ensure they don't exceed parent limits.
   *
   * @param subAllocations the list of sub-allocation CostSplit objects
   * @param prefix the prefix for error messages
   * @param parentCpuCostNs the parent's CPU cost limit
   * @param parentMemoryCostBytes the parent's memory cost limit
   * @param errors the list to add validation errors to
   */
  private static void validateCostSplitSubAllocations(List<CostSplit> subAllocations, String prefix,
                                                     long parentCpuCostNs, long parentMemoryCostBytes,
                                                     List<String> errors) {
    if (subAllocations.isEmpty()) {
      errors.add(prefix + " cannot be empty when specified");
      return;
    }

    Set<String> subCostIds = new HashSet<>();
    long totalSubCpuCost = 0;
    long totalSubMemoryCost = 0;

    for (int i = 0; i < subAllocations.size(); i++) {
      CostSplit subAllocation = subAllocations.get(i);
      String subPrefix = prefix + "[" + i + "]";

      if (subAllocation == null) {
        errors.add(subPrefix + " cannot be null");
        continue;
      }

      // Validate sub-allocation costId
      String subCostId = subAllocation.getCostId();
      if (subCostId == null || subCostId.trim().isEmpty()) {
        errors.add(subPrefix + ".costId cannot be null or empty");
      } else {
        // Check for duplicate sub-allocation costIds
        if (subCostIds.contains(subCostId)) {
          errors.add(subPrefix + ".costId '" + subCostId + "' is duplicated within sub-allocations");
        } else {
          subCostIds.add(subCostId);
        }

        if (!isValidCostId(subCostId)) {
          errors.add(subPrefix + ".costId '" + subCostId + "' contains invalid characters");
        }
      }

      // Validate sub-allocation costs
      long subCpuCostNs = subAllocation.getCpuCostNs();
      long subMemoryCostBytes = subAllocation.getMemoryCostBytes();

      if (subCpuCostNs < 0) {
        errors.add(subPrefix + ".cpuCostNs cannot be negative, got: " + subCpuCostNs);
      } else if (subCpuCostNs == 0) {
        errors.add(subPrefix + ".cpuCostNs should be positive, got: " + subCpuCostNs);
      } else {
        totalSubCpuCost += subCpuCostNs;
      }

      if (subMemoryCostBytes < 0) {
        errors.add(subPrefix + ".memoryCostBytes cannot be negative, got: " + subMemoryCostBytes);
      } else if (subMemoryCostBytes == 0) {
        errors.add(subPrefix + ".memoryCostBytes should be positive, got: " + subMemoryCostBytes);
      } else {
        totalSubMemoryCost += subMemoryCostBytes;
      }

      // Sub-allocations should not have their own sub-allocations (prevent deep nesting)
      if (subAllocation.getSubAllocations() != null && !subAllocation.getSubAllocations().isEmpty()) {
        errors.add(subPrefix + ".subAllocations nested sub-allocations are not supported");
      }
    }

    // Validate that sub-allocations don't exceed parent limits
    if (totalSubCpuCost > parentCpuCostNs) {
      errors.add(prefix + " total CPU cost (" + totalSubCpuCost
          + "ns) exceeds parent limit (" + parentCpuCostNs + "ns)");
    }

    if (totalSubMemoryCost > parentMemoryCostBytes) {
      errors.add(prefix + " total memory cost (" + totalSubMemoryCost
          + " bytes) exceeds parent limit (" + parentMemoryCostBytes + " bytes)");
    }
  }

  /**
   * Validates that a costId contains only valid characters.
   * Cost IDs should be alphanumeric with underscores, hyphens, and dots allowed.
   *
   * @param costId the cost ID to validate
   * @return true if the cost ID is valid, false otherwise
   */
  private static boolean isValidCostId(String costId) {
    if (costId == null || costId.trim().isEmpty()) {
      return false;
    }

    // Allow alphanumeric characters, underscores, hyphens, and dots
    // This covers table names, tenant names, and other common identifiers in Pinot
    return costId.matches("^[a-zA-Z0-9_.-]+$");
  }
}
