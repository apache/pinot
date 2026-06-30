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
package org.apache.pinot.common.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.spi.accounting.WorkloadBudgetManager;
import org.apache.pinot.spi.accounting.WorkloadBudgetManagerFactory;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for workload budget operations shared between broker and server debug endpoints.
 */
public class WorkloadBudgetUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadBudgetUtils.class);

  private WorkloadBudgetUtils() {
    // Utility class
  }

  /**
   * Get the instance cost (budget) information for workloads.
   * <p>
   * If workloadNames is null or empty, returns budget information for all workloads.
   * Otherwise, returns budget information for the specified workloads.
   * </p>
   *
   * <p>Example response for single workload:
   * <pre>
   * [
   *  {
   *   "workloadName": "testWorkload",
   *   "cpuBudgetNs": 5000000,
   *   "memoryBudgetBytes": 104857600,
   *   "cpuRemainingNs": 3500000,
   *   "memoryRemainingBytes": 73400320
   *  }
   * ]
   * </pre>
   *
   * <p>Example response for multiple workloads:
   * <pre>
   * [
   *   {
   *     "workloadName": "testWorkload1",
   *     "cpuBudgetNs": 5000000,
   *     "memoryBudgetBytes": 104857600,
   *     "cpuRemainingNs": 3500000,
   *     "memoryRemainingBytes": 73400320
   *   },
   *   {
   *     "workloadName": "testWorkload2",
   *     "cpuBudgetNs": 10000000,
   *     "memoryBudgetBytes": 209715200,
   *     "cpuRemainingNs": 8000000,
   *     "memoryRemainingBytes": 157286400
   *   }
   * ]
   * </pre>
   *
   * @param workloadNames comma-separated list of workload names to query, or null/empty for all workloads
   * @param instanceId the instance ID for logging purposes
   * @return JSON string containing workload budget information including CPU and memory limits and remaining capacity.
   * @throws WebApplicationException with one of the following status codes:
   *         <ul>
   *         <li>404 - if a specific workload is not found</li>
   *         <li>500 - if the WorkloadBudgetManager is not available</li>
   *         </ul>
   */
  public static String getWorkloadBudgetStats(String workloadNames, String instanceId) {
    try {
      WorkloadBudgetManager workloadBudgetManager = requireWorkloadBudgetManager();
      // If no workload names specified, return all workloads
      if (workloadNames == null || workloadNames.trim().isEmpty()) {
        Map<String, WorkloadBudgetManager.BudgetStats> allBudgetStats = workloadBudgetManager.getAllBudgetStats();
        List<Map<String, Object>> response = new ArrayList<>();
        for (Map.Entry<String, WorkloadBudgetManager.BudgetStats> entry : allBudgetStats.entrySet()) {
          response.add(toWorkloadBudgetMap(entry.getKey(), entry.getValue()));
        }
        return ResourceUtils.convertToJsonString(response);
      }

      List<String> requestedWorkloads = parseCommaSeparatedNames(workloadNames);
      if (requestedWorkloads.isEmpty()) {
        throw new WebApplicationException("No valid workload names provided", Response.Status.BAD_REQUEST);
      }
      List<Map<String, Object>> response = new ArrayList<>();
      for (String workloadName : requestedWorkloads) {
        WorkloadBudgetManager.BudgetStats budgetStats = workloadBudgetManager.getBudgetStats(workloadName);
        if (budgetStats == null) {
          LOGGER.warn("No budget stats found for workload: {} on instance: {}", workloadName, instanceId);
          throw new WebApplicationException("Workload not found: " + workloadName, Response.Status.NOT_FOUND);
        }
        response.add(toWorkloadBudgetMap(workloadName, budgetStats));
      }
      return ResourceUtils.convertToJsonString(response);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("Error getting workload info for workloads: {} on instance: {}", workloadNames, instanceId, e);
      throw new WebApplicationException("Failed to get workload info: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Handles a workload refresh request for a server or broker instance.
   * This method enforces strict error handling - any null InstanceCost or processing failure
   * will cause the entire operation to fail immediately without applying partial updates.
   *
   * @param requestString The JSON request string containing workload refresh data
   * @param instanceId The instance ID for logging and error messages
   * @return JAX-RS Response with appropriate status code and message
   *
   * example requestString:
   * {
   *     "foo": {"cpuCostNs": 1000000, "memoryCostBytes": 1000000},
   *     "bar": {"cpuCostNs": 500000, "memoryCostBytes": 500000}
   * }
   */
  public static Response handleRefreshRequest(String requestString, String instanceId) {
    try {
      WorkloadBudgetManager budgetManager = requireWorkloadBudgetManager();

      Map<String, InstanceCost> workloadToCostMap = JsonUtils.stringToObject(requestString,
          new TypeReference<Map<String, InstanceCost>>() { });
      LOGGER.info("Processing {} workloads on instance: {}", workloadToCostMap.size(), instanceId);

      // Validate all workloads first
      for (Map.Entry<String, InstanceCost> entry : workloadToCostMap.entrySet()) {
        String workloadName = entry.getKey();
        InstanceCost instanceCost = entry.getValue();
        if (instanceCost == null) {
          return buildErrorResponse(Response.Status.BAD_REQUEST,
              String.format("InstanceCost is null for workload: %s on instance: %s", workloadName, instanceId), null);
        }
      }
      // Apply all updates - fail immediately on any error
      for (Map.Entry<String, InstanceCost> entry : workloadToCostMap.entrySet()) {
        String workloadName = entry.getKey();
        InstanceCost instanceCost = entry.getValue();
        try {
          budgetManager.addOrUpdateWorkload(workloadName, instanceCost.getCpuCostNs(),
              instanceCost.getMemoryCostBytes());
          LOGGER.info("Updated workload: {} on instance: {}", workloadName, instanceId);
        } catch (Exception e) {
          return buildErrorResponse(Response.Status.INTERNAL_SERVER_ERROR,
              String.format("Failed to update workload: %s on instance: %s", workloadName, instanceId), e);
        }
      }
      String message = String.format("Successfully updated %d workloads on instance: %s",
          workloadToCostMap.size(), instanceId);
      LOGGER.info(message);
      return Response.ok(message).build();
    } catch (WebApplicationException e) {
      return Response.status(e.getResponse().getStatus()).entity(e.getMessage()).build();
    } catch (Exception e) {
      return buildErrorResponse(Response.Status.INTERNAL_SERVER_ERROR,
          "Error processing workload refresh request: " + e.getMessage(), e);
    }
  }

  /**
   * Handles delete workload configuration requests via query parameter.
   * Accepts comma-separated workload names and deletes them from the WorkloadBudgetManager.
   * This method enforces strict error handling - any deletion failure will cause the entire
   * operation to fail immediately without continuing to delete remaining workloads.
   *
   * @param workloadNamesParam Comma-separated workload names (e.g., "foo,bar,baz")
   * @param instanceId The instance ID for logging and error messages
   * @return JAX-RS Response with appropriate status code and message
   */
  public static Response handleDeleteRequest(String workloadNamesParam, String instanceId) {
    try {
      WorkloadBudgetManager budgetManager = requireWorkloadBudgetManager();
      if (workloadNamesParam == null || workloadNamesParam.trim().isEmpty()) {
        return buildErrorResponse(Response.Status.BAD_REQUEST, "Missing required query parameter: workloadNames",
            null);
      }
      List<String> workloadNames = parseCommaSeparatedNames(workloadNamesParam);
      if (workloadNames.isEmpty()) {
        return buildErrorResponse(Response.Status.BAD_REQUEST, "No valid workload names provided", null);
      }
      LOGGER.info("Deleting {} workloads on instance: {}: {}", workloadNames.size(), instanceId, workloadNames);
      // Delete all workloads - fail immediately on any error
      for (String workloadName : workloadNames) {
        try {
          budgetManager.deleteWorkload(workloadName);
          LOGGER.info("Deleted workload: {} on instance: {}", workloadName, instanceId);
        } catch (Exception e) {
          return buildErrorResponse(Response.Status.INTERNAL_SERVER_ERROR,
              String.format("Failed to delete workload: %s on instance: %s ", workloadName, instanceId), e);
        }
      }
      String message = String.format("Successfully deleted %d workloads on instance: %s",
          workloadNames.size(), instanceId);
      LOGGER.info(message);
      return Response.ok(message).build();
    } catch (WebApplicationException e) {
      return Response.status(e.getResponse().getStatus()).entity(e.getMessage()).build();
    } catch (Exception e) {
      return buildErrorResponse(Response.Status.INTERNAL_SERVER_ERROR,
          "Error processing workload delete request: " + e.getMessage(), e);
    }
  }

  /** Returns a non-null WorkloadBudgetManager or throws a 500 WebApplicationException (and logs a warning). */
  private static WorkloadBudgetManager requireWorkloadBudgetManager() {
    WorkloadBudgetManager workloadBudgetManager = WorkloadBudgetManagerFactory.get();
    if (workloadBudgetManager == null) {
      LOGGER.warn("WorkloadBudgetManager is not available");
      throw new WebApplicationException("WorkloadBudgetManager not available", Response.Status.INTERNAL_SERVER_ERROR);
    }
    return workloadBudgetManager;
  }

  /**
   * Builds an error Response with logging and optional exception.
   */
  private static Response buildErrorResponse(Response.Status status, String errorMsg, Exception e) {
    if (e != null) {
      LOGGER.error(errorMsg, e);
    } else {
      LOGGER.warn(errorMsg);
    }
    return Response.status(status).entity(errorMsg).build();
  }

  /**
   * Parses a comma-separated string into a list of trimmed, non-empty values.
   * @param commaSeparated the comma-separated string
   * @return list of trimmed, non-empty values
   */
  private static List<String> parseCommaSeparatedNames(String commaSeparated) {
    List<String> result = new ArrayList<>();
    if (commaSeparated != null) {
      for (String name : commaSeparated.split(",")) {
        String trimmed = name.trim();
        if (!trimmed.isEmpty()) {
          result.add(trimmed);
        }
      }
    }
    return result;
  }

  /** Builds a stable JSON-serializable map for a single workload's budget stats. */
  private static Map<String, Object> toWorkloadBudgetMap(String workloadName,
                                                         WorkloadBudgetManager.BudgetStats stats) {
    Map<String, Object> map = new HashMap<>();
    map.put("workloadName", workloadName);
    map.put("cpuBudgetNs", stats._initialCpuBudget);
    map.put("memoryBudgetBytes", stats._initialMemoryBudget);
    map.put("cpuRemainingNs", stats._cpuRemaining);
    map.put("memoryRemainingBytes", stats._memoryRemaining);
    return map;
  }
}
