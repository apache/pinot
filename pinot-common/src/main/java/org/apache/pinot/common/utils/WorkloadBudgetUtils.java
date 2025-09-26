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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.spi.accounting.WorkloadBudgetManager;
import org.apache.pinot.spi.trace.Tracing;
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
   * Get the instance cost (budget) information for a specific workload.
   * <p>
   * Returns CPU and memory budget information enforced on this instance for the workload.
   *
   * <p>Example response:
   * <pre>
   * {
   *   "workloadName": "testWorkload",
   *   "cpuBudgetNs": 5000000,
   *   "memoryBudgetBytes": 104857600,
   *   "cpuRemainingNs": 3500000,
   *   "memoryRemainingBytes": 73400320
   * }
   * </pre>
   *
   * @param workloadName the name of the workload to query
   * @param instanceId the instance ID for logging purposes
   * @return JSON string containing workload budget information including CPU and memory limits and remaining capacity
   * @throws WebApplicationException with one of the following status codes:
   *         <ul>
   *         <li>400 - if the workload name is invalid</li>
   *         <li>404 - if the workload is not found</li>
   *         <li>500 - if the WorkloadBudgetManager is not available</li>
   *         </ul>
   */
  public static String getWorkloadBudgetStats(String workloadName, String instanceId) {
    // Input validation
    if (workloadName == null || workloadName.trim().isEmpty()) {
      throw new WebApplicationException("Workload name cannot be null or empty",
          Response.Status.BAD_REQUEST);
    }
    try {
      WorkloadBudgetManager workloadBudgetManager = requireWorkloadBudgetManager(instanceId);

      WorkloadBudgetManager.BudgetStats budgetStats = workloadBudgetManager.getBudgetStats(workloadName);
      if (budgetStats == null) {
        LOGGER.warn("No budget stats found for workload: {} on instance: {}", workloadName, instanceId);
        throw new WebApplicationException("Workload not found: " + workloadName, Response.Status.NOT_FOUND);
      }
      return ResourceUtils.convertToJsonString(toWorkloadBudgetMap(workloadName, budgetStats));
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("Error getting workload info for workload: {} on instance: {}", workloadName, instanceId, e);
      throw new WebApplicationException("Failed to get workload info: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get the instance cost (budget) information for all workloads.
   * <p>
   * Returns CPU and memory budget information enforced on this instance for all workloads.
   *
   * <p>Example response:
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
   * @param instanceId the instance ID for logging purposes
   * @return JSON string containing list of workload budget information including CPU and memory limits and remaining
   *         capacity
   * @throws WebApplicationException on failure to retrieve the workload budget information
   */
  public static String getAllWorkloadBudgetStats(String instanceId) {
    try {
      WorkloadBudgetManager workloadBudgetManager = requireWorkloadBudgetManager(instanceId);
      Map<String, WorkloadBudgetManager.BudgetStats> allBudgetStats = workloadBudgetManager.getAllBudgetStats();
      List<Map<String, Object>> response = new ArrayList<>();
      for (Map.Entry<String, WorkloadBudgetManager.BudgetStats> entry : allBudgetStats.entrySet()) {
        response.add(toWorkloadBudgetMap(entry.getKey(), entry.getValue()));
      }
      return ResourceUtils.convertToJsonString(response);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("Error getting all workload info on instance: {}", instanceId, e);
      throw new WebApplicationException("Failed to get all workload info: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /** Returns a non-null WorkloadBudgetManager or throws a 500 WebApplicationException (and logs a warning). */
  private static WorkloadBudgetManager requireWorkloadBudgetManager(String instanceId) {
    WorkloadBudgetManager workloadBudgetManager = Tracing.ThreadAccountantOps.getWorkloadBudgetManager();
    if (workloadBudgetManager == null) {
      LOGGER.warn("WorkloadBudgetManager is not available on instance: {}", instanceId);
      throw new WebApplicationException("WorkloadBudgetManager is not available",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return workloadBudgetManager;
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
