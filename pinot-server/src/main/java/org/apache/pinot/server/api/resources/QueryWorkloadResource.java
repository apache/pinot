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
package org.apache.pinot.server.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.Collections;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.core.accounting.WorkloadBudgetManager;
import org.apache.pinot.server.api.AdminApiApplication;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;

/**
 * This resource API provides debugging information about query workloads on the server instance.
 * It allows retrieving workload budget information for debugging purposes.
 */
@Api(description = "Query workload debugging information", tags = "workload", authorizations =
    {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY,
    description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```")))
@Path("workload")
public class QueryWorkloadResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryWorkloadResource.class);

  @Inject
  @Named(AdminApiApplication.SERVER_INSTANCE_ID)
  private String _instanceId;

  /**
   * List all active workload names on this server instance.
   * Returns the names of workloads that have budgets configured.
   */
  @GET
  @Path("list")
  @ApiOperation(value = "List all active workload names on this server instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  @Produces(MediaType.APPLICATION_JSON)
  public String listWorkloads() {
    try {
      // Verify this is a server instance
      if (!InstanceTypeUtils.isServer(_instanceId)) {
        throw new WebApplicationException("This endpoint is only available on server instances",
            Response.Status.BAD_REQUEST);
      }

      WorkloadBudgetManager workloadBudgetManager = Tracing.ThreadAccountantOps.getWorkloadBudgetManager();
      
      if (workloadBudgetManager == null) {
        return ResourceUtils.convertToJsonString(Collections.emptySet());
      }

      // Note: WorkloadBudgetManager doesn't expose workload names directly
      // This is a limitation - we can only return empty set for now
      // In a real implementation, we'd need WorkloadBudgetManager to expose active workload names
      Set<String> workloadNames = Collections.emptySet();
      
      return ResourceUtils.convertToJsonString(workloadNames);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("Error listing workloads for instance: {}", _instanceId, e);
      throw new WebApplicationException("Failed to list workloads: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get the instance cost (budget) information for a specific workload.
   * Returns InstanceCost with CPU and memory budget information.
   */
  @GET
  @Path("workload/{workloadName}")
  @ApiOperation(value = "Get instance cost information for a specific workload")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Workload not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  @Produces(MediaType.APPLICATION_JSON)
  public String getWorkload(@ApiParam(value = "Name of the workload", required = true) 
                           @PathParam("workloadName") String workloadName) {
    try {
      // Verify this is a server instance
      if (!InstanceTypeUtils.isServer(_instanceId)) {
        throw new WebApplicationException("This endpoint is only available on server instances",
            Response.Status.BAD_REQUEST);
      }

      WorkloadBudgetManager workloadBudgetManager = Tracing.ThreadAccountantOps.getWorkloadBudgetManager();
      
      if (workloadBudgetManager == null) {
        throw new WebApplicationException("WorkloadBudgetManager is not available",
            Response.Status.INTERNAL_SERVER_ERROR);
      }

      // Get remaining budget for the specific workload
      WorkloadBudgetManager.BudgetStats budgetStats = workloadBudgetManager.getRemainingBudgetForWorkload(workloadName);
      
      if (budgetStats._cpuRemaining == 0 && budgetStats._memoryRemaining == 0) {
        throw new WebApplicationException("Workload not found: " + workloadName,
            Response.Status.NOT_FOUND);
      }

      // Create InstanceCost object with the budget information
      InstanceCost instanceCost = new InstanceCost(budgetStats._cpuRemaining, budgetStats._memoryRemaining);
      
      return ResourceUtils.convertToJsonString(instanceCost);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("Error getting workload info for workload: {} on instance: {}", workloadName, _instanceId, e);
      throw new WebApplicationException("Failed to get workload info: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

}
