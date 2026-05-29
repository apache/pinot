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
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.utils.WorkloadBudgetUtils;
import org.apache.pinot.server.api.AdminApiApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This resource handles workload budget updates sent from the controller via direct HTTP calls,
 */
@Api(tags = "QueryWorkload")
@Path("/")
public class QueryWorkloadConfigResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryWorkloadConfigResource.class);

  @Inject
  @Named(AdminApiApplication.SERVER_INSTANCE_ID)
  private String _instanceId;

  /**
   * Refreshes query workload configurations on this instance.
   * <p>
   * This endpoint adds or updates workload budgets sent from the controller.
   * It supports batch updates of multiple workloads in a single request.
   * </p>
   *
   * <p><b>Request Body Example:</b></p>
   * <pre>{@code
   *{
   *     "foo": {"cpuCostNs": 1000000, "memoryCostBytes": 1000000},
   *     "bar": {"cpuCostNs": 500000, "memoryCostBytes": 500000}
   *}
   * }</pre>
   *
   * @param requestString JSON request containing workload-to-cost map
   * @return HTTP 200 (success), 400 (bad request), or 500 (error)
   */
  @POST
  @Path("/queryWorkloadConfigs")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Refresh query workload configuration",
      notes = "Adds or updates workload budget configuration on this instance. Supports batch updates.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully updated workload configuration"),
      @ApiResponse(code = 400, message = "Invalid request body"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Response refreshQueryWorkloadConfig(String requestString) {
    return WorkloadBudgetUtils.handleRefreshRequest(requestString, _instanceId);
  }

  /**
   * Gets budget statistics for workloads on this instance.
   * <p>
   * If workloadNames query parameter is not provided, returns budget information for all workloads.
   * If workloadNames is provided as a comma-separated list, returns budget information for those specific workloads.
   * </p>
   * <p>
   * Returns a single object if exactly one workload is requested, otherwise returns an array.
   * </p>
   *
   * @param workloadNames Optional comma-separated list of workload names to query
   * @return JSON string with workload budget statistics (single object or array)
   */
  @GET
  @Path("/queryWorkloadConfigs")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get instance cost information for workloads",
      notes = "Returns all workloads if workloadNames is not specified, otherwise returns specified workloads")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Workload not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String getWorkloadBudgetStats(
      @ApiParam(value = "Comma-separated list of workload names (optional)")
      @QueryParam("workloadNames") String workloadNames
  ) {
    return WorkloadBudgetUtils.getWorkloadBudgetStats(workloadNames, _instanceId);
  }

  /**
   * Deletes query workload configurations from this instance.
   * <p>
   * This endpoint removes workload budgets sent from the controller.
   * It supports batch deletion of multiple workloads via comma-separated query parameter.
   * </p>
   *
   * <p><b>Example:</b></p>
   * <pre>{@code
   * DELETE /queryWorkloadConfigs?workloadNames=foo,bar,baz
   * }</pre>
   *
   * @param workloadNamesParam Comma-separated list of workload names to delete
   * @return HTTP 200 (success), 400 (bad request), or 500 (error)
   */
  @DELETE
  @Path("/queryWorkloadConfigs")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Delete query workload configuration",
      notes = "Removes workload budget configuration from this instance. Supports batch deletions")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully deleted workload configuration"),
      @ApiResponse(code = 400, message = "Invalid or missing workloadNames parameter"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Response deleteQueryWorkloadConfig(@QueryParam("workloadNames") String workloadNamesParam) {
    return WorkloadBudgetUtils.handleDeleteRequest(workloadNamesParam, _instanceId);
  }
}
