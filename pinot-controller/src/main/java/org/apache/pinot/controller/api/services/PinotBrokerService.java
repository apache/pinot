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
package org.apache.pinot.controller.api.services;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.resources.Constants;
import org.apache.pinot.controller.api.resources.InstanceInfo;
import org.apache.pinot.controller.api.resources.SuccessResponse;
import org.apache.pinot.spi.utils.CommonConstants;


@Api(tags = Constants.BROKER_TAG, authorizations = {@Authorization(value = CommonConstants.SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(
    apiKeyAuthDefinitions = @ApiKeyAuthDefinition(
        name = HttpHeaders.AUTHORIZATION,
        in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = CommonConstants.SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public interface PinotBrokerService {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers")
  @ApiOperation(value = "List tenants and tables to brokers mappings",
      notes = "List tenants and tables to brokers mappings")
  Map<String, Map<String, List<String>>> listBrokersMapping(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/tenants")
  @ApiOperation(value = "List tenants to brokers mappings", notes = "List tenants to brokers mappings")
  Map<String, List<String>> getTenantsToBrokersMapping(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/tenants/{tenantName}")
  @ApiOperation(value = "List brokers for a given tenant", notes = "List brokers for a given tenant")
  List<String> getBrokersForTenant(
      @ApiParam(value = "Name of the tenant", required = true) @PathParam("tenantName") String tenantName,
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/tables")
  @ApiOperation(value = "List tables to brokers mappings", notes = "List tables to brokers mappings")
  Map<String, List<String>> getTablesToBrokersMapping(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/tables/{tableName}")
  @ApiOperation(value = "List brokers for a given table", notes = "List brokers for a given table")
  List<String> getBrokersForTable(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/brokers")
  @ApiOperation(value = "List tenants and tables to brokers mappings",
      notes = "List tenants and tables to brokers mappings")
  Map<String, Map<String, List<InstanceInfo>>> listBrokersMappingV2(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/brokers/tenants")
  @ApiOperation(value = "List tenants to brokers mappings", notes = "List tenants to brokers mappings")
  Map<String, List<InstanceInfo>> getTenantsToBrokersMappingV2(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/brokers/tenants/{tenantName}")
  @ApiOperation(value = "List brokers for a given tenant", notes = "List brokers for a given tenant")
  List<InstanceInfo> getBrokersForTenantV2(
      @ApiParam(value = "Name of the tenant", required = true) @PathParam("tenantName") String tenantName,
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/brokers/tables")
  @ApiOperation(value = "List tables to brokers mappings", notes = "List tables to brokers mappings")
  Map<String, List<InstanceInfo>> getTablesToBrokersMappingV2(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state);

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/brokers/tables/{tableName}")
  @ApiOperation(value = "List brokers for a given table", notes = "List brokers for a given table")
  List<InstanceInfo> getBrokersForTableV2(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state);

  @POST
  @Path("/brokers/instances/{instanceName}/qps")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Enable/disable the query rate limiting for a broker instance",
      notes = "Enable/disable the query rate limiting for a broker instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 404, message = "Instance not found"), @ApiResponse(code = 500, message = "Internal error")
  })
  SuccessResponse toggleQueryRateLimiting(
      @ApiParam(value = "Broker instance name", required = true, example = "Broker_my.broker.com_30000")
      @PathParam("instanceName") String brokerInstanceName,
      @ApiParam(value = "ENABLE|DISABLE", allowableValues = "ENABLE, DISABLE", required = true) @QueryParam("state")
          String state);
}
