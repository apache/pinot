/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.ws.rs.core.MediaType.*;


/**
 * <ul>
 *   <li>Sample curl call to create broker tenant</li>
 *   curl -i -X POST -H 'Content-Type: application/json' -d
 *   '{
 *     "role" : "broker",
 *     "numberOfInstances : "5",
 *     "name" : "brokerOne"
 *   }' http://localhost:1234/tenants
 *
 *   <li>Sample curl call to create server tenant</li>
 *   curl -i -X POST -H 'Content-Type: application/json' -d
 *   '{
 *     "role" : "server",
 *     "numberOfInstances : "5",
 *     "name" : "serverOne",
 *     "offlineInstances" : "3",
 *     "realtimeInstances" : "2"
 *   }' http://localhost:1234/tenants
 * </ul>
 */
@Api(tags = Constants.TENANT_TAG)
@Path("/")
public class PinotTenantRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      PinotTenantRestletResource.class);
  private static final String TENANT_NAME = "tenantName";

  @Inject
  ControllerConf controllerConf;
  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;
  @Inject
  ControllerMetrics metrics;

  @POST
  @Path("/tenants")
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = " Create a tenant")
  @ApiResponses( {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Error creating tenant")
  })
  public SuccessResponse createTenant(Tenant tenant) {
    PinotResourceManagerResponse response;
    switch (tenant.getTenantRole()) {
      case BROKER:
        response = pinotHelixResourceManager.createBrokerTenant(tenant);
        break;
      case SERVER:
        response = pinotHelixResourceManager.createServerTenant(tenant);
        break;
      default:
        throw new RuntimeException("Not a valid tenant creation call");
    }
    if (response.isSuccessful()) {
      return new SuccessResponse("Successfully created tenant");
    }
    metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_TENANT_CREATE_ERROR, 1L);
    throw new WebApplicationException("Failed to create tenant", Response.Status.INTERNAL_SERVER_ERROR);
  }

  /*
   * For tenant update
   */
  // TODO: should be /tenant/{tenantName}
  @PUT
  @Path("/tenants")
  @Consumes(APPLICATION_JSON)
  @ApiOperation(value =  "Update a tenant")
  @ApiResponses(value =  {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Failed to update the tenant")
  })
  public SuccessResponse updateTenant(Tenant tenant) {
    PinotResourceManagerResponse response;
    switch (tenant.getTenantRole()) {
      case BROKER:
        response = pinotHelixResourceManager.updateBrokerTenant(tenant);
        break;
      case SERVER:
        response = pinotHelixResourceManager.updateServerTenant(tenant);
        break;
      default:
        throw new RuntimeException("Not a valid tenant update call");
    }
    if (response.isSuccessful()) {
      return new SuccessResponse("Updated tenant");
    }
    metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_TENANT_UPDATE_ERROR, 1L);
    throw new WebApplicationException("Failed to update tenant", Response.Status.INTERNAL_SERVER_ERROR);
  }

  public static class TenantMetadata {
    @JsonProperty(value = "ServerInstances")
    Set<String> serverInstances;
    @JsonProperty(value = "BrokerInstances")
    Set<String> brokerInstances;
    @JsonProperty(TENANT_NAME)
    String tenantName;
  }

  @GET
  @Path("/tenants")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List all tenants")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Error reading tenants list")
  })
  public TenantsList getAllTenants(
      @ApiParam(value = "Tenant type", required = false, allowableValues = "[BROKER, SERVER]", defaultValue = "")
      @QueryParam("type") @DefaultValue("") String type) {
    TenantsList tenants = new TenantsList();

    if (type == null || type.isEmpty() || type.equalsIgnoreCase("server")) {
      tenants.serverTenants = pinotHelixResourceManager.getAllServerTenantNames();
    }
    if (type == null || type.isEmpty() || type.equalsIgnoreCase("broker")) {
      tenants.brokerTenants = pinotHelixResourceManager.getAllBrokerTenantNames();
    }
    return tenants;
  }


  @GET
  @Path("/tenants/{tenantName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get tenant information")
  @ApiResponses(value =  {
      @ApiResponse(code = 200, message = "Success", response = TenantMetadata.class),
      @ApiResponse(code = 404, message = "Tenant not found"),
      @ApiResponse(code = 500, message = "Server error reading tenant information")
  })
  public TenantMetadata getTenantMetadata(
      @ApiParam(value = "Tenant name", required = true)
      @PathParam("tenantName")
      String tenantName,
      @ApiParam(value = "tenant type", required = false, defaultValue = "", allowableValues = "[SERVER, BROKER]")
      @QueryParam("type") @DefaultValue("") String type) {

    TenantMetadata tenantMeta = new TenantMetadata();
    if (type == null || type.isEmpty()) {
      tenantMeta.serverInstances = pinotHelixResourceManager.getAllInstancesForServerTenant(tenantName);
      tenantMeta.brokerInstances = pinotHelixResourceManager.getAllInstancesForBrokerTenant(tenantName);
    } else {
      if (type.equalsIgnoreCase("server")) {
        tenantMeta.serverInstances = pinotHelixResourceManager.getAllInstancesForServerTenant(tenantName);
      }
      if (type.equalsIgnoreCase("broker")) {
        tenantMeta.brokerInstances =  pinotHelixResourceManager.getAllInstancesForBrokerTenant(tenantName);
      }
    }
    tenantMeta.tenantName = tenantName;
    return tenantMeta;
  }

  public static class TenantsList {
    @JsonProperty("SERVER_TENANTS")
    Set<String> serverTenants;
    @JsonProperty("BROKER_TENANTS")
    Set<String> brokerTenants;
  }

  // GET ?? really ??
  // TODO: FIXME: This API is horribly bad design doing too many unrelated operations and giving
  // different responses for each. That's a bad way to structure APIs because clients have no good way
  // to parse response. Maintaining old behavior for backward compatibility.
  // CHANGE-ALERT: This is not backward compatible. We've changed this API from GET to POST because:
  //   1. That is correct
  //   2. with GET, we need to write our own routing logic to avoid conflict since this is same as the API above
  @POST
  @Path("/tenants/{tenantName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Change tenant state")
  @ApiResponses(value =  {
      @ApiResponse(code = 200, message = "Success", response = String.class),
      @ApiResponse(code = 404, message = "Tenant not found"),
      @ApiResponse(code = 500, message = "Server error reading tenant information")
  })
  public String changeTenantState(
      @ApiParam(value = "Tenant name", required = true)
      @PathParam("tenantName")
      String tenantName,
      @ApiParam(value = "tenant type", required = false, defaultValue = "", allowableValues = "[SERVER, BROKER]")
      @QueryParam("type") String type,
      @ApiParam(value = "state", required = true, defaultValue = "", allowableValues = "[enable, disable, drop]")
      @QueryParam("state") @DefaultValue("") String state) {
    TenantMetadata tenantMetadata = getTenantMetadata(tenantName, type);
    Set<String> allInstances = new HashSet<>();
    if (tenantMetadata.brokerInstances != null) {
      allInstances.addAll(tenantMetadata.brokerInstances);
    }
    if (tenantMetadata.serverInstances != null) {
      allInstances.addAll(tenantMetadata.serverInstances);
    }
    // TODO: do not support drop. It's same as DELETE
    if (StateType.DROP.name().equalsIgnoreCase(state)) {
      if (!allInstances.isEmpty()) {
        throw new WebApplicationException("Tenant " + tenantName + " has live instance", Response.Status.BAD_REQUEST);
      }
      pinotHelixResourceManager.deleteBrokerTenantFor(tenantName);
      pinotHelixResourceManager.deleteOfflineServerTenantFor(tenantName);
      pinotHelixResourceManager.deleteRealtimeServerTenantFor(tenantName);
      try {
        return new ObjectMapper().writeValueAsString(new SuccessResponse("Deleted tenant " + tenantName));
      } catch (JsonProcessingException e) {
         LOGGER.error("Error serializing response to json");
        return "{\"message\" : \"Deleted tenant\" " + tenantName + "}";
      }
    }

    boolean enable = StateType.ENABLE.name().equalsIgnoreCase(state) ? true : false;
    JSONObject instanceResult = new JSONObject();
    String instance = null;
    try {
      for (String i : allInstances) {
        instance = i;
        if (enable) {
          instanceResult.put(instance, pinotHelixResourceManager.enableInstance(instance));
        } else {
          instanceResult.put(instance, pinotHelixResourceManager.disableInstance(instance));
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error enabling/disabling instances for tenant: {}", tenantName, e);
      metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_INSTANCE_POST_ERROR, 1L);
      throw new WebApplicationException("Error during " + type + " operation for instance: " + instance, Response.Status.INTERNAL_SERVER_ERROR);

    }
    return instanceResult.toString();
  }

  @DELETE
  @Path("/tenants/{tenantName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Delete a tenant")
  @ApiResponses(value = {
      @ApiResponse(code=200, message = "Success"),
      @ApiResponse(code = 400, message = "Tenant can not be deleted"),
      @ApiResponse(code = 404, message = "Tenant not found"),
      @ApiResponse(code = 500, message = "Error deleting tenant")})
  public SuccessResponse deleteTenant(
      @ApiParam(value = "Tenant name", required = true)
      @PathParam("tenantName") String tenantName,
      @ApiParam(value = "Tenant type", required = true, allowableValues = "[SERVER, BROKER]")
      @QueryParam("type") @DefaultValue("") String type) {

    if (type == null || type.isEmpty()) {
      throw new WebApplicationException("Tenant type (BROKER | SERVER) is required as query parameter",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    TenantRole tenantRole = TenantRole.valueOf(type.toUpperCase());
    PinotResourceManagerResponse res = null;
    switch (tenantRole) {
      case BROKER:
        if (pinotHelixResourceManager.isBrokerTenantDeletable(tenantName)) {
          res = pinotHelixResourceManager.deleteBrokerTenantFor(tenantName);
        } else {
          throw new WebApplicationException("Broker tenant is not null, can not delete it",
              Response.Status.BAD_REQUEST);
        }
        break;
      case SERVER:
          if (pinotHelixResourceManager.isServerTenantDeletable(tenantName)) {
            res = pinotHelixResourceManager.deleteOfflineServerTenantFor(tenantName);
            if (res.isSuccessful()) {
              res = pinotHelixResourceManager.deleteRealtimeServerTenantFor(tenantName);
            }
          } else {
            throw new WebApplicationException("Server tenant is not null, can not delete it",
                Response.Status.BAD_REQUEST);
          }
        break;
      default:
        break;
    }
    if (res.isSuccessful()) {
      return new SuccessResponse("Successfully deleted tenant " + tenantName);
    }
    metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_TENANT_DELETE_ERROR, 1L);
    throw new WebApplicationException("Error deleting tenant", Response.Status.INTERNAL_SERVER_ERROR);
  }
}
