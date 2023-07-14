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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.ClientErrorException;
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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.INSTANCE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotInstanceRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotInstanceRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  public static class Instances {
    List<String> _instances;

    public Instances(@JsonProperty("instances") List<String> instances) {
      _instances = instances;
    }

    public List<String> getInstances() {
      return _instances;
    }
  }

  @GET
  @Path("/instances")
  @Authorize(targetType = TargetType.CLUSTER, action = "GetInstances")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List all instances")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public Instances getAllInstances() {
    return new Instances(_pinotHelixResourceManager.getAllInstances());
  }

  @GET
  @Path("/instances/{instanceName}")
  @Authorize(targetType = TargetType.CLUSTER, action = "GetInstance")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get instance information", produces = MediaType.APPLICATION_JSON)
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public String getInstance(
      @ApiParam(value = "Instance name", required = true, example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName) {
    InstanceConfig instanceConfig = _pinotHelixResourceManager.getHelixInstanceConfig(instanceName);
    if (instanceConfig == null) {
      throw new ControllerApplicationException(LOGGER, "Instance " + instanceName + " not found",
          Response.Status.NOT_FOUND);
    }
    ObjectNode response = JsonUtils.newObjectNode();
    response.put("instanceName", instanceConfig.getInstanceName());
    response.put("hostName", instanceConfig.getHostName());
    response.put("enabled", instanceConfig.getInstanceEnabled());
    response.put("port", instanceConfig.getPort());
    response.set("tags", JsonUtils.objectToJsonNode(instanceConfig.getTags()));
    response.set("pools", JsonUtils.objectToJsonNode(instanceConfig.getRecord().getMapField(InstanceUtils.POOL_KEY)));
    response.put("grpcPort", getGrpcPort(instanceConfig));
    response.put("adminPort", getAdminPort(instanceConfig));
    response.put("queryServicePort", getQueryServicePort(instanceConfig));
    response.put("queryMailboxPort", getQueryMailboxPort(instanceConfig));
    String queriesDisabled = instanceConfig.getRecord().getSimpleField(CommonConstants.Helix.QUERIES_DISABLED);
    if ("true".equalsIgnoreCase(queriesDisabled)) {
      response.put(CommonConstants.Helix.QUERIES_DISABLED, "true");
    }
    response.put("systemResourceInfo", JsonUtils.objectToJsonNode(getSystemResourceInfo(instanceConfig)));
    return response.toString();
  }

  private int getGrpcPort(InstanceConfig instanceConfig) {
    String grpcPortStr = instanceConfig.getRecord().getSimpleField(CommonConstants.Helix.Instance.GRPC_PORT_KEY);
    if (grpcPortStr != null) {
      try {
        return Integer.parseInt(grpcPortStr);
      } catch (Exception e) {
        LOGGER.warn("Illegal gRPC port: {} for instance: {}", grpcPortStr, instanceConfig.getInstanceName());
      }
    }
    return Instance.NOT_SET_GRPC_PORT_VALUE;
  }

  private int getAdminPort(InstanceConfig instanceConfig) {
    String adminPortStr = instanceConfig.getRecord().getSimpleField(CommonConstants.Helix.Instance.ADMIN_PORT_KEY);
    if (adminPortStr != null) {
      try {
        return Integer.parseInt(adminPortStr);
      } catch (Exception e) {
        LOGGER.warn("Illegal admin port: {} for instance: {}", adminPortStr, instanceConfig.getInstanceName());
      }
    }
    return Instance.NOT_SET_ADMIN_PORT_VALUE;
  }

  private int getQueryServicePort(InstanceConfig instanceConfig) {
    String queryServicePortStr = instanceConfig.getRecord().getSimpleField(
        CommonConstants.Helix.Instance.MULTI_STAGE_QUERY_ENGINE_SERVICE_PORT_KEY);
    if (queryServicePortStr != null) {
      try {
        return Integer.parseInt(queryServicePortStr);
      } catch (Exception e) {
        LOGGER.warn("Illegal service port: {} for instance: {}", queryServicePortStr, instanceConfig.getInstanceName());
      }
    }
    return Instance.NOT_SET_GRPC_PORT_VALUE;
  }

  private int getQueryMailboxPort(InstanceConfig instanceConfig) {
    String queryMailboxPortStr = instanceConfig.getRecord().getSimpleField(
        CommonConstants.Helix.Instance.MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY);
    if (queryMailboxPortStr != null) {
      try {
        return Integer.parseInt(queryMailboxPortStr);
      } catch (Exception e) {
        LOGGER.warn("Illegal mailbox port: {} for instance: {}", queryMailboxPortStr, instanceConfig.getInstanceName());
      }
    }
    return Instance.NOT_SET_GRPC_PORT_VALUE;
  }

  private Map<String, String> getSystemResourceInfo(InstanceConfig instanceConfig) {
    return instanceConfig.getRecord().getMapField(CommonConstants.Helix.Instance.SYSTEM_RESOURCE_INFO_KEY);
  }

  @POST
  @Path("/instances")
  @Authorize(targetType = TargetType.CLUSTER, action = "CreateInstance")
  @Authenticate(AccessType.CREATE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Create a new instance", consumes = MediaType.APPLICATION_JSON,
      notes = "Creates a new instance with given instance config")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 409, message = "Instance already exists"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public SuccessResponse addInstance(
      @ApiParam("Whether to update broker resource for broker instance") @QueryParam("updateBrokerResource")
      @DefaultValue("false") boolean updateBrokerResource, Instance instance) {
    String instanceId = InstanceUtils.getHelixInstanceId(instance);
    LOGGER.info("Instance creation request received for instance: {}, updateBrokerResource: {}", instanceId,
        updateBrokerResource);
    try {
      PinotResourceManagerResponse response = _pinotHelixResourceManager.addInstance(instance, updateBrokerResource);
      return new SuccessResponse(response.getMessage());
    } catch (ClientErrorException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), e.getResponse().getStatus());
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to create instance: " + instanceId,
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @PUT
  @Path("/instances/{instanceName}/state")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Enable/disable an instance", notes = "Enable/disable an instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public SuccessResponse toggleInstanceState(
      @ApiParam(value = "Instance name", required = true, example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName,
      @ApiParam(value = "enable|disable", required = true) @QueryParam("state") String state) {
    if (!_pinotHelixResourceManager.instanceExists(instanceName)) {
      throw new ControllerApplicationException(LOGGER, "Instance '" + instanceName + "' does not exist",
          Response.Status.NOT_FOUND);
    }

    if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
      PinotResourceManagerResponse response = _pinotHelixResourceManager.enableInstance(instanceName);
      if (!response.isSuccessful()) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to enable instance '" + instanceName + "': " + response.getMessage(),
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    } else if (StateType.DISABLE.name().equalsIgnoreCase(state)) {
      PinotResourceManagerResponse response = _pinotHelixResourceManager.disableInstance(instanceName);
      if (!response.isSuccessful()) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to disable instance '" + instanceName + "': " + response.getMessage(),
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    } else {
      throw new ControllerApplicationException(LOGGER, "Unknown state '" + state + "'", Response.Status.BAD_REQUEST);
    }
    return new SuccessResponse("Request to " + state + " instance '" + instanceName + "' is successful");
  }

  @Deprecated
  @POST
  @Path("/instances/{instanceName}/state")
  @Authorize(targetType = TargetType.CLUSTER, action = "UpdateInstance")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Enable/disable/drop an instance", notes = "Enable/disable/drop an instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 409, message = "Instance cannot be dropped"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public SuccessResponse toggleInstanceStateDeprecated(
      @ApiParam(value = "Instance name", required = true, example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName, String state) {
    if (!_pinotHelixResourceManager.instanceExists(instanceName)) {
      throw new ControllerApplicationException(LOGGER, "Instance " + instanceName + " not found",
          Response.Status.NOT_FOUND);
    }

    if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
      PinotResourceManagerResponse response = _pinotHelixResourceManager.enableInstance(instanceName);
      if (!response.isSuccessful()) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to enable instance " + instanceName + " - " + response.getMessage(),
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    } else if (StateType.DISABLE.name().equalsIgnoreCase(state)) {
      PinotResourceManagerResponse response = _pinotHelixResourceManager.disableInstance(instanceName);
      if (!response.isSuccessful()) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to disable instance " + instanceName + " - " + response.getMessage(),
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    } else if (StateType.DROP.name().equalsIgnoreCase(state)) {
      PinotResourceManagerResponse response = _pinotHelixResourceManager.dropInstance(instanceName);
      if (!response.isSuccessful()) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to drop instance " + instanceName + " - " + response.getMessage(), Response.Status.CONFLICT);
      }
    } else {
      throw new ControllerApplicationException(LOGGER, "Unknown state " + state + " for instance request",
          Response.Status.BAD_REQUEST);
    }
    return new SuccessResponse("Request to " + state + " instance " + instanceName + " is successful");
  }

  @DELETE
  @Path("/instances/{instanceName}")
  @Authorize(targetType = TargetType.CLUSTER, action = "DeleteInstance")
  @Authenticate(AccessType.DELETE)
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Drop an instance", notes = "Drop an instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 409, message = "Instance cannot be dropped"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public SuccessResponse dropInstance(
      @ApiParam(value = "Instance name", required = true, example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName) {
    boolean instanceExists = _pinotHelixResourceManager.instanceExists(instanceName);
    // NOTE: Even if instance config does not exist, still try to delete remaining instance ZK nodes in case some nodes
    //       are created again due to race condition (state transition messages added after instance is dropped).
    PinotResourceManagerResponse response = _pinotHelixResourceManager.dropInstance(instanceName);
    if (response.isSuccessful()) {
      if (instanceExists) {
        return new SuccessResponse("Successfully dropped instance");
      } else {
        throw new ControllerApplicationException(LOGGER, "Instance " + instanceName + " not found",
            Response.Status.NOT_FOUND);
      }
    } else {
      throw new ControllerApplicationException(LOGGER,
          "Failed to drop instance " + instanceName + " - " + response.getMessage(), Response.Status.CONFLICT);
    }
  }

  @PUT
  @Path("/instances/{instanceName}")
  @Authorize(targetType = TargetType.CLUSTER, action = "UpdateInstance")
  @Authenticate(AccessType.UPDATE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the specified instance", consumes = MediaType.APPLICATION_JSON,
      notes = "Update specified instance with given instance config")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public SuccessResponse updateInstance(
      @ApiParam(value = "Instance name", required = true, example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName,
      @ApiParam("Whether to update broker resource for broker instance") @QueryParam("updateBrokerResource")
      @DefaultValue("false") boolean updateBrokerResource, Instance instance) {
    LOGGER.info("Instance update request received for instance: {}, updateBrokerResource: {}", instanceName,
        updateBrokerResource);
    try {
      PinotResourceManagerResponse response =
          _pinotHelixResourceManager.updateInstance(instanceName, instance, updateBrokerResource);
      return new SuccessResponse(response.getMessage());
    } catch (ClientErrorException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), e.getResponse().getStatus());
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to update instance: " + instanceName,
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @PUT
  @Path("/instances/{instanceName}/updateTags")
  @Authorize(targetType = TargetType.CLUSTER, action = "UpdateTags")
  @Authenticate(AccessType.UPDATE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the tags of the specified instance", consumes = MediaType.APPLICATION_JSON,
      notes = "Update the tags of the specified instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public SuccessResponse updateInstanceTags(
      @ApiParam(value = "Instance name", required = true, example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName,
      @ApiParam(value = "Comma separated tags list", required = true) @QueryParam("tags") String tags,
      @ApiParam("Whether to update broker resource for broker instance") @QueryParam("updateBrokerResource")
      @DefaultValue("false") boolean updateBrokerResource) {
    LOGGER.info("Instance update request received for instance: {}, tags: {}, updateBrokerResource: {}", instanceName,
        tags, updateBrokerResource);
    if (tags == null) {
      throw new ControllerApplicationException(LOGGER, "Must provide tags to update", Response.Status.BAD_REQUEST);
    }
    try {
      PinotResourceManagerResponse response =
          _pinotHelixResourceManager.updateInstanceTags(instanceName, tags, updateBrokerResource);
      return new SuccessResponse(response.getMessage());
    } catch (ClientErrorException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), e.getResponse().getStatus());
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to update instance: %s with tags: %s", instanceName, tags),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @POST
  @Path("/instances/{instanceName}/updateBrokerResource")
  @Authorize(targetType = TargetType.CLUSTER, action = "UpdateResource")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the tables served by the specified broker instance in the broker resource", notes =
      "Broker resource should be updated when a new broker instance is added, or the tags for an existing broker are "
          + "changed. Updating broker resource requires reading all the table configs, which can be costly for large "
          + "cluster. Consider updating broker resource for each table individually.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public SuccessResponse updateBrokerResource(
      @ApiParam(value = "Instance name", required = true, example = "Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName) {
    LOGGER.info("Update broker resource request received for instance: {}", instanceName);
    try {
      PinotResourceManagerResponse response = _pinotHelixResourceManager.updateBrokerResource(instanceName);
      return new SuccessResponse(response.getMessage());
    } catch (ClientErrorException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), e.getResponse().getStatus());
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to update broker resource for instance: " + instanceName,
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @GET
  @Path("/instances/dropInstance/validate")
  @Authorize(targetType = TargetType.CLUSTER, action = "GetInstances")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Check if it's safe to drop the given instances. If not list all the reasons why its not safe.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public List<OperationValidationResponse> instanceDropSafetyCheck(
      @ApiParam(value = "Instance names", required = true,
          example = "Broker_my.broker.com_30000")
      @QueryParam("instanceNames") List<String> instanceNames) {
    LOGGER.info("Performing safety check on drop operation request received for instances: {}", instanceNames);
    try {
      return instanceNames.stream()
              .map(instance -> _pinotHelixResourceManager.instanceDropSafetyCheck(instance))
              .collect(Collectors.toList());
    } catch (ClientErrorException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), e.getResponse().getStatus());
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to check the safety for instance drop operation.",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
