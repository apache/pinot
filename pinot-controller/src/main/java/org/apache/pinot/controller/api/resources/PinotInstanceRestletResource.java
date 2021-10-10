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
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.INSTANCE_TAG)
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

  private Map<String, String> getSystemResourceInfo(InstanceConfig instanceConfig) {
    return instanceConfig.getRecord().getMapField(CommonConstants.Helix.Instance.SYSTEM_RESOURCE_INFO_KEY);
  }

  @POST
  @Path("/instances")
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
  public SuccessResponse addInstance(Instance instance) {
    LOGGER.info("Instance creation request received for instance: {}", InstanceUtils.getHelixInstanceId(instance));
    if (!_pinotHelixResourceManager.addInstance(instance).isSuccessful()) {
      throw new ControllerApplicationException(LOGGER, "Instance already exists", Response.Status.CONFLICT);
    }
    return new SuccessResponse("Instance successfully created");
  }

  @POST
  @Path("/instances/{instanceName}/state")
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
  public SuccessResponse toggleInstanceState(
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
    if (!_pinotHelixResourceManager.instanceExists(instanceName)) {
      throw new ControllerApplicationException(LOGGER, "Instance " + instanceName + " not found",
          Response.Status.NOT_FOUND);
    }

    PinotResourceManagerResponse response = _pinotHelixResourceManager.dropInstance(instanceName);
    if (!response.isSuccessful()) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to drop instance " + instanceName + " - " + response.getMessage(), Response.Status.CONFLICT);
    }
    return new SuccessResponse("Successfully dropped instance");
  }

  @PUT
  @Path("/instances/{instanceName}")
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
      @PathParam("instanceName") String instanceName, Instance instance) {
    LOGGER.info("Instance update request received for instance: {}", instanceName);
    PinotResourceManagerResponse response = _pinotHelixResourceManager.updateInstance(instanceName, instance);
    if (!response.isSuccessful()) {
      throw new ControllerApplicationException(LOGGER, "Failure to update instance. Reason: " + response.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return new SuccessResponse("Instance successfully updated");
  }

  @PUT
  @Path("/instances/{instanceName}/updateTags")
  @Authenticate(AccessType.UPDATE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the tags of the specified instance", consumes = MediaType.APPLICATION_JSON,
      notes = "Update the tags of the specified instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public SuccessResponse updateInstanceTags(
      @ApiParam(value = "Instance name", required = true, example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName,
      @ApiParam(value = "Comma separated tags list", required = true) @QueryParam("tags") String tags) {
    LOGGER.info("Instance update request received for instance: {} and tags: {}", instanceName, tags);
    if (tags == null) {
      throw new ControllerApplicationException(LOGGER, "Must provide tags to update", Response.Status.BAD_REQUEST);
    }
    PinotResourceManagerResponse response = _pinotHelixResourceManager.updateInstanceTags(instanceName, tags);
    if (!response.isSuccessful()) {
      throw new ControllerApplicationException(LOGGER,
          "Failure to update instance: " + instanceName + " with tags: " + tags + ". Reason: " + response.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return new SuccessResponse("Successfully updated tags for instance: " + instanceName + " tags: " + tags);
  }
}
