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
import com.linkedin.pinot.controller.api.pojos.Instance;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.INSTANCE_TAG)
@Path("/")
public class PinotInstanceRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      PinotInstanceRestletResource.class);
  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;

  public static  class Instances
  {
    List<String> instances;
    public Instances(@JsonProperty("instances") List<String> instances) {
      this.instances = instances;
    }

    public List<String> getInstances() {
      return instances;
    }

    public Instances setInstances(List<String> instances) {
      this.instances = instances;
      return this;
    }
  }

  @GET
  @Path("/instances")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List all instances")
  @ApiResponses(value = {@ApiResponse(code=200, message = "Success"),
      @ApiResponse(code=500, message = "Error reading instances")})
  public Instances getAllInstances(
  ) {
    return new Instances(pinotHelixResourceManager.getAllInstances());
  }

  @GET
  @Path("/instances/{instanceName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get instance information", produces = MediaType.APPLICATION_JSON)
  @ApiResponses(value = {@ApiResponse(code=200, message = "Success"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code=500, message = "Error reading instances")})
  public String getInstance(
      @ApiParam(value = "Instance name", required = true,
          example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName)
  {

    if (!pinotHelixResourceManager.instanceExists(instanceName)) {
      throw new ControllerApplicationException(LOGGER, "Instance " + instanceName + " does not exist",
          javax.ws.rs.core.Response.Status.NOT_FOUND);
    }
    InstanceConfig instanceConfig = pinotHelixResourceManager.getHelixInstanceConfig(instanceName);
    JSONObject response = new JSONObject();
    try {
      response.put("instanceName", instanceConfig.getInstanceName());
      response.put("hostName", instanceConfig.getHostName());
      response.put("enabled", instanceConfig.getInstanceEnabled());
      response.put("port", instanceConfig.getPort());
      response.put("tags", new JSONArray(instanceConfig.getTags()));
    } catch (JSONException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    return response.toString();
  }

  @POST
  @Path("/instances")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Create a new instance", consumes = MediaType.APPLICATION_JSON, notes = "Creates a new instance with given instance config")
  @ApiResponses(value = {@ApiResponse(code = 200, message="Instance successfully created"),
      @ApiResponse(code=409, message="Instance exists already"),
      @ApiResponse(code = 500, message = "Internal error")})
  public SuccessResponse addInstance(
      Instance instance
  ) {
    LOGGER.info("Instance creation request received for instance " + instance.toInstanceId());
    final PinotResourceManagerResponse resp = pinotHelixResourceManager.addInstance(instance);
    if (resp.status == PinotResourceManagerResponse.ResponseStatus.failure) {
      throw new ControllerApplicationException(LOGGER, "Instance already exists",
          javax.ws.rs.core.Response.Status.CONFLICT);
    }
    return new SuccessResponse("Instance successfully created");
  }

  // TODO: @consumes text/plain but swagger doc says json. Does that work. It's better this way if it works
  @POST
  @Path("/instances/{instanceName}/state")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Create a new instance", consumes = MediaType.APPLICATION_JSON, notes = "Creates a new instance with given instance config")
  @ApiResponses(value = {@ApiResponse(code = 200, message="Instance successfully created"),
      @ApiResponse(code=409, message="Instance exists already"),
      @ApiResponse(code = 400, message="Bad Request"),
      @ApiResponse(code = 500, message = "Internal error")})
  public SuccessResponse addInstance(
      @ApiParam(value = "Instance name", required = true,
          example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName,
      String state
  ) {
    // TODO: state should be moved to path or form parameter
    if (! pinotHelixResourceManager.instanceExists(instanceName)) {
      throw new ControllerApplicationException(LOGGER, "Instance " + instanceName + " does not exist",
          Response.Status.NOT_FOUND);
    }
    PinotResourceManagerResponse response;
    if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
      response = pinotHelixResourceManager.enableInstance(instanceName);
    } else if (StateType.DISABLE.name().equalsIgnoreCase(state)) {
      response = pinotHelixResourceManager.disableInstance(instanceName);
    } else if (StateType.DROP.name().equalsIgnoreCase(state)) {
      response = pinotHelixResourceManager.dropInstance(instanceName);
    } else {
     throw new ControllerApplicationException(LOGGER, "Unknown state " + state + " for instance request",
         Response.Status.BAD_REQUEST);
    }
    if (response.isSuccessful()) {
      return new SuccessResponse("Request to " + state + " instance " + instanceName  + " is successful");
    }
    throw new ControllerApplicationException(LOGGER, "Failed to " + state + " instance " + instanceName,
        Response.Status.INTERNAL_SERVER_ERROR);
  }

  @DELETE
  @Path("/instances/{instanceName}")
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Delete an instance", consumes = MediaType.APPLICATION_JSON,
      notes = "Deletes an instance of given name")
  @ApiResponses(value = {@ApiResponse(code = 200, message="Instance successfully deleted"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 409, message = "Forbidden operation typically because the instance is live or "
          + "idealstates still contain some information of this instance"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 500, message = "Internal error")})
  public SuccessResponse deleteInstance(
      @ApiParam(value = "Instance name", required = true,
          example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName
  ) {
    if (! pinotHelixResourceManager.instanceExists(instanceName)) {
      throw new ControllerApplicationException(LOGGER, "Instance " + instanceName + " does not exist",
          Response.Status.NOT_FOUND);
    }
    if (!pinotHelixResourceManager.isInstanceDroppable(instanceName)) {
      throw new ControllerApplicationException(LOGGER, "Instance " + instanceName + " is live or it still appears in the idealstate",
          Response.Status.CONFLICT);
    }
    PinotResourceManagerResponse response = pinotHelixResourceManager.dropInstance(instanceName);
    if (!response.isSuccessful()) {
      LOGGER.error("Failed to delete instance: {}, response: {}", instanceName, response.message);
      throw new ControllerApplicationException(LOGGER, "Failed to delete instance", Response.Status.INTERNAL_SERVER_ERROR);
    }
    return new SuccessResponse("Successfully deleted instance");
  }
}
