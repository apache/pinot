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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Response;
import com.linkedin.pinot.common.restlet.swagger.Responses;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.api.pojos.Instance;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import java.util.List;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Restlet to manage Pinot instances.
 */

public class PinotInstanceRestletResource extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      PinotInstanceRestletResource.class);

  private final ObjectMapper mapper;

  public PinotInstanceRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
    mapper = new ObjectMapper();
  }

  @Override
  @Post("json")
  public Representation post(Representation entity) {
    StringRepresentation presentation;

    try {
      final String instanceName = (String) getRequest().getAttributes().get(INSTANCE_NAME);

      if (instanceName == null) {
        // This is a request to create an instance
        try {
          final Instance instance = mapper.readValue(ByteStreams.toByteArray(entity.getStream()), Instance.class);
          presentation = addInstance(instance);
        } catch (final Exception e) {
          presentation = new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
          LOGGER.error("Caught exception while processing post request", e);
          ControllerRestApplication.getControllerMetrics()
              .addMeteredGlobalValue(ControllerMeter.CONTROLLER_INTERNAL_ERROR, 1L);
          setStatus(Status.SERVER_ERROR_INTERNAL);
        }
      } else {
        // This is a request to toggle the state of an instance
        if (_pinotHelixResourceManager.instanceExists(instanceName)) {
          final String state = getRequest().getEntityAsText().trim();

          if (isValidState(state)) {
            presentation = toggleInstanceState(instanceName, state);
          } else {
            LOGGER.error(INVALID_STATE_ERROR);
            setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
            return new StringRepresentation(INVALID_STATE_ERROR);
          }
        } else {
          setStatus(Status.CLIENT_ERROR_NOT_FOUND);
          presentation = new StringRepresentation("Error: Instance " + instanceName + " not found.");
        }
      }
    } catch (final Exception e) {
      presentation = new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
      LOGGER.error("Caught exception while processing post request", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_INSTANCE_POST_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }

    return presentation;
  }

  @HttpVerb("post")
  @Summary("Adds an instance")
  @Tags({ "instance" })
  @Paths({ "/instances", "/instances/" })
  @Responses({
      @Response(statusCode = "200", description = "The instance was created successfully"),
      @Response(statusCode = "409", description = "The instance already exists and no action was taken"),
      @Response(statusCode = "500", description = "Failed to create the instance")
  })
  private StringRepresentation addInstance(
      @Parameter(name = "instance", in = "body", description = "The instance to add", required = true)
      Instance instance) throws JSONException {
    StringRepresentation presentation;
    LOGGER.info("Instance creation request received for instance " + instance.toInstanceId());
    final PinotResourceManagerResponse resp = _pinotHelixResourceManager.addInstance(instance);
    if (resp.status == PinotResourceManagerResponse.ResponseStatus.failure) {
      setStatus(Status.CLIENT_ERROR_CONFLICT);
    }
    presentation = new StringRepresentation(resp.toJSON().toString());
    return presentation;
  }

  /**
   * URI mapping:
   * "/instances", "/instances/" : Lists all the instances
   * "/instances/{instanceName}" : Gets information about an instance
   *
   * {@inheritDoc}
   * @see org.restlet.resource.ServerResource#get()
   */
  @Override
  @Get
  public Representation get() {
    Representation presentation;
    try {
      final String instanceName = (String) getRequest().getAttributes().get(INSTANCE_NAME);

      if (instanceName == null) {
        presentation = getAllInstances();
      } else {
        presentation = getInstanceInformation(instanceName);
      }

    } catch (final Exception e) {
      presentation = new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
      LOGGER.error("Caught exception while processing post request", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_INSTANCE_GET_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  /**
   * Gets the information for an instance.
   *
   * @param instanceName The instance name
   */
  @HttpVerb("get")
  @Summary("Gets information for an instance")
  @Tags({ "instance" })
  @Paths({ "/instances/{instanceName}", "/instances/{instanceName}/" })
  @Responses({
      @Response(statusCode = "200", description = "Information about the specified instance"),
      @Response(statusCode = "404", description = "The specified instance does not exist"),
      @Response(statusCode = "500", description = "There was an error while fetching information for the given instance")
  })
  private Representation getInstanceInformation(
      @Parameter(
          name = "instanceName",
          description = "The name of the instance (eg. Server_1.2.3.4_1234 or Broker_someHost.example.com_2345)",
          in = "path",
          required = true)
      String instanceName) {
    try {
      if (!_pinotHelixResourceManager.instanceExists(instanceName)) {
        setStatus(Status.CLIENT_ERROR_NOT_FOUND);
        return new StringRepresentation("Error: Instance " + instanceName + " not found.");
      }

      InstanceConfig instanceConfig = _pinotHelixResourceManager.getHelixInstanceConfig(instanceName);

      JSONObject response = new JSONObject();
      response.put("instanceName", instanceConfig.getInstanceName());
      response.put("hostName", instanceConfig.getHostName());
      response.put("enabled", instanceConfig.getInstanceEnabled());
      response.put("port", instanceConfig.getPort());
      response.put("tags", new JSONArray(instanceConfig.getTags()));

      return new StringRepresentation(response.toString());
    } catch (Exception e) {
      LOGGER.warn("Caught exception while fetching information for instance {}", instanceName, e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation("{}");
    }
  }

  /**
   * Get all instances in the cluster
   * @return List of all instances in the cluster.
   * @throws JSONException
   */
  @HttpVerb("get")
  @Summary("Views all instances")
  @Tags({ "instance" })
  @Paths({ "/instances", "/instances/" })
  @Responses({
      @Response(statusCode = "200", description = "A list of instances")
  })
  private Representation getAllInstances() throws JSONException {
    JSONObject object = new JSONObject();
    JSONArray instanceArray = new JSONArray();

    List<String> instanceNames = _pinotHelixResourceManager.getAllInstances();
    for (String instanceName : instanceNames) {
      instanceArray.put(instanceName);
    }

    object.put("instances", instanceArray);
    return new StringRepresentation(object.toString());
  }

  /**
   *
   * @param instanceName: Name of the instance to enable/disable/drop
   * @param state: One of '{enable|disable|drop}'
   * @return StringRepresentation of state after trying to enable/disable/drop instance.
   * @throws JSONException
   */
  @HttpVerb("post")
  @Summary("Enable, disable or drop an instance")
  @Tags({ "instance" })
  @Paths({ "/instances/{instanceName}/state", "/instances/{instanceName}/state" })
  @Responses({
      @Response(statusCode = "200", description = "The instance state was changed successfully"),
      @Response(statusCode = "400", description = "The state given was not enable, disable or drop"),
      @Response(statusCode = "404", description = "The instance was not found")
  })
  private StringRepresentation toggleInstanceState(
      @Parameter(name = "instanceName", in = "path",
          description = "The name of the instance for which to toggle its state", required = true)
      String instanceName,
      @Parameter(name = "state", in = "body",
          description = "The desired instance state, either enable, disable or drop", required = true)
      String state) throws JSONException {
    if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
      return new StringRepresentation(_pinotHelixResourceManager.enableInstance(instanceName).toJSON().toString());

    } else if (StateType.DISABLE.name().equalsIgnoreCase(state)) {
      return new StringRepresentation(_pinotHelixResourceManager.disableInstance(instanceName).toJSON().toString());

    } else if (StateType.DROP.name().equalsIgnoreCase(state)) {
      return new StringRepresentation(_pinotHelixResourceManager.dropInstance(instanceName).toJSON().toString());

    } else {
      LOGGER.error(INVALID_INSTANCE_URI_ERROR);
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation(INVALID_INSTANCE_URI_ERROR);
    }
  }

  @Override
  @Delete
  public Representation delete() {
    Representation presentation;
    try {
      final String instanceName = (String) getRequest().getAttributes().get(INSTANCE_NAME);
      presentation = deleteInstanceInformation(instanceName);
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while deleting an instance ", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_INSTANCE_DELETE_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  /**
   * Deletes an instance
   *
   * @param instanceName
   */
  @HttpVerb("delete")
  @Summary("Deletes an instance")
  @Tags({ "instance" })
  @Paths({ "/instances/{instanceName}", "/instances/{instanceName}/" })
  @Responses({
      @Response(statusCode = "200", description = "The instance has been deleted successfully"),
      @Response(statusCode = "404", description = "The specified instance does not exist"),
      @Response(statusCode = "409", description = "Forbidden operation typically because the instance is live or "
          + "idealstates still contain some information of this instance \n"),
      @Response(statusCode = "500", description = "There was an error while cleaning files for the instance.")
  })
  private Representation deleteInstanceInformation(
      @Parameter(
          name = "instanceName",
          description = "The name of the instance (eg. Server_1.2.3.4_1234 or Broker_someHost.example.com_2345)",
          in = "path",
          required = true)
          String instanceName) {
    try {
      PinotResourceManagerResponse response;

      // Check that the user input for an instance is correct
      if (!_pinotHelixResourceManager.instanceExists(instanceName)) {
        setStatus(Status.CLIENT_ERROR_NOT_FOUND);
        response = new PinotResourceManagerResponse("Instance " + instanceName + " does not exist.", false);
        return new StringRepresentation(response.toJSON().toString());
      }

      // Check that the instance is safe to drop
      if (!_pinotHelixResourceManager.isInstanceDroppable(instanceName)) {
        setStatus(Status.CLIENT_ERROR_CONFLICT);
        response = new PinotResourceManagerResponse("Instance " + instanceName
            + " is live or it still appears in the idealstate.", false);
        return new StringRepresentation(response.toJSON().toString());
      }

      // Delete the instance information from Helix storage
      response = _pinotHelixResourceManager.dropInstance(instanceName);
      if (!response.isSuccessful()) {
        setStatus(Status.SERVER_ERROR_INTERNAL);
      }

      return new StringRepresentation(response.toJSON().toString());
    } catch (Exception e) {
      LOGGER.warn("Caught exception while deleting information for instance {}", instanceName, e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation("Caught exception while deleting information for instance " + instanceName);
    }
  }
}
