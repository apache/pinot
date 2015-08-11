/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.restlet.resources;

import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.linkedin.pinot.controller.api.pojos.Instance;
import com.linkedin.pinot.controller.api.swagger.HttpVerb;
import com.linkedin.pinot.controller.api.swagger.Parameter;
import com.linkedin.pinot.controller.api.swagger.Paths;
import com.linkedin.pinot.controller.api.swagger.Summary;
import com.linkedin.pinot.controller.api.swagger.Tags;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;


/**
 * Sep 30, 2014
 *
 */

public class PinotInstanceRestletResource extends PinotRestletResourceBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotInstanceRestletResource.class);

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
    StringRepresentation presentation = null;
    try {
      final Instance instance = mapper.readValue(ByteStreams.toByteArray(entity.getStream()), Instance.class);
      presentation = addInstance(instance);
    } catch (final Exception e) {
      presentation = new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
      LOGGER.error("Caught exception while processing post request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  @HttpVerb("post")
  @Summary("Adds an instance")
  @Tags({ "instance" })
  @Paths({ "/instances", "/instances/" })
  private StringRepresentation addInstance(Instance instance) throws JSONException {
    StringRepresentation presentation;
    final PinotResourceManagerResponse resp = _pinotHelixResourceManager.addInstance(instance);
    LOGGER.info("instace create request recieved for instance : " + instance.toInstanceId());
    presentation = new StringRepresentation(resp.toJSON().toString());
    return presentation;
  }

  /**
   * URI mapping:
   * "/instances", "/instances/" : Lists all the instances
   * "/instances/{instanceName}/state?={state}":
   * - state = 'enable'  : Enable the instanceName
   * - state = 'disable' : Disable the instanceName
   * - state = 'drop'    : Drop the instanceName
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
      final String state = getReference().getQueryAsForm().getValues(STATE);

      if (!isValidState(state)) {
        return new StringRepresentation(INVALID_STATE_ERROR);
      }

      if (instanceName == null) {
        presentation = getAllInstances();
      } else if (_pinotHelixResourceManager.instanceExists(instanceName)) {
        presentation = toggleInstanceState(instanceName, state);
      } else {
        presentation = new StringRepresentation("Error: Instance " + instanceName + " not found.");
      }

    } catch (final Exception e) {
      presentation = new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
      LOGGER.error("Caught exception while processing post request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
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
  private Representation getAllInstances() throws JSONException {
    JSONObject object = new JSONObject();
    JSONArray instanceArray = new JSONArray();

    List<String> instanceNames = _pinotHelixResourceManager.getAllInstanceNames();
    for (String instanceName : instanceNames) {
      instanceArray.put(instanceName);
    }

    object.put("Instances", instanceArray);
    return new StringRepresentation(object.toString());
  }

  /**
   *
   * @param instanceName: Name of the instance to enable/disable/drop
   * @param state: One of '{enable|disable|drop}'
   * @return StringRepresentation of state after trying to enable/disable/drop instance.
   * @throws JSONException
   */
  @HttpVerb("get")
  @Summary("Enable, disable or drop an instance")
  @Tags({ "instance" })
  @Paths({ "/instances/{instanceName}", "/instances/{instanceName}/" })
  private StringRepresentation toggleInstanceState(@Parameter(name = "instanceName", in = "path",
      description = "The name of the instance for which to toggle its state", required = true) String instanceName,
      @Parameter(name = "state", in = "query", description = "The desired instance state, either enable or disable",
          required = true) String state) throws JSONException {

    if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
      return new StringRepresentation(_pinotHelixResourceManager.enableInstance(instanceName).toJSON().toString());

    } else if (StateType.DISABLE.name().equalsIgnoreCase(state)) {
      return new StringRepresentation(_pinotHelixResourceManager.disableInstance(instanceName).toJSON().toString());

    } else if (StateType.DROP.name().equalsIgnoreCase(state)) {
      return new StringRepresentation(_pinotHelixResourceManager.dropInstance(instanceName).toJSON().toString());

    } else {
      return new StringRepresentation(
          "Incorrect URI: must be of form: /instances/{instanceName}[?{state}={enable|disable|drop}]");
    }
  }
}
