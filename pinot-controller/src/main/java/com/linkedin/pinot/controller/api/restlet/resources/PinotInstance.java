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

import com.linkedin.pinot.controller.api.swagger.HttpVerb;
import com.linkedin.pinot.controller.api.swagger.Parameter;
import com.linkedin.pinot.controller.api.swagger.Paths;
import com.linkedin.pinot.controller.api.swagger.Summary;
import com.linkedin.pinot.controller.api.swagger.Tags;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONException;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.linkedin.pinot.controller.api.pojos.Instance;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;


/**
 * Sep 30, 2014
 *
 */

public class PinotInstance extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotInstance.class);

  private static final String INSTANCE_NAME = "instanceName";
  private static final String STATUS = "status";
  private static final String DISABLE = "disable";
  private static final String ENABLE = "enable";

  private final PinotHelixResourceManager manager;
  private final ObjectMapper mapper;

  public PinotInstance() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
    manager = (PinotHelixResourceManager) getApplication().getContext().getAttributes().get(PinotHelixResourceManager.class.toString());
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
  @Tags({"instance"})
  @Paths({
      "/instances",
      "/instances/"
  })
  private StringRepresentation addInstance(Instance instance)
      throws JSONException {
    StringRepresentation presentation;
    final PinotResourceManagerResponse resp = manager.addInstance(instance);
    LOGGER.info("instace create request recieved for instance : " + instance.toInstanceId());
    presentation = new StringRepresentation(resp.toJSON().toString());
    return presentation;
  }

  @Override
  @Get
  public Representation get() {
    StringRepresentation presentation;
    try {
      final String instanceName = (String) getRequest().getAttributes().get(INSTANCE_NAME);
      final String status = getReference().getQueryAsForm().getValues(STATUS);
      if (status == null) {
        presentation = new StringRepresentation("Not a valid GET call, do nothing!");
      } else {
        presentation = toggleInstanceStatus(instanceName, status);
      }

    } catch (final Exception e) {
      presentation = new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
      LOGGER.error("Caught exception while processing post request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  @HttpVerb("get")
  @Summary("Toggles an instance's status")
  @Tags({"instance"})
  @Paths({
      "/instances/{instanceName}",
      "/instances/{instanceName}/"
  })
  private StringRepresentation toggleInstanceStatus(
      @Parameter(name = "instanceName", in = "path", description = "The name of the instance for which to toggle its status", required = true)
      String instanceName,
      @Parameter(name = "status", in = "query", description = "The desired instance status, either enable or disable", required = true)
      String status)
      throws JSONException {
    if (status.equalsIgnoreCase(ENABLE)) {
      return new StringRepresentation(manager.enableInstance(instanceName).toJSON().toString());
    } else if (status.equalsIgnoreCase(DISABLE)) {
      return new StringRepresentation(manager.disableInstance(instanceName).toJSON().toString());
    }
    // TODO Error out
    return null;
  }
}
