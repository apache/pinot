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

import org.apache.commons.lang3.exception.ExceptionUtils;
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
import com.linkedin.pinot.controller.ControllerConf;
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
      final PinotResourceManagerResponse resp = manager.addInstance(instance);
      LOGGER.info("instace create request recieved for instance : " + instance.toInstanceId());
      presentation = new StringRepresentation(resp.toJSON().toString());
    } catch (final Exception e) {
      presentation = new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
      LOGGER.error("Caught exception while processing post request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  @Override
  @Get
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      final String instanceName = (String) getRequest().getAttributes().get(INSTANCE_NAME);
      final String status = getReference().getQueryAsForm().getValues(STATUS);
      if (status == null) {
        presentation = new StringRepresentation("Not a valid GET call, do nothing!");
      } else {
        if (status.equals(ENABLE)) {
          presentation = new StringRepresentation(manager.enableInstance(instanceName).toJSON().toString());
        }
        if (status.equals(DISABLE)) {
          presentation = new StringRepresentation(manager.disableInstance(instanceName).toJSON().toString());
        }
      }

    } catch (final Exception e) {
      presentation = new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
      LOGGER.error("Caught exception while processing post request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

}
