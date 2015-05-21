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
package com.linkedin.pinot.controller.api.reslet.resources;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.Put;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 24, 2014
 *   sample curl call
 *  curl -i -X POST -H 'Content-Type: application/json' -d
    '{"resourceName":"resourceName","timeColumnName":"timeColumnName",
    "timeType":"timeType","numInstances":2,"numReplicas":3,"retentionTimeUnit":"retentionTimeUnit",
    "retentionTimeValue":"retentionTimeValue","pushFrequency":"pushFrequency"}'
    http://localhost:8998/resource
 *
 */

@Deprecated
public class PinotDataResource extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotDataResource.class);

  private final ControllerConf conf;
  private final PinotHelixResourceManager manager;
  private final ObjectMapper mapper;

  public PinotDataResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
    conf = (ControllerConf) getApplication().getContext().getAttributes().get(ControllerConf.class.toString());
    manager =
        (PinotHelixResourceManager) getApplication().getContext().getAttributes()
            .get(PinotHelixResourceManager.class.toString());
    mapper = new ObjectMapper();
  }

  @Deprecated
  @Override
  @Put("json")
  public Representation put(Representation entity) {
    StringRepresentation presentation = null;
    try {
      final DataResource resource = mapper.readValue(ByteStreams.toByteArray(entity.getStream()), DataResource.class);
      if (resource.isDataResourceUpdate()) {
        presentation = new StringRepresentation(manager.handleUpdateDataResource(resource).toJSON().toString());
      } else if (resource.isDataResourceConfigUpdate()) {
        presentation = new StringRepresentation(manager.handleUpdateDataResourceConfig(resource).toJSON().toString());
      } else if (resource.isBrokerResourceUpdate()) {
        presentation = new StringRepresentation(manager.handleUpdateBrokerResource(resource).toJSON().toString());
      } else {
        throw new RuntimeException("Not an updated request");
      }
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing put request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  @Deprecated
  @Override
  @Delete
  public Representation delete() {
    StringRepresentation presentation = null;
    try {
      final String resourceName = (String) getRequest().getAttributes().get("resourceName");
      String resourceRealtimeName = BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName);
      String offlineResourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resourceName);
      String respString = "";
      if (ZKMetadataProvider.getOfflineResourceZKMetadata(manager.getPropertyStore(), offlineResourceName) != null) {
        PinotResourceManagerResponse offlineResp = manager.deleteResource(offlineResourceName);
        respString += offlineResp.toJSON().toString() + "\n";
      }
      if (ZKMetadataProvider.getRealtimeResourceZKMetadata(manager.getPropertyStore(), resourceRealtimeName) != null) {
        PinotResourceManagerResponse realtimeResp = manager.deleteResource(resourceRealtimeName);
        respString += realtimeResp.toJSON().toString() + "\n";
      }
      if (respString.length() < 1) {
        respString = "No related resource found.\n";
      }
      presentation = new StringRepresentation(respString);
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing delete request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  /**
   *  called with optional resourceName
   *  if resourceName is not present then it sends back a list of
   * @return
   */
  @Deprecated
  @Override
  @Get
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      final String resourceName = (String) getRequest().getAttributes().get("resourceName");

      if (resourceName == null) {
        final JSONObject ret = new JSONObject();
        final JSONArray retArray = new JSONArray();

        for (final String resource : manager.getAllResourceNames()) {
          retArray.put(resource);
        }
        ret.put("resources", retArray);

        presentation = new StringRepresentation(ret.toString(), MediaType.APPLICATION_JSON);
      } else {
        final JSONObject ret = new JSONObject();
        for (final String resource : manager.getAllResourceNames()) {
          if (resource.equals(BrokerRequestUtils.getOfflineResourceNameForResource(resourceName))) {
            ret.put(BrokerRequestUtils.getOfflineResourceNameForResource(resourceName),
                manager.getOfflineDataResourceZKMetadata(BrokerRequestUtils.getOfflineResourceNameForResource(resourceName)));
          }
          if (resource.equals(BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName))) {
            ret.put(BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName),
                manager.getRealtimeDataResourceZKMetadata(BrokerRequestUtils.getRealtimeResourceNameForResource(
                    resourceName)));
          }
        }
        JSONObject resourceGetRet = new JSONObject();
        resourceGetRet.put(CommonConstants.Helix.DataSource.RESOURCE_NAME, resourceName);
        resourceGetRet.put("config", ret);
        presentation = new StringRepresentation(resourceGetRet.toString(), MediaType.APPLICATION_JSON);
      }
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing get request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  @Deprecated
  @Override
  @Post("json")
  public Representation post(Representation entity) {
    StringRepresentation presentation = null;
    try {
      final DataResource resource = mapper.readValue(ByteStreams.toByteArray(entity.getStream()), DataResource.class);
      if (resource.isCreatedDataResource()) {
        presentation = new StringRepresentation(manager.handleCreateNewDataResource(resource).toJSON().toString());
      } else {
        throw new RuntimeException("Not a created request");
      }
    } catch (final Exception e) {
      presentation = exceptionToStringRepresentation(e);
      LOGGER.error("Caught exception while processing post request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

  private StringRepresentation exceptionToStringRepresentation(Exception e) {
    return new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
  }

}
