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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.services.PinotInstanceService;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/")
public class PinotInstanceRestletResource implements PinotInstanceService {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotInstanceRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Override
  public Instances getAllInstances() {
    return new Instances(_pinotHelixResourceManager.getAllInstances());
  }

  @Override
  public String getInstance(String instanceName) {
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

  @Override
  public SuccessResponse addInstance(boolean updateBrokerResource, Instance instance) {
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


  @Override
  public SuccessResponse toggleInstanceState(String instanceName, String state) {
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

  @Override
  public SuccessResponse dropInstance(String instanceName) {
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

  @Override
  public SuccessResponse updateInstance(String instanceName, boolean updateBrokerResource, Instance instance) {
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

  @Override
  public SuccessResponse updateInstanceTags(String instanceName, String tags, boolean updateBrokerResource) {
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

  @Override
  public SuccessResponse updateBrokerResource(String instanceName) {
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
}
