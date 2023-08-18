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

package org.apache.pinot.tools.service.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.service.PinotServiceManager;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Controller.CONFIG_OF_CONTROLLER_METRICS_PREFIX;
import static org.apache.pinot.spi.utils.CommonConstants.Controller.DEFAULT_METRICS_PREFIX;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "Startable", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotServiceManagerInstanceResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotServiceManagerInstanceResource.class);

  @Inject
  private PinotServiceManager _pinotServiceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/instances")
  @ApiOperation(value = "Get Pinot Instances Status")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Instance Status"), @ApiResponse(code = 500, message = "Internal server error")
  })
  public Map<String, PinotInstanceStatus> getPinotAllInstancesStatus() {
    Map<String, PinotInstanceStatus> results = new HashMap<>();
    for (String instanceId : _pinotServiceManager.getRunningInstanceIds()) {
      results.put(instanceId, _pinotServiceManager.getInstanceStatus(instanceId));
    }
    return results;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/instances/{instanceName}")
  @ApiOperation(value = "Get Pinot Instance Status")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Instance Status"), @ApiResponse(code = 404, message = "Instance Not Found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public PinotInstanceStatus getPinotInstanceStatus(
      @ApiParam(value = "Name of the instance") @PathParam("instanceName") String instanceName) {
    List<String> instanceIds = _pinotServiceManager.getRunningInstanceIds();
    if (instanceIds.contains(instanceName)) {
      return _pinotServiceManager.getInstanceStatus(instanceName);
    }
    throw new WebApplicationException(String.format("Instance [%s] not found.", instanceName),
        Response.Status.NOT_FOUND);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/instances/{instanceName}")
  @ApiOperation(value = "Stop a Pinot Instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Pinot Instance is Stopped"), @ApiResponse(code = 404, message = "Instance "
                                                                                                          + "Not "
                                                                                                          + "Found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Response stopPinotInstance(
      @ApiParam(value = "Name of the instance") @PathParam("instanceName") String instanceName) {
    List<String> instanceIds = _pinotServiceManager.getRunningInstanceIds();
    if (instanceIds.contains(instanceName)) {
      if (_pinotServiceManager.stopPinotInstanceById(instanceName)) {
        return Response.ok().build();
      } else {
        throw new WebApplicationException(String.format("Failed to stop a Pinot instance [%s]", instanceName),
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    }
    throw new WebApplicationException(String.format("Instance [%s] not found.", instanceName),
        Response.Status.NOT_FOUND);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/instances/{role}")
  @ApiOperation(value = "Start a Pinot instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Pinot instance is started"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 404, message = "Pinot Role Not Found"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  public PinotInstanceStatus startPinotInstance(
      @ApiParam(value = "A Role of Pinot Instance to start: CONTROLLER/BROKER/SERVER/MINION") @PathParam("role")
      String role, @ApiParam(value = "true|false") @QueryParam("autoMode") boolean autoMode, String confStr) {
    ServiceRole serviceRole;
    try {
      serviceRole = ServiceRole.valueOf(role.toUpperCase());
    } catch (Exception e) {
      throw new WebApplicationException("Unrecognized Role: " + role, Response.Status.NOT_FOUND);
    }
    Map<String, Object> properties = new HashMap<>();
    try {
      properties = CommonsConfigurationUtils.toMap(JsonUtils.stringToObject(confStr, Configuration.class));
    } catch (IOException e) {
      if (!autoMode) {
        throw new WebApplicationException("Unable to deserialize Conf String to Configuration Object",
            Response.Status.BAD_REQUEST);
      }
    }

    if (autoMode) {
      updateConfiguration(serviceRole, properties);
    }
    try {
      String instanceName = _pinotServiceManager.startRole(serviceRole, properties);
      if (instanceName != null) {
        LOGGER.info("Successfully started Pinot [{}] instance [{}]", serviceRole, instanceName);
        return _pinotServiceManager.getInstanceStatus(instanceName);
      }
      throw new WebApplicationException(String.format("Unable to start a Pinot [%s]", serviceRole),
          Response.Status.INTERNAL_SERVER_ERROR);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing POST request", e);
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private void updateConfiguration(ServiceRole role, Map<String, Object> properties) {
    switch (role) {
      case CONTROLLER:
        String controllerHost =
            Optional.ofNullable(properties.get(ControllerConf.CONTROLLER_HOST)).map(Object::toString).orElse(null);
        if (controllerHost == null) {
          try {
            controllerHost = NetUtils.getHostAddress();
          } catch (Exception e) {
            controllerHost = "localhost";
          }
          properties.put(ControllerConf.CONTROLLER_HOST, controllerHost);
        }

        String controllerPort =
            Optional.ofNullable(properties.get(ControllerConf.CONTROLLER_PORT)).map(Object::toString).orElse(null);
        if (controllerPort == null) {
          controllerPort = Integer.toString(PinotConfigUtils.getAvailablePort());
          properties.put(ControllerConf.CONTROLLER_PORT, controllerPort);
        }

        if (!properties.containsKey(ControllerConf.DATA_DIR)) {
          properties.put(ControllerConf.DATA_DIR,
              PinotConfigUtils.TMP_DIR + String.format("Controller_%s_%s/data", controllerHost, controllerPort));
        }

        if (!properties.containsKey(CONFIG_OF_CONTROLLER_METRICS_PREFIX)) {
          properties.put(CONFIG_OF_CONTROLLER_METRICS_PREFIX,
              String.format("%s.%s_%s", DEFAULT_METRICS_PREFIX, controllerHost, controllerPort));
        }

        break;
      case BROKER:

        if (!properties.containsKey(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT)) {
          properties.put(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, PinotConfigUtils.getAvailablePort());
        }
        if (!properties.containsKey(CommonConstants.Broker.METRICS_CONFIG_PREFIX)) {
          String hostname;
          try {
            hostname = NetUtils.getHostAddress();
          } catch (Exception e) {
            hostname = "localhost";
          }
          properties.put(CommonConstants.Broker.CONFIG_OF_METRICS_NAME_PREFIX,
              String.format("%s%s_%s", CommonConstants.Broker.DEFAULT_METRICS_NAME_PREFIX, hostname,
                  properties.get(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT)));
        }
        return;
      case SERVER:
        if (!properties.containsKey(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST)) {
          String hostname;
          try {
            hostname = NetUtils.getHostAddress();
          } catch (Exception e) {
            hostname = "localhost";
          }
          properties.put(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST, hostname);
        }
        if (!properties.containsKey(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT)) {
          properties.put(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, PinotConfigUtils.getAvailablePort());
        }
        if (!properties.containsKey(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT)) {
          properties.put(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT, PinotConfigUtils.getAvailablePort());
        }
        if (!properties.containsKey(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_DIR)) {
          properties.put(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_DIR,
              PinotConfigUtils.TMP_DIR + String.format("Server_%s_%s/data",
                  properties.get(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST),
                  properties.get(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT)));
        }
        if (!properties.containsKey(CommonConstants.Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR)) {
          properties.put(CommonConstants.Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR,
              PinotConfigUtils.TMP_DIR + String.format("Server_%s_%s/segment",
                  properties.get(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST),
                  properties.get(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT)));
        }
        if (!properties.containsKey(CommonConstants.Server.PINOT_SERVER_METRICS_PREFIX)) {
          properties.put(CommonConstants.Server.PINOT_SERVER_METRICS_PREFIX,
              String.format("%s%s_%s", CommonConstants.Server.DEFAULT_METRICS_PREFIX,
                  properties.get(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST),
                  properties.get(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT)));
        }
        return;
      case MINION:

        if (!properties.containsKey(CommonConstants.Helix.KEY_OF_MINION_PORT)) {
          properties.put(CommonConstants.Helix.KEY_OF_MINION_PORT, PinotConfigUtils.getAvailablePort());
        }
        if (!properties.containsKey(CommonConstants.Minion.CONFIG_OF_METRICS_PREFIX_KEY)) {
          String hostname;
          try {
            hostname = NetUtils.getHostAddress();
          } catch (Exception e) {
            hostname = "localhost";
          }
          properties.put(CommonConstants.Minion.CONFIG_OF_METRICS_PREFIX_KEY,
              String.format("%s%s_%s", CommonConstants.Minion.CONFIG_OF_METRICS_PREFIX, hostname,
                  properties.get(CommonConstants.Helix.KEY_OF_MINION_PORT)));
        }
        return;
      default:
        break;
    }
  }
}
