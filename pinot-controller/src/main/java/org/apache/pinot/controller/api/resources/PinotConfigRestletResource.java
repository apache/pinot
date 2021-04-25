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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.PinotConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.LoggerFactory;


/**
 * Endpoints for CRUD of PinotConfig.
 * PinotConfig is a group of the offline table config, realtime table config and schema.
 */
@Api(tags = Constants.CONFIG_TAG)
@Path("/")
public class PinotConfigRestletResource {

  public static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PinotConfigRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  AccessControlFactory _accessControlFactory;
  AccessControlUtils _accessControlUtils = new AccessControlUtils();

  /**
   * List all PinotConfigs, where each PinotConfig is a group of the offline table config, realtime table config and schema.
   * This is equivalent to a list of all raw table names
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/configs")
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Lists all pinot configs in cluster", notes = "Lists all pinot configs in cluster")
  public String listConfigs() {
    try {
      List<String> rawTableNames = _pinotHelixResourceManager.getAllRawTables();
      Collections.sort(rawTableNames);

      ArrayNode configsList = JsonUtils.newArrayNode();
      for (String rawTableName : rawTableNames) {
        configsList.add(rawTableName);
      }
      return configsList.toString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Gets the PinotConfig for the provided config name, by fetching the offline table config for configName_OFFLINE,
   * realtime table config for configName_REALTIME and schema for configName
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/configs/{configName}")
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Get the pinot config for a given configName", notes = "Get the pinot config for a given configName")
  public String getConfig(
      @ApiParam(value = "Config name", required = true) @PathParam("configName") String configName) {

    try {
      Schema schema = _pinotHelixResourceManager.getSchema(configName);
      TableConfig offlineTableConfig = _pinotHelixResourceManager.getOfflineTableConfig(configName);
      TableConfig realtimeTableConfig = _pinotHelixResourceManager.getRealtimeTableConfig(configName);
      PinotConfig config = new PinotConfig(configName, offlineTableConfig, realtimeTableConfig, schema);
      return config.toJsonString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Creates a PinotConfig using the configStr, by creating the schema from PinotConfig,
   * followed by the realtimeTableConfig and offlineTableConfig as applicable.
   * Validated the configs before applying.
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/configs")
  @ApiOperation(value = "Create the PinotConfig using the configStr json", notes = "Create the PinotConfig using the configStr json")
  public SuccessResponse addConfig(String configStr, @Context HttpHeaders httpHeaders, @Context Request request) {
    PinotConfig pinotConfig;
    try {
      pinotConfig = JsonUtils.stringToObject(configStr, PinotConfig.class);
      validateConfig(pinotConfig);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, String.format("Invalid config. %s", e.getMessage()),
          Response.Status.BAD_REQUEST, e);
    }

    TableConfig offlineTableConfig = pinotConfig.getOfflineTableConfig();
    TableConfig realtimeTableConfig = pinotConfig.getRealtimeTableConfig();
    Schema schema = pinotConfig.getSchema();

    try {
      String endpointUrl = request.getRequestURL().toString();
      validatePermissions(schema.getSchemaName(), AccessType.CREATE, httpHeaders, endpointUrl);

      if (offlineTableConfig != null) {
        tuneConfig(offlineTableConfig, schema);
        validatePermissions(offlineTableConfig.getTableName(), AccessType.CREATE, httpHeaders, endpointUrl);
      }
      if (realtimeTableConfig != null) {
        tuneConfig(realtimeTableConfig, schema);
        validatePermissions(realtimeTableConfig.getTableName(), AccessType.CREATE, httpHeaders, endpointUrl);
      }

      _pinotHelixResourceManager.addSchema(schema, true);
      LOGGER.info("Added schema: {}", schema.getSchemaName());
      if (offlineTableConfig != null) {
        _pinotHelixResourceManager.addTable(offlineTableConfig);
        LOGGER.info("Added offline table config: {}", offlineTableConfig.getTableName());
      }
      if (realtimeTableConfig != null) {
        _pinotHelixResourceManager.addTable(realtimeTableConfig);
        LOGGER.info("Added realtime table config: {}", realtimeTableConfig.getTableName());
      }

      return new SuccessResponse("Config " + pinotConfig.getConfigName() + " successfully added");
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
      if (e instanceof PinotHelixResourceManager.InvalidTableConfigException) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Invalid config: %s", pinotConfig.getConfigName()), Response.Status.BAD_REQUEST, e);
      } else if (e instanceof PinotHelixResourceManager.TableAlreadyExistsException) {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
      } else {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
  }

  /**
   * Deletes the PinotConfig by deleting the schema configName, the offline table config for configName_OFFLINE and
   * the realtime table config for configName_REALTIME
   */
  @DELETE
  @Path("/configs/{configName}")
  @Authenticate(AccessType.DELETE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Delete the pinot config", notes = "Delete the pinot config")
  public SuccessResponse deleteConfig(
      @ApiParam(value = "Config name", required = true) @PathParam("configName") String configName) {

    try {
      _pinotHelixResourceManager.deleteRealtimeTable(configName);
      LOGGER.info("Deleted realtime table: {}", configName);
      _pinotHelixResourceManager.deleteOfflineTable(configName);
      LOGGER.info("Deleted offline table: {}", configName);
      Schema schema = _pinotHelixResourceManager.getSchema(configName);
      if (schema != null) {
        _pinotHelixResourceManager.deleteSchema(schema);
      }
      LOGGER.info("Deleted schema: {}", configName);
      return new SuccessResponse("Deleted config: " + configName);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Updated the PinotConfig by updating the schema configName,
   * then updating the offlineTableConfig or creating a new one if it doesn't already exist in the cluster,
   * then updating the realtimeTableConfig or creating a new one if it doesn't already exist in the cluster.
   */
  @PUT
  @Path("/configs/{configName}")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the pinot config provided by the configStr json", notes = "Update the pinot config")
  public SuccessResponse updateConfig(
      @ApiParam(value = "Config name", required = true) @PathParam("configName") String configName,
      @ApiParam(value = "Reload the table if the new schema is backward compatible") @DefaultValue("false") @QueryParam("reload") boolean reload,
      String configStr)
      throws Exception {
    PinotConfig pinotConfig;
    try {
      pinotConfig = JsonUtils.stringToObject(configStr, PinotConfig.class);
      Preconditions.checkState(pinotConfig.getConfigName().equals(configName),
          "configName in pinotConfig: %s must match provided configName: %s", pinotConfig.getConfigName(), configName);
      validateConfig(pinotConfig);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, String.format("Invalid config: %s", configName),
          Response.Status.BAD_REQUEST, e);
    }

    TableConfig offlineTableConfig = pinotConfig.getOfflineTableConfig();
    TableConfig realtimeTableConfig = pinotConfig.getRealtimeTableConfig();
    Schema schema = pinotConfig.getSchema();

    try {
      _pinotHelixResourceManager.updateSchema(schema, reload);
      LOGGER.info("Updated schema: {}", configName);

      if (offlineTableConfig != null) {
        tuneConfig(offlineTableConfig, schema);
        if (_pinotHelixResourceManager.hasOfflineTable(configName)) {
          _pinotHelixResourceManager.updateTableConfig(offlineTableConfig);
          LOGGER.info("Updated offline table config: {}", configName);
        } else {
          _pinotHelixResourceManager.addTable(offlineTableConfig);
          LOGGER.info("Created offline table config: {}", configName);
        }
        if (realtimeTableConfig != null) {
          tuneConfig(realtimeTableConfig, schema);
          if (_pinotHelixResourceManager.hasRealtimeTable(configName)) {
            _pinotHelixResourceManager.updateTableConfig(realtimeTableConfig);
            LOGGER.info("Updated realtime table config: {}", configName);
          } else {
            _pinotHelixResourceManager.addTable(realtimeTableConfig);
            LOGGER.info("Created realtime table config: {}", configName);
          }
        }
      }
    } catch (PinotHelixResourceManager.InvalidTableConfigException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid config for: %s, %s", configName, e.getMessage()), Response.Status.BAD_REQUEST, e);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to update config for: %s, %s", configName, e.getMessage()),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }

    return new SuccessResponse("Config updated for " + configName);
  }

  /**
   * Validates the pinot config as provided in the configStr json, by validating the schema,
   * the realtime table config and the offline table config
   */
  @POST
  @Path("/configs/validate")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validate the pinot config", notes = "Validate the pinot config")
  public String validateConfig(String configStr) {
    PinotConfig pinotConfig;
    try {
      pinotConfig = JsonUtils.stringToObject(configStr, PinotConfig.class);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, String.format("Invalid pinot config json string: %s", configStr),
          Response.Status.BAD_REQUEST, e);
    }
    return validateConfig(pinotConfig);
  }

  private void tuneConfig(TableConfig tableConfig, Schema schema) {
    TableConfigUtils.applyTunerConfig(tableConfig, schema);
    TableConfigUtils.ensureMinReplicas(tableConfig, _controllerConf.getDefaultTableMinReplicas());
    TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, _controllerConf.getDimTableMaxSize());
  }

  private String validateConfig(PinotConfig pinotConfig) {
    String configName = pinotConfig.getConfigName();
    TableConfig offlineTableConfig = pinotConfig.getOfflineTableConfig();
    TableConfig realtimeTableConfig = pinotConfig.getRealtimeTableConfig();
    Schema schema = pinotConfig.getSchema();
    try {
      Preconditions.checkState(offlineTableConfig != null || realtimeTableConfig != null,
          "Must provide at least one of realtimeTableConfig or offlineTableConfig for adding config: %s", configName);
      Preconditions.checkState(schema != null, "Must provide schema for adding config: %s", configName);
      Preconditions.checkState(!configName.isEmpty(), "configName cannot be empty in PinotConfig");

      Preconditions
          .checkState(configName.equals(schema.getSchemaName()), "ConfigName: %s must be equal to schemaName: %s",
              configName, schema.getSchemaName());
      SchemaUtils.validate(schema);

      if (offlineTableConfig != null) {
        String rawTableName = TableNameBuilder.extractRawTableName(offlineTableConfig.getTableName());
        Preconditions.checkState(rawTableName.equals(configName),
            "Name in offlineTableConfig: %s must be equal to configName: %s", rawTableName, configName);
        TableConfigUtils.validateTableName(offlineTableConfig);
        TableConfigUtils.validate(offlineTableConfig, schema);
      }
      if (realtimeTableConfig != null) {
        String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableConfig.getTableName());
        Preconditions.checkState(rawTableName.equals(configName),
            "Name in realtimeTableConfig: %s must be equal to configName: %s", rawTableName, configName);
        TableConfigUtils.validateTableName(realtimeTableConfig);
        TableConfigUtils.validate(realtimeTableConfig, schema);
      }
      TableConfigUtils.verifyHybridTableConfigs(configName, offlineTableConfig, realtimeTableConfig);

      return pinotConfig.toJsonString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid config: %s. %s", configName, e.getMessage()), Response.Status.BAD_REQUEST, e);
    }
  }

  private void validatePermissions(String resourceName, AccessType accessType, HttpHeaders httpHeaders,
      String endpointUrl) {
    _accessControlUtils
        .validatePermission(resourceName, accessType, httpHeaders, endpointUrl, _accessControlFactory.create());
  }
}
