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
import org.apache.pinot.spi.config.TableConfigs;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.LoggerFactory;


/**
 * Endpoints for CRUD of {@link TableConfigs}.
 * {@link TableConfigs} is a group of the offline table config, realtime table config and schema for the same tableName.
 */
@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class TableConfigsRestletResource {

  public static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TableConfigsRestletResource.class);

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
   * List all {@link TableConfigs}, where each is a group of the offline table config, realtime table config and schema for the same tableName.
   * This is equivalent to a list of all raw table names
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tableConfigs")
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Lists all TableConfigs in cluster", notes = "Lists all TableConfigs in cluster")
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
   * Gets the {@link TableConfigs} for the provided raw tableName, by fetching the offline table config for tableName_OFFLINE,
   * realtime table config for tableName_REALTIME and schema for tableName
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tableConfigs/{tableName}")
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Get the TableConfigs for a given raw tableName", notes = "Get the TableConfigs for a given raw tableName")
  public String getConfig(
      @ApiParam(value = "Raw table name", required = true) @PathParam("tableName") String tableName) {

    try {
      Schema schema = _pinotHelixResourceManager.getSchema(tableName);
      TableConfig offlineTableConfig = _pinotHelixResourceManager.getOfflineTableConfig(tableName);
      TableConfig realtimeTableConfig = _pinotHelixResourceManager.getRealtimeTableConfig(tableName);
      TableConfigs config = new TableConfigs(tableName, schema, offlineTableConfig, realtimeTableConfig);
      return config.toPrettyJsonString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Creates a {@link TableConfigs} using the <code>tableConfigsStr</code>, by creating the schema,
   * followed by the realtime tableConfig and offline tableConfig as applicable, from the {@link TableConfigs}.
   * Validates the configs before applying.
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tableConfigs")
  @ApiOperation(value = "Add the TableConfigs using the tableConfigsStr json", notes = "Add the TableConfigs using the tableConfigsStr json")
  public SuccessResponse addConfig(String tableConfigsStr, @Context HttpHeaders httpHeaders, @Context Request request) {
    TableConfigs tableConfigs;
    try {
      tableConfigs = JsonUtils.stringToObject(tableConfigsStr, TableConfigs.class);
      validateConfig(tableConfigs);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, String.format("Invalid TableConfigs. %s", e.getMessage()),
          Response.Status.BAD_REQUEST, e);
    }

    TableConfig offlineTableConfig = tableConfigs.getOffline();
    TableConfig realtimeTableConfig = tableConfigs.getRealtime();
    Schema schema = tableConfigs.getSchema();

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

      return new SuccessResponse("TableConfigs " + tableConfigs.getTableName() + " successfully added");
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
      if (e instanceof PinotHelixResourceManager.InvalidTableConfigException) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Invalid TableConfigs: %s", tableConfigs.getTableName()), Response.Status.BAD_REQUEST, e);
      } else if (e instanceof PinotHelixResourceManager.TableAlreadyExistsException) {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
      } else {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
  }

  /**
   * Deletes the {@link TableConfigs} by deleting the schema tableName, the offline table config for tableName_OFFLINE and
   * the realtime table config for tableName_REALTIME
   */
  @DELETE
  @Path("/tableConfigs/{tableName}")
  @Authenticate(AccessType.DELETE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Delete the TableConfigs", notes = "Delete the TableConfigs")
  public SuccessResponse deleteConfig(
      @ApiParam(value = "TableConfigs name i.e. raw table name", required = true) @PathParam("tableName") String tableName) {

    try {
      _pinotHelixResourceManager.deleteRealtimeTable(tableName);
      LOGGER.info("Deleted realtime table: {}", tableName);
      _pinotHelixResourceManager.deleteOfflineTable(tableName);
      LOGGER.info("Deleted offline table: {}", tableName);
      Schema schema = _pinotHelixResourceManager.getSchema(tableName);
      if (schema != null) {
        _pinotHelixResourceManager.deleteSchema(schema);
      }
      LOGGER.info("Deleted schema: {}", tableName);
      return new SuccessResponse("Deleted TableConfigs: " + tableName);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Updated the {@link TableConfigs} by updating the schema tableName,
   * then updating the offline tableConfig or creating a new one if it doesn't already exist in the cluster,
   * then updating the realtime tableConfig or creating a new one if it doesn't already exist in the cluster.
   */
  @PUT
  @Path("/tableConfigs/{tableName}")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the TableConfigs provided by the tableConfigsStr json", notes = "Update the TableConfigs provided by the tableConfigsStr json")
  public SuccessResponse updateConfig(
      @ApiParam(value = "TableConfigs name i.e. raw table name", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Reload the table if the new schema is backward compatible") @DefaultValue("false") @QueryParam("reload") boolean reload,
      String tableConfigsStr)
      throws Exception {
    TableConfigs tableConfigs;
    try {
      tableConfigs = JsonUtils.stringToObject(tableConfigsStr, TableConfigs.class);
      Preconditions.checkState(tableConfigs.getTableName().equals(tableName),
          "'tableName' in TableConfigs: %s must match provided tableName: %s", tableConfigs.getTableName(), tableName);
      validateConfig(tableConfigs);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, String.format("Invalid TableConfigs: %s", tableName),
          Response.Status.BAD_REQUEST, e);
    }

    TableConfig offlineTableConfig = tableConfigs.getOffline();
    TableConfig realtimeTableConfig = tableConfigs.getRealtime();
    Schema schema = tableConfigs.getSchema();

    try {
      _pinotHelixResourceManager.updateSchema(schema, reload);
      LOGGER.info("Updated schema: {}", tableName);

      if (offlineTableConfig != null) {
        tuneConfig(offlineTableConfig, schema);
        if (_pinotHelixResourceManager.hasOfflineTable(tableName)) {
          _pinotHelixResourceManager.updateTableConfig(offlineTableConfig);
          LOGGER.info("Updated offline table config: {}", tableName);
        } else {
          _pinotHelixResourceManager.addTable(offlineTableConfig);
          LOGGER.info("Created offline table config: {}", tableName);
        }
        if (realtimeTableConfig != null) {
          tuneConfig(realtimeTableConfig, schema);
          if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
            _pinotHelixResourceManager.updateTableConfig(realtimeTableConfig);
            LOGGER.info("Updated realtime table config: {}", tableName);
          } else {
            _pinotHelixResourceManager.addTable(realtimeTableConfig);
            LOGGER.info("Created realtime table config: {}", tableName);
          }
        }
      }
    } catch (PinotHelixResourceManager.InvalidTableConfigException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid TableConfigs for: %s, %s", tableName, e.getMessage()), Response.Status.BAD_REQUEST, e);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to update TableConfigs for: %s, %s", tableName, e.getMessage()),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }

    return new SuccessResponse("TableConfigs updated for " + tableName);
  }

  /**
   * Validates the {@link TableConfigs} as provided in the tableConfigsStr json, by validating the schema,
   * the realtime table config and the offline table config
   */
  @POST
  @Path("/tableConfigs/validate")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validate the TableConfigs", notes = "Validate the TableConfigs")
  public String validateConfig(String tableConfigsStr) {
    TableConfigs tableConfigs;
    try {
      tableConfigs = JsonUtils.stringToObject(tableConfigsStr, TableConfigs.class);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, String.format("Invalid TableConfigs json string: %s", tableConfigsStr),
          Response.Status.BAD_REQUEST, e);
    }
    return validateConfig(tableConfigs);
  }

  private void tuneConfig(TableConfig tableConfig, Schema schema) {
    TableConfigUtils.applyTunerConfig(tableConfig, schema);
    TableConfigUtils.ensureMinReplicas(tableConfig, _controllerConf.getDefaultTableMinReplicas());
    TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, _controllerConf.getDimTableMaxSize());
  }

  private String validateConfig(TableConfigs tableConfigs) {
    String tableName = tableConfigs.getTableName();
    TableConfig offlineTableConfig = tableConfigs.getOffline();
    TableConfig realtimeTableConfig = tableConfigs.getRealtime();
    Schema schema = tableConfigs.getSchema();
    try {
      Preconditions.checkState(offlineTableConfig != null || realtimeTableConfig != null,
          "Must provide at least one of 'realtime' or 'offline' table configs for adding TableConfigs: %s", tableName);
      Preconditions.checkState(schema != null, "Must provide 'schema' for adding TableConfigs: %s", tableName);
      Preconditions.checkState(!tableName.isEmpty(), "'tableName' cannot be empty in TableConfigs");

      Preconditions
          .checkState(tableName.equals(schema.getSchemaName()), "'tableName': %s must be equal to 'schemaName' from 'schema': %s",
              tableName, schema.getSchemaName());
      SchemaUtils.validate(schema);

      if (offlineTableConfig != null) {
        String rawTableName = TableNameBuilder.extractRawTableName(offlineTableConfig.getTableName());
        Preconditions.checkState(rawTableName.equals(tableName),
            "Name in 'offline' table config: %s must be equal to 'tableName': %s", rawTableName, tableName);
        TableConfigUtils.validateTableName(offlineTableConfig);
        TableConfigUtils.validate(offlineTableConfig, schema);
      }
      if (realtimeTableConfig != null) {
        String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableConfig.getTableName());
        Preconditions.checkState(rawTableName.equals(tableName),
            "Name in 'realtime' table config: %s must be equal to 'tableName': %s", rawTableName, tableName);
        TableConfigUtils.validateTableName(realtimeTableConfig);
        TableConfigUtils.validate(realtimeTableConfig, schema);
      }
      TableConfigUtils.verifyHybridTableConfigs(tableName, offlineTableConfig, realtimeTableConfig);

      return tableConfigs.toPrettyJsonString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid TableConfigs: %s. %s", tableName, e.getMessage()), Response.Status.BAD_REQUEST, e);
    }
  }

  private void validatePermissions(String resourceName, AccessType accessType, HttpHeaders httpHeaders,
      String endpointUrl) {
    _accessControlUtils
        .validatePermission(resourceName, accessType, httpHeaders, endpointUrl, _accessControlFactory.create());
  }
}
