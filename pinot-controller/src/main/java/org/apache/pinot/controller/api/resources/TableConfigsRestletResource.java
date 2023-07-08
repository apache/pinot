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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.InvalidTableConfigException;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.tuner.TableConfigTunerUtils;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.RBACAuthorization;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.TableConfigs;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * Endpoints for CRUD of {@link TableConfigs}.
 * {@link TableConfigs} is a group of the offline table config, realtime table config and schema for the same tableName.
 */
@Api(tags = Constants.TABLE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class TableConfigsRestletResource {

  public static final Logger LOGGER = LoggerFactory.getLogger(TableConfigsRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  AccessControlFactory _accessControlFactory;

  /**
   * List all {@link TableConfigs}, where each is a group of the offline table config, realtime table config and
   * schema for the same tableName.
   * This is equivalent to a list of all raw table names
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tableConfigs")
  @RBACAuthorization(targetType = "cluster", permission = "GetTableConfigs")
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
   * Gets the {@link TableConfigs} for the provided raw tableName, by fetching the offline table config for
   * tableName_OFFLINE,
   * realtime table config for tableName_REALTIME and schema for tableName
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tableConfigs/{tableName}")
  @RBACAuthorization(targetType = "table", targetId = "tableName", permission = "GetConfig")
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Get the TableConfigs for a given raw tableName",
      notes = "Get the TableConfigs for a given raw tableName")
  public String getConfig(
      @ApiParam(value = "Raw table name", required = true) @PathParam("tableName") String tableName) {

    try {
      Schema schema = _pinotHelixResourceManager.getTableSchema(tableName);
      TableConfig offlineTableConfig = _pinotHelixResourceManager.getOfflineTableConfig(tableName);
      TableConfig realtimeTableConfig = _pinotHelixResourceManager.getRealtimeTableConfig(tableName);
      TableConfigs config = new TableConfigs(tableName, schema, offlineTableConfig, realtimeTableConfig);
      return config.toJsonString();
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
  @ApiOperation(value = "Add the TableConfigs using the tableConfigsStr json",
      notes = "Add the TableConfigs using the tableConfigsStr json")
  @ManualAuthorization // performed after parsing table configs
  public ConfigSuccessResponse addConfig(
      String tableConfigsStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    Pair<TableConfigs, Map<String, Object>> tableConfigsAndUnrecognizedProps;
    TableConfigs tableConfigs;
    try {
      tableConfigsAndUnrecognizedProps =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigsStr, TableConfigs.class);
      tableConfigs = tableConfigsAndUnrecognizedProps.getLeft();
      validateConfig(tableConfigs, typesToSkip);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, String.format("Invalid TableConfigs. %s", e.getMessage()),
          Response.Status.BAD_REQUEST, e);
    }

    String rawTableName = tableConfigs.getTableName();
    if (_pinotHelixResourceManager.hasOfflineTable(rawTableName) || _pinotHelixResourceManager
        .hasRealtimeTable(rawTableName) || _pinotHelixResourceManager.getSchema(rawTableName) != null) {
      throw new ControllerApplicationException(LOGGER,
          String.format("TableConfigs: %s already exists. Use PUT to update existing config", rawTableName),
          Response.Status.BAD_REQUEST);
    }

    // validate permission
    TableConfig offlineTableConfig = tableConfigs.getOffline();
    TableConfig realtimeTableConfig = tableConfigs.getRealtime();
    Schema schema = tableConfigs.getSchema();

    try {
      String endpointUrl = request.getRequestURL().toString();
      AccessControl accessControl = _accessControlFactory.create();
      AccessControlUtils
          .validatePermission(schema.getSchemaName(), AccessType.CREATE, httpHeaders, endpointUrl, accessControl);

      if (offlineTableConfig != null) {
        tuneConfig(offlineTableConfig, schema);
        AccessControlUtils
            .validatePermission(offlineTableConfig.getTableName(), AccessType.CREATE, httpHeaders, endpointUrl,
                accessControl);
      }
      if (realtimeTableConfig != null) {
        tuneConfig(realtimeTableConfig, schema);
        AccessControlUtils
            .validatePermission(realtimeTableConfig.getTableName(), AccessType.CREATE, httpHeaders, endpointUrl,
                accessControl);
      }

      if(!accessControl.hasRBACAccess(httpHeaders, "table", schema.getSchemaName(), "AddConfigs")) {
        throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
      }

      try {
        _pinotHelixResourceManager.addSchema(schema, false, false);
        LOGGER.info("Added schema: {}", schema.getSchemaName());
        if (offlineTableConfig != null) {
          _pinotHelixResourceManager.addTable(offlineTableConfig);
          LOGGER.info("Added offline table config: {}", offlineTableConfig.getTableName());
        }
        if (realtimeTableConfig != null) {
          _pinotHelixResourceManager.addTable(realtimeTableConfig);
          LOGGER.info("Added realtime table config: {}", realtimeTableConfig.getTableName());
        }
      } catch (Exception e) {
        // In case of exception when adding any of the above configs, revert all configs added
        // Invoke delete on tables whether they exist or not, to account for metadata/segments etc.
        _pinotHelixResourceManager.deleteRealtimeTable(rawTableName);
        _pinotHelixResourceManager.deleteOfflineTable(rawTableName);
        _pinotHelixResourceManager.deleteSchema(schema.getSchemaName());
        throw e;
      }

      return new ConfigSuccessResponse("TableConfigs " + tableConfigs.getTableName() + " successfully added",
          tableConfigsAndUnrecognizedProps.getRight());
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
      if (e instanceof InvalidTableConfigException) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Invalid TableConfigs: %s", tableConfigs.getTableName()), Response.Status.BAD_REQUEST, e);
      } else if (e instanceof TableAlreadyExistsException) {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
      } else {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
  }

  /**
   * Deletes the {@link TableConfigs} by deleting the schema tableName, the offline table config for
   * tableName_OFFLINE and
   * the realtime table config for tableName_REALTIME
   */
  @DELETE
  @Path("/tableConfigs/{tableName}")
  @RBACAuthorization(targetType = "table", targetId = "tableName", permission = "DeleteTable")
  @Authenticate(AccessType.DELETE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Delete the TableConfigs", notes = "Delete the TableConfigs")
  public SuccessResponse deleteConfig(
      @ApiParam(value = "TableConfigs name i.e. raw table name", required = true) @PathParam("tableName")
          String tableName) {

    try {
      boolean tableExists = false;
      if (_pinotHelixResourceManager.hasRealtimeTable(tableName) || _pinotHelixResourceManager
          .hasOfflineTable(tableName)) {
        tableExists = true;
      }
      // Delete whether tables exist or not
      _pinotHelixResourceManager.deleteRealtimeTable(tableName);
      LOGGER.info("Deleted realtime table: {}", tableName);
      _pinotHelixResourceManager.deleteOfflineTable(tableName);
      LOGGER.info("Deleted offline table: {}", tableName);
      boolean schemaExists = _pinotHelixResourceManager.deleteSchema(tableName);
      LOGGER.info("Deleted schema: {}", tableName);
      if (tableExists || schemaExists) {
        return new SuccessResponse("Deleted TableConfigs: " + tableName);
      } else {
        return new SuccessResponse(
            "TableConfigs: " + tableName + " don't exist. Invoked delete anyway to clean stale metadata/segments");
      }
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Updated the {@link TableConfigs} by updating the schema tableName,
   * then updating the offline tableConfig or creating a new one if it doesn't already exist in the cluster,
   * then updating the realtime tableConfig or creating a new one if it doesn't already exist in the cluster.
   *
   * The option to skip table config validation (validationTypesToSkip) and force update the table schema
   * (forceTableSchemaUpdate) are provided for testing purposes and should be used with caution.
   */
  @PUT
  @Path("/tableConfigs/{tableName}")
  @RBACAuthorization(targetType = "table", targetId = "tableName", permission = "UpdateConfigs")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the TableConfigs provided by the tableConfigsStr json",
      notes = "Update the TableConfigs provided by the tableConfigsStr json")
  public ConfigSuccessResponse updateConfig(
      @ApiParam(value = "TableConfigs name i.e. raw table name", required = true) @PathParam("tableName")
          String tableName,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip,
      @ApiParam(value = "Reload the table if the new schema is backward compatible") @DefaultValue("false")
      @QueryParam("reload") boolean reload,
      @ApiParam(value = "Force update the table schema") @DefaultValue("false")
      @QueryParam("forceTableSchemaUpdate") boolean forceTableSchemaUpdate,
      String tableConfigsStr) throws Exception {
    Pair<TableConfigs, Map<String, Object>> tableConfigsAndUnrecognizedProps;
    TableConfigs tableConfigs;
    try {
      tableConfigsAndUnrecognizedProps =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigsStr, TableConfigs.class);
      tableConfigs = tableConfigsAndUnrecognizedProps.getLeft();
      Preconditions.checkState(tableConfigs.getTableName().equals(tableName),
          "'tableName' in TableConfigs: %s must match provided tableName: %s", tableConfigs.getTableName(), tableName);

      validateConfig(tableConfigs, typesToSkip);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, String.format("Invalid TableConfigs: %s", tableName),
          Response.Status.BAD_REQUEST, e);
    }

    if (!_pinotHelixResourceManager.hasOfflineTable(tableName) && !_pinotHelixResourceManager
        .hasRealtimeTable(tableName)) {
      throw new ControllerApplicationException(LOGGER,
          String.format("TableConfigs: %s does not exist. Use POST to create it first.", tableName),
          Response.Status.BAD_REQUEST);
    }

    TableConfig offlineTableConfig = tableConfigs.getOffline();
    TableConfig realtimeTableConfig = tableConfigs.getRealtime();
    Schema schema = tableConfigs.getSchema();

    try {
      _pinotHelixResourceManager.updateSchema(schema, reload, forceTableSchemaUpdate);
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
    } catch (InvalidTableConfigException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid TableConfigs for: %s, %s", tableName, e.getMessage()), Response.Status.BAD_REQUEST, e);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to update TableConfigs for: %s, %s", tableName, e.getMessage()),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }

    return new ConfigSuccessResponse("TableConfigs updated for " + tableName,
        tableConfigsAndUnrecognizedProps.getRight());
  }

  /**
   * Validates the {@link TableConfigs} as provided in the tableConfigsStr json, by validating the schema,
   * the realtime table config and the offline table config
   */
  @POST
  @Path("/tableConfigs/validate")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validate the TableConfigs", notes = "Validate the TableConfigs")
  @ManualAuthorization // performed after parsing TableConfigs
  public String validateConfig(String tableConfigsStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    Pair<TableConfigs, Map<String, Object>> tableConfigsAndUnrecognizedProps;
    TableConfigs tableConfigs;
    try {
      tableConfigsAndUnrecognizedProps =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigsStr, TableConfigs.class);
      tableConfigs = tableConfigsAndUnrecognizedProps.getLeft();
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid TableConfigs json string: %s", tableConfigsStr), Response.Status.BAD_REQUEST, e);
    }

    // validate permission
    String endpointUrl = request.getRequestURL().toString();
    AccessControl accessControl = _accessControlFactory.create();
    Schema schema = tableConfigs.getSchema();
    TableConfig offlineTableConfig = tableConfigs.getOffline();
    TableConfig realtimeTableConfig = tableConfigs.getRealtime();

    AccessControlUtils
        .validatePermission(schema.getSchemaName(), AccessType.READ, httpHeaders, endpointUrl, accessControl);

    if (offlineTableConfig != null) {
      tuneConfig(offlineTableConfig, schema);
      AccessControlUtils
          .validatePermission(offlineTableConfig.getTableName(), AccessType.READ, httpHeaders, endpointUrl,
              accessControl);
    }
    if (realtimeTableConfig != null) {
      tuneConfig(realtimeTableConfig, schema);
      AccessControlUtils
          .validatePermission(realtimeTableConfig.getTableName(), AccessType.READ, httpHeaders, endpointUrl,
              accessControl);
    }

    if(!accessControl.hasRBACAccess(httpHeaders, "table", schema.getSchemaName(),
        "ValidateConfigs")) {
      throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
    }

    TableConfigs validatedTableConfigs = validateConfig(tableConfigs, typesToSkip);
    ObjectNode response = JsonUtils.objectToJsonNode(validatedTableConfigs).deepCopy();
    response.set("unrecognizedProperties", JsonUtils.objectToJsonNode(tableConfigsAndUnrecognizedProps.getRight()));
    return response.toString();
  }

  private void tuneConfig(TableConfig tableConfig, Schema schema) {
    TableConfigTunerUtils.applyTunerConfigs(_pinotHelixResourceManager, tableConfig, schema, Collections.emptyMap());
    TableConfigUtils.ensureMinReplicas(tableConfig, _controllerConf.getDefaultTableMinReplicas());
    TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, _controllerConf.getDimTableMaxSize());
  }

  private TableConfigs validateConfig(TableConfigs tableConfigs, @Nullable String typesToSkip) {
    String rawTableName = tableConfigs.getTableName();
    TableConfig offlineTableConfig = tableConfigs.getOffline();
    TableConfig realtimeTableConfig = tableConfigs.getRealtime();
    Schema schema = tableConfigs.getSchema();
    try {
      Preconditions.checkState(offlineTableConfig != null || realtimeTableConfig != null,
          "Must provide at least one of 'realtime' or 'offline' table configs for adding TableConfigs: %s",
          rawTableName);
      Preconditions.checkState(schema != null, "Must provide 'schema' for adding TableConfigs: %s", rawTableName);
      Preconditions.checkState(!rawTableName.isEmpty(), "'tableName' cannot be empty in TableConfigs");

      Preconditions.checkState(rawTableName.equals(schema.getSchemaName()),
          "'tableName': %s must be equal to 'schemaName' from 'schema': %s", rawTableName, schema.getSchemaName());
      SchemaUtils.validate(schema);
      boolean allowTableNameWithDatabase = _controllerConf.getProperty(
          CommonConstants.Helix.ALLOW_TABLE_NAME_WITH_DATABASE,
          CommonConstants.Helix.DEFAULT_ALLOW_TABLE_NAME_WITH_DATABASE);
      if (offlineTableConfig != null) {
        String offlineRawTableName = TableNameBuilder.extractRawTableName(offlineTableConfig.getTableName());
        Preconditions.checkState(offlineRawTableName.equals(rawTableName),
            "Name in 'offline' table config: %s must be equal to 'tableName': %s", offlineRawTableName, rawTableName);
        TableConfigUtils.validateTableName(offlineTableConfig, allowTableNameWithDatabase);
        TableConfigUtils.validate(offlineTableConfig, schema, typesToSkip, _controllerConf.isDisableIngestionGroovy());
      }
      if (realtimeTableConfig != null) {
        String realtimeRawTableName = TableNameBuilder.extractRawTableName(realtimeTableConfig.getTableName());
        Preconditions.checkState(realtimeRawTableName.equals(rawTableName),
            "Name in 'realtime' table config: %s must be equal to 'tableName': %s", realtimeRawTableName, rawTableName);
        TableConfigUtils.validateTableName(realtimeTableConfig, allowTableNameWithDatabase);
        TableConfigUtils.validate(realtimeTableConfig, schema, typesToSkip, _controllerConf.isDisableIngestionGroovy());
      }
      if (offlineTableConfig != null && realtimeTableConfig != null) {
        TableConfigUtils.verifyHybridTableConfigs(rawTableName, offlineTableConfig, realtimeTableConfig);
      }

      return tableConfigs;
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid TableConfigs: %s. %s", rawTableName, e.getMessage()), Response.Status.BAD_REQUEST, e);
    }
  }
}
