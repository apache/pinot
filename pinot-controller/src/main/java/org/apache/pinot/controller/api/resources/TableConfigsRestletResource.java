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

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
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
import org.apache.pinot.common.exception.TableConfigBackwardIncompatibleException;
import org.apache.pinot.common.exception.TableConfigVersionMismatchException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.LogicalTableConfigUtils;
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
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.tuner.TableConfigTunerUtils;
import org.apache.pinot.controller.util.TaskConfigUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.TableConfigs;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableConfigRedactionUtils;
import org.apache.pinot.spi.config.table.TableConfigValidatorRegistry;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/// Endpoints for CRUD of [TableConfigs].
/// [TableConfigs] is a group of the offline table config, realtime table config and schema for the same tableName.
@Api(tags = Constants.TABLE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class TableConfigsRestletResource {

  public static final Logger LOGGER = LoggerFactory.getLogger(TableConfigsRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @Inject
  PinotTaskManager _pinotTaskManager;

  @Inject
  ControllerConf _controllerConf;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  AccessControlFactory _accessControlFactory;

  /// List all [TableConfigs] in database provided in header, where each is a group of the offline table config,
  /// realtime table config and schema for the same tableName.
  /// This is equivalent to a list of all raw table names in provided database
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tableConfigs")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TABLE_CONFIG)
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Lists all TableConfigs in cluster", notes = "Lists all TableConfigs in cluster")
  public String listConfigs(@Context HttpHeaders headers) {
    String databaseName = headers.getHeaderString(DATABASE);
    try {
      List<String> rawTableNames = _pinotHelixResourceManager.getAllRawTables(databaseName);
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

  /// Gets the [TableConfigs] for the provided raw tableName, by fetching the offline table config for
  /// tableName_OFFLINE,
  /// realtime table config for tableName_REALTIME and schema for tableName
  @GET
  @Produces({MediaType.APPLICATION_JSON, RedactedTableConfigsResponse.MEDIA_TYPE})
  @Path("/tableConfigs/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_TABLE_CONFIGS)
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Get the TableConfigs for a given raw tableName",
      notes = "Returns a versioned redacted response. Submit the complete response to PUT when retaining markers.")
  public String getConfig(
      @ApiParam(value = "Raw table name", required = true) @PathParam("tableName") String tableName,
      @Context HttpHeaders headers) {
    try {
      tableName = DatabaseUtils.translateTableName(tableName, headers);
      Pair<Schema, Integer> schemaWithVersion = _pinotHelixResourceManager.getSchemaWithVersion(tableName);
      if (schemaWithVersion == null) {
        throw new NotFoundException(
            String.format("Schema does not exist for table %s Use POST to create it first.", tableName));
      }
      Schema schema = schemaWithVersion.getLeft();
      Pair<TableConfig, Integer> offlineTableConfigWithVersion =
          _pinotHelixResourceManager.getTableConfigWithVersion(
              TableNameBuilder.OFFLINE.tableNameWithType(tableName), false, false);
      Pair<TableConfig, Integer> realtimeTableConfigWithVersion =
          _pinotHelixResourceManager.getTableConfigWithVersion(
              TableNameBuilder.REALTIME.tableNameWithType(tableName), false, false);
      TableConfig offlineTableConfig =
          offlineTableConfigWithVersion != null ? offlineTableConfigWithVersion.getLeft() : null;
      TableConfig realtimeTableConfig =
          realtimeTableConfigWithVersion != null ? realtimeTableConfigWithVersion.getLeft() : null;
      Map<String, Integer> baseVersions = new LinkedHashMap<>();
      baseVersions.put(RedactedTableConfigsResponse.SCHEMA_VERSION_KEY, schemaWithVersion.getRight());
      if (offlineTableConfigWithVersion != null) {
        baseVersions.put(RedactedTableConfigsResponse.OFFLINE_VERSION_KEY, offlineTableConfigWithVersion.getRight());
      } else {
        baseVersions.put(RedactedTableConfigsResponse.OFFLINE_VERSION_KEY,
            RedactedTableConfigsResponse.ABSENT_VERSION);
      }
      if (realtimeTableConfigWithVersion != null) {
        baseVersions.put(RedactedTableConfigsResponse.REALTIME_VERSION_KEY,
            realtimeTableConfigWithVersion.getRight());
      } else {
        baseVersions.put(RedactedTableConfigsResponse.REALTIME_VERSION_KEY,
            RedactedTableConfigsResponse.ABSENT_VERSION);
      }
      TableConfigs config = new TableConfigs(tableName, schema,
          offlineTableConfig != null ? TableConfigRedactionUtils.redact(offlineTableConfig) : null,
          realtimeTableConfig != null ? TableConfigRedactionUtils.redact(realtimeTableConfig) : null);
      return JsonUtils.objectToString(new RedactedTableConfigsResponse(config, baseVersions));
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to retrieve TableConfigs",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /// Creates a [TableConfigs] using the `tableConfigsStr`, by creating the schema,
  /// followed by the realtime tableConfig and offline tableConfig as applicable, from the [TableConfigs].
  /// Validates the configs before applying.
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tableConfigs")
  @ApiOperation(value = "Add the TableConfigs using the tableConfigsStr json",
      notes = "Add the TableConfigs using the tableConfigsStr json")
  @ManualAuthorization // performed after parsing table configs
  public ConfigSuccessResponse addConfig(
      String tableConfigsStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip,
      @DefaultValue("false") @QueryParam("ignoreActiveTasks") boolean ignoreActiveTasks,
      @Context HttpHeaders httpHeaders, @Context Request request)
      throws Exception {
    Pair<TableConfigs, Map<String, Object>> tableConfigsAndUnrecognizedProps;
    try {
      tableConfigsAndUnrecognizedProps =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigsStr, TableConfigs.class);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, "Invalid TableConfigs JSON", Response.Status.BAD_REQUEST);
    }
    TableConfigs tableConfigs = tableConfigsAndUnrecognizedProps.getLeft();
    TableConfig offlineTableConfig = tableConfigs.getOffline();
    TableConfig realtimeTableConfig = tableConfigs.getRealtime();
    try {
      if (offlineTableConfig != null) {
        offlineTableConfig = TableConfigRedactionUtils.restoreRedactedValues(offlineTableConfig, null);
      }
      if (realtimeTableConfig != null) {
        realtimeTableConfig = TableConfigRedactionUtils.restoreRedactedValues(realtimeTableConfig, null);
      }
    } catch (IllegalArgumentException e) {
      throw new ControllerApplicationException(LOGGER, "Invalid TableConfigs", Response.Status.BAD_REQUEST);
    }
    tableConfigs = new TableConfigs(tableConfigs.getTableName(), tableConfigs.getSchema(), offlineTableConfig,
        realtimeTableConfig);
    String databaseName = DatabaseUtils.extractDatabaseFromHttpHeaders(httpHeaders);
    String rawTableName = DatabaseUtils.translateTableName(tableConfigs.getTableName(), databaseName);
    if (_pinotHelixResourceManager.hasOfflineTable(rawTableName) || _pinotHelixResourceManager.hasRealtimeTable(
        rawTableName) || _pinotHelixResourceManager.getSchema(rawTableName) != null) {
      throw new ControllerApplicationException(LOGGER,
          String.format("TableConfigs: %s already exists. Use PUT to update existing config", rawTableName),
          Response.Status.BAD_REQUEST);
    }

    validateConfig(tableConfigs, databaseName, typesToSkip);
    tableConfigs.setTableName(rawTableName);

    offlineTableConfig = tableConfigs.getOffline();
    realtimeTableConfig = tableConfigs.getRealtime();
    Schema schema = tableConfigs.getSchema();

    try {
      // validate permission
      String endpointUrl = request.getRequestURL().toString();
      AccessControl accessControl = _accessControlFactory.create();
      AccessControlUtils.validatePermission(rawTableName, AccessType.CREATE, httpHeaders, endpointUrl,
          accessControl);
      if (!accessControl.hasAccess(httpHeaders, TargetType.TABLE, rawTableName, Actions.Table.CREATE_TABLE)) {
        throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
      }

      if (offlineTableConfig != null) {
        applyTuning(offlineTableConfig, schema);
        if (!ignoreActiveTasks) {
          PinotTableRestletResource.tableTasksValidation(offlineTableConfig, _pinotHelixTaskResourceManager);
        }
      }
      if (realtimeTableConfig != null) {
        applyTuning(realtimeTableConfig, schema);
        if (!ignoreActiveTasks) {
          PinotTableRestletResource.tableTasksValidation(realtimeTableConfig, _pinotHelixTaskResourceManager);
        }
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

      return new ConfigSuccessResponse("TableConfigs " + rawTableName + " successfully added",
          tableConfigsAndUnrecognizedProps.getRight());
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
      if (e instanceof InvalidTableConfigException) {
        throw new ControllerApplicationException(LOGGER,
            redactedTableConfigsError("Invalid TableConfigs: " + rawTableName, e, tableConfigs),
            Response.Status.BAD_REQUEST);
      } else if (e instanceof TableAlreadyExistsException) {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
      } else if (e instanceof ControllerApplicationException) {
        throw e;
      } else {
        throw new ControllerApplicationException(LOGGER, "Failed to add TableConfigs: " + rawTableName,
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    }
  }

  /// Deletes the [TableConfigs] by deleting the schema tableName, the offline table config for
  /// tableName_OFFLINE and
  /// the realtime table config for tableName_REALTIME
  @DELETE
  @Path("/tableConfigs/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.DELETE_TABLE)
  @Authenticate(AccessType.DELETE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Delete the TableConfigs", notes = "Delete the TableConfigs")
  public SuccessResponse deleteConfig(
      @ApiParam(value = "TableConfigs name i.e. raw table name", required = true) @PathParam("tableName")
      String tableName,
      @DefaultValue("false") @QueryParam("ignoreActiveTasks") boolean ignoreActiveTasks,
      @Context HttpHeaders headers) {
    try {
      if (TableNameBuilder.isOfflineTableResource(tableName) || TableNameBuilder.isRealtimeTableResource(tableName)) {
        throw new ControllerApplicationException(LOGGER, "Invalid table name: " + tableName + ". Use raw table name.",
            Response.Status.BAD_REQUEST);
      }

      tableName = DatabaseUtils.translateTableName(tableName, headers);

      // Validate the table is not referenced in any logical table config.
      List<LogicalTableConfig> allLogicalTableConfigs =
          ZKMetadataProvider.getAllLogicalTableConfigs(_pinotHelixResourceManager.getPropertyStore());
      for (LogicalTableConfig logicalTableConfig : allLogicalTableConfigs) {
        if (LogicalTableConfigUtils.checkPhysicalTableRefExists(logicalTableConfig, tableName)) {
          throw new ControllerApplicationException(LOGGER,
              String.format("Cannot delete table config: %s because it is referenced in logical table: %s",
                  tableName, logicalTableConfig.getTableName()), Response.Status.CONFLICT);
        }
      }

      boolean tableExists =
          _pinotHelixResourceManager.hasRealtimeTable(tableName) || _pinotHelixResourceManager.hasOfflineTable(
              tableName);
      PinotTableRestletResource.tableTasksCleanup(TableNameBuilder.REALTIME.tableNameWithType(tableName),
          ignoreActiveTasks, _pinotHelixResourceManager, _pinotHelixTaskResourceManager);
      // Delete whether tables exist or not
      _pinotHelixResourceManager.deleteRealtimeTable(tableName);
      LOGGER.info("Deleted realtime table: {}", tableName);
      PinotTableRestletResource.tableTasksCleanup(TableNameBuilder.OFFLINE.tableNameWithType(tableName),
          ignoreActiveTasks, _pinotHelixResourceManager, _pinotHelixTaskResourceManager);
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

  /// Updated the [TableConfigs] by updating the schema tableName,
  /// then updating the offline tableConfig or creating a new one if it doesn't already exist in the cluster,
  /// then updating the realtime tableConfig or creating a new one if it doesn't already exist in the cluster.
  ///
  /// The option to skip table config validation (validationTypesToSkip) and force update the table schema
  /// (forceTableSchemaUpdate) are provided for testing purposes and should be used with caution.
  @PUT
  @Path("/tableConfigs/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.UPDATE_TABLE_CONFIGS)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the TableConfigs provided by the tableConfigsStr json")
  public ConfigSuccessResponse updateConfig(
      @ApiParam(value = "TableConfigs name i.e. raw table name", required = true) @PathParam("tableName")
      String tableName,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip,
      @ApiParam(value = "Reload the table if the new schema is backward compatible") @DefaultValue("false")
      @QueryParam("reload") boolean reload,
      @ApiParam(value = "Force update the table schema") @DefaultValue("false") @QueryParam("forceTableSchemaUpdate")
      boolean forceTableSchemaUpdate, String tableConfigsStr, @Context HttpHeaders headers)
      throws Exception {
    String databaseName = DatabaseUtils.extractDatabaseFromHttpHeaders(headers);
    tableName = DatabaseUtils.translateTableName(tableName, databaseName);
    ParsedTableConfigsUpdate parsedUpdate;
    TableConfigs tableConfigs;
    Pair<Schema, Integer> storedSchemaWithVersion;
    Map<TableType, TableConfig> storedTableConfigs = new LinkedHashMap<>();
    Map<TableType, Integer> expectedVersions = new LinkedHashMap<>();
    TableType typeToCreate = null;
    try {
      parsedUpdate = parseTableConfigsUpdate(tableConfigsStr);
      tableConfigs = parsedUpdate._tableConfigs;
      Preconditions.checkState(
          DatabaseUtils.translateTableName(tableConfigs.getTableName(), databaseName).equals(tableName),
          "'tableName' in TableConfigs: %s must match provided tableName: %s", tableConfigs.getTableName(), tableName);
      tableConfigs.setTableName(tableName);
      TableConfig offlineTableConfig = tableConfigs.getOffline();
      TableConfig realtimeTableConfig = tableConfigs.getRealtime();
      storedSchemaWithVersion = _pinotHelixResourceManager.getSchemaWithVersion(tableName);
      if (storedSchemaWithVersion == null) {
        throw staleTableConfigsResponse();
      }
      if (parsedUpdate._envelope
          && !parsedUpdate._baseVersions.get(RedactedTableConfigsResponse.SCHEMA_VERSION_KEY)
          .equals(storedSchemaWithVersion.getRight())) {
        throw staleTableConfigsResponse();
      }

      Pair<TableConfig, Integer> storedOfflineTableConfigWithVersion =
          _pinotHelixResourceManager.getTableConfigWithVersion(
              TableNameBuilder.OFFLINE.tableNameWithType(tableName), false, false);
      Pair<TableConfig, Integer> storedRealtimeTableConfigWithVersion =
          _pinotHelixResourceManager.getTableConfigWithVersion(
              TableNameBuilder.REALTIME.tableNameWithType(tableName), false, false);
      typeToCreate = compareTableTypeSnapshot(TableType.OFFLINE, offlineTableConfig,
          storedOfflineTableConfigWithVersion, parsedUpdate, storedTableConfigs, expectedVersions, typeToCreate);
      typeToCreate = compareTableTypeSnapshot(TableType.REALTIME, realtimeTableConfig,
          storedRealtimeTableConfigWithVersion, parsedUpdate, storedTableConfigs, expectedVersions, typeToCreate);

      TableConfig storedOfflineTableConfig = storedTableConfigs.get(TableType.OFFLINE);
      TableConfig storedRealtimeTableConfig = storedTableConfigs.get(TableType.REALTIME);
      if (offlineTableConfig != null) {
        offlineTableConfig =
            TableConfigRedactionUtils.restoreRedactedValues(offlineTableConfig, storedOfflineTableConfig);
      }
      if (realtimeTableConfig != null) {
        realtimeTableConfig =
            TableConfigRedactionUtils.restoreRedactedValues(realtimeTableConfig, storedRealtimeTableConfig);
      }
      tableConfigs = new TableConfigs(tableName, tableConfigs.getSchema(), offlineTableConfig, realtimeTableConfig);
      validateConfig(tableConfigs, databaseName, typesToSkip,
          new ExistingTableConfigs(storedOfflineTableConfig, storedRealtimeTableConfig));
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, "Invalid TableConfigs JSON", Response.Status.BAD_REQUEST);
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Invalid TableConfigs: " + tableName,
          Response.Status.BAD_REQUEST);
    }

    if (!_pinotHelixResourceManager.hasOfflineTable(tableName) && !_pinotHelixResourceManager.hasRealtimeTable(
        tableName)) {
      throw new ControllerApplicationException(LOGGER,
          String.format("TableConfigs: %s does not exist. Use POST to create it first.", tableName),
          Response.Status.BAD_REQUEST);
    }

    TableConfig offlineTableConfig = tableConfigs.getOffline();
    TableConfig realtimeTableConfig = tableConfigs.getRealtime();
    Schema schema = tableConfigs.getSchema();

    try {
      Map<TableType, TableConfig> submittedTableConfigs = new LinkedHashMap<>();
      if (offlineTableConfig != null) {
        submittedTableConfigs.put(TableType.OFFLINE, offlineTableConfig);
      }
      if (realtimeTableConfig != null) {
        submittedTableConfigs.put(TableType.REALTIME, realtimeTableConfig);
      }
      if (typeToCreate != null && parsedUpdate._envelope) {
        throw new ControllerApplicationException(LOGGER,
            "A versioned TableConfigs edit cannot add a missing table type; use the table creation flow, then "
                + "retrieve a fresh TableConfigs response before editing",
            Response.Status.CONFLICT);
      } else if (typeToCreate == null) {
        for (TableConfig tableConfig : submittedTableConfigs.values()) {
          applyTuning(tableConfig, schema);
        }
        _pinotHelixResourceManager.updateTableConfigsAtomically(schema, storedSchemaWithVersion.getRight(),
            submittedTableConfigs, expectedVersions, reload, forceTableSchemaUpdate);
        LOGGER.info("Atomically updated TableConfigs: {}", tableName);
      } else {
        // Keep the legacy literal-body behavior for adding a missing type. Versioned envelopes use the snapshot-gated
        // path above so a marker-bearing edit can never race a schema or existing-type update.
        _pinotHelixResourceManager.updateSchema(schema, reload, forceTableSchemaUpdate);
        for (Map.Entry<TableType, TableConfig> entry : submittedTableConfigs.entrySet()) {
          applyTuning(entry.getValue(), schema);
          if (entry.getKey() == typeToCreate) {
            _pinotHelixResourceManager.addTable(entry.getValue());
          } else {
            _pinotHelixResourceManager.updateTableConfig(
                entry.getValue(), expectedVersions.get(entry.getKey()), forceTableSchemaUpdate);
          }
        }
      }
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (TableConfigVersionMismatchException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          "TableConfigs changed while the update was in progress; retry the request", Response.Status.CONFLICT);
    } catch (TableConfigBackwardIncompatibleException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          redactedTableConfigsError("Invalid TableConfigs for: " + tableName, e, tableConfigs),
          Response.Status.BAD_REQUEST);
    } catch (InvalidTableConfigException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          redactedTableConfigsError("Invalid TableConfigs for: " + tableName, e, tableConfigs),
          Response.Status.BAD_REQUEST);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, "Failed to update TableConfigs for: " + tableName,
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    return new ConfigSuccessResponse("TableConfigs updated for " + tableName,
        parsedUpdate._unrecognizedProperties);
  }

  @Nullable
  private static TableType compareTableTypeSnapshot(TableType tableType, @Nullable TableConfig submittedConfig,
      @Nullable Pair<TableConfig, Integer> storedConfigWithVersion, ParsedTableConfigsUpdate parsedUpdate,
      Map<TableType, TableConfig> storedConfigs, Map<TableType, Integer> expectedVersions,
      @Nullable TableType typeToCreate) {
    String versionKey = tableType == TableType.OFFLINE
        ? RedactedTableConfigsResponse.OFFLINE_VERSION_KEY
        : RedactedTableConfigsResponse.REALTIME_VERSION_KEY;
    if (parsedUpdate._envelope) {
      int baseVersion = parsedUpdate._baseVersions.get(versionKey);
      if (baseVersion == RedactedTableConfigsResponse.ABSENT_VERSION) {
        if (storedConfigWithVersion != null) {
          throw staleTableConfigsResponse();
        }
        if (submittedConfig != null) {
          Preconditions.checkState(typeToCreate == null,
              "A TableConfigs update cannot create more than one table type");
          return tableType;
        }
        return typeToCreate;
      }
      if (storedConfigWithVersion == null || baseVersion != storedConfigWithVersion.getRight()) {
        throw staleTableConfigsResponse();
      }
      storedConfigs.put(tableType, storedConfigWithVersion.getLeft());
      expectedVersions.put(tableType, baseVersion);
      return typeToCreate;
    }

    if (submittedConfig == null) {
      return typeToCreate;
    }
    if (storedConfigWithVersion == null) {
      Preconditions.checkState(typeToCreate == null,
          "A TableConfigs update cannot create more than one table type");
      return tableType;
    }
    storedConfigs.put(tableType, storedConfigWithVersion.getLeft());
    expectedVersions.put(tableType, storedConfigWithVersion.getRight());
    return typeToCreate;
  }

  private static ParsedTableConfigsUpdate parseTableConfigsUpdate(String tableConfigsString)
      throws IOException {
    JsonNode requestNode = JsonUtils.stringToJsonNode(tableConfigsString);
    if (RedactedTableConfigsResponse.isEnvelope(requestNode)) {
      Pair<RedactedTableConfigsResponse, Map<String, Object>> responseAndUnrecognizedProperties =
          JsonUtils.stringToObjectAndUnrecognizedProperties(
              tableConfigsString, RedactedTableConfigsResponse.class);
      return new ParsedTableConfigsUpdate(responseAndUnrecognizedProperties.getLeft().getConfigs(),
          responseAndUnrecognizedProperties.getLeft().getBaseVersions(),
          true, responseAndUnrecognizedProperties.getRight());
    }

    Pair<TableConfigs, Map<String, Object>> tableConfigsAndUnrecognizedProperties =
        JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigsString, TableConfigs.class);
    TableConfigs tableConfigs = tableConfigsAndUnrecognizedProperties.getLeft();
    TableConfig offline = tableConfigs.getOffline() != null
        ? TableConfigRedactionUtils.restoreRedactedValues(tableConfigs.getOffline(), null) : null;
    TableConfig realtime = tableConfigs.getRealtime() != null
        ? TableConfigRedactionUtils.restoreRedactedValues(tableConfigs.getRealtime(), null) : null;
    return new ParsedTableConfigsUpdate(
        new TableConfigs(tableConfigs.getTableName(), tableConfigs.getSchema(), offline, realtime), Map.of(),
        false, tableConfigsAndUnrecognizedProperties.getRight());
  }

  private static ControllerApplicationException staleTableConfigsResponse() {
    return new ControllerApplicationException(LOGGER,
        "TableConfigs changed after they were read; retrieve them again before updating", Response.Status.CONFLICT);
  }

  private static final class ParsedTableConfigsUpdate {
    private final TableConfigs _tableConfigs;
    private final Map<String, Integer> _baseVersions;
    private final boolean _envelope;
    private final Map<String, Object> _unrecognizedProperties;

    private ParsedTableConfigsUpdate(TableConfigs tableConfigs, Map<String, Integer> baseVersions, boolean envelope,
        Map<String, Object> unrecognizedProperties) {
      _tableConfigs = tableConfigs;
      _baseVersions = baseVersions;
      _envelope = envelope;
      _unrecognizedProperties = unrecognizedProperties;
    }
  }

  /// Validates the [TableConfigs] as provided in the tableConfigsStr json, by validating the schema,
  /// the realtime table config and the offline table config
  @POST
  @Path("/tableConfigs/validate")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validate the TableConfigs", notes = "Validate the TableConfigs")
  @ManualAuthorization // performed after parsing TableConfigs
  public String validateConfig(String tableConfigsStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: "
          + "(ALL|TASK|UPSERT|TENANT|MINION_INSTANCES)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    Pair<TableConfigs, Map<String, Object>> tableConfigsAndUnrecognizedProps =
        parseAndValidateTableConfigs(tableConfigsStr, typesToSkip, httpHeaders, request);
    TableConfigs tableConfigs = tableConfigsAndUnrecognizedProps.getLeft();
    ObjectNode response = JsonUtils.objectToJsonNode(tableConfigs).deepCopy();
    response.set("unrecognizedProperties", JsonUtils.objectToJsonNode(tableConfigsAndUnrecognizedProps.getRight()));
    return response.toString();
  }

  /// Validates and tunes the [TableConfigs] as provided in the tableConfigsStr json, by applying tuner configs,
  /// ensuring min replicas and storage quota constraints, and returns the tuned TableConfigs.
  @POST
  @Path("/tableConfigs/tune")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Tune the TableConfigs",
      notes = "Validates and applies tuning (tuner configs, min replicas, storage quota) to the TableConfigs, "
          + "returning the result that would be stored on create/update")
  @ManualAuthorization // performed after parsing TableConfigs
  public String tuneConfig(String tableConfigsStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: "
          + "(ALL|TASK|UPSERT|TENANT|MINION_INSTANCES)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    Pair<TableConfigs, Map<String, Object>> tableConfigsAndUnrecognizedProps =
        parseAndValidateTableConfigs(tableConfigsStr, typesToSkip, httpHeaders, request);
    TableConfigs tableConfigs = tableConfigsAndUnrecognizedProps.getLeft();
    Schema schema = tableConfigs.getSchema();
    if (tableConfigs.getOffline() != null) {
      applyTuning(tableConfigs.getOffline(), schema);
    }
    if (tableConfigs.getRealtime() != null) {
      applyTuning(tableConfigs.getRealtime(), schema);
    }
    ObjectNode response = JsonUtils.objectToJsonNode(tableConfigs).deepCopy();
    response.set("unrecognizedProperties", JsonUtils.objectToJsonNode(tableConfigsAndUnrecognizedProps.getRight()));
    return response.toString();
  }

  private Pair<TableConfigs, Map<String, Object>> parseAndValidateTableConfigs(String tableConfigsStr,
      @Nullable String typesToSkip, HttpHeaders httpHeaders, Request request) {
    Pair<TableConfigs, Map<String, Object>> tableConfigsAndUnrecognizedProps;
    try {
      tableConfigsAndUnrecognizedProps =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigsStr, TableConfigs.class);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, "Invalid TableConfigs JSON", Response.Status.BAD_REQUEST);
    }
    String databaseName = DatabaseUtils.extractDatabaseFromHttpHeaders(httpHeaders);
    TableConfigs tableConfigs = tableConfigsAndUnrecognizedProps.getLeft();
    validateConfig(tableConfigs, databaseName, typesToSkip);
    String rawTableName = DatabaseUtils.translateTableName(tableConfigs.getTableName(), databaseName);
    tableConfigs.setTableName(rawTableName);

    // Cluster-aware validations are exclusive to the validate/tune pre-flight endpoints so that users get fail-fast
    // feedback on tenant/minion issues without re-running them in the create/update paths (which already perform the
    // equivalent checks inline or via PinotHelixResourceManager). Active-task validation is intentionally excluded
    // here: it applies only on the create/update path (gated by the ignoreActiveTasks flag) so that validate/tune of an
    // existing table with running tasks is not blocked.
    Set<TableConfigUtils.ValidationType> skipTypes = TableConfigUtils.parseTypesToSkipString(typesToSkip);
    try {
      if (tableConfigs.getOffline() != null) {
        validateClusterAwareConfig(tableConfigs.getOffline(), skipTypes);
      }
      if (tableConfigs.getRealtime() != null) {
        validateClusterAwareConfig(tableConfigs.getRealtime(), skipTypes);
      }
    } catch (ControllerApplicationException e) {
      // Already logged by the inner constructor; let it propagate as-is.
      throw e;
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          redactedTableConfigsError("Invalid TableConfigs: " + rawTableName, e, tableConfigs),
          Response.Status.BAD_REQUEST);
    }

    // validate permission
    String endpointUrl = request.getRequestURL().toString();
    AccessControl accessControl = _accessControlFactory.create();
    AccessControlUtils.validatePermission(rawTableName, AccessType.READ, httpHeaders, endpointUrl, accessControl);
    if (!accessControl.hasAccess(httpHeaders, TargetType.TABLE, rawTableName, Actions.Table.VALIDATE_TABLE_CONFIGS)) {
      throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
    }
    return tableConfigsAndUnrecognizedProps;
  }

  private void validateClusterAwareConfig(TableConfig tableConfig, Set<TableConfigUtils.ValidationType> skipTypes) {
    if (skipTypes.contains(TableConfigUtils.ValidationType.ALL)) {
      return;
    }
    if (!skipTypes.contains(TableConfigUtils.ValidationType.TENANT)) {
      _pinotHelixResourceManager.validateTableTenantConfig(tableConfig);
    }
    if (!skipTypes.contains(TableConfigUtils.ValidationType.MINION_INSTANCES)) {
      _pinotHelixResourceManager.validateTableTaskMinionInstanceTagConfig(tableConfig);
    }
  }

  private void applyTuning(TableConfig tableConfig, Schema schema) {
    TableConfigTunerUtils.applyTunerConfigs(_pinotHelixResourceManager, tableConfig, schema, Map.of());
    TableConfigUtils.ensureMinReplicas(tableConfig, _controllerConf.getDefaultTableMinReplicas());
    TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, _controllerConf.getDimTableMaxSize());
  }

  /// Validates the provided TableConfigs. Hybrid table validation is performed only on the provided
  /// configs and does not check for conflicts with existing tables in the cluster.
  private void validateConfig(TableConfigs tableConfigs, String database, @Nullable String typesToSkip) {
    validateConfig(tableConfigs, database, typesToSkip, null);
  }

  private void validateConfig(TableConfigs tableConfigs, String database, @Nullable String typesToSkip,
      @Nullable ExistingTableConfigs existingTableConfigs) {
    String rawTableName = DatabaseUtils.translateTableName(tableConfigs.getTableName(), database);
    TableConfig offlineTableConfig = tableConfigs.getOffline();
    TableConfig realtimeTableConfig = tableConfigs.getRealtime();
    Schema schema = tableConfigs.getSchema();
    try {
      Preconditions.checkState(offlineTableConfig != null || realtimeTableConfig != null,
          "Must provide at least one of 'realtime' or 'offline' table configs for adding TableConfigs: %s",
          rawTableName);
      Preconditions.checkState(schema != null, "Must provide 'schema' for adding TableConfigs: %s", rawTableName);
      String schemaName = DatabaseUtils.translateTableName(schema.getSchemaName(), database);
      Preconditions.checkState(!rawTableName.isEmpty(), "'tableName' cannot be empty in TableConfigs");

      Preconditions.checkState(rawTableName.equals(schemaName),
          "'tableName': %s must be equal to 'schemaName' from 'schema': %s", rawTableName, schema.getSchemaName());
      SchemaUtils.validateIngestionTransformVolatility(schema, _pinotHelixResourceManager.getSchema(schemaName));
      SchemaUtils.validate(schema);
      if (offlineTableConfig != null) {
        String offlineRawTableName = DatabaseUtils.translateTableName(
            TableNameBuilder.extractRawTableName(offlineTableConfig.getTableName()), database);
        Preconditions.checkState(offlineRawTableName.equals(rawTableName),
            "Name in 'offline' table config: %s must be equal to 'tableName': %s", offlineRawTableName, rawTableName);
        TableConfigUtils.validateTableName(offlineTableConfig);
        TableConfig existingOfflineTableConfig = existingTableConfigs != null
            ? existingTableConfigs._offline : _pinotHelixResourceManager.getOfflineTableConfig(rawTableName, false,
                false);
        TableConfigUtils.validate(offlineTableConfig, schema, typesToSkip,
            existingOfflineTableConfig);
        TaskConfigUtils.validateTaskConfigs(tableConfigs.getOffline(), schema, _pinotTaskManager, typesToSkip);
        TableConfigValidatorRegistry.validate(offlineTableConfig, schema);
      }
      if (realtimeTableConfig != null) {
        String realtimeRawTableName = DatabaseUtils.translateTableName(
            TableNameBuilder.extractRawTableName(realtimeTableConfig.getTableName()), database);
        Preconditions.checkState(realtimeRawTableName.equals(rawTableName),
            "Name in 'realtime' table config: %s must be equal to 'tableName': %s", realtimeRawTableName, rawTableName);
        TableConfigUtils.validateTableName(realtimeTableConfig);
        TableConfig existingRealtimeTableConfig = existingTableConfigs != null
            ? existingTableConfigs._realtime : _pinotHelixResourceManager.getRealtimeTableConfig(rawTableName, false,
                false);
        TableConfigUtils.validate(realtimeTableConfig, schema, typesToSkip,
            existingRealtimeTableConfig);
        TaskConfigUtils.validateTaskConfigs(tableConfigs.getRealtime(), schema, _pinotTaskManager, typesToSkip);
        TableConfigValidatorRegistry.validate(realtimeTableConfig, schema);
      }
      if (offlineTableConfig != null && realtimeTableConfig != null) {
        TableConfigUtils.verifyHybridTableConfigs(rawTableName, offlineTableConfig, realtimeTableConfig);
      }
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          redactedTableConfigsError("Invalid TableConfigs: " + rawTableName, e, tableConfigs),
          Response.Status.BAD_REQUEST);
    }
  }

  private static final class ExistingTableConfigs {
    private final TableConfig _offline;
    private final TableConfig _realtime;

    private ExistingTableConfigs(@Nullable TableConfig offline, @Nullable TableConfig realtime) {
      _offline = offline;
      _realtime = realtime;
    }
  }

  private static String redactedTableConfigsError(String prefix, Exception exception, TableConfigs tableConfigs) {
    String diagnostic = exception.getMessage();
    if (tableConfigs.getOffline() != null) {
      diagnostic = TableConfigRedactionUtils.redactDiagnostic(diagnostic, tableConfigs.getOffline());
    }
    if (tableConfigs.getRealtime() != null) {
      diagnostic = TableConfigRedactionUtils.redactDiagnostic(diagnostic, tableConfigs.getRealtime());
    }
    return prefix + ": " + (diagnostic != null ? diagnostic : "Invalid table config");
  }
}
