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
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.exception.TableConfigBackwardIncompatibleException;
import org.apache.pinot.common.exception.TableConfigVersionConflictException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.LogicalTableConfigUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
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
import org.apache.pinot.spi.config.table.TableConfigValidatorRegistry;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * Endpoints for CRUD of {@link TableConfigs}.
 * {@link TableConfigs} is a group of the offline table config, realtime table config and schema for the same tableName.
 */
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

  /**
   * List all {@link TableConfigs} in database provided in header, where each is a group of the offline table config,
   * realtime table config and schema for the same tableName.
   * This is equivalent to a list of all raw table names in provided database
   */
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

  /**
   * Gets the {@link TableConfigs} for the provided raw tableName, by fetching the offline table config for
   * tableName_OFFLINE,
   * realtime table config for tableName_REALTIME and schema for tableName
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tableConfigs/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_TABLE_CONFIGS)
  @Authenticate(AccessType.READ)
  @ApiOperation(value = "Get the TableConfigs for a given raw tableName",
      notes = "Get the TableConfigs for a given raw tableName")
  public String getConfig(
      @ApiParam(value = "Raw table name", required = true) @PathParam("tableName") String tableName,
      @Context HttpHeaders headers) {
    try {
      tableName = DatabaseUtils.translateTableName(tableName, headers);
      Schema schema = _pinotHelixResourceManager.getTableSchema(tableName);
      if (schema == null) {
        throw new NotFoundException(
            String.format("Schema does not exist for table %s Use POST to create it first.", tableName));
      }
      TableConfig offlineTableConfig = _pinotHelixResourceManager.getOfflineTableConfig(tableName, false, false);
      TableConfig realtimeTableConfig = _pinotHelixResourceManager.getRealtimeTableConfig(tableName, false, false);
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
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip,
      @DefaultValue("false") @QueryParam("ignoreActiveTasks") boolean ignoreActiveTasks,
      @Context HttpHeaders httpHeaders, @Context Request request)
      throws Exception {
    Pair<TableConfigs, Map<String, Object>> tableConfigsAndUnrecognizedProps;
    try {
      tableConfigsAndUnrecognizedProps =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigsStr, TableConfigs.class);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Invalid TableConfigs. " + e.getMessage(),
          Response.Status.BAD_REQUEST, e);
    }
    TableConfigs tableConfigs = tableConfigsAndUnrecognizedProps.getLeft();
    String databaseName = DatabaseUtils.extractDatabaseFromHttpHeaders(httpHeaders);
    String rawTableName = DatabaseUtils.translateTableName(tableConfigs.getTableName(), databaseName);
    if (_pinotHelixResourceManager.hasOfflineTable(rawTableName) || _pinotHelixResourceManager.hasRealtimeTable(
        rawTableName) || _pinotHelixResourceManager.getSchema(rawTableName) != null) {
      throw new ControllerApplicationException(LOGGER,
          String.format("TableConfigs: %s already exists. Use PUT to update existing config", rawTableName),
          Response.Status.BAD_REQUEST);
    }

    // Permission check runs BEFORE validateNoDeprecatedConfigs (which reads stored configs from ZK on update-mode
    // paths). Mirrors the /tableConfigs/validate ordering and prevents an unauthenticated caller from probing
    // stored config contents via the deprecation-diff response shape.
    String endpointUrl = request.getRequestURL().toString();
    AccessControl accessControl = _accessControlFactory.create();
    AccessControlUtils.validatePermission(rawTableName, AccessType.CREATE, httpHeaders, endpointUrl, accessControl);
    if (!accessControl.hasAccess(httpHeaders, TargetType.TABLE, rawTableName, Actions.Table.CREATE_TABLE)) {
      throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
    }

    List<String> deprecationWarnings;
    try {
      deprecationWarnings = validateNoDeprecatedConfigs(tableConfigs, JsonUtils.stringToJsonNode(tableConfigsStr),
          databaseName)._warnings;
      validateConfig(tableConfigs, databaseName, typesToSkip);
      tableConfigs.setTableName(rawTableName);
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (IOException | IllegalArgumentException | IllegalStateException e) {
      throw new ControllerApplicationException(LOGGER, "Invalid TableConfigs. " + e.getMessage(),
          Response.Status.BAD_REQUEST, e);
    } catch (RuntimeException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
      LOGGER.warn("Failed to validate TableConfigs for create of table: {}", rawTableName, e);
      throw e;
    }

    TableConfig offlineTableConfig = tableConfigs.getOffline();
    TableConfig realtimeTableConfig = tableConfigs.getRealtime();
    Schema schema = tableConfigs.getSchema();

    try {

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
          tableConfigsAndUnrecognizedProps.getRight(), deprecationWarnings);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
      if (e instanceof InvalidTableConfigException) {
        throw new ControllerApplicationException(LOGGER, String.format("Invalid TableConfigs: %s. Reason: %s",
            rawTableName, e.getMessage()), Response.Status.BAD_REQUEST, e);
      } else if (e instanceof TableAlreadyExistsException) {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
      } else if (e instanceof ControllerApplicationException) {
        throw e;
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

  /**
   * Updates the {@link TableConfigs} by updating the schema tableName,
   * then updating the offline tableConfig or creating a new one if it doesn't already exist in the cluster,
   * then updating the realtime tableConfig or creating a new one if it doesn't already exist in the cluster.
   *
   * <p><b>Atomicity caveat (HTTP 409 CONFLICT response):</b> the offline and realtime sub-config writes are
   * issued sequentially with independent per-sub-type CAS version checks. If the realtime CAS fails after the
   * offline CAS has already succeeded, the offline write has ALREADY landed at v+1 — the response will be HTTP
   * 409 but the underlying state is partially applied. Clients receiving 409 MUST re-read both sub-configs and
   * retry the full transaction; do NOT interpret 409 as "no change applied". A future PR may collapse the two
   * writes into a single Helix multi-write; until then this behaviour is documented contract, not a bug.
   *
   * <p>The option to skip table config validation (validationTypesToSkip) and force update the table schema
   * (forceTableSchemaUpdate) are provided for testing purposes and should be used with caution.
   */
  @PUT
  @Path("/tableConfigs/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.UPDATE_TABLE_CONFIGS)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the TableConfigs provided by the tableConfigsStr json", notes = "Update the "
      + "TableConfigs provided by the tableConfigsStr json")
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
    Pair<TableConfigs, Map<String, Object>> tableConfigsAndUnrecognizedProps;
    TableConfigs tableConfigs;
    DeprecationValidationResult deprecationResult;

    // Existence check runs before deprecation validation so a PUT to a missing TableConfigs reports the actual
    // problem (table does not exist) instead of a misleading "deprecated property" 400 from create-mode fallback.
    if (!_pinotHelixResourceManager.hasOfflineTable(tableName) && !_pinotHelixResourceManager.hasRealtimeTable(
        tableName)) {
      throw new ControllerApplicationException(LOGGER,
          String.format("TableConfigs: %s does not exist. Use POST to create it first.", tableName),
          Response.Status.BAD_REQUEST);
    }

    try {
      tableConfigsAndUnrecognizedProps =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigsStr, TableConfigs.class);
      tableConfigs = tableConfigsAndUnrecognizedProps.getLeft();
      deprecationResult = validateNoDeprecatedConfigs(tableConfigs, JsonUtils.stringToJsonNode(tableConfigsStr),
          databaseName);
      validateConfig(tableConfigs, databaseName, typesToSkip);
      Preconditions.checkState(
          DatabaseUtils.translateTableName(tableConfigs.getTableName(), databaseName).equals(tableName),
          "'tableName' in TableConfigs: %s must match provided tableName: %s", tableConfigs.getTableName(), tableName);
      tableConfigs.setTableName(tableName);
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (IOException | IllegalArgumentException | IllegalStateException e) {
      // Narrowed to client-input exception types so transient ZK / Helix errors propagate as 5xx instead of being
      // mis-reported as 400.
      throw new ControllerApplicationException(LOGGER, "Invalid TableConfigs: " + tableName + ". " + e.getMessage(),
          Response.Status.BAD_REQUEST, e);
    } catch (RuntimeException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      LOGGER.warn("Failed to validate TableConfigs for update of table: {}", tableName, e);
      throw e;
    }

    TableConfig offlineTableConfig = tableConfigs.getOffline();
    TableConfig realtimeTableConfig = tableConfigs.getRealtime();
    Schema schema = tableConfigs.getSchema();

    try {
      _pinotHelixResourceManager.updateSchema(schema, reload, forceTableSchemaUpdate);
      LOGGER.info("Updated schema: {}", tableName);

      if (offlineTableConfig != null) {
        applyTuning(offlineTableConfig, schema);
        if (_pinotHelixResourceManager.hasOfflineTable(tableName)) {
          // Version-checked CAS: a concurrent writer that landed between the deprecation-diff read and this write
          // bumps the znode version, so the CAS fails and we return 5xx — preventing a deprecated key from
          // slipping past the diff via a racing update on the same sub-config.
          _pinotHelixResourceManager.updateTableConfig(offlineTableConfig,
              deprecationResult._offlineExpectedVersion, forceTableSchemaUpdate);
          LOGGER.info("Updated offline table config: {}", tableName);
        } else {
          _pinotHelixResourceManager.addTable(offlineTableConfig);
          LOGGER.info("Created offline table config: {}", tableName);
        }
      }
      if (realtimeTableConfig != null) {
        applyTuning(realtimeTableConfig, schema);
        if (_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
          _pinotHelixResourceManager.updateTableConfig(realtimeTableConfig,
              deprecationResult._realtimeExpectedVersion, forceTableSchemaUpdate);
          LOGGER.info("Updated realtime table config: {}", tableName);
        } else {
          _pinotHelixResourceManager.addTable(realtimeTableConfig);
          LOGGER.info("Created realtime table config: {}", tableName);
        }
      }
    } catch (TableConfigVersionConflictException e) {
      // CAS lost on at least one sub-config (offline or realtime). Note: the offline write may have already
      // landed before the realtime CAS failed — TableConfigs PUT writes each sub-type sequentially. The 409
      // response signals that the caller should re-read both sub-configs and retry the full transaction.
      throw new ControllerApplicationException(LOGGER,
          String.format("Conflict updating TableConfigs for %s: %s. Re-read both sub-configs and retry.", tableName,
              e.getMessage()), Response.Status.CONFLICT, e);
    } catch (TableConfigBackwardIncompatibleException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid TableConfigs for: %s, %s", tableName, e.getMessage()), Response.Status.BAD_REQUEST, e);
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
        tableConfigsAndUnrecognizedProps.getRight(), deprecationResult._warnings);
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
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: "
          + "(ALL|TASK|UPSERT|TENANT|MINION_INSTANCES|ACTIVE_TASKS)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    TableConfigsValidationResult result =
        parseAndValidateTableConfigs(tableConfigsStr, typesToSkip, httpHeaders, request);
    ObjectNode response = JsonUtils.objectToJsonNode(result._tableConfigs).deepCopy();
    response.set("unrecognizedProperties", JsonUtils.objectToJsonNode(result._unrecognizedProps));
    if (!result._deprecationWarnings.isEmpty()) {
      response.set("deprecationWarnings", JsonUtils.objectToJsonNode(result._deprecationWarnings));
    }
    return response.toString();
  }

  /**
   * Validates and tunes the {@link TableConfigs} as provided in the tableConfigsStr json, by applying tuner configs,
   * ensuring min replicas and storage quota constraints, and returns the tuned TableConfigs.
   */
  @POST
  @Path("/tableConfigs/tune")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Tune the TableConfigs",
      notes = "Validates and applies tuning (tuner configs, min replicas, storage quota) to the TableConfigs, "
          + "returning the result that would be stored on create/update")
  @ManualAuthorization // performed after parsing TableConfigs
  public String tuneConfig(String tableConfigsStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: "
          + "(ALL|TASK|UPSERT|TENANT|MINION_INSTANCES|ACTIVE_TASKS)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    TableConfigsValidationResult result =
        parseAndValidateTableConfigs(tableConfigsStr, typesToSkip, httpHeaders, request);
    TableConfigs tableConfigs = result._tableConfigs;
    Schema schema = tableConfigs.getSchema();
    if (tableConfigs.getOffline() != null) {
      applyTuning(tableConfigs.getOffline(), schema);
    }
    if (tableConfigs.getRealtime() != null) {
      applyTuning(tableConfigs.getRealtime(), schema);
    }
    ObjectNode response = JsonUtils.objectToJsonNode(tableConfigs).deepCopy();
    response.set("unrecognizedProperties", JsonUtils.objectToJsonNode(result._unrecognizedProps));
    if (!result._deprecationWarnings.isEmpty()) {
      response.set("deprecationWarnings", JsonUtils.objectToJsonNode(result._deprecationWarnings));
    }
    return response.toString();
  }

  private TableConfigsValidationResult parseAndValidateTableConfigs(String tableConfigsStr,
      @Nullable String typesToSkip, HttpHeaders httpHeaders, Request request) {
    Pair<TableConfigs, Map<String, Object>> tableConfigsAndUnrecognizedProps;
    JsonNode tableConfigsJson;
    try {
      tableConfigsAndUnrecognizedProps =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigsStr, TableConfigs.class);
      tableConfigsJson = JsonUtils.stringToJsonNode(tableConfigsStr);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, "Invalid TableConfigs json string. " + e.getMessage(),
          Response.Status.BAD_REQUEST, e);
    }
    String databaseName = DatabaseUtils.extractDatabaseFromHttpHeaders(httpHeaders);
    TableConfigs tableConfigs = tableConfigsAndUnrecognizedProps.getLeft();
    String rawTableName = DatabaseUtils.translateTableName(tableConfigs.getTableName(), databaseName);

    // Cluster-aware validations are exclusive to the validate/tune pre-flight endpoints so that users get fail-fast
    // feedback on tenant/minion/active-task issues without re-running them in the create/update paths (which already
    // perform the equivalent checks inline or via PinotHelixResourceManager).
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
          String.format("Invalid TableConfigs: %s. %s", rawTableName, e.getMessage()), Response.Status.BAD_REQUEST, e);
    }

    // Validate permission BEFORE running validateNoDeprecatedConfigs (which reads stored configs from ZK).
    // Mirrors PinotTableRestletResource.checkTableConfig and prevents an unauthenticated caller from probing
    // table existence via the deprecation-diff response shape.
    // setTableName is deferred until AFTER validateConfig because it has the side effect of overwriting the
    // schema name (TableConfigs.setTableName calls _schema.setSchemaName), which would mask the schema-mismatch
    // check inside validateConfig.
    String endpointUrl = request.getRequestURL().toString();
    AccessControl accessControl = _accessControlFactory.create();
    AccessControlUtils.validatePermission(rawTableName, AccessType.READ, httpHeaders, endpointUrl, accessControl);
    if (!accessControl.hasAccess(httpHeaders, TargetType.TABLE, rawTableName, Actions.Table.VALIDATE_TABLE_CONFIGS)) {
      throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
    }

    List<String> deprecationWarnings;
    try {
      deprecationWarnings = validateNoDeprecatedConfigs(tableConfigs, tableConfigsJson, databaseName)._warnings;
      validateConfig(tableConfigs, databaseName, typesToSkip);
      tableConfigs.setTableName(rawTableName);
    } catch (ControllerApplicationException e) {
      // Preserve the upstream status (e.g. 500 from validateConfig); don't downgrade to 400.
      throw e;
    } catch (IllegalArgumentException | IllegalStateException e) {
      throw new ControllerApplicationException(LOGGER, "Invalid TableConfigs. " + e.getMessage(),
          Response.Status.BAD_REQUEST, e);
    } catch (RuntimeException e) {
      LOGGER.warn("Failed to validate TableConfigs for: {}", tableConfigs.getTableName(), e);
      throw e;
    }
    return new TableConfigsValidationResult(tableConfigs, tableConfigsAndUnrecognizedProps.getRight(),
        deprecationWarnings);
  }

  /// Carries the parsed [TableConfigs], the map of unrecognized JSON properties, and the list of deprecation
  /// warnings out of [#parseAndValidateTableConfigs] so the calling REST methods (`/validate`, `/tune`) can
  /// surface the same set of fields on their response without duplicating the parsing pipeline.
  private static final class TableConfigsValidationResult {
    final TableConfigs _tableConfigs;
    final Map<String, Object> _unrecognizedProps;
    final List<String> _deprecationWarnings;

    TableConfigsValidationResult(TableConfigs tableConfigs, Map<String, Object> unrecognizedProps,
        List<String> deprecationWarnings) {
      _tableConfigs = tableConfigs;
      _unrecognizedProps = unrecognizedProps;
      _deprecationWarnings = deprecationWarnings;
    }
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
    if (!skipTypes.contains(TableConfigUtils.ValidationType.ACTIVE_TASKS)) {
      PinotTableRestletResource.tableTasksValidation(tableConfig, _pinotHelixTaskResourceManager);
    }
  }

  private void applyTuning(TableConfig tableConfig, Schema schema) {
    TableConfigTunerUtils.applyTunerConfigs(_pinotHelixResourceManager, tableConfig, schema, Collections.emptyMap());
    TableConfigUtils.ensureMinReplicas(tableConfig, _controllerConf.getDefaultTableMinReplicas());
    TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, _controllerConf.getDimTableMaxSize());
  }


  /**
   * Validates the provided TableConfigs. Hybrid table validation is performed only on the provided
   * configs and does not check for conflicts with existing tables in the cluster.
   */
  private void validateConfig(TableConfigs tableConfigs, String database, @Nullable String typesToSkip) {
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
      SchemaUtils.validate(schema);
      if (offlineTableConfig != null) {
        String offlineRawTableName = DatabaseUtils.translateTableName(
            TableNameBuilder.extractRawTableName(offlineTableConfig.getTableName()), database);
        Preconditions.checkState(offlineRawTableName.equals(rawTableName),
            "Name in 'offline' table config: %s must be equal to 'tableName': %s", offlineRawTableName, rawTableName);
        TableConfigUtils.validateTableName(offlineTableConfig);
        TableConfigUtils.validate(offlineTableConfig, schema, typesToSkip);
        TaskConfigUtils.validateTaskConfigs(tableConfigs.getOffline(), schema, _pinotTaskManager, typesToSkip);
        TableConfigValidatorRegistry.validate(offlineTableConfig, schema);
      }
      if (realtimeTableConfig != null) {
        String realtimeRawTableName = DatabaseUtils.translateTableName(
            TableNameBuilder.extractRawTableName(realtimeTableConfig.getTableName()), database);
        Preconditions.checkState(realtimeRawTableName.equals(rawTableName),
            "Name in 'realtime' table config: %s must be equal to 'tableName': %s", realtimeRawTableName, rawTableName);
        TableConfigUtils.validateTableName(realtimeTableConfig);
        TableConfigUtils.validate(realtimeTableConfig, schema, typesToSkip);
        TaskConfigUtils.validateTaskConfigs(tableConfigs.getRealtime(), schema, _pinotTaskManager, typesToSkip);
        TableConfigValidatorRegistry.validate(realtimeTableConfig, schema);
      }
      if (offlineTableConfig != null && realtimeTableConfig != null) {
        TableConfigUtils.verifyHybridTableConfigs(rawTableName, offlineTableConfig, realtimeTableConfig);
      }
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Invalid TableConfigs: %s. %s", rawTableName, e.getMessage()), Response.Status.BAD_REQUEST, e);
    }
  }

  /// Validates the offline and realtime sub-configs for deprecated properties. For each sub-type, if a stored
  /// table config already exists for `rawTableName` the validation runs in update mode (diffing against the
  /// stored config), otherwise it runs in create mode. Aggregated warnings from all sub-types are returned along
  /// with the ZK znode versions observed at diff time so callers can issue a version-checked CAS on the
  /// subsequent write.
  private DeprecationValidationResult validateNoDeprecatedConfigs(TableConfigs tableConfigs,
      JsonNode tableConfigsJson, String database) {
    // All call sites verify tableName non-null before reaching here; assert the invariant so a future refactor
    // that moves the call earlier fails loudly instead of silently skipping the deprecation pass.
    Preconditions.checkState(tableConfigs.getTableName() != null,
        "tableName must be set before deprecation validation");
    String rawTableName = DatabaseUtils.translateTableName(tableConfigs.getTableName(), database);
    List<String> warnings = new ArrayList<>();
    int offlineExpectedVersion = -1;
    int realtimeExpectedVersion = -1;
    JsonNode offlineTableConfigJson = subConfigJson(tableConfigsJson, TableType.OFFLINE);
    if (offlineTableConfigJson != null) {
      ReadStoredConfigResult read =
          readStoredTableConfigJsonWithVersion(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
      offlineExpectedVersion = read._version;
      String prefix = TableType.OFFLINE.name().toLowerCase();
      if (read._json == null) {
        warnings.addAll(DeprecatedTableConfigValidationUtils.validateOnCreate(offlineTableConfigJson, prefix));
      } else {
        warnings.addAll(DeprecatedTableConfigValidationUtils.validateOnUpdate(offlineTableConfigJson, read._json,
            prefix));
      }
    }
    JsonNode realtimeTableConfigJson = subConfigJson(tableConfigsJson, TableType.REALTIME);
    if (realtimeTableConfigJson != null) {
      ReadStoredConfigResult read =
          readStoredTableConfigJsonWithVersion(TableNameBuilder.REALTIME.tableNameWithType(rawTableName));
      realtimeExpectedVersion = read._version;
      String prefix = TableType.REALTIME.name().toLowerCase();
      if (read._json == null) {
        warnings.addAll(DeprecatedTableConfigValidationUtils.validateOnCreate(realtimeTableConfigJson, prefix));
      } else {
        warnings.addAll(DeprecatedTableConfigValidationUtils.validateOnUpdate(realtimeTableConfigJson, read._json,
            prefix));
      }
    }
    return new DeprecationValidationResult(warnings, offlineExpectedVersion, realtimeExpectedVersion);
  }

  /// Read result paired with the ZK znode version observed at read time. Used to thread the CAS expected-version
  /// through to the subsequent `updateTableConfig` write.
  private static final class ReadStoredConfigResult {
    @Nullable
    final JsonNode _json;
    final int _version;

    ReadStoredConfigResult(@Nullable JsonNode json, int version) {
      _json = json;
      _version = version;
    }
  }

  /// Aggregated deprecation-diff result. Carries the per-sub-type ZK znode versions so the subsequent
  /// `updateTableConfig` call can issue a version-checked CAS. A `-1` version means the sub-type did not exist at
  /// diff time (create path), so no version check is required.
  static final class DeprecationValidationResult {
    final List<String> _warnings;
    final int _offlineExpectedVersion;
    final int _realtimeExpectedVersion;

    DeprecationValidationResult(List<String> warnings, int offlineExpectedVersion, int realtimeExpectedVersion) {
      _warnings = warnings;
      _offlineExpectedVersion = offlineExpectedVersion;
      _realtimeExpectedVersion = realtimeExpectedVersion;
    }

    List<String> warnings() {
      return _warnings;
    }
  }

  private ReadStoredConfigResult readStoredTableConfigJsonWithVersion(String tableNameWithType) {
    Stat stat = new Stat();
    ZNRecord stored = ZKMetadataProvider.getTableConfigZNRecord(_pinotHelixResourceManager.getPropertyStore(),
        tableNameWithType, stat);
    return new ReadStoredConfigResult(TableConfigSerDeUtils.toRawJsonNode(stored),
        stored == null ? -1 : stat.getVersion());
  }

  @Nullable
  private static JsonNode subConfigJson(JsonNode tableConfigsJson, TableType type) {
    // Mirror what Jackson populates on the deserialized POJO: TableConfigs uses @JsonProperty("offline") /
    // @JsonProperty("realtime"), so any uppercase variant in the raw user JSON is already silently ignored at
    // deserialization time. We use the same lowercase-only lookup here so the deprecation pass agrees with what
    // ends up in the stored config. The Jackson-ignores-uppercase invariant is locked at the SPI layer by
    // TableConfigsSerializationTest#testUppercaseOfflineRealtimeKeysAreIgnoredByJackson — if Jackson is ever
    // configured to ACCEPT_CASE_INSENSITIVE_PROPERTIES, that SPI test fails first, surfacing the regression
    // before any request reaches this code path.
    return tableConfigsJson.get(type.name().toLowerCase());
  }
}
