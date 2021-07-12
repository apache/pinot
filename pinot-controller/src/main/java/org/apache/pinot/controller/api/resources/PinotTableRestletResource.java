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
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
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
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.exception.SchemaNotFoundException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.InvalidTableConfigException;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfigConstants;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.recommender.RecommenderDriver;
import org.apache.pinot.controller.tuner.TableConfigTunerUtils;
import org.apache.pinot.controller.util.TableIngestionStatusHelper;
import org.apache.pinot.controller.util.TableMetadataReader;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableStats;
import org.apache.pinot.spi.config.table.TableStatus;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotTableRestletResource {
  /**
   * URI Mappings:
   * - "/tables", "/tables/": List all the tables
   * - "/tables/{tableName}", "/tables/{tableName}/": List config for specified table.
   *
   * - "/tables/{tableName}?state={state}"
   *   Set the state for the specified {tableName} to the specified {state} (enable|disable|drop).
   *
   * - "/tables/{tableName}?type={type}"
   *   List all tables of specified type, type can be one of {offline|realtime}.
   *
   *   Set the state for the specified {tableName} to the specified {state} (enable|disable|drop).
   *   * - "/tables/{tableName}?state={state}&amp;type={type}"
   *
   *   Set the state for the specified {tableName} of specified type to the specified {state} (enable|disable|drop).
   *   Type here is type of the table, one of 'offline|realtime'.
   * {@inheritDoc}
   */

  public static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PinotTableRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @Inject
  ControllerConf _controllerConf;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  ExecutorService _executorService;

  @Inject
  AccessControlFactory _accessControlFactory;
  AccessControlUtils _accessControlUtils = new AccessControlUtils();

  @Inject
  Executor _executor;

  @Inject
  HttpConnectionManager _connectionManager;

  /**
   * API to create a table. Before adding, validations will be done (min number of replicas,
   * checking offline and realtime table configs match, checking for tenants existing)
   * @param tableConfigStr
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @ApiOperation(value = "Adds a table", notes = "Adds a table")
  public SuccessResponse addTable(String tableConfigStr, @Context HttpHeaders httpHeaders, @Context Request request) {
    // TODO introduce a table config ctor with json string.
    TableConfig tableConfig;
    String tableName;
    try {
      tableConfig = JsonUtils.stringToObject(tableConfigStr, TableConfig.class);

      // validate permission
      tableName = tableConfig.getTableName();
      String endpointUrl = request.getRequestURL().toString();
      _accessControlUtils
          .validatePermission(tableName, AccessType.CREATE, httpHeaders, endpointUrl, _accessControlFactory.create());

      Schema schema = _pinotHelixResourceManager.getSchemaForTableConfig(tableConfig);

      TableConfigTunerUtils.applyTunerConfig(_pinotHelixResourceManager, tableConfig, schema);

      // TableConfigUtils.validate(...) is used across table create/update.
      TableConfigUtils.validate(tableConfig, schema);
      // TableConfigUtils.validateTableName(...) checks table name rules.
      // So it won't effect already created tables.
      TableConfigUtils.validateTableName(tableConfig);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
    try {
      try {
        TableConfigUtils.ensureMinReplicas(tableConfig, _controllerConf.getDefaultTableMinReplicas());
        TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, _controllerConf.getDimTableMaxSize());
        checkHybridTableConfig(TableNameBuilder.extractRawTableName(tableName), tableConfig);
      } catch (Exception e) {
        throw new InvalidTableConfigException(e);
      }
      _pinotHelixResourceManager.addTable(tableConfig);
      // TODO: validate that table was created successfully
      // (in realtime case, metadata might not have been created but would be created successfully in the next run of the validation manager)
      return new SuccessResponse("Table " + tableName + " succesfully added");
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
      if (e instanceof InvalidTableConfigException) {
        String errStr = String.format("Invalid table config for table %s: %s", tableName, e.getMessage());
        throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST, e);
      } else if (e instanceof TableAlreadyExistsException) {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
      } else {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/recommender")
  @ApiOperation(value = "Recommend config", notes = "Recommend a config with input json")
  public String recommendConfig(String inputStr) {
    try {
      return RecommenderDriver.run(inputStr);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @ApiOperation(value = "Lists all tables in cluster", notes = "Lists all tables in cluster")
  public String listTableConfigs(@ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    try {
      List<String> tableNames;
      TableType tableType = null;
      if (tableTypeStr != null) {
        tableType = TableType.valueOf(tableTypeStr.toUpperCase());
      }

      if (tableType == null) {
        tableNames = _pinotHelixResourceManager.getAllRawTables();
      } else {
        if (tableType == TableType.REALTIME) {
          tableNames = _pinotHelixResourceManager.getAllRealtimeTables();
        } else {
          tableNames = _pinotHelixResourceManager.getAllOfflineTables();
        }
      }

      Collections.sort(tableNames);
      return JsonUtils.newObjectNode().set("tables", JsonUtils.objectToJsonNode(tableNames)).toString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private String listTableConfigs(String tableName, @Nullable String tableTypeStr) {
    try {
      ObjectNode ret = JsonUtils.newObjectNode();

      if ((tableTypeStr == null || TableType.OFFLINE.name().equalsIgnoreCase(tableTypeStr))
          && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
        TableConfig tableConfig = _pinotHelixResourceManager.getOfflineTableConfig(tableName);
        Preconditions.checkNotNull(tableConfig);
        ret.set(TableType.OFFLINE.name(), tableConfig.toJsonNode());
      }

      if ((tableTypeStr == null || TableType.REALTIME.name().equalsIgnoreCase(tableTypeStr))
          && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        TableConfig tableConfig = _pinotHelixResourceManager.getRealtimeTableConfig(tableName);
        Preconditions.checkNotNull(tableConfig);
        ret.set(TableType.REALTIME.name(), tableConfig.toJsonNode());
      }
      return ret.toString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}")
  @ApiOperation(value = "Get/Enable/Disable/Drop a table", notes =
      "Get/Enable/Disable/Drop a table. If table name is the only parameter specified "
          + ", the tableconfig will be printed")
  public String alterTableStateOrListTableConfig(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "enable|disable|drop") @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    try {
      if (stateStr == null) {
        return listTableConfigs(tableName, tableTypeStr);
      }

      StateType stateType = Constants.validateState(stateStr);
      TableType tableType = Constants.validateTableType(tableTypeStr);

      // validate if user has permission to change the table state
      String endpointUrl = request.getRequestURL().toString();
      _accessControlUtils
          .validatePermission(tableName, AccessType.UPDATE, httpHeaders, endpointUrl, _accessControlFactory.create());

      ArrayNode ret = JsonUtils.newArrayNode();
      boolean tableExists = false;

      if (tableType != TableType.REALTIME && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
        String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        ObjectNode offline = JsonUtils.newObjectNode();
        tableExists = true;

        offline.put("tableName", offlineTableName);
        offline.set("state",
            JsonUtils.objectToJsonNode(_pinotHelixResourceManager.toggleTableState(offlineTableName, stateType)));
        ret.add(offline);
      }

      if (tableType != TableType.OFFLINE && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
        ObjectNode realtime = JsonUtils.newObjectNode();
        tableExists = true;

        realtime.put("tableName", realtimeTableName);
        realtime.set("state",
            JsonUtils.objectToJsonNode(_pinotHelixResourceManager.toggleTableState(realtimeTableName, stateType)));
        ret.add(realtime);
      }

      if (tableExists) {
        return ret.toString();
      } else {
        throw new ControllerApplicationException(LOGGER, "Table '" + tableName + "' does not exist",
            Response.Status.BAD_REQUEST);
      }
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @DELETE
  @Path("/tables/{tableName}")
  @Authenticate(AccessType.DELETE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Deletes a table", notes = "Deletes a table")
  public SuccessResponse deleteTable(
      @ApiParam(value = "Name of the table to delete", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    TableType tableType = Constants.validateTableType(tableTypeStr);

    List<String> tablesDeleted = new LinkedList<>();
    try {
      boolean tableExist = false;
      if (verifyTableType(tableName, tableType, TableType.OFFLINE)) {
        tableExist = _pinotHelixResourceManager.hasOfflineTable(tableName);
        // Even the table name does not exist, still go on to delete remaining table metadata in case a previous delete
        // did not complete.
        _pinotHelixResourceManager.deleteOfflineTable(tableName);
        if (tableExist) {
          tablesDeleted.add(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
        }
      }
      if (verifyTableType(tableName, tableType, TableType.REALTIME)) {
        tableExist = _pinotHelixResourceManager.hasRealtimeTable(tableName);
        // Even the table name does not exist, still go on to delete remaining table metadata in case a previous delete
        // did not complete.
        _pinotHelixResourceManager.deleteRealtimeTable(tableName);
        if (tableExist) {
          tablesDeleted.add(TableNameBuilder.REALTIME.tableNameWithType(tableName));
        }
      }
      if (!tablesDeleted.isEmpty()) {
        return new SuccessResponse("Tables: " + tablesDeleted + " deleted");
      }
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    throw new ControllerApplicationException(LOGGER,
        "Table '" + tableName + "' with type " + tableType + " does not exist", Response.Status.NOT_FOUND);
  }

  //   Return true iff the table is of the expectedType based on the given tableName and tableType. The truth table:
  //        tableType   TableNameBuilder.getTableTypeFromTableName(tableName)   Return value
  //     1. null      null (i.e., table has no type suffix)           true
  //     2. null      not_null                              typeFromTableName == expectedType
  //     3. not_null      null                                    tableType == expectedType
  //     4. not_null      not_null                          tableType==typeFromTableName==expectedType
  private boolean verifyTableType(String tableName, TableType tableType, TableType expectedType) {
    if (tableType != null && tableType != expectedType) {
      return false;
    }
    TableType typeFromTableName = TableNameBuilder.getTableTypeFromTableName(tableName);
    return typeFromTableName == null || typeFromTableName == expectedType;
  }

  @PUT
  @Path("/tables/{tableName}")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Updates table config for a table", notes = "Updates table config for a table")
  public SuccessResponse updateTableConfig(
      @ApiParam(value = "Name of the table to update", required = true) @PathParam("tableName") String tableName,
      String tableConfigString)
      throws Exception {
    TableConfig tableConfig;
    try {
      tableConfig = JsonUtils.stringToObject(tableConfigString, TableConfig.class);
      Schema schema = _pinotHelixResourceManager.getSchemaForTableConfig(tableConfig);
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      String msg = String.format("Invalid table config: %s with error: %s", tableName, e.getMessage());
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }

    try {
      String tableNameWithType = tableConfig.getTableName();
      if (!TableNameBuilder.forType(tableConfig.getTableType()).tableNameWithType(tableName)
          .equals(tableNameWithType)) {
        throw new ControllerApplicationException(LOGGER,
            "Request table " + tableName + " does not match table name in the body " + tableNameWithType,
            Response.Status.BAD_REQUEST);
      }

      if (!_pinotHelixResourceManager.hasTable(tableNameWithType)) {
        throw new ControllerApplicationException(LOGGER, "Table " + tableNameWithType + " does not exist",
            Response.Status.NOT_FOUND);
      }

      try {
        TableConfigUtils.ensureMinReplicas(tableConfig, _controllerConf.getDefaultTableMinReplicas());
        TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, _controllerConf.getDimTableMaxSize());
        checkHybridTableConfig(TableNameBuilder.extractRawTableName(tableName), tableConfig);
      } catch (Exception e) {
        throw new InvalidTableConfigException(e);
      }
      _pinotHelixResourceManager.updateTableConfig(tableConfig);
    } catch (InvalidTableConfigException e) {
      String errStr = String.format("Failed to update configuration for %s due to: %s", tableName, e.getMessage());
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST, e);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw e;
    }

    return new SuccessResponse("Table config updated for " + tableName);
  }

  @POST
  @Path("/tables/validate")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validate table config for a table", notes =
      "This API returns the table config that matches the one you get from 'GET /tables/{tableName}'."
          + " This allows us to validate table config before apply.")
  public String checkTableConfig(String tableConfigStr) {
    TableConfig tableConfig;
    try {
      tableConfig = JsonUtils.stringToObject(tableConfigStr, TableConfig.class);
    } catch (IOException e) {
      String msg = String.format("Invalid table config json string: %s", tableConfigStr);
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
    return validateConfig(tableConfig, _pinotHelixResourceManager.getSchemaForTableConfig(tableConfig));
  }

  @Deprecated
  @POST
  @Path("/tables/validateTableAndSchema")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validate table config for a table along with specified schema", notes =
      "Deprecated. Use /tableConfigs/validate instead."
          + "Validate given table config and schema. If specified schema is null, attempt to retrieve schema using the "
          + "table name. This API returns the table config that matches the one you get from 'GET /tables/{tableName}'."
          + " This allows us to validate table config before apply.")
  public String validateTableAndSchema(TableAndSchemaConfig tableSchemaConfig) {
    TableConfig tableConfig = tableSchemaConfig.getTableConfig();
    Schema schema = tableSchemaConfig.getSchema();
    if (schema == null) {
      schema = _pinotHelixResourceManager.getSchemaForTableConfig(tableConfig);
    }
    return validateConfig(tableSchemaConfig.getTableConfig(), schema);
  }

  private String validateConfig(TableConfig tableConfig, Schema schema) {
    try {
      if (schema == null) {
        throw new SchemaNotFoundException("Got empty schema");
      }

      TableConfigUtils.validate(tableConfig, schema);
      ObjectNode tableConfigValidateStr = JsonUtils.newObjectNode();
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        tableConfigValidateStr.set(TableType.OFFLINE.name(), tableConfig.toJsonNode());
      } else {
        tableConfigValidateStr.set(TableType.REALTIME.name(), tableConfig.toJsonNode());
      }
      return tableConfigValidateStr.toString();
    } catch (Exception e) {
      String msg = String.format("Invalid table config: %s. %s", tableConfig.getTableName(), e.getMessage());
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/tables/{tableName}/rebalance")
  @ApiOperation(value = "Rebalances a table (reassign instances and segments for a table)", notes = "Rebalances a table (reassign instances and segments for a table)")
  public RebalanceResult rebalance(
      @ApiParam(value = "Name of the table to rebalance", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Whether to rebalance table in dry-run mode") @DefaultValue("false") @QueryParam("dryRun") boolean dryRun,
      @ApiParam(value = "Whether to reassign instances before reassigning segments") @DefaultValue("false") @QueryParam("reassignInstances") boolean reassignInstances,
      @ApiParam(value = "Whether to reassign CONSUMING segments for real-time table") @DefaultValue("false") @QueryParam("includeConsuming") boolean includeConsuming,
      @ApiParam(value = "Whether to rebalance table in bootstrap mode (regardless of minimum segment movement, reassign all segments in a round-robin fashion as if adding new segments to an empty table)") @DefaultValue("false") @QueryParam("bootstrap") boolean bootstrap,
      @ApiParam(value = "Whether to allow downtime for the rebalance") @DefaultValue("false") @QueryParam("downtime") boolean downtime,
      @ApiParam(value = "For no-downtime rebalance, minimum number of replicas to keep alive during rebalance, or maximum number of replicas allowed to be unavailable if value is negative") @DefaultValue("1") @QueryParam("minAvailableReplicas") int minAvailableReplicas,
      @ApiParam(value = "Whether to use best-efforts to rebalance (not fail the rebalance when the no-downtime contract cannot be achieved)") @DefaultValue("false") @QueryParam("bestEfforts") boolean bestEfforts) {

    String tableNameWithType = constructTableNameWithType(tableName, tableTypeStr);

    Configuration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.addProperty(RebalanceConfigConstants.DRY_RUN, dryRun);
    rebalanceConfig.addProperty(RebalanceConfigConstants.REASSIGN_INSTANCES, reassignInstances);
    rebalanceConfig.addProperty(RebalanceConfigConstants.INCLUDE_CONSUMING, includeConsuming);
    rebalanceConfig.addProperty(RebalanceConfigConstants.BOOTSTRAP, bootstrap);
    rebalanceConfig.addProperty(RebalanceConfigConstants.DOWNTIME, downtime);
    rebalanceConfig.addProperty(RebalanceConfigConstants.MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME, minAvailableReplicas);
    rebalanceConfig.addProperty(RebalanceConfigConstants.BEST_EFFORTS, bestEfforts);

    try {
      if (dryRun || downtime) {
        // For dry-run or rebalance with downtime, directly return the rebalance result as it should return immediately
        return _pinotHelixResourceManager.rebalanceTable(tableNameWithType, rebalanceConfig);
      } else {
        // Make a dry-run first to get the target assignment
        rebalanceConfig.setProperty(RebalanceConfigConstants.DRY_RUN, true);
        RebalanceResult dryRunResult = _pinotHelixResourceManager.rebalanceTable(tableNameWithType, rebalanceConfig);

        if (dryRunResult.getStatus() == RebalanceResult.Status.DONE) {
          // If dry-run succeeded, run rebalance asynchronously
          rebalanceConfig.setProperty(RebalanceConfigConstants.DRY_RUN, false);
          _executorService.submit(() -> {
            try {
              _pinotHelixResourceManager.rebalanceTable(tableNameWithType, rebalanceConfig);
            } catch (Throwable t) {
              LOGGER.error("Caught exception/error while rebalancing table: {}", tableNameWithType, t);
            }
          });
          return new RebalanceResult(RebalanceResult.Status.IN_PROGRESS,
              "In progress, check controller logs for updates", dryRunResult.getInstanceAssignment(),
              dryRunResult.getSegmentAssignment());
        } else {
          // If dry-run failed or is no-op, return the dry-run result
          return dryRunResult;
        }
      }
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.NOT_FOUND);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/state")
  @ApiOperation(value = "Get current table state", notes = "Get current table state")
  public String getTableState(
      @ApiParam(value = "Name of the table to get its state", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr) {
    String tableNameWithType = constructTableNameWithType(tableName, tableTypeStr);
    try {
      ObjectNode data = JsonUtils.newObjectNode();
      data.put("state", _pinotHelixResourceManager.isTableEnabled(tableNameWithType) ? "enabled" : "disabled");
      return data.toString();
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(LOGGER, "Failed to find table: " + tableNameWithType,
          Response.Status.NOT_FOUND);
    }
  }

  @GET
  @Path("/tables/{tableName}/stats")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "table stats", notes = "Provides metadata info/stats about the table.")
  public String getTableStats(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    ObjectNode ret = JsonUtils.newObjectNode();
    if ((tableTypeStr == null || TableType.OFFLINE.name().equalsIgnoreCase(tableTypeStr)) && _pinotHelixResourceManager
        .hasOfflineTable(tableName)) {
      String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName);
      TableStats tableStats = _pinotHelixResourceManager.getTableStats(tableNameWithType);
      ret.set(TableType.OFFLINE.name(), JsonUtils.objectToJsonNode(tableStats));
    }
    if ((tableTypeStr == null || TableType.REALTIME.name().equalsIgnoreCase(tableTypeStr)) && _pinotHelixResourceManager
        .hasRealtimeTable(tableName)) {
      String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
      TableStats tableStats = _pinotHelixResourceManager.getTableStats(tableNameWithType);
      ret.set(TableType.REALTIME.name(), JsonUtils.objectToJsonNode(tableStats));
    }
    return ret.toString();
  }

  private String constructTableNameWithType(String tableName, String tableTypeStr) {
    TableType tableType;
    try {
      tableType = TableType.valueOf(tableTypeStr.toUpperCase());
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Illegal table type: " + tableTypeStr,
          Response.Status.BAD_REQUEST);
    }
    return TableNameBuilder.forType(tableType).tableNameWithType(tableName);
  }

  private void checkHybridTableConfig(String rawTableName, TableConfig tableConfig) {
    if (tableConfig.getTableType() == TableType.REALTIME) {
      if (_pinotHelixResourceManager.hasOfflineTable(rawTableName)) {
        TableConfigUtils
            .verifyHybridTableConfigs(rawTableName, _pinotHelixResourceManager.getOfflineTableConfig(rawTableName),
                tableConfig);
      }
    } else {
      if (_pinotHelixResourceManager.hasRealtimeTable(rawTableName)) {
        TableConfigUtils.verifyHybridTableConfigs(rawTableName, tableConfig,
            _pinotHelixResourceManager.getRealtimeTableConfig(rawTableName));
      }
    }
  }

  @GET
  @Path("/tables/{tableName}/status")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "table status", notes = "Provides status of the table including ingestion status")
  public String getTableStatus(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    try {
      TableType tableType = Constants.validateTableType(tableTypeStr);
      if (tableType == null) {
        throw new ControllerApplicationException(LOGGER, "Table type should either be realtime|offline",
            Response.Status.BAD_REQUEST);
      }
      String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
      if (!_pinotHelixResourceManager.hasTable(tableNameWithType)) {
        throw new ControllerApplicationException(LOGGER,
            "Specified table name: " + tableName + " of type: " + tableTypeStr + " does not exist.",
            Response.Status.BAD_REQUEST);
      }
      TableStatus.IngestionStatus ingestionStatus = null;
      if (TableType.OFFLINE == tableType) {
        ingestionStatus = TableIngestionStatusHelper
            .getOfflineTableIngestionStatus(tableNameWithType, _pinotHelixResourceManager,
                _pinotHelixTaskResourceManager);
      } else {
        ingestionStatus = TableIngestionStatusHelper.getRealtimeTableIngestionStatus(tableNameWithType,
            _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000, _executor, _connectionManager,
            _pinotHelixResourceManager);
      }
      TableStatus tableStatus = new TableStatus(ingestionStatus);
      return JsonUtils.objectToPrettyString(tableStatus);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Failed to get status (ingestion status) for table %s. Reason: %s", tableName, e.getMessage()),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @GET
  @Path("tables/{tableName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the aggregate metadata of all segments for a table", notes = "Get the aggregate metadata of all segments for a table")
  public String getTableAggregateMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Columns name", allowMultiple = true) @QueryParam("columns") @DefaultValue("") List<String> columns) {
    LOGGER.info("Received a request to fetch aggregate metadata for a table {}", tableName);
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == TableType.REALTIME) {
      throw new ControllerApplicationException(LOGGER, "Table type : " + tableTypeStr + " not yet supported.",
          Response.Status.NOT_IMPLEMENTED);
    }
    String tableNameWithType =
        Utils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    SegmentsValidationAndRetentionConfig segmentsConfig =
        tableConfig != null ? tableConfig.getValidationConfig() : null;
    int numReplica = segmentsConfig == null ? 1 : Integer.parseInt(segmentsConfig.getReplication());

    String segmentsMetadata;
    try {
      JsonNode segmentsMetadataJson = getAggregateMetadataFromServer(tableNameWithType, columns, numReplica);
      segmentsMetadata = JsonUtils.objectToPrettyString(segmentsMetadataJson);
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    } catch (IOException ioe) {
      throw new ControllerApplicationException(LOGGER, "Error parsing Pinot server response: " + ioe.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, ioe);
    }
    return segmentsMetadata;
  }

  /**
   * This is a helper method to get the metadata for all segments for a given table name.
   * @param tableNameWithType name of the table along with its type
   * @param columns name of the columns
   * @param numReplica num or replica for the table
   * @return aggregated metadata of the table segments
   */
  private JsonNode getAggregateMetadataFromServer(String tableNameWithType, List<String> columns, int numReplica)
      throws InvalidConfigException, IOException {
    TableMetadataReader tableMetadataReader =
        new TableMetadataReader(_executor, _connectionManager, _pinotHelixResourceManager);
    return tableMetadataReader.getAggregateTableMetadata(tableNameWithType, columns, numReplica,
        _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
  }
}
