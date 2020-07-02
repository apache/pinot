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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfigConstants;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.core.util.ReplicationUtils;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
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
  ControllerConf _controllerConf;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  ExecutorService _executorService;

  /**
   * API to create a table. Before adding, validations will be done (min number of replicas,
   * checking offline and realtime table configs match, checking for tenants existing)
   * @param tableConfigStr
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @ApiOperation(value = "Adds a table", notes = "Adds a table")
  public SuccessResponse addTable(String tableConfigStr) {
    // TODO introduce a table config ctor with json string.
    TableConfig tableConfig;
    try {
      tableConfig = JsonUtils.stringToObject(tableConfigStr, TableConfig.class);
      TableConfigUtils.validate(tableConfig);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
    String tableName = tableConfig.getTableName();
    try {
      ensureMinReplicas(tableConfig);
      verifyTableConfigs(tableConfig);
      _pinotHelixResourceManager.addTable(tableConfig);
      // TODO: validate that table was created successfully
      // (in realtime case, metadata might not have been created but would be created successfully in the next run of the validation manager)
      return new SuccessResponse("Table " + tableName + " succesfully added");
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
      if (e instanceof PinotHelixResourceManager.InvalidTableConfigException) {
        String errStr = String.format("Invalid table config for table %s: %s", tableName, e.getMessage());
        throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST, e);
      } else if (e instanceof PinotHelixResourceManager.TableAlreadyExistsException) {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
      } else {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
      }
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
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    try {
      if (stateStr == null) {
        return listTableConfigs(tableName, tableTypeStr);
      }

      StateType stateType = Constants.validateState(stateStr);
      TableType tableType = Constants.validateTableType(tableTypeStr);

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
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Updates table config for a table", notes = "Updates table config for a table")
  public SuccessResponse updateTableConfig(
      @ApiParam(value = "Name of the table to update", required = true) @PathParam("tableName") String tableName,
      String tableConfigString)
      throws Exception {
    TableConfig tableConfig;
    try {
      tableConfig = JsonUtils.stringToObject(tableConfigString, TableConfig.class);
      TableConfigUtils.validate(tableConfig);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Invalid table config", Response.Status.BAD_REQUEST, e);
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

      ensureMinReplicas(tableConfig);
      verifyTableConfigs(tableConfig);
      _pinotHelixResourceManager.updateTableConfig(tableConfig);
    } catch (PinotHelixResourceManager.InvalidTableConfigException e) {
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
    try {
      TableConfig tableConfig = JsonUtils.stringToObject(tableConfigStr, TableConfig.class);
      TableConfigUtils.validate(tableConfig);
      ObjectNode tableConfigValidateStr = JsonUtils.newObjectNode();
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        tableConfigValidateStr.set(TableType.OFFLINE.name(), tableConfig.toJsonNode());
      } else {
        tableConfigValidateStr.set(TableType.REALTIME.name(), tableConfig.toJsonNode());
      }
      return tableConfigValidateStr.toString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Invalid table config", Response.Status.BAD_REQUEST, e);
    }
  }

  private void ensureMinReplicas(TableConfig tableConfig) {
    // For self-serviced cluster, ensure that the tables are created with at least min replication factor irrespective
    // of table configuration value
    SegmentsValidationAndRetentionConfig segmentsConfig = tableConfig.getValidationConfig();
    int configMinReplication = _controllerConf.getDefaultTableMinReplicas();
    boolean verifyReplicasPerPartition;
    boolean verifyReplication;

    try {
      verifyReplicasPerPartition = ReplicationUtils.useReplicasPerPartition(tableConfig);
      verifyReplication = ReplicationUtils.useReplication(tableConfig);
    } catch (Exception e) {
      String errorMsg = String.format("Invalid tableIndexConfig or streamConfig: %s", e.getMessage());
      throw new PinotHelixResourceManager.InvalidTableConfigException(errorMsg, e);
    }

    if (verifyReplication) {
      int requestReplication;
      try {
        requestReplication = segmentsConfig.getReplicationNumber();
        if (requestReplication < configMinReplication) {
          LOGGER.info("Creating table with minimum replication factor of: {} instead of requested replication: {}",
              configMinReplication, requestReplication);
          segmentsConfig.setReplication(String.valueOf(configMinReplication));
        }
      } catch (NumberFormatException e) {
        throw new PinotHelixResourceManager.InvalidTableConfigException("Invalid replication number", e);
      }
    }

    if (verifyReplicasPerPartition) {
      String replicasPerPartitionStr = segmentsConfig.getReplicasPerPartition();
      if (replicasPerPartitionStr == null) {
        throw new PinotHelixResourceManager.InvalidTableConfigException(
            "Field replicasPerPartition needs to be specified");
      }
      try {
        int replicasPerPartition = Integer.valueOf(replicasPerPartitionStr);
        if (replicasPerPartition < configMinReplication) {
          LOGGER.info(
              "Creating table with minimum replicasPerPartition of: {} instead of requested replicasPerPartition: {}",
              configMinReplication, replicasPerPartition);
          segmentsConfig.setReplicasPerPartition(String.valueOf(configMinReplication));
        }
      } catch (NumberFormatException e) {
        throw new PinotHelixResourceManager.InvalidTableConfigException(
            "Invalid value for replicasPerPartition: '" + replicasPerPartitionStr + "'", e);
      }
    }
  }

  /**
   * Verify table configs if it's a hybrid table, i.e. having both offline and real-time sub-tables.
   */
  private void verifyTableConfigs(TableConfig newTableConfig) {
    String rawTableName = TableNameBuilder.extractRawTableName(newTableConfig.getTableName());
    LOGGER.info("Validating table configs for Table: {}", rawTableName);

    TableConfig tableConfigToCompare = null;
    if (newTableConfig.getTableType() == TableType.REALTIME) {
      if (_pinotHelixResourceManager.hasOfflineTable(rawTableName)) {
        tableConfigToCompare = _pinotHelixResourceManager.getOfflineTableConfig(rawTableName);
      }
    } else {
      if (_pinotHelixResourceManager.hasRealtimeTable(rawTableName)) {
        tableConfigToCompare = _pinotHelixResourceManager.getRealtimeTableConfig(rawTableName);
      }
    }

    // Check if it is a hybrid table or not. If not, there's no need to verify anything.
    if (tableConfigToCompare == null) {
      LOGGER.info(
          "Table: {} is not a hybrid table. Skipping consistency check across realtime and offline parts of the table.",
          rawTableName);
      return;
    }

    SegmentsValidationAndRetentionConfig newSegmentConfig = newTableConfig.getValidationConfig();
    SegmentsValidationAndRetentionConfig SegmentConfigToCompare = tableConfigToCompare.getValidationConfig();

    String newTimeColumnName = newSegmentConfig.getTimeColumnName();
    String existingTimeColumnName = SegmentConfigToCompare.getTimeColumnName();
    if (!Objects.equal(existingTimeColumnName, newTimeColumnName)) {
      throw new PinotHelixResourceManager.InvalidTableConfigException(String
          .format("Time column names are different! Existing time column name: %s. New time column name: %s",
              existingTimeColumnName, newTimeColumnName));
    }

    TimeUnit existingTimeColumnType = SegmentConfigToCompare.getTimeType();
    TimeUnit newTimeColumnType = newSegmentConfig.getTimeType();
    if (existingTimeColumnType != newTimeColumnType) {
      throw new PinotHelixResourceManager.InvalidTableConfigException(String
          .format("Time column types are different! Existing time column type: %s. New time column type: %s",
              existingTimeColumnType, newTimeColumnType));
    }

    // TODO: validate time unit size but now there's no metadata for that in table config.
    LOGGER.info("Finished validating tables config for Table: {}", rawTableName);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
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
    TableType tableType;
    try {
      tableType = TableType.valueOf(tableTypeStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new ControllerApplicationException(LOGGER, "Illegal table type: " + tableTypeStr,
          Response.Status.BAD_REQUEST);
    }
    String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);

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
}
