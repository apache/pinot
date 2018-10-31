/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.resources;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.exception.TableNotFoundException;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.restlet.resources.RebalanceResult;
import com.linkedin.pinot.common.restlet.resources.ResourceUtils;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import com.linkedin.pinot.controller.helix.core.rebalance.RebalanceUserConfigConstants;
import com.linkedin.pinot.core.realtime.stream.StreamConfig;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
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
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.EnumUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @ApiOperation(value = "Adds a table", notes = "Adds a table")
  public SuccessResponse addTable(String tableConfigStr) throws Exception {
    // TODO introduce a table config ctor with json string.
    TableConfig tableConfig;
    String tableName;
    try {
      JSONObject tableConfigJson = new JSONObject(tableConfigStr);
      tableConfig = TableConfig.fromJSONConfig(tableConfigJson);
      tableName = tableConfig.getTableName();
    } catch (IOException | JSONException | IllegalArgumentException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
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
        throw e;
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
      CommonConstants.Helix.TableType tableType = null;
      if (tableTypeStr != null) {
        tableType = CommonConstants.Helix.TableType.valueOf(tableTypeStr.toUpperCase());
      }

      if (tableType == null) {
        tableNames = _pinotHelixResourceManager.getAllRawTables();
      } else {
        if (tableType == CommonConstants.Helix.TableType.REALTIME) {
          tableNames = _pinotHelixResourceManager.getAllRealtimeTables();
        } else {
          tableNames = _pinotHelixResourceManager.getAllOfflineTables();
        }
      }

      Collections.sort(tableNames);
      JSONArray tableArray = new JSONArray(tableNames);
      JSONObject resultObject = new JSONObject();
      resultObject.put("tables", tableArray);
      return resultObject.toString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private String listTableConfigs(@Nonnull String tableName, @Nullable String tableTypeStr) {
    try {
      JSONObject ret = new JSONObject();

      if ((tableTypeStr == null || CommonConstants.Helix.TableType.OFFLINE.name().equalsIgnoreCase(tableTypeStr))
          && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
        TableConfig tableConfig = _pinotHelixResourceManager.getOfflineTableConfig(tableName);
        Preconditions.checkNotNull(tableConfig);
        ret.put(CommonConstants.Helix.TableType.OFFLINE.name(), TableConfig.toJSONConfig(tableConfig));
      }

      if ((tableTypeStr == null || CommonConstants.Helix.TableType.REALTIME.name().equalsIgnoreCase(tableTypeStr))
          && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        TableConfig tableConfig = _pinotHelixResourceManager.getRealtimeTableConfig(tableName);
        Preconditions.checkNotNull(tableConfig);
        ret.put(CommonConstants.Helix.TableType.REALTIME.name(), TableConfig.toJSONConfig(tableConfig));
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
      Constants.validateState(stateStr);
      JSONArray ret = new JSONArray();
      boolean tableExists = false;

      if ((tableTypeStr == null || CommonConstants.Helix.TableType.OFFLINE.name().equalsIgnoreCase(tableTypeStr))
          && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
        String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        JSONObject offline = new JSONObject();
        tableExists = true;

        offline.put(FileUploadPathProvider.TABLE_NAME, offlineTableName);
        offline.put(FileUploadPathProvider.STATE, toggleTableState(offlineTableName, stateStr));
        ret.put(offline);
      }

      if ((tableTypeStr == null || CommonConstants.Helix.TableType.REALTIME.name().equalsIgnoreCase(tableTypeStr))
          && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        String realTimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
        JSONObject realTime = new JSONObject();
        tableExists = true;

        realTime.put(FileUploadPathProvider.TABLE_NAME, realTimeTableName);
        realTime.put(FileUploadPathProvider.STATE, toggleTableState(realTimeTableName, stateStr));
        ret.put(realTime);
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
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr) {
    List<String> tablesDeleted = new LinkedList<>();
    try {
      if (tableTypeStr == null || tableTypeStr.equalsIgnoreCase(CommonConstants.Helix.TableType.OFFLINE.name())) {
        _pinotHelixResourceManager.deleteOfflineTable(tableName);
        tablesDeleted.add(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
      }
      if (tableTypeStr == null || tableTypeStr.equalsIgnoreCase(CommonConstants.Helix.TableType.REALTIME.name())) {
        _pinotHelixResourceManager.deleteRealtimeTable(tableName);
        tablesDeleted.add(TableNameBuilder.REALTIME.tableNameWithType(tableName));
      }
      return new SuccessResponse("Table deleted " + tablesDeleted);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @PUT
  @Path("/tables/{tableName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Updates table config for a table", notes = "Updates table config for a table")
  public SuccessResponse updateTableConfig(
      @ApiParam(value = "Name of the table to update", required = true) @PathParam("tableName") String tableName,
      String tableConfigStr) throws Exception {
    TableConfig tableConfig;
    try {
      JSONObject tableConfigJson = new JSONObject(tableConfigStr);
      tableConfig = TableConfig.fromJSONConfig(tableConfigJson);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    }

    try {
      CommonConstants.Helix.TableType tableType = tableConfig.getTableType();
      String configTableName = tableConfig.getTableName();
      String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
      if (!configTableName.equals(tableNameWithType)) {
        throw new ControllerApplicationException(LOGGER,
            "Request table " + tableNameWithType + " does not match table name in the body " + configTableName,
            Response.Status.BAD_REQUEST);
      }

      if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
        if (!_pinotHelixResourceManager.hasOfflineTable(tableName)) {
          throw new ControllerApplicationException(LOGGER, "Table " + tableName + " does not exist",
              Response.Status.BAD_REQUEST);
        }
      } else {
        if (!_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
          throw new ControllerApplicationException(LOGGER, "Table " + tableName + " does not exist",
              Response.Status.NOT_FOUND);
        }
      }

      ensureMinReplicas(tableConfig);
      verifyTableConfigs(tableConfig);
      _pinotHelixResourceManager.setExistingTableConfig(tableConfig, tableNameWithType, tableType);
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
  public String checkTableConfig(String tableConfigStr) throws Exception {
    try {
      JSONObject tableConfigValidateStr = new JSONObject();
      TableConfig tableConfig = TableConfig.fromJSONConfig(new JSONObject(tableConfigStr));
      if (tableConfig.getTableType() == CommonConstants.Helix.TableType.OFFLINE) {
        tableConfigValidateStr.put(CommonConstants.Helix.TableType.OFFLINE.name(),
            TableConfig.toJSONConfig(tableConfig));
      } else {
        tableConfigValidateStr.put(CommonConstants.Helix.TableType.REALTIME.name(),
            TableConfig.toJSONConfig(tableConfig));
      }
      return tableConfigValidateStr.toString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    }
  }

  // TODO: move this method into PinotHelixResourceManager
  private PinotResourceManagerResponse toggleTableState(String tableName, String state) {
    if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
      return _pinotHelixResourceManager.toggleTableState(tableName, true);
    } else if (StateType.DISABLE.name().equalsIgnoreCase(state)) {
      return _pinotHelixResourceManager.toggleTableState(tableName, false);
    } else if (StateType.DROP.name().equalsIgnoreCase(state)) {
      return _pinotHelixResourceManager.dropTable(tableName);
    } else {
      String errorMessage = "Invalid state: " + state + ", must be one of {enable|disable|drop}";
      LOGGER.info(errorMessage);
      return PinotResourceManagerResponse.failure(errorMessage);
    }
  }

  private void ensureMinReplicas(TableConfig config) {
    // For self-serviced cluster, ensure that the tables are created with at least min replication factor irrespective
    // of table configuration value
    SegmentsValidationAndRetentionConfig segmentsConfig = config.getValidationConfig();
    int configMinReplication = _controllerConf.getDefaultTableMinReplicas();
    boolean verifyReplicasPerPartition = false;
    boolean verifyReplication = true;

    if (config.getTableType() == CommonConstants.Helix.TableType.REALTIME) {
      StreamConfig streamConfig;
      try {
        streamConfig = new StreamConfig(config.getIndexingConfig().getStreamConfigs());
      } catch (Exception e) {
        String errorMsg = String.format("Invalid tableIndexConfig or streamConfig: %s", e.getMessage());
        throw new PinotHelixResourceManager.InvalidTableConfigException(errorMsg, e);
      }
      verifyReplicasPerPartition = streamConfig.hasLowLevelConsumerType();
      verifyReplication = streamConfig.hasHighLevelConsumerType();
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
    if (newTableConfig.getTableType() == CommonConstants.Helix.TableType.REALTIME) {
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
    if (!existingTimeColumnName.equals(newTimeColumnName)) {
      throw new PinotHelixResourceManager.InvalidTableConfigException(
          String.format("Time column names are different! Existing time column name: %s. New time column name: %s",
              existingTimeColumnName, newTimeColumnName));
    }

    String newTimeColumnType = newSegmentConfig.getTimeType();
    String existingTimeColumnType = SegmentConfigToCompare.getTimeType();
    if (!existingTimeColumnType.equalsIgnoreCase(newTimeColumnType)) {
      throw new PinotHelixResourceManager.InvalidTableConfigException(
          String.format("Time column types are different! Existing time column type: %s. New time column type: %s",
              existingTimeColumnType, newTimeColumnType));
    }

    // TODO: validate time unit size but now there's no metadata for that in table config.
    LOGGER.info("Finished validating tables config for Table: {}", rawTableName);
  }

  /**
   * Rebalance a table.
   * @return if in DRY_RUN, the target idealstate/partition-map is returned. Else an indication of success/failure in
   *         triggering the rebalance is returned.
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/rebalance")
  @ApiOperation(value = "Rebalances segments of a table across servers", notes = "Rebalances segments of a table across servers")
  public String rebalance(
      @ApiParam(value = "Name of the table to rebalance") @Nonnull @PathParam("tableName") String tableName,
      @ApiParam(value = "offline|realtime") @Nonnull @QueryParam("type") String tableType,
      @ApiParam(value = "true|false") @Nonnull @DefaultValue("true") @QueryParam("dryrun") Boolean dryRun,
      @ApiParam(value = "true|false") @DefaultValue("false") @QueryParam("includeConsuming") Boolean includeConsuming,
      @ApiParam(value = "true|false") @DefaultValue("false") @QueryParam("downtime") Boolean downtime) {

    if (tableType != null && !EnumUtils.isValidEnum(CommonConstants.Helix.TableType.class, tableType.toUpperCase())) {
      throw new ControllerApplicationException(LOGGER, "Illegal table type " + tableType, Response.Status.BAD_REQUEST);
    }

    Configuration rebalanceUserConfig = new PropertiesConfiguration();
    rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.DRYRUN, dryRun);
    rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.INCLUDE_CONSUMING, includeConsuming);
    rebalanceUserConfig.addProperty(RebalanceUserConfigConstants.DOWNTIME, downtime);

    TableType type = TableType.valueOf(tableType.toUpperCase());
    if (type == TableType.OFFLINE && (!_pinotHelixResourceManager.hasOfflineTable(tableName))
        || type == TableType.REALTIME && (!_pinotHelixResourceManager.hasRealtimeTable(tableName))) {
      throw new ControllerApplicationException(LOGGER, "Table " + tableName + " does not exist",
          Response.Status.NOT_FOUND);
    }

    RebalanceResult result;
    try {
      if (dryRun) {
        result = _pinotHelixResourceManager.rebalanceTable(tableName, type, rebalanceUserConfig);
        result.setStatus("Rebalance attempted in dry-run mode.");
      } else {
        // run rebalance asynchronously
        _executorService.submit(new Runnable() {
          @Override
          public void run() {
            try {
              _pinotHelixResourceManager.rebalanceTable(tableName, type, rebalanceUserConfig);
            } catch (Throwable e) {
              // catch all throwables to prevent losing the thread
              LOGGER.error("Encountered error during rebalance for table {}", tableName, e);
            }
          }
        });
        result = new RebalanceResult();
        result.setStatus("Rebalance for table " + tableName + " in progress. Check controller logs for updates.");
      }
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.NOT_FOUND);
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    }
    return ResourceUtils.convertToJsonString(result);
  }
}
