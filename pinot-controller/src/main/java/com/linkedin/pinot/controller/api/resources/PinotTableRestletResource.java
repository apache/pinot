/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


@Api(tags = "table")
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
   * @see org.restlet.resource.ServerResource#get()
   */

  public static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PinotTableRestletResource.class);

  private final static String STATE = "state";
  private final static String TABLE_NAME = "tableName";

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @ApiOperation(value = "Adds a table", notes = "Adds a table")
  public SuccessResponse addTable(String tableConfigStr) {
    try {
      // TODO introduce a table config ctor with json string.
      JSONObject tableConfigJson = new JSONObject(tableConfigStr);
      TableConfig tableConfig = TableConfig.fromJSONConfig(tableConfigJson);
      final String tableName = tableConfig.getTableName();
      try {
        ensureMinReplicas(tableConfig);
        _pinotHelixResourceManager.addTable(tableConfig);
        return new SuccessResponse("Table " + tableName + " succesfully added");
      } catch (Exception e) {
        ControllerRestApplication.getControllerMetrics()
            .addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
        if (e instanceof PinotHelixResourceManager.InvalidTableConfigException) {
          LOGGER.info("Invalid table config for table: {}, {}", tableName, e.getMessage());
          throw new WebApplicationException("Invalid table config:" + e.getStackTrace(), Response.Status.BAD_REQUEST);
        } else if (e instanceof PinotHelixResourceManager.TableAlreadyExistsException) {
          LOGGER.info("Table: {} already exists", tableName);
          throw new WebApplicationException("Table already exists", Response.Status.BAD_REQUEST);
        } else {
          LOGGER.error("Caught exception while adding table: {}", tableName, e);
          throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
      }
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @ApiOperation(value = "Lists all tables in cluster", notes = "Lists all tables in cluster")
  public JSONObject listTableConfigs(
  ) {
    try {
      List<String> rawTables = _pinotHelixResourceManager.getAllRawTables();
      Collections.sort(rawTables);
      JSONArray tableArray = new JSONArray(rawTables);
      JSONObject resultObject = new JSONObject();
      resultObject.put("tables", tableArray);
      return resultObject;
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}")
  @ApiOperation(value = "Lists tableconfig for a table", notes = "Lists tableconfig for a table")
  public JSONObject listTableConfigs(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    try {
      JSONObject ret = new JSONObject();

      if ((tableTypeStr == null || CommonConstants.Helix.TableType.OFFLINE.name().equalsIgnoreCase(tableTypeStr)) && _pinotHelixResourceManager.hasOfflineTable(
          tableName)) {
        TableConfig tableConfig = _pinotHelixResourceManager.getOfflineTableConfig(tableName);
        Preconditions.checkNotNull(tableConfig);
        ret.put(CommonConstants.Helix.TableType.OFFLINE.name(), TableConfig.toJSONConfig(tableConfig));
      }

      if ((tableTypeStr == null || CommonConstants.Helix.TableType.REALTIME.name().equalsIgnoreCase(tableTypeStr)) && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        TableConfig tableConfig = _pinotHelixResourceManager.getRealtimeTableConfig(tableName);
        Preconditions.checkNotNull(tableConfig);
        ret.put(CommonConstants.Helix.TableType.REALTIME.name(), TableConfig.toJSONConfig(tableConfig));
      }
      return ret;
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}")
  @ApiOperation(value = "Enable/Disable/Drop a table", notes = "Enable/Disable/Drop a table")
  public JSONArray alterTableState(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "enable|disable|drop", required = true) @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    try {
      JSONArray ret = new JSONArray();
      boolean tableExists = false;

      if ((tableTypeStr == null || CommonConstants.Helix.TableType.OFFLINE.name().equalsIgnoreCase(tableTypeStr)) && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
        String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        JSONObject offline = new JSONObject();
        tableExists = true;

        offline.put(TABLE_NAME, offlineTableName);
        offline.put(STATE, toggleTableState(offlineTableName, stateStr).toJSON().toString());
        ret.put(offline);
      }

      if ((tableTypeStr == null || CommonConstants.Helix.TableType.REALTIME.name().equalsIgnoreCase(tableTypeStr)) && _pinotHelixResourceManager
          .hasRealtimeTable(tableName)) {
        String realTimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
        JSONObject realTime = new JSONObject();
        tableExists = true;

        realTime.put(TABLE_NAME, realTimeTableName);
        realTime.put(STATE, toggleTableState(realTimeTableName, stateStr).toJSON().toString());
        ret.put(realTime);
      }
      if (tableExists) {
        return ret;
      } else {
        throw new WebApplicationException("Table '" + tableName + "' does not exist", Response.Status.BAD_REQUEST);
      }
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }

  }

  @DELETE
  @Path("/tables/{tableName}")
  @ApiOperation(value = "Deletes a table", notes = "Deletes a table")
  public SuccessResponse deleteTable(
      @ApiParam(value = "Name of the table to delete", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
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
      throw new WebApplicationException(e);
    }
  }

  @PUT
  @Path("/tables/{tableName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Updates table config for a table ", notes = "Deletes a table of specific type")
  public SuccessResponse updateTableConfig(
      @ApiParam(value = "Name of the table to update", required = true) @PathParam("tableName") String tableName,
      String tableConfigStr
  ) {
    TableConfig tableConfig;
    try {
      JSONObject tableConfigJson = new JSONObject(tableConfigStr);
      tableConfig = TableConfig.fromJSONConfig(tableConfigJson);
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }

    try {
      CommonConstants.Helix.TableType tableType = tableConfig.getTableType();
      String configTableName = tableConfig.getTableName();
      String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
      if (!configTableName.equals(tableNameWithType)) {
        throw new WebApplicationException(
            "Request table " + tableNameWithType + " does not match table name in the body " + configTableName,
            Response.Status.BAD_REQUEST);
      }

      if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
        if (!_pinotHelixResourceManager.hasOfflineTable(tableName)) {
          throw new WebApplicationException("Table " + tableName + " does not exist", Response.Status.BAD_REQUEST);
        }
      } else {
        if (!_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
          throw new WebApplicationException("Table " + tableName + " does not exist", Response.Status.BAD_REQUEST);
        }
      }

      ensureMinReplicas(tableConfig);
      _pinotHelixResourceManager.setExistingTableConfig(tableConfig, tableNameWithType, tableType);
    } catch (PinotHelixResourceManager.InvalidTableConfigException e) {
      LOGGER.info("Failed to update configuration for table {}, message: {}", tableName, e.getMessage());
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(
          ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
    } catch (Exception e) {
      LOGGER.error("Failed to update table configuration for table: {}", tableName, e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(
          ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new WebApplicationException("Internal error while updating table configuration", Response.Status.INTERNAL_SERVER_ERROR);
    }

    return new SuccessResponse("Table config updated for " + tableName);
  }

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
      return new PinotResourceManagerResponse("Failed: " + errorMessage, false);
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
      KafkaStreamMetadata kafkaStreamMetadata;
      try {
        kafkaStreamMetadata = new KafkaStreamMetadata(config.getIndexingConfig().getStreamConfigs());
      } catch (Exception e) {
        throw new PinotHelixResourceManager.InvalidTableConfigException("Invalid tableIndexConfig or streamConfigs", e);
      }
      verifyReplicasPerPartition = kafkaStreamMetadata.hasSimpleKafkaConsumerType();
      verifyReplication = kafkaStreamMetadata.hasHighLevelKafkaConsumerType();
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

}
