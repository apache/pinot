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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotTableRestletResource extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      PinotTableRestletResource.class);

  public PinotTableRestletResource()
      throws IOException {
    File baseDataDir = new File(_controllerConf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    File tempDir = new File(baseDataDir, "schemasTemp");
    if (!tempDir.exists()) {
      FileUtils.forceMkdir(tempDir);
    }
  }

  @Override
  @Post("json")
  public Representation post(Representation entity) {
    try {
      TableConfig tableConfig = TableConfig.fromJSONConfig(new JSONObject(entity.getText()));
      try {
        addTable(tableConfig);
      } catch (Exception e) {
        String tableName = tableConfig.getTableName();
        if (e instanceof PinotHelixResourceManager.InvalidTableConfigException) {
          LOGGER.info("Invalid table config for table: {}, {}", tableName, e.getMessage());
          setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
        } else if (e instanceof PinotHelixResourceManager.TableAlreadyExistsException) {
          LOGGER.info("Table: {} already exists", tableName);
          setStatus(Status.CLIENT_ERROR_CONFLICT);
        } else {
          LOGGER.error("Caught exception while adding table: {}", tableName, e);
          setStatus(Status.SERVER_ERROR_INTERNAL);
        }
        ControllerRestApplication.getControllerMetrics()
            .addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
        return new StringRepresentation("Failed: " + e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
      }
      return new StringRepresentation("Success");
    } catch (Exception e) {
      LOGGER.info("Caught exception while deserializing table config", e);
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation("Failed: " + e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
    }
  }

  @HttpVerb("post")
  @Summary("Adds a table")
  @Tags({ "table" })
  @Paths({ "/tables", "/tables/" })
  private void addTable(TableConfig config) throws IOException {
    ensureMinReplicas(config);

    _pinotHelixResourceManager.addTable(config);
  }

  private void ensureMinReplicas(TableConfig config) {
    // For self-serviced cluster, ensure that the tables are created with at least min replication factor irrespective
    // of table configuration value
    SegmentsValidationAndRetentionConfig segmentsConfig = config.getValidationConfig();
    int configMinReplication = _controllerConf.getDefaultTableMinReplicas();
    boolean verifyReplicasPerPartition = false;
    boolean verifyReplication = true;

    if (config.getTableType() == TableType.REALTIME) {
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
  @Override
  @Get
  public Representation get() {
    final String tableName = (String) getRequest().getAttributes().get(TABLE_NAME);
    final String state = getReference().getQueryAsForm().getValues(STATE);
    final String tableType = getReference().getQueryAsForm().getValues(TABLE_TYPE);

    if (tableType != null && !isValidTableType(tableType)) {
      String errorMessage = "Invalid table type: " + tableType + ", must be one of {offline|realtime}";
      LOGGER.info(errorMessage);
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation("Failed: " + errorMessage);
    }

    if (tableName == null) {
      try {
        return getAllTables();
      } catch (Exception e) {
        LOGGER.error("Caught exception while fetching table ", e);
        ControllerRestApplication.getControllerMetrics()
            .addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_GET_ERROR, 1L);
        setStatus(Status.SERVER_ERROR_INTERNAL);
        return exceptionToStringRepresentation(e);
      }
    }

    try {
      if (state == null) {
        return getTable(tableName, tableType);
      } else if (isValidState(state)) {
        return setTableState(tableName, tableType, state);
      } else {
        String errorMessage = "Invalid state: " + state + ", must be one of {enable|disable|drop}";
        LOGGER.info(errorMessage);
        setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
        return new StringRepresentation("Failed: " + errorMessage);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while fetching table ", e);
      ControllerRestApplication.getControllerMetrics()
          .addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_GET_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("get")
  @Summary("Views a table's configuration")
  @Tags({ "table" })
  @Paths({ "/tables/{tableName}", "/tables/{tableName}/" })
  private Representation getTable(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to toggle its state",
          required = true) String tableName,
      @Parameter(name = "type", in = "query", description = "Type of table, Offline or Realtime", required = true) String tableType)
      throws JSONException, IOException {
    JSONObject ret = new JSONObject();

    if ((tableType == null || TableType.OFFLINE.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      TableConfig tableConfig = _pinotHelixResourceManager.getOfflineTableConfig(tableName);
      Preconditions.checkNotNull(tableConfig);
      ret.put(TableType.OFFLINE.name(), TableConfig.toJSONConfig(tableConfig));
    }

    if ((tableType == null || TableType.REALTIME.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      TableConfig tableConfig = _pinotHelixResourceManager.getRealtimeTableConfig(tableName);
      Preconditions.checkNotNull(tableConfig);
      ret.put(TableType.REALTIME.name(), TableConfig.toJSONConfig(tableConfig));
    }

    return new StringRepresentation(ret.toString(2));
  }

  @HttpVerb("get")
  @Summary("Views all tables' configuration")
  @Tags({ "table" })
  @Paths({ "/tables", "/tables/" })
  private Representation getAllTables()
      throws JSONException {
    List<String> rawTables = _pinotHelixResourceManager.getAllRawTables();
    Collections.sort(rawTables);
    JSONArray tableArray = new JSONArray(rawTables);
    JSONObject resultObject = new JSONObject();
    resultObject.put("tables", tableArray);
    return new StringRepresentation(resultObject.toString(2));
  }

  @HttpVerb("get")
  @Summary("Enable, disable or drop a table")
  @Tags({ "table" })
  @Paths({ "/tables/{tableName}", "/table/{tableName}/" })
  private StringRepresentation setTableState(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to toggle its state",
          required = true) String tableName,
      @Parameter(name = "type", in = "query", description = "Type of table, Offline or Realtime", required = false) String type,
      @Parameter(name = "state", in = "query", description = "The desired table state, either enable or disable",
          required = true) String state)
      throws JSONException {

    JSONArray ret = new JSONArray();
    boolean tableExists = false;

    if ((type == null || TableType.OFFLINE.name().equalsIgnoreCase(type))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      JSONObject offline = new JSONObject();
      tableExists = true;

      offline.put(TABLE_NAME, offlineTableName);
      offline.put(STATE, toggleTableState(offlineTableName, state).toJSON().toString());
      ret.put(offline);
    }

    if ((type == null || TableType.REALTIME.name().equalsIgnoreCase(type))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      String realTimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      JSONObject realTime = new JSONObject();
      tableExists = true;

      realTime.put(TABLE_NAME, realTimeTableName);
      realTime.put(STATE, toggleTableState(realTimeTableName, state).toJSON().toString());
      ret.put(realTime);
    }
    if (tableExists) {
      return new StringRepresentation(ret.toString());
    } else {
      String errorMessage = "Table: " + tableName + " does not exist";
      LOGGER.info(errorMessage);
      setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation("Failed: " + errorMessage);
    }
  }

  /**
   * Set the state of the specified table to the specified value.
   *
   * @param tableName: Name of table for which to set the state.
   * @param state: One of [enable|disable|drop].
   * @return
   */
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
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new PinotResourceManagerResponse("Failed: " + errorMessage, false);
    }
  }

  @Override
  @Delete
  public Representation delete() {
    final String tableName = (String) getRequest().getAttributes().get(TABLE_NAME);
    final String type = getReference().getQueryAsForm().getValues(TABLE_TYPE);
    // TODO: fix the problem where deleteTable always return true unless tableName is null
    if (!deleteTable(tableName, type)) {
      String errorMessage = "Table: " + tableName + " does not exist";
      LOGGER.info(errorMessage);
      setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation("Failed: " + errorMessage);
    }
    return new StringRepresentation("Success");
  }

  @HttpVerb("delete")
  @Summary("Deletes a table")
  @Tags({ "table" })
  @Paths({ "/tables/{tableName}", "/tables/{tableName}/" })
  private boolean deleteTable(@Parameter(name = "tableName", in = "path",
      description = "The name of the table to delete", required = true) String tableName, @Parameter(name = "type",
      in = "query", description = "The type of table to delete, either offline or realtime") String type) {
    if (tableName == null) {
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return false;
    }
    if (type == null || type.equalsIgnoreCase(TableType.OFFLINE.name())) {
      _pinotHelixResourceManager.deleteOfflineTable(tableName);
    }
    if (type == null || type.equalsIgnoreCase(TableType.REALTIME.name())) {
      _pinotHelixResourceManager.deleteRealtimeTable(tableName);
    }
    return true;
  }

  @Override
  @Put
  public Representation put(Representation entity) {
    String inputTableName = (String) getRequest().getAttributes().get(TABLE_NAME);
    if (inputTableName == null) {
      LOGGER.info("Table name cannot be null");
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation("{\"error\" : \"Table name is required. Input: null\"}");
    }

    return updateTableConfig(inputTableName, entity);
  }

  /*
   * NOTE: There is inconsistency in these APIs. GET returns OFFLINE + REALTIME configuration
   * in a single response but POST and this PUT request only operate on either offline or realtime
   * table configuration. If we make this API take both realtime and offline table configuration
   * then the update is not guaranteed to be transactional for both table types. This is more of a PATCH request
   * than PUT.
   */
  @HttpVerb("put")
  @Summary("Update table configuration. Request body is offline or realtime table configuration")
  @Tags({"table"})
  @Paths({"/tables/{tableName}"})
  public Representation updateTableConfig(@Parameter(name = "tableName", in = "path",
      description = "Table name (without type)") String tableName, Representation entity) {
    TableConfig tableConfig;
    try {
      tableConfig = TableConfig.fromJSONConfig(new JSONObject(entity.getText()));
    } catch (JSONException e) {
      return errorResponseRepresentation(Status.CLIENT_ERROR_BAD_REQUEST, "Invalid json in table configuration");
    } catch (IOException e) {
      LOGGER.error("Failed to read request body while updating configuration for table: {}", tableName, e);
      return errorResponseRepresentation(Status.SERVER_ERROR_INTERNAL, "Failed to read request");
    }
    try {
      TableType tableType = tableConfig.getTableType();
      String configTableName = tableConfig.getTableName();
      String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
      if (!configTableName.equals(tableNameWithType)) {
        return errorResponseRepresentation(Status.CLIENT_ERROR_BAD_REQUEST,
            "Request table " + tableNameWithType + " does not match table name in the body " + configTableName);
      }

      if (tableType == TableType.OFFLINE) {
        if (!_pinotHelixResourceManager.hasOfflineTable(tableName)) {
          return errorResponseRepresentation(Status.CLIENT_ERROR_NOT_FOUND, "Table " + tableName + " does not exist");
        }
      } else {
        if (!_pinotHelixResourceManager.hasRealtimeTable(tableName)) {
          return errorResponseRepresentation(Status.CLIENT_ERROR_NOT_FOUND, "Table " + tableName + " does not exist");
        }
      }

      ensureMinReplicas(tableConfig);
      _pinotHelixResourceManager.setExistingTableConfig(tableConfig, tableNameWithType, tableType);
      return responseRepresentation(Status.SUCCESS_OK, "{\"status\" : \"Success\"}");
    } catch (PinotHelixResourceManager.InvalidTableConfigException e) {
      LOGGER.info("Failed to update configuration for table {}, message: {}", tableName, e.getMessage());
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(
          ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      return errorResponseRepresentation(Status.CLIENT_ERROR_BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOGGER.error("Failed to update table configuration for table: {}", tableName, e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(
          ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      return errorResponseRepresentation(Status.SERVER_ERROR_INTERNAL,
          "Internal error while updating table configuration");
    }
  }
}
