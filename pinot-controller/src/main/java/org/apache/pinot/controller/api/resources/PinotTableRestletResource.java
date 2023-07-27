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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.AccessOption;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.exception.SchemaNotFoundException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.controllerjob.ControllerJobType;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.TableSegmentValidationInfo;
import org.apache.pinot.common.utils.helix.HelixHelper;
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
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceProgressStats;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.controller.recommender.RecommenderDriver;
import org.apache.pinot.controller.tuner.TableConfigTunerUtils;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.controller.util.TableIngestionStatusHelper;
import org.apache.pinot.controller.util.TableMetadataReader;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableStats;
import org.apache.pinot.spi.config.table.TableStatus;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.zookeeper.data.Stat;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.TABLE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
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

  public static final Logger LOGGER = LoggerFactory.getLogger(PinotTableRestletResource.class);

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
  @ManualAuthorization // performed after parsing table configs
  public ConfigSuccessResponse addTable(String tableConfigStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    // TODO introduce a table config ctor with json string.
    Pair<TableConfig, Map<String, Object>> tableConfigAndUnrecognizedProperties;
    TableConfig tableConfig;
    String tableName;
    try {
      tableConfigAndUnrecognizedProperties =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigStr, TableConfig.class);
      tableConfig = tableConfigAndUnrecognizedProperties.getLeft();

      // validate permission
      tableName = tableConfig.getTableName();
      String endpointUrl = request.getRequestURL().toString();
      AccessControlUtils.validatePermission(tableName, AccessType.CREATE, httpHeaders, endpointUrl,
          _accessControlFactory.create());
      if (!_accessControlFactory.create()
          .hasAccess(httpHeaders, TargetType.TABLE, tableName, Actions.Table.CREATE_TABLE)) {
        throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
      }

      Schema schema = _pinotHelixResourceManager.getSchemaForTableConfig(tableConfig);

      TableConfigTunerUtils.applyTunerConfigs(_pinotHelixResourceManager, tableConfig, schema, Collections.emptyMap());

      // TableConfigUtils.validate(...) is used across table create/update.
      TableConfigUtils.validate(tableConfig, schema, typesToSkip, _controllerConf.isDisableIngestionGroovy());
      // TableConfigUtils.validateTableName(...) checks table name rules.
      // So it won't affect already created tables.
      boolean allowTableNameWithDatabase =
          _controllerConf.getProperty(CommonConstants.Helix.ALLOW_TABLE_NAME_WITH_DATABASE,
              CommonConstants.Helix.DEFAULT_ALLOW_TABLE_NAME_WITH_DATABASE);
      TableConfigUtils.validateTableName(tableConfig, allowTableNameWithDatabase);
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
      // (in realtime case, metadata might not have been created but would be created successfully in the next run of
      // the validation manager)
      return new ConfigSuccessResponse("Table " + tableName + " successfully added",
          tableConfigAndUnrecognizedProperties.getRight());
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
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.RECOMMEND_CONFIG)
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
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TABLE)
  @ApiOperation(value = "Lists all tables in cluster", notes = "Lists all tables in cluster")
  public String listTables(@ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Task type") @QueryParam("taskType") String taskType,
      @ApiParam(value = "name|creationTime|lastModifiedTime") @QueryParam("sortType") String sortTypeStr,
      @ApiParam(value = "true|false") @QueryParam("sortAsc") @DefaultValue("true") boolean sortAsc) {
    try {
      TableType tableType = null;
      if (tableTypeStr != null) {
        tableType = TableType.valueOf(tableTypeStr.toUpperCase());
      }
      SortType sortType = sortTypeStr != null ? SortType.valueOf(sortTypeStr.toUpperCase()) : SortType.NAME;

      List<String> tableNamesWithType = tableType == null ? _pinotHelixResourceManager.getAllTables()
          : (tableType == TableType.REALTIME ? _pinotHelixResourceManager.getAllRealtimeTables()
              : _pinotHelixResourceManager.getAllOfflineTables());

      if (StringUtils.isNotBlank(taskType)) {
        Set<String> tableNamesForTaskType = new HashSet<>();
        for (String tableNameWithType : tableNamesWithType) {
          TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
          if (tableConfig != null && tableConfig.getTaskConfig() != null && tableConfig.getTaskConfig()
              .isTaskTypeEnabled(taskType)) {
            tableNamesForTaskType.add(tableNameWithType);
          }
        }
        tableNamesWithType.retainAll(tableNamesForTaskType);
      }

      List<String> tableNames;
      if (sortType == SortType.NAME) {
        if (tableType == null && StringUtils.isBlank(taskType)) {
          List<String> rawTableNames = tableNamesWithType.stream().map(TableNameBuilder::extractRawTableName).distinct()
              .collect(Collectors.toList());
          rawTableNames.sort(sortAsc ? null : Comparator.reverseOrder());
          tableNames = rawTableNames;
        } else {
          tableNamesWithType.sort(sortAsc ? null : Comparator.reverseOrder());
          tableNames = tableNamesWithType;
        }
      } else {
        int sortFactor = sortAsc ? 1 : -1;
        ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
        int numTables = tableNamesWithType.size();
        List<String> zkPaths = new ArrayList<>(numTables);
        for (String tableNameWithType : tableNamesWithType) {
          zkPaths.add(ZKMetadataProvider.constructPropertyStorePathForResourceConfig(tableNameWithType));
        }
        Stat[] stats = propertyStore.getStats(zkPaths, AccessOption.PERSISTENT);
        for (int i = 0; i < numTables; i++) {
          Preconditions.checkState(stats[i] != null, "Failed to read ZK stats for table: %s",
              tableNamesWithType.get(i));
        }
        IntComparator comparator;
        if (sortType == SortType.CREATIONTIME) {
          comparator = (i, j) -> Long.compare(stats[i].getCtime(), stats[j].getCtime()) * sortFactor;
        } else {
          assert sortType == SortType.LASTMODIFIEDTIME;
          comparator = (i, j) -> Long.compare(stats[i].getMtime(), stats[j].getMtime()) * sortFactor;
        }
        Swapper swapper = (i, j) -> {
          Stat tempStat = stats[i];
          stats[i] = stats[j];
          stats[j] = tempStat;

          String tempTableName = tableNamesWithType.get(i);
          tableNamesWithType.set(i, tableNamesWithType.get(j));
          tableNamesWithType.set(j, tempTableName);
        };
        Arrays.quickSort(0, numTables, comparator, swapper);
        tableNames = tableNamesWithType;
      }

      return JsonUtils.newObjectNode().set("tables", JsonUtils.objectToJsonNode(tableNames)).toString();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private enum SortType {
    NAME, CREATIONTIME, LASTMODIFIEDTIME
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
  @ManualAuthorization
  @Path("/tables/{tableName}")
  @ApiOperation(value = "Lists the table configs")
  public String alterTableStateOrListTableConfig(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "enable|disable|drop") @QueryParam("state") String stateStr,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    try {
      if (StringUtils.isBlank(stateStr)) {
        if (!_accessControlFactory.create()
            .hasAccess(httpHeaders, TargetType.TABLE, tableName, Actions.Table.GET_TABLE_CONFIG)) {
          throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
        }
        return listTableConfigs(tableName, tableTypeStr);
      }

      // TODO: DO NOT allow toggling state with GET request

      StateType stateType = Constants.validateState(stateStr);
      TableType tableType = Constants.validateTableType(tableTypeStr);

      // validate if user has permission to change the table state
      String endpointUrl = request.getRequestURL().toString();
      AccessControlUtils.validatePermission(tableName, AccessType.UPDATE, httpHeaders, endpointUrl,
          _accessControlFactory.create());

      // Check access for different state types
      AccessControl accessControl = _accessControlFactory.create();
      switch (stateType) {
        case ENABLE:
          if (!accessControl.hasAccess(httpHeaders, TargetType.TABLE, tableName, Actions.Table.ENABLE_TABLE)) {
            throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
          }
          break;
        case DISABLE:
          if (!accessControl.hasAccess(httpHeaders, TargetType.TABLE, tableName, Actions.Table.DISABLE_TABLE)) {
            throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
          }
          break;
        case DROP:
          if (!accessControl.hasAccess(httpHeaders, TargetType.TABLE, tableName, Actions.Table.DELETE_TABLE)) {
            throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
          }
          break;
        default:
          throw new ControllerApplicationException(LOGGER, "Invalid state type: " + stateType,
              Response.Status.BAD_REQUEST);
      }

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
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.DELETE_TABLE)
  @Authenticate(AccessType.DELETE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Deletes a table", notes = "Deletes a table")
  public SuccessResponse deleteTable(
      @ApiParam(value = "Name of the table to delete", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Retention period for the table segments (e.g. 12h, 3d); If not set, the retention period "
          + "will default to the first config that's not null: the cluster setting, then '7d'. Using 0d or -1d will "
          + "instantly delete segments without retention") @QueryParam("retention") String retentionPeriod) {
    TableType tableType = Constants.validateTableType(tableTypeStr);

    List<String> tablesDeleted = new LinkedList<>();
    try {
      boolean tableExist = false;
      if (verifyTableType(tableName, tableType, TableType.OFFLINE)) {
        tableExist = _pinotHelixResourceManager.hasOfflineTable(tableName);
        // Even the table name does not exist, still go on to delete remaining table metadata in case a previous delete
        // did not complete.
        _pinotHelixResourceManager.deleteOfflineTable(tableName, retentionPeriod);
        if (tableExist) {
          tablesDeleted.add(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
        }
      }
      if (verifyTableType(tableName, tableType, TableType.REALTIME)) {
        tableExist = _pinotHelixResourceManager.hasRealtimeTable(tableName);
        // Even the table name does not exist, still go on to delete remaining table metadata in case a previous delete
        // did not complete.
        _pinotHelixResourceManager.deleteRealtimeTable(tableName, retentionPeriod);
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
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.UPDATE_TABLE_CONFIG)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Updates table config for a table", notes = "Updates table config for a table")
  public ConfigSuccessResponse updateTableConfig(
      @ApiParam(value = "Name of the table to update", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, String tableConfigString)
      throws Exception {
    Pair<TableConfig, Map<String, Object>> tableConfigJsonPojoWithUnparsableProps;
    TableConfig tableConfig;
    try {
      tableConfigJsonPojoWithUnparsableProps =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigString, TableConfig.class);
      tableConfig = tableConfigJsonPojoWithUnparsableProps.getLeft();
      Schema schema = _pinotHelixResourceManager.getSchemaForTableConfig(tableConfig);
      TableConfigUtils.validate(tableConfig, schema, typesToSkip, _controllerConf.isDisableIngestionGroovy());
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
    return new ConfigSuccessResponse("Table config updated for " + tableName,
        tableConfigJsonPojoWithUnparsableProps.getRight());
  }

  @POST
  @Path("/tables/validate")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validate table config for a table",
      notes = "This API returns the table config that matches the one you get from 'GET /tables/{tableName}'."
          + " This allows us to validate table config before apply.")
  @ManualAuthorization // performed after parsing TableConfig
  public ObjectNode checkTableConfig(String tableConfigStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    Pair<TableConfig, Map<String, Object>> tableConfig;
    try {
      tableConfig = JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigStr, TableConfig.class);
    } catch (IOException e) {
      String msg = String.format("Invalid table config json string: %s", tableConfigStr);
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }

    // validate permission
    String tableName = tableConfig.getLeft().getTableName();
    String endpointUrl = request.getRequestURL().toString();
    AccessControlUtils.validatePermission(tableName, AccessType.READ, httpHeaders, endpointUrl,
        _accessControlFactory.create());
    if (!_accessControlFactory.create()
        .hasAccess(httpHeaders, TargetType.TABLE, tableName, Actions.Table.VALIDATE_TABLE_CONFIGS)) {
      throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
    }

    ObjectNode validationResponse =
        validateConfig(tableConfig.getLeft(), _pinotHelixResourceManager.getSchemaForTableConfig(tableConfig.getLeft()),
            typesToSkip);
    validationResponse.set("unrecognizedProperties", JsonUtils.objectToJsonNode(tableConfig.getRight()));
    return validationResponse;
  }

  @Deprecated
  @POST
  @Path("/tables/validateTableAndSchema")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validate table config for a table along with specified schema",
      notes = "Deprecated. Use /tableConfigs/validate instead."
          + "Validate given table config and schema. If specified schema is null, attempt to retrieve schema using the "
          + "table name. This API returns the table config that matches the one you get from 'GET /tables/{tableName}'."
          + " This allows us to validate table config before apply.")
  @ManualAuthorization // performed after parsing TableAndSchemaConfig
  public String validateTableAndSchema(TableAndSchemaConfig tableSchemaConfig,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip, @Context HttpHeaders httpHeaders,
      @Context Request request) {
    TableConfig tableConfig = tableSchemaConfig.getTableConfig();
    Schema schema = tableSchemaConfig.getSchema();

    if (schema == null) {
      schema = _pinotHelixResourceManager.getSchemaForTableConfig(tableConfig);
    }

    // validate permission
    String schemaName = schema != null ? schema.getSchemaName() : null;
    String endpointUrl = request.getRequestURL().toString();
    AccessControlUtils.validatePermission(schemaName, AccessType.READ, httpHeaders, endpointUrl,
        _accessControlFactory.create());
    if (!_accessControlFactory.create()
        .hasAccess(httpHeaders, TargetType.TABLE, tableConfig.getTableName(), Actions.Table.VALIDATE_TABLE_CONFIGS)) {
      throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
    }

    return validateConfig(tableSchemaConfig.getTableConfig(), schema, typesToSkip).toString();
  }

  private ObjectNode validateConfig(TableConfig tableConfig, Schema schema, @Nullable String typesToSkip) {
    try {
      if (schema == null) {
        throw new SchemaNotFoundException("Got empty schema");
      }
      TableConfigUtils.validate(tableConfig, schema, typesToSkip, _controllerConf.isDisableIngestionGroovy());
      ObjectNode tableConfigValidateStr = JsonUtils.newObjectNode();
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        tableConfigValidateStr.set(TableType.OFFLINE.name(), tableConfig.toJsonNode());
      } else {
        tableConfigValidateStr.set(TableType.REALTIME.name(), tableConfig.toJsonNode());
      }
      return tableConfigValidateStr;
    } catch (Exception e) {
      String msg = String.format("Invalid table config: %s. %s", tableConfig.getTableName(), e.getMessage());
      throw new ControllerApplicationException(LOGGER, msg, Response.Status.BAD_REQUEST, e);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/tables/{tableName}/rebalance")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.REBALANCE_TABLE)
  @ApiOperation(value = "Rebalances a table (reassign instances and segments for a table)",
      notes = "Rebalances a table (reassign instances and segments for a table)")
  public RebalanceResult rebalance(
      @ApiParam(value = "Name of the table to rebalance", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Whether to rebalance table in dry-run mode") @DefaultValue("false") @QueryParam("dryRun")
      boolean dryRun,
      @ApiParam(value = "Whether to reassign instances before reassigning segments") @DefaultValue("false")
      @QueryParam("reassignInstances") boolean reassignInstances,
      @ApiParam(value = "Whether to reassign CONSUMING segments for real-time table") @DefaultValue("false")
      @QueryParam("includeConsuming") boolean includeConsuming, @ApiParam(
      value = "Whether to rebalance table in bootstrap mode (regardless of minimum segment movement, reassign all "
          + "segments in a round-robin fashion as if adding new segments to an empty table)") @DefaultValue("false")
  @QueryParam("bootstrap") boolean bootstrap,
      @ApiParam(value = "Whether to allow downtime for the rebalance") @DefaultValue("false") @QueryParam("downtime")
      boolean downtime,
      @ApiParam(value = "For no-downtime rebalance, minimum number of replicas to keep alive during rebalance, or "
          + "maximum number of replicas allowed to be unavailable if value is negative") @DefaultValue("1")
  @QueryParam("minAvailableReplicas") int minAvailableReplicas, @ApiParam(
      value = "Whether to use best-efforts to rebalance (not fail the rebalance when the no-downtime contract cannot "
          + "be achieved)") @DefaultValue("false") @QueryParam("bestEfforts") boolean bestEfforts, @ApiParam(
      value = "How often to check if external view converges with ideal states") @DefaultValue("1000")
  @QueryParam("externalViewCheckIntervalInMs") long externalViewCheckIntervalInMs,
      @ApiParam(value = "How long to wait till external view converges with ideal states") @DefaultValue("3600000")
      @QueryParam("externalViewStabilizationTimeoutInMs") long externalViewStabilizationTimeoutInMs,
      @ApiParam(value = "Whether to update segment target tier as part of the rebalance") @DefaultValue("false")
      @QueryParam("updateTargetTier") boolean updateTargetTier) {

    String tableNameWithType = constructTableNameWithType(tableName, tableTypeStr);

    Configuration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.addProperty(RebalanceConfigConstants.DRY_RUN, dryRun);
    rebalanceConfig.addProperty(RebalanceConfigConstants.REASSIGN_INSTANCES, reassignInstances);
    rebalanceConfig.addProperty(RebalanceConfigConstants.INCLUDE_CONSUMING, includeConsuming);
    rebalanceConfig.addProperty(RebalanceConfigConstants.BOOTSTRAP, bootstrap);
    rebalanceConfig.addProperty(RebalanceConfigConstants.DOWNTIME, downtime);
    rebalanceConfig.addProperty(RebalanceConfigConstants.MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME, minAvailableReplicas);
    rebalanceConfig.addProperty(RebalanceConfigConstants.BEST_EFFORTS, bestEfforts);
    rebalanceConfig.addProperty(RebalanceConfigConstants.EXTERNAL_VIEW_CHECK_INTERVAL_IN_MS,
        externalViewCheckIntervalInMs);
    rebalanceConfig.addProperty(RebalanceConfigConstants.EXTERNAL_VIEW_STABILIZATION_TIMEOUT_IN_MS,
        externalViewStabilizationTimeoutInMs);
    rebalanceConfig.addProperty(RebalanceConfigConstants.UPDATE_TARGET_TIER, updateTargetTier);
    rebalanceConfig.addProperty(RebalanceConfigConstants.JOB_ID, TableRebalancer.createUniqueRebalanceJobIdentifier());

    try {
      if (dryRun || downtime) {
        // For dry-run or rebalance with downtime, directly return the rebalance result as it should return immediately
        return _pinotHelixResourceManager.rebalanceTable(tableNameWithType, rebalanceConfig, false);
      } else {
        // Make a dry-run first to get the target assignment
        rebalanceConfig.setProperty(RebalanceConfigConstants.DRY_RUN, true);
        RebalanceResult dryRunResult =
            _pinotHelixResourceManager.rebalanceTable(tableNameWithType, rebalanceConfig, false);

        if (dryRunResult.getStatus() == RebalanceResult.Status.DONE) {
          // If dry-run succeeded, run rebalance asynchronously
          rebalanceConfig.setProperty(RebalanceConfigConstants.DRY_RUN, false);
          _executorService.submit(() -> {
            try {
              _pinotHelixResourceManager.rebalanceTable(tableNameWithType, rebalanceConfig, true);
            } catch (Throwable t) {
              LOGGER.error("Caught exception/error while rebalancing table: {}", tableNameWithType, t);
            }
          });
          return new RebalanceResult(dryRunResult.getJobId(), RebalanceResult.Status.IN_PROGRESS,
              "In progress, check controller logs for updates", dryRunResult.getInstanceAssignment(),
              dryRunResult.getTierInstanceAssignment(), dryRunResult.getSegmentAssignment());
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
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_STATE)
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

  @PUT
  @Path("/tables/{tableName}/state")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Enable/disable a table", notes = "Enable/disable a table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  public SuccessResponse toggleTableState(
      @ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "enable|disable", required = true) @QueryParam("state") String state) {
    String tableNameWithType = constructTableNameWithType(tableName, tableTypeStr);
    StateType stateType;
    if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
      stateType = StateType.ENABLE;
    } else if (StateType.DISABLE.name().equalsIgnoreCase(state)) {
      stateType = StateType.DISABLE;
    } else {
      throw new ControllerApplicationException(LOGGER, "Unknown state '" + state + "'", Response.Status.BAD_REQUEST);
    }
    if (!_pinotHelixResourceManager.hasTable(tableNameWithType)) {
      throw new ControllerApplicationException(LOGGER, "Table '" + tableName + "' does not exist",
          Response.Status.NOT_FOUND);
    }
    PinotResourceManagerResponse response = _pinotHelixResourceManager.toggleTableState(tableNameWithType, stateType);
    if (response.isSuccessful()) {
      return new SuccessResponse("Request to " + state + " table '" + tableNameWithType + "' is successful");
    } else {
      throw new ControllerApplicationException(LOGGER,
          "Failed to " + state + " table '" + tableNameWithType + "': " + response.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/rebalanceStatus/{jobId}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_REBALANCE_STATUS)
  @ApiOperation(value = "Gets detailed stats of a rebalance operation",
      notes = "Gets detailed stats of a rebalance operation")
  public ServerRebalanceJobStatusResponse rebalanceStatus(
      @ApiParam(value = "Rebalance Job Id", required = true) @PathParam("jobId") String jobId)
      throws JsonProcessingException {
    Map<String, String> controllerJobZKMetadata =
        _pinotHelixResourceManager.getControllerJobZKMetadata(jobId, ControllerJobType.TABLE_REBALANCE);

    if (controllerJobZKMetadata == null) {
      throw new ControllerApplicationException(LOGGER, "Failed to find controller job id: " + jobId,
          Response.Status.NOT_FOUND);
    }
    TableRebalanceProgressStats tableRebalanceProgressStats =
        JsonUtils.stringToObject(controllerJobZKMetadata.get(RebalanceConfigConstants.REBALANCE_PROGRESS_STATS),
            TableRebalanceProgressStats.class);
    long timeSinceStartInSecs = 0L;
    if (!tableRebalanceProgressStats.getStatus().equals(RebalanceResult.Status.DONE)) {
      timeSinceStartInSecs =
          (System.currentTimeMillis() - tableRebalanceProgressStats.getStartTimeMs()) / 1000;
    }

    ServerRebalanceJobStatusResponse serverRebalanceJobStatusResponse = new ServerRebalanceJobStatusResponse();
    serverRebalanceJobStatusResponse.setTableRebalanceProgressStats(tableRebalanceProgressStats);
    serverRebalanceJobStatusResponse.setTimeElapsedSinceStartInSeconds(timeSinceStartInSecs);
    return serverRebalanceJobStatusResponse;
  }

  @GET
  @Path("/tables/{tableName}/stats")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "table stats", notes = "Provides metadata info/stats about the table.")
  public String getTableStats(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    ObjectNode ret = JsonUtils.newObjectNode();
    if ((tableTypeStr == null || TableType.OFFLINE.name().equalsIgnoreCase(tableTypeStr))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName);
      TableStats tableStats = _pinotHelixResourceManager.getTableStats(tableNameWithType);
      ret.set(TableType.OFFLINE.name(), JsonUtils.objectToJsonNode(tableStats));
    }
    if ((tableTypeStr == null || TableType.REALTIME.name().equalsIgnoreCase(tableTypeStr))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
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
        TableConfigUtils.verifyHybridTableConfigs(rawTableName,
            _pinotHelixResourceManager.getOfflineTableConfig(rawTableName), tableConfig);
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
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
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
        ingestionStatus =
            TableIngestionStatusHelper.getOfflineTableIngestionStatus(tableNameWithType, _pinotHelixResourceManager,
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
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the aggregate metadata of all segments for a table",
      notes = "Get the aggregate metadata of all segments for a table")
  public String getTableAggregateMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Columns name", allowMultiple = true) @QueryParam("columns") @DefaultValue("")
      List<String> columns) {
    LOGGER.info("Received a request to fetch aggregate metadata for a table {}", tableName);
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == TableType.REALTIME) {
      throw new ControllerApplicationException(LOGGER, "Table type : " + tableTypeStr + " not yet supported.",
          Response.Status.NOT_IMPLEMENTED);
    }
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    int numReplica = tableConfig == null ? 1 : tableConfig.getReplication();

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

  @GET
  @Path("table/{tableName}/jobs")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_CONTROLLER_JOBS)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get list of controller jobs for this table",
      notes = "Get list of controller jobs for this table")
  public Map<String, Map<String, String>> getControllerJobs(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Comma separated list of job types") @QueryParam("jobTypes") @Nullable String jobTypesString) {
    TableType tableTypeFromRequest = Constants.validateTableType(tableTypeStr);
    List<String> tableNamesWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableTypeFromRequest,
            LOGGER);
    Set<ControllerJobType> validJobTypes =
        java.util.Arrays.stream(ControllerJobType.values()).collect(Collectors.toSet());
    Set<ControllerJobType> jobTypesToFilter = null;
    if (StringUtils.isNotEmpty(jobTypesString)) {
      try {
        jobTypesToFilter = new HashSet<>(java.util.Arrays.asList(StringUtils.split(jobTypesString, ','))).stream()
            .map(type -> ControllerJobType.valueOf(type)).collect(Collectors.toSet());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Valid Types are: " + validJobTypes);
      }
    }
    Map<String, Map<String, String>> result = new HashMap<>();
    for (String tableNameWithType : tableNamesWithType) {
      result.putAll(_pinotHelixResourceManager.getAllJobsForTable(tableNameWithType,
          jobTypesToFilter == null ? validJobTypes : jobTypesToFilter));
    }

    return result;
  }

  @POST
  @Path("tables/{tableName}/timeBoundary")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.UPDATE_TABLE_CONFIG)
  @ApiOperation(value = "Set hybrid table query time boundary based on offline segments' metadata", notes = "Set "
      + "hybrid table query time boundary based on offline segments' metadata")
  @Produces(MediaType.APPLICATION_JSON)
  public SuccessResponse setTimeBoundary(
      @ApiParam(value = "Name of the hybrid table (without type suffix)", required = true) @PathParam("tableName")
      String tableName)
      throws Exception {
    // Validate its a hybrid table
    if (!_pinotHelixResourceManager.hasRealtimeTable(tableName) || !_pinotHelixResourceManager.hasOfflineTable(
        tableName)) {
      throw new ControllerApplicationException(LOGGER, "Table isn't a hybrid table", Response.Status.NOT_FOUND);
    }

    // Call all servers to validate all segments loaded and return the time boundary (max end time of all segments)
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    long timeBoundaryMs = validateSegmentStateForTable(offlineTableName);
    if (timeBoundaryMs < 0) {
      throw new ControllerApplicationException(LOGGER,
          "No segments found for offline table : " + offlineTableName + ". Could not update time boundary.",
          Response.Status.SERVICE_UNAVAILABLE);
    }

    // Set the timeBoundary in tableIdealState
    IdealState idealState =
        HelixHelper.updateIdealState(_pinotHelixResourceManager.getHelixZkManager(), offlineTableName, is -> {
          is.getRecord()
              .setSimpleField(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY, Long.toString(timeBoundaryMs));
          return is;
        }, RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 1.2f));

    if (idealState == null) {
      throw new ControllerApplicationException(LOGGER, "Could not update time boundary",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    return new SuccessResponse("Time boundary successfully updated to: " + timeBoundaryMs);
  }

  @DELETE
  @Path("tables/{tableName}/timeBoundary")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.DELETE_TIME_BOUNDARY)
  @ApiOperation(value = "Delete hybrid table query time boundary", notes = "Delete hybrid table query time boundary")
  @Produces(MediaType.APPLICATION_JSON)
  public SuccessResponse deleteTimeBoundary(
      @ApiParam(value = "Name of the hybrid table (without type suffix)", required = true) @PathParam("tableName")
      String tableName) {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (!_pinotHelixResourceManager.hasTable(offlineTableName)) {
      throw new ControllerApplicationException(LOGGER, "Failed to find table: " + offlineTableName,
          Response.Status.NOT_FOUND);
    }

    // Delete the timeBoundary in tableIdealState
    IdealState idealState =
        HelixHelper.updateIdealState(_pinotHelixResourceManager.getHelixZkManager(), offlineTableName, is -> {
          is.getRecord().getSimpleFields().remove(CommonConstants.IdealState.HYBRID_TABLE_TIME_BOUNDARY);
          return is;
        }, RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 1.2f));

    if (idealState == null) {
      throw new ControllerApplicationException(LOGGER, "Could not remove time boundary",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    return new SuccessResponse("Time boundary successfully removed");
  }

  private long validateSegmentStateForTable(String offlineTableName)
      throws InvalidConfigException, JsonProcessingException {
    // Call all servers to validate offline table state
    Map<String, List<String>> serverToSegments = _pinotHelixResourceManager.getServerToSegmentsMap(offlineTableName);
    BiMap<String, String> serverEndPoints =
        _pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, serverEndPoints);
    List<String> serverUrls = new ArrayList<>();
    BiMap<String, String> endpointsToServers = serverEndPoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String reloadTaskStatusEndpoint = endpoint + "/tables/" + offlineTableName + "/allSegmentsLoaded";
      serverUrls.add(reloadTaskStatusEndpoint);
    }

    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, null, true, 10000);

    if (serviceResponse._failedResponseCount > 0) {
      throw new ControllerApplicationException(LOGGER, "Could not validate table segment status",
          Response.Status.SERVICE_UNAVAILABLE);
    }

    long timeBoundaryMs = -1;
    // Validate all responses
    for (String response : serviceResponse._httpResponses.values()) {
      TableSegmentValidationInfo tableSegmentValidationInfo =
          JsonUtils.stringToObject(response, TableSegmentValidationInfo.class);
      if (!tableSegmentValidationInfo.isValid()) {
        throw new ControllerApplicationException(LOGGER, "Table segment validation failed",
            Response.Status.PRECONDITION_FAILED);
      }
      timeBoundaryMs = Math.max(timeBoundaryMs, tableSegmentValidationInfo.getMaxEndTimeMs());
    }

    return timeBoundaryMs;
  }
}
