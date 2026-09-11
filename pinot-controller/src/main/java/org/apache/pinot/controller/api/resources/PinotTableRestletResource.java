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

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
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
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.helix.AccessOption;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.exception.RebalanceInProgressException;
import org.apache.pinot.common.exception.SchemaNotFoundException;
import org.apache.pinot.common.exception.TableConfigBackwardIncompatibleException;
import org.apache.pinot.common.exception.TableConfigVersionMismatchException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.response.server.TableIndexMetadataResponse;
import org.apache.pinot.common.restlet.resources.BatchConfig;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.restlet.resources.RebalanceResult;
import org.apache.pinot.common.restlet.resources.ServerRebalanceJobStatusResponse;
import org.apache.pinot.common.restlet.resources.TableSegmentValidationInfo;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.LogicalTableConfigUtils;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.InvalidTableConfigException;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.controller.helix.core.WatermarkInductionResult;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceManager;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.controller.recommender.RecommenderDriver;
import org.apache.pinot.controller.tuner.TableConfigTunerUtils;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.controller.util.TableIngestionStatusHelper;
import org.apache.pinot.controller.util.TableMetadataReader;
import org.apache.pinot.controller.util.TaskConfigUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableConfigRedactionUtils;
import org.apache.pinot.spi.config.table.TableConfigValidatorRegistry;
import org.apache.pinot.spi.config.table.TableStatsHumanReadable;
import org.apache.pinot.spi.config.table.TableStatus;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.controller.ControllerJobType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.ConfigValidationException;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.zookeeper.data.Stat;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.TABLE_TAG, authorizations = {
    @Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)
})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")
}))
@Path("/")
public class PinotTableRestletResource {
  /// URI Mappings:
  /// - "/tables", "/tables/": List all the tables
  /// - "/tables/{tableName}", "/tables/{tableName}/": List config for specified table.
  ///
  /// - "/tables/{tableName}?state={state}"
  ///   Set the state for the specified {tableName} to the specified {state} (enable|disable|drop).
  ///
  /// - "/tables/{tableName}?type={type}"
  ///   List all tables of specified type, type can be one of {offline|realtime}.
  ///
  ///   Set the state for the specified {tableName} to the specified {state} (enable|disable|drop).
  ///   \* - "/tables/{tableName}?state={state}&amp;type={type}"
  ///
  ///   Set the state for the specified {tableName} of specified type to the specified {state} (enable|disable|drop).
  ///   Type here is type of the table, one of 'offline|realtime'.
  /// {@inheritDoc}

  public static final Logger LOGGER = LoggerFactory.getLogger(PinotTableRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  TableRebalanceManager _tableRebalanceManager;

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

  @Inject
  Executor _executor;

  @Inject
  HttpClientConnectionManager _connectionManager;

  /// API to create a table. Before adding, validations will be done (min number of replicas, checking offline and
  /// realtime table configs match, checking for tenants existing).
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @ApiOperation(value = "Adds a table", notes = "Adds a table")
  @ManualAuthorization // performed after parsing table configs
  public ConfigSuccessResponse addTable(String tableConfigStr,
      @ApiParam(value = "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip,
      @DefaultValue("false") @QueryParam("ignoreActiveTasks") boolean ignoreActiveTasks,
      @Context HttpHeaders httpHeaders, @Context Request request)
      throws IOException {
    // TODO introduce a table config ctor with json string.
    Pair<TableConfig, Map<String, Object>> tableConfigAndUnrecognizedProperties;
    TableConfig tableConfig = null;
    String tableNameWithType;
    Schema schema;
    try {
      tableConfigAndUnrecognizedProperties =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigStr, TableConfig.class);
      tableConfig = tableConfigAndUnrecognizedProperties.getLeft();
      tableConfig = TableConfigRedactionUtils.restoreRedactedValues(tableConfig, null);
      tableNameWithType = DatabaseUtils.translateTableName(tableConfig.getTableName(), httpHeaders);
      tableConfig.setTableName(tableNameWithType);

      // validate permission
      ResourceUtils.checkPermissionAndAccess(tableNameWithType, request, httpHeaders,
          AccessType.CREATE, Actions.Table.CREATE_TABLE, _accessControlFactory, LOGGER);

      // fail if table entry is present in IS. This saves all the validation checks if table already exists
      if (_pinotHelixResourceManager.hasTable(tableNameWithType)) {
        throw new TableAlreadyExistsException("Table config for " + tableNameWithType
            + " already exists. If this is unexpected, try deleting the table to remove all metadata associated"
            + " with it before attempting to recreate.");
      }

      schema = _pinotHelixResourceManager.getTableSchema(tableNameWithType);
      Preconditions.checkState(schema != null, "Failed to find schema for table: %s", tableNameWithType);

      TableConfigTunerUtils.applyTunerConfigs(_pinotHelixResourceManager, tableConfig, schema, Map.of());

      TableConfigValidationUtils.validateTableConfig(
          tableConfig, schema, typesToSkip, _pinotHelixResourceManager, _controllerConf, _pinotTaskManager);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, "Invalid table config JSON", Response.Status.BAD_REQUEST);
    } catch (TableAlreadyExistsException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, redactedConfigError("Invalid table config", e, tableConfig),
          Response.Status.BAD_REQUEST);
    }
    try {
      if (!ignoreActiveTasks) {
        tableTasksValidation(tableConfig, _pinotHelixTaskResourceManager);
      }
      _pinotHelixResourceManager.addTable(tableConfig);
      // TODO: validate that table was created successfully
      // (in realtime case, metadata might not have been created but would be created successfully in the next run of
      // the validation manager)
      LOGGER.info("Successfully added table: {}", tableNameWithType);
      return new ConfigSuccessResponse("Table " + tableNameWithType + " successfully added",
          tableConfigAndUnrecognizedProperties.getRight());
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
      if (e instanceof InvalidTableConfigException) {
        throw new ControllerApplicationException(LOGGER,
            redactedConfigError("Invalid table config for table " + tableNameWithType, e, tableConfig),
            Response.Status.BAD_REQUEST);
      } else if (e instanceof TableAlreadyExistsException) {
        throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
      } else if (e instanceof ControllerApplicationException) {
        throw e;
      } else {
        throw new ControllerApplicationException(LOGGER, "Failed to add table " + tableNameWithType,
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    }
  }

  @POST
  @Path("/tables/{tableName}/copy")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.CREATE_TABLE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Copy a table's schema and config from another cluster", notes = "Non upsert table only")
  public CopyTableResponse copyTable(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName, String payload,
      @ApiParam(value = "Include verbose information in response")
      @QueryParam("verbose") @DefaultValue("false") boolean verbose,
      @ApiParam(value = "Dry run mode") @QueryParam("dryRun") @DefaultValue("true") boolean dryRun,
      @Context HttpHeaders headers) {
    try {
      LOGGER.info("[copyTable] received request for table: {}", tableName);
      tableName = DatabaseUtils.translateTableName(tableName, headers);

      if (_pinotHelixResourceManager.hasRealtimeTable(tableName)
          || _pinotHelixResourceManager.hasOfflineTable(tableName)) {
        throw new TableAlreadyExistsException("Table config for " + tableName
            + " already exists. If this is unexpected, try deleting the table to remove all metadata associated"
            + " with it before attempting to recreate.");
      }

      CopyTablePayload copyTablePayload = JsonUtils.stringToObject(payload, CopyTablePayload.class);
      String sourceControllerUri = copyTablePayload.getSourceClusterUri();
      Map<String, String> requestHeaders = copyTablePayload.getHeaders();

      LOGGER.info("[copyTable] Start copying table: {}", tableName);

      ControllerRequestURLBuilder urlBuilder = ControllerRequestURLBuilder.baseUrl(sourceControllerUri);

      URI schemaUri = new URI(urlBuilder.forTableSchemaGet(tableName));
      SimpleHttpResponse schemaResponse = HttpClient.wrapAndThrowHttpException(
          HttpClient.getInstance().sendGetRequest(schemaUri, requestHeaders));
      String schemaJson = schemaResponse.getResponse();
      Schema schema = Schema.fromString(schemaJson);

      URI tableConfigUri = new URI(urlBuilder.forTableGet(tableName));
      Map<String, String> tableConfigRequestHeaders =
          requestHeaders != null ? new HashMap<>(requestHeaders) : new HashMap<>();
      tableConfigRequestHeaders.put(HttpHeaders.ACCEPT, RedactedTableConfigResponse.MEDIA_TYPE);
      SimpleHttpResponse tableConfigResponse = HttpClient.wrapAndThrowHttpException(
          HttpClient.getInstance().sendGetRequest(tableConfigUri, tableConfigRequestHeaders));
      String tableConfigJson = tableConfigResponse.getResponse();
      LOGGER.info("[copyTable] Fetched table config for table: {}", tableName);
      JsonNode tableConfigResponseNode = JsonUtils.stringToJsonNode(tableConfigJson);
      JsonNode tableConfigNode = unwrapTableConfigResponse(tableConfigResponseNode);

      URI watermarkUri = new URI(urlBuilder.forConsumerWatermarksGet(tableName));
      SimpleHttpResponse watermarkResponse = HttpClient.wrapAndThrowHttpException(
          HttpClient.getInstance().sendGetRequest(watermarkUri, requestHeaders));
      String watermarkJson = watermarkResponse.getResponse();
      LOGGER.info("[copyTable] Fetched watermarks for table: {}. Result: {}", tableName, watermarkJson);
      WatermarkInductionResult watermarkInductionResult =
          JsonUtils.stringToObject(watermarkJson, WatermarkInductionResult.class);

      boolean hasOffline = tableConfigNode.has(TableType.OFFLINE.name());
      boolean hasRealtime = tableConfigNode.has(TableType.REALTIME.name());
      TableConfig realtimeTableConfig;
      try {
        if (hasOffline && !hasRealtime) {
          throw new IllegalStateException("pure offline table copy not supported yet");
        }

        ObjectNode realtimeTableConfigNode = (ObjectNode) tableConfigNode.get(TableType.REALTIME.name());
        tweakRealtimeTableConfig(realtimeTableConfigNode, copyTablePayload);
        applyCredentialOverrides(realtimeTableConfigNode, copyTablePayload.getCredentialOverrides());
        realtimeTableConfig = TableConfigRedactionUtils.restoreRedactedValues(
            JsonUtils.jsonNodeToObject(realtimeTableConfigNode, TableConfig.class), null);
        if (realtimeTableConfig.getUpsertConfig() != null) {
          throw new IllegalStateException("upsert table copy not supported");
        }
        // Run the complete validation stack before dry-run returns and before the schema is persisted. This also
        // prevents copying a legacy non-immutable transform into a new ingestion pipeline.
        TableConfigValidationUtils.validateTableConfig(
            realtimeTableConfig, schema, null, _pinotHelixResourceManager, _controllerConf, _pinotTaskManager);
      } catch (ConfigValidationException | IllegalArgumentException | IllegalStateException e) {
        throw new ControllerApplicationException(LOGGER, "Invalid copied table config", Response.Status.BAD_REQUEST);
      }
      LOGGER.info("[copyTable] Successfully fetched and tweaked table config for table: {}", tableName);

      if (dryRun) {
        return new CopyTableResponse("success", "Dry run", schema,
            TableConfigRedactionUtils.redact(realtimeTableConfig), watermarkInductionResult);
      }

      List<StreamConfig> streamConfigs = IngestionConfigUtils.getStreamConfigs(realtimeTableConfig);
      List<StreamMetadata> streamMetadataList = getStreamMetadataList(streamConfigs, watermarkInductionResult);

      _pinotHelixResourceManager.addSchema(schema, true, false);
      LOGGER.info("[copyTable] Successfully added schema for table: {}", tableName);
      // Add the table with designated starting kafka offset and segment sequence number to create consuming segments
      _pinotHelixResourceManager.addTable(realtimeTableConfig, streamMetadataList);
      LOGGER.info("[copyTable] Successfully added table config: {} with designated high watermark", tableName);
      CopyTableResponse response = new CopyTableResponse("success", "Table copied successfully", null, null, null);
      if (hasOffline) {
        response = new CopyTableResponse("warn", "detect offline too; it will only copy real-time segments",
            null, null, null);
      }
      if (verbose) {
        response.setSchema(schema);
        response.setTableConfig(TableConfigRedactionUtils.redact(realtimeTableConfig));
        response.setWatermarkInductionResult(watermarkInductionResult);
      }
      return response;
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Error copying table: " + tableName,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @VisibleForTesting
  static JsonNode unwrapTableConfigResponse(JsonNode responseNode)
      throws IOException {
    if (!RedactedTableConfigResponse.isEnvelope(responseNode)) {
      throw new IOException("Source controller did not return the requested table config representation");
    }
    RedactedTableConfigResponse response =
        JsonUtils.jsonNodeToObject(responseNode, RedactedTableConfigResponse.class);
    return JsonUtils.objectToJsonNode(response.getConfigs());
  }

  @VisibleForTesting
  List<StreamMetadata> getStreamMetadataList(List<StreamConfig> streamConfigs,
      WatermarkInductionResult watermarkInductionResult)
      throws Exception {
    Map<Integer, Integer> streamPartitionCountMap =
        _pinotHelixResourceManager.getRealtimeSegmentManager().getPartitionCountMap(streamConfigs);
    Map<Integer, List<PartitionGroupMetadata>> partitionGroupMetadataByStreamConfigIndex = new HashMap<>();
    for (WatermarkInductionResult.Watermark watermark : watermarkInductionResult.getWatermarks()) {
      int streamConfigIndex =
          IngestionConfigUtils.getStreamConfigIndexFromPinotPartitionId(watermark.getPartitionGroupId());
      Preconditions.checkArgument(streamConfigIndex >= 0 && streamConfigIndex < streamConfigs.size(),
          "Invalid stream config index %s from watermark partition ID %s. Expected index in range [0, %s)",
          streamConfigIndex, watermark.getPartitionGroupId(), streamConfigs.size());
      partitionGroupMetadataByStreamConfigIndex.computeIfAbsent(streamConfigIndex, ignored -> new ArrayList<>()).add(
          new PartitionGroupMetadata(watermark.getPartitionGroupId(), new LongMsgOffset(watermark.getOffset()),
              watermark.getSequenceNumber()));
    }

    // Iterate in order by streamConfigIndex to ensure deterministic ordering
    List<StreamMetadata> streamMetadataList = new ArrayList<>(partitionGroupMetadataByStreamConfigIndex.size());
    for (int streamConfigIndex = 0; streamConfigIndex < streamConfigs.size(); streamConfigIndex++) {
      List<PartitionGroupMetadata> partitionGroupMetadataList =
          partitionGroupMetadataByStreamConfigIndex.get(streamConfigIndex);
      if (partitionGroupMetadataList == null) {
        // No watermarks for this stream config index, skip it
        continue;
      }
      Integer partitionCount = streamPartitionCountMap.get(streamConfigIndex);
      Preconditions.checkState(partitionCount != null,
          "Cannot find partition count for stream config index: %s", streamConfigIndex);
      streamMetadataList.add(new StreamMetadata(streamConfigs.get(streamConfigIndex),
          partitionCount, partitionGroupMetadataList));
    }
    return streamMetadataList;
  }

  /// Helper method to tweak the realtime table config. This method is used to set the broker and server tenants, and
  /// optionally replace the pool tags in the instance assignment config.
  ///
  /// @param realtimeTableConfigNode The JSON object representing the realtime table config.
  /// @param copyTablePayload The payload containing tenant and tag pool replacement information.
  @VisibleForTesting
  static void tweakRealtimeTableConfig(ObjectNode realtimeTableConfigNode, CopyTablePayload copyTablePayload) {
    String brokerTenant = copyTablePayload.getBrokerTenant();
    String serverTenant = copyTablePayload.getServerTenant();
    Map<String, String> tagPoolReplacementMap = copyTablePayload.getTagPoolReplacementMap();

    ObjectNode tenantConfig = (ObjectNode) realtimeTableConfigNode.get("tenants");
    tenantConfig.put("broker", brokerTenant);
    tenantConfig.put("server", serverTenant);
    if (tagPoolReplacementMap == null || tagPoolReplacementMap.isEmpty()) {
      return;
    }
    JsonNode instanceAssignmentConfigMap = realtimeTableConfigNode.get("instanceAssignmentConfigMap");
    if (instanceAssignmentConfigMap == null) {
      return;
    }
    java.util.Iterator<Map.Entry<String, JsonNode>> iterator = instanceAssignmentConfigMap.properties().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, JsonNode> entry = iterator.next();
      JsonNode instanceAssignmentConfig = entry.getValue();
      ObjectNode tagPoolConfig = (ObjectNode) instanceAssignmentConfig.get("tagPoolConfig");
      String srcTag = tagPoolConfig.get("tag").asText();
      if (tagPoolReplacementMap.containsKey(srcTag)) {
        tagPoolConfig.put("tag", tagPoolReplacementMap.get(srcTag));
      }
    }
  }

  /// Replaces source-cluster redaction markers with caller-supplied credentials. Requiring an exact JSON Pointer to a
  /// marker-bearing leaf preserves copy without adding a raw source-read path or allowing this field to mutate ordinary
  /// configuration values.
  @VisibleForTesting
  static void applyCredentialOverrides(ObjectNode tableConfigNode, @Nullable Map<String, String> overrides) {
    if (overrides == null || overrides.isEmpty()) {
      return;
    }
    for (Map.Entry<String, String> override : overrides.entrySet()) {
      JsonPointer pointer = JsonPointer.compile(override.getKey());
      Preconditions.checkArgument(!pointer.matches(), "Credential override must identify a table config field");
      JsonNode parent = tableConfigNode.at(pointer.head());
      JsonPointer leaf = pointer.last();
      JsonNode current;
      if (parent.isObject() && leaf.mayMatchProperty()) {
        current = parent.get(leaf.getMatchingProperty());
      } else if (parent.isArray() && leaf.mayMatchElement()) {
        current = parent.get(leaf.getMatchingIndex());
      } else {
        throw new IllegalArgumentException("Credential override does not identify a table config field");
      }
      String value = override.getValue();
      Preconditions.checkArgument(value != null && !value.contains(TableConfigRedactionUtils.REDACTION_MARKER),
          "Credential override must contain a literal replacement");
      Preconditions.checkArgument(current != null && current.isTextual()
              && TableConfigRedactionUtils.isValidCredentialOverride(
                  tableConfigNode, override.getKey(), value),
          "Credential override target is not a redacted credential");
      if (parent.isObject()) {
        ((ObjectNode) parent).put(leaf.getMatchingProperty(), value);
      } else {
        ((ArrayNode) parent).set(leaf.getMatchingIndex(), JsonUtils.objectToJsonNode(value));
      }
    }
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/recommender")
  @Authenticate(AccessType.READ)
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
  public String listTables(@ApiParam(value = "realtime|offline|dimension") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Task type") @QueryParam("taskType") String taskType,
      @ApiParam(value = "name|creationTime|lastModifiedTime") @QueryParam("sortType") String sortTypeStr,
      @ApiParam(value = "true|false") @QueryParam("sortAsc") @DefaultValue("true") boolean sortAsc,
      @Context HttpHeaders headers) {
    try {
      boolean isDimensionTable = "dimension".equalsIgnoreCase(tableTypeStr);
      TableType tableType = null;
      if (isDimensionTable) {
        // Dimension is a property (isDimTable) of an OFFLINE table.
        tableType = TableType.OFFLINE;
      } else if (tableTypeStr != null) {
        tableType = TableType.valueOf(tableTypeStr.toUpperCase());
      }
      SortType sortType = sortTypeStr != null ? SortType.valueOf(sortTypeStr.toUpperCase()) : SortType.NAME;

      String database = headers.getHeaderString(DATABASE);

      // If tableTypeStr is dimension, then tableType is set to TableType.OFFLINE.
      // So, checking the isDimensionTable to get the list of dimension tables only.
      List<String> tableNamesWithType =
          isDimensionTable ? _pinotHelixResourceManager.getAllDimensionTables(database)
              : tableType == null ? _pinotHelixResourceManager.getAllTables(database)
                  : (tableType == TableType.REALTIME ? _pinotHelixResourceManager.getAllRealtimeTables(database)
                      : _pinotHelixResourceManager.getAllOfflineTables(database));

      if (StringUtils.isNotBlank(taskType)) {
        tableNamesWithType.retainAll(_pinotTaskManager.getTablesForTaskType(taskType, tableNamesWithType));
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

  @GET
  @Path("/tables/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_TABLE_CONFIG)
  @Produces({MediaType.APPLICATION_JSON, RedactedTableConfigResponse.MEDIA_TYPE})
  @ApiOperation(value = "Lists the table configs",
      notes = "Returns a versioned redacted response. Submit the complete response to PUT when retaining markers.")
  public String listTableConfigs(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr, @Context HttpHeaders headers) {
    try {
      tableName = DatabaseUtils.translateTableName(tableName, headers);
      Map<String, TableConfig> configs = new LinkedHashMap<>();
      Map<String, Integer> baseVersions = new LinkedHashMap<>();

      if ((tableTypeStr == null || TableType.OFFLINE.name().equalsIgnoreCase(tableTypeStr))
          && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
        Pair<TableConfig, Integer> tableConfigWithVersion = getUnresolvedTableConfigWithVersion(
            _pinotHelixResourceManager, TableNameBuilder.OFFLINE.tableNameWithType(tableName));
        Preconditions.checkNotNull(tableConfigWithVersion);
        configs.put(TableType.OFFLINE.name(), TableConfigRedactionUtils.redact(tableConfigWithVersion.getLeft()));
        baseVersions.put(TableType.OFFLINE.name(), tableConfigWithVersion.getRight());
      }

      if ((tableTypeStr == null || TableType.REALTIME.name().equalsIgnoreCase(tableTypeStr))
          && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        Pair<TableConfig, Integer> tableConfigWithVersion = getUnresolvedTableConfigWithVersion(
            _pinotHelixResourceManager, TableNameBuilder.REALTIME.tableNameWithType(tableName));
        Preconditions.checkNotNull(tableConfigWithVersion);
        configs.put(TableType.REALTIME.name(), TableConfigRedactionUtils.redact(tableConfigWithVersion.getLeft()));
        baseVersions.put(TableType.REALTIME.name(), tableConfigWithVersion.getRight());
      }

      if (configs.isEmpty()) {
        String tableNameWithType = tableTypeStr != null ? tableName + "_" + tableTypeStr.toUpperCase() : tableName;
        throw new TableNotFoundException("Table " + tableNameWithType + " does not exist");
      }
      Pair<Schema, Integer> schemaWithVersion = _pinotHelixResourceManager.getSchemaWithVersion(
          TableNameBuilder.extractRawTableName(tableName));
      Preconditions.checkNotNull(schemaWithVersion, "Failed to find schema for table: %s", tableName);
      baseVersions.put(RedactedTableConfigResponse.SCHEMA_VERSION_KEY, schemaWithVersion.getRight());
      return JsonUtils.objectToString(new RedactedTableConfigResponse(configs, baseVersions));
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.NOT_FOUND, e);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to retrieve table config",
          Response.Status.INTERNAL_SERVER_ERROR);
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
          + "instantly delete segments without retention") @QueryParam("retention") String retentionPeriod,
      @DefaultValue("false") @QueryParam("ignoreActiveTasks") boolean ignoreActiveTasks,
      @Context HttpHeaders headers) {
    TableType tableType = Constants.validateTableType(tableTypeStr);

    List<String> tablesDeleted = new LinkedList<>();
    try {
      tableName = DatabaseUtils.translateTableName(tableName, headers);
      validateLogicalTableReference(tableName, tableType);
      boolean tableExist = false;
      if (verifyTableType(tableName, tableType, TableType.OFFLINE)) {
        String tableWithType = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        tableTasksCleanup(tableWithType, ignoreActiveTasks, _pinotHelixResourceManager, _pinotHelixTaskResourceManager);
        tableExist = _pinotHelixResourceManager.hasOfflineTable(tableName);
        // Even the table name does not exist, still go on to delete remaining table metadata in case a previous delete
        // did not complete.
        _pinotHelixResourceManager.deleteOfflineTable(tableName, retentionPeriod);
        if (tableExist) {
          tablesDeleted.add(tableWithType);
        }
      }
      if (verifyTableType(tableName, tableType, TableType.REALTIME)) {
        String tableWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
        tableTasksCleanup(tableWithType, ignoreActiveTasks, _pinotHelixResourceManager, _pinotHelixTaskResourceManager);
        tableExist = _pinotHelixResourceManager.hasRealtimeTable(tableName);
        // Even the table name does not exist, still go on to delete remaining table metadata in case a previous delete
        // did not complete.
        _pinotHelixResourceManager.deleteRealtimeTable(tableName, retentionPeriod);
        if (tableExist) {
          tablesDeleted.add(tableWithType);
        }
      }
      if (!tablesDeleted.isEmpty()) {
        tablesDeleted.forEach(deletedTableName -> LOGGER.info("Successfully deleted table: {}", deletedTableName));
        return new SuccessResponse("Tables: " + tablesDeleted + " deleted");
      }
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    throw new ControllerApplicationException(LOGGER,
        "Table '" + tableName + "' with type " + tableType + " does not exist", Response.Status.NOT_FOUND);
  }

  public static void tableTasksValidation(TableConfig tableConfig,
      PinotHelixTaskResourceManager pinotHelixTaskResourceManager) {
    if (tableConfig.getTaskConfig() == null) {
      return;
    }
    String tableWithType = tableConfig.getTableName();
    Map<String, Map<String, String>> taskTypeConfigsMap = tableConfig.getTaskConfig().getTaskTypeConfigsMap();
    for (String taskType : taskTypeConfigsMap.keySet()) {
      Map<String, TaskState> taskStates;
      try {
        taskStates = pinotHelixTaskResourceManager.getTaskStatesByTable(taskType, tableWithType);
      } catch (IllegalArgumentException e) {
        LOGGER.info(e.getMessage());
        return;
      }
      if (!taskStates.isEmpty()) {
        throw new ControllerApplicationException(LOGGER, "The table has dangling task data, try performing table "
            + "delete operation in case the delete operation was not completed successfully, else delete the tasks "
            + "manually through DELETE /tasks/task/{taskName} endpoint. Please try again once the dangling tasks are "
            + "cleaned up", Response.Status.BAD_REQUEST);
      }
    }
  }

  public static void tableTasksCleanup(String tableWithType, boolean ignoreActiveTasks,
      PinotHelixResourceManager pinotHelixResourceManager, PinotHelixTaskResourceManager pinotHelixTaskResourceManager)
      throws IOException {
    TableConfig tableConfig = getUnresolvedTableConfig(pinotHelixResourceManager, tableWithType);
    if (tableConfig == null || tableConfig.getTaskConfig() == null) {
      return;
    }
    Map<String, Map<String, String>> taskTypeConfigsMap = tableConfig.getTaskConfig().getTaskTypeConfigsMap();
    Set<String> taskTypes = taskTypeConfigsMap.keySet();
    boolean tableConfigChanged = false;
    for (String taskType : taskTypes) {
      // remove the task schedules to avoid task being scheduled during table deletion
      tableConfigChanged =
          tableConfigChanged || taskTypeConfigsMap.get(taskType).remove(PinotTaskManager.SCHEDULE_KEY) != null;
    }
    if (tableConfigChanged) {
      try {
        pinotHelixResourceManager.updateTableConfig(tableConfig);
      } catch (Exception e) {
        LOGGER.warn("Unable to remove task schedules before table deletion for {}", tableWithType);
      }
    }
    List<String> pendingTasks = new ArrayList<>();
    for (String taskType : taskTypes) {
      Map<String, TaskState> taskStates;
      try {
        taskStates = pinotHelixTaskResourceManager.getTaskStatesByTable(taskType, tableWithType);
      } catch (IllegalArgumentException e) {
        LOGGER.info(e.getMessage());
        continue;
      }
      for (String taskName : taskStates.keySet()) {
        if (TaskState.IN_PROGRESS.equals(taskStates.get(taskName))
            && pinotHelixTaskResourceManager.getTaskCount(taskName).getRunning() > 0) {
          pendingTasks.add(taskName);
        } else {
          pinotHelixTaskResourceManager.deleteTask(taskName, true);
        }
      }
    }
    if (!ignoreActiveTasks && !pendingTasks.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "The table has " + pendingTasks.size() + " active running tasks "
          + ": " + pendingTasks + ". The task schedules have been cleared, so new tasks should not be generated. "
          + "Please try again once there are no more active tasks", Response.Status.BAD_REQUEST);
    }
  }

  @Nullable
  static TableConfig getUnresolvedTableConfig(PinotHelixResourceManager resourceManager, String tableWithType) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableWithType);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableWithType);
    if (tableType == TableType.REALTIME) {
      return resourceManager.getRealtimeTableConfig(rawTableName, false, false);
    }
    if (tableType == TableType.OFFLINE) {
      return resourceManager.getOfflineTableConfig(rawTableName, false, false);
    }
    return null;
  }

  @Nullable
  static Pair<TableConfig, Integer> getUnresolvedTableConfigWithVersion(
      PinotHelixResourceManager resourceManager, String tableWithType) {
    if (TableNameBuilder.getTableTypeFromTableName(tableWithType) == null) {
      return null;
    }
    return resourceManager.getTableConfigWithVersion(tableWithType, false, false);
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

  private void validateLogicalTableReference(String tableName, TableType tableType) {
    String tableNameWithType =
        tableType == null ? tableName : TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    List<LogicalTableConfig> allLogicalTableConfigs =
        ZKMetadataProvider.getAllLogicalTableConfigs(_pinotHelixResourceManager.getPropertyStore());
    for (LogicalTableConfig logicalTableConfig : allLogicalTableConfigs) {
      if (LogicalTableConfigUtils.checkPhysicalTableRefExists(logicalTableConfig, tableNameWithType)) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Cannot delete table config: %s because it is referenced in logical table: %s",
                tableName, logicalTableConfig.getTableName()), Response.Status.CONFLICT);
      }
    }
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
      @QueryParam("validationTypesToSkip") @Nullable String typesToSkip,
      @ApiParam(value = "Force config changes")
      @QueryParam("force") @DefaultValue("false") boolean force,
      @Context HttpHeaders headers,
      String tableConfigString)
      throws Exception {
    ParsedTableConfigUpdate parsedUpdate;
    Map<TableType, TableConfig> tableConfigs = new LinkedHashMap<>();
    Map<TableType, TableConfig> storedTableConfigs = new LinkedHashMap<>();
    Map<TableType, Integer> expectedVersions = new LinkedHashMap<>();
    Pair<Schema, Integer> schemaWithVersion;
    try {
      parsedUpdate = parseTableConfigUpdate(tableConfigString);
      String rawTableName = DatabaseUtils.translateTableName(TableNameBuilder.extractRawTableName(tableName), headers);
      schemaWithVersion = _pinotHelixResourceManager.getSchemaWithVersion(rawTableName);
      Preconditions.checkNotNull(schemaWithVersion, "Failed to find schema for table: %s", rawTableName);
      Integer schemaBaseVersion = parsedUpdate._baseVersions.get(RedactedTableConfigResponse.SCHEMA_VERSION_KEY);
      if (schemaBaseVersion != null && !schemaBaseVersion.equals(schemaWithVersion.getRight())) {
        throw new ControllerApplicationException(LOGGER,
            "Table schema changed after it was read; retrieve the table config again before updating",
            Response.Status.CONFLICT);
      }
      for (Map.Entry<TableType, TableConfig> entry : parsedUpdate._tableConfigs.entrySet()) {
        TableType tableType = entry.getKey();
        TableConfig tableConfig = entry.getValue();
        String tableNameWithType = DatabaseUtils.translateTableName(tableConfig.getTableName(), headers);
        tableConfig.setTableName(tableNameWithType);
        String tableNameFromPath = DatabaseUtils.translateTableName(
            TableNameBuilder.forType(tableType).tableNameWithType(tableName), headers);
        if (!tableNameFromPath.equals(tableNameWithType)) {
          throw new ControllerApplicationException(LOGGER,
              "Request table " + tableNameFromPath + " does not match table name in the body " + tableNameWithType,
              Response.Status.BAD_REQUEST);
        }

        Pair<TableConfig, Integer> storedTableConfigWithVersion =
            getUnresolvedTableConfigWithVersion(_pinotHelixResourceManager, tableNameWithType);
        if (storedTableConfigWithVersion == null) {
          throw new ControllerApplicationException(LOGGER, "Table " + tableNameWithType + " does not exist",
              Response.Status.NOT_FOUND);
        }
        TableConfig storedTableConfig = storedTableConfigWithVersion.getLeft();
        int currentVersion = storedTableConfigWithVersion.getRight();
        Integer baseVersion = parsedUpdate._baseVersions.get(tableType.name());
        if (baseVersion != null && baseVersion != currentVersion) {
          throw new ControllerApplicationException(LOGGER,
              "Table config changed after it was read; retrieve it again before updating", Response.Status.CONFLICT);
        }
        expectedVersions.put(tableType, baseVersion != null ? baseVersion : currentVersion);
        tableConfig = TableConfigRedactionUtils.restoreRedactedValues(tableConfig, storedTableConfig);
        storedTableConfigs.put(tableType, storedTableConfig);
        tableConfigs.put(tableType, tableConfig);
      }
      boolean coordinatedHybridUpdate = tableConfigs.size() == 2;
      for (Map.Entry<TableType, TableConfig> entry : tableConfigs.entrySet()) {
        TableConfigValidationUtils.validateTableConfig(
            entry.getValue(), schemaWithVersion.getLeft(), typesToSkip, _pinotHelixResourceManager, _controllerConf,
            _pinotTaskManager, storedTableConfigs.get(entry.getKey()), !coordinatedHybridUpdate);
      }
      if (coordinatedHybridUpdate) {
        TableConfigUtils.verifyHybridTableConfigs(rawTableName, tableConfigs.get(TableType.OFFLINE),
            tableConfigs.get(TableType.REALTIME));
      }
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, "Invalid table config JSON", Response.Status.BAD_REQUEST);
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Invalid table config: " + tableName,
          Response.Status.BAD_REQUEST);
    }

    try {
      for (TableConfig tableConfig : tableConfigs.values()) {
        if (!_pinotHelixResourceManager.hasTable(tableConfig.getTableName())) {
          throw new ControllerApplicationException(LOGGER, "Table " + tableConfig.getTableName() + " does not exist",
              Response.Status.NOT_FOUND);
        }
      }
      if (parsedUpdate._envelope) {
        _pinotHelixResourceManager.updateTableConfigsAtomicallyWithSchemaCheck(schemaWithVersion.getLeft(),
            schemaWithVersion.getRight(), tableConfigs, expectedVersions, force);
      } else {
        TableConfig tableConfig = tableConfigs.values().iterator().next();
        _pinotHelixResourceManager.updateTableConfig(
            tableConfig, expectedVersions.get(tableConfig.getTableType()), force);
      }
    } catch (TableConfigVersionMismatchException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          "Table config changed while the update was in progress; retry the request", Response.Status.CONFLICT);
    } catch (TableConfigBackwardIncompatibleException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          "Invalid update for table " + tableName, Response.Status.BAD_REQUEST);
    } catch (InvalidTableConfigException e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          "Invalid table config for table " + tableName, Response.Status.BAD_REQUEST);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, "Failed to update table " + tableName,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    LOGGER.info("Successfully updated table configs: {}", tableConfigs.keySet());
    return new ConfigSuccessResponse("Table config updated for " + tableName,
        parsedUpdate._unrecognizedProperties);
  }

  private static ParsedTableConfigUpdate parseTableConfigUpdate(String tableConfigString)
      throws IOException {
    JsonNode requestNode = JsonUtils.stringToJsonNode(tableConfigString);
    if (RedactedTableConfigResponse.isEnvelope(requestNode)) {
      Pair<RedactedTableConfigResponse, Map<String, Object>> responseAndUnrecognizedProperties =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigString, RedactedTableConfigResponse.class);
      Map<TableType, TableConfig> configs = new LinkedHashMap<>();
      responseAndUnrecognizedProperties.getLeft().getConfigs().forEach((typeName, tableConfig) -> {
        TableType tableType = TableType.valueOf(typeName);
        configs.put(tableType, tableConfig);
      });
      return new ParsedTableConfigUpdate(configs, responseAndUnrecognizedProperties.getLeft().getBaseVersions(), true,
          responseAndUnrecognizedProperties.getRight());
    }

    Pair<TableConfig, Map<String, Object>> tableConfigAndUnrecognizedProperties =
        JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigString, TableConfig.class);
    // Bare requests remain supported for literal updates, but cannot carry a response marker. Requiring the complete
    // envelope for markers prevents a nested redacted config from becoming a valid legacy update body.
    TableConfig tableConfig =
        TableConfigRedactionUtils.restoreRedactedValues(tableConfigAndUnrecognizedProperties.getLeft(), null);
    return new ParsedTableConfigUpdate(Map.of(tableConfig.getTableType(), tableConfig), Map.of(), false,
        tableConfigAndUnrecognizedProperties.getRight());
  }

  private static final class ParsedTableConfigUpdate {
    private final Map<TableType, TableConfig> _tableConfigs;
    private final Map<String, Integer> _baseVersions;
    private final boolean _envelope;
    private final Map<String, Object> _unrecognizedProperties;

    private ParsedTableConfigUpdate(Map<TableType, TableConfig> tableConfigs,
        Map<String, Integer> baseVersions, boolean envelope,
        Map<String, Object> unrecognizedProperties) {
      _tableConfigs = tableConfigs;
      _baseVersions = baseVersions;
      _envelope = envelope;
      _unrecognizedProperties = unrecognizedProperties;
    }
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
    Pair<TableConfig, Map<String, Object>> tableConfigAndUnrecognizedProperties;
    try {
      tableConfigAndUnrecognizedProperties =
          JsonUtils.stringToObjectAndUnrecognizedProperties(tableConfigStr, TableConfig.class);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, "Invalid table config JSON", Response.Status.BAD_REQUEST);
    }
    TableConfig tableConfig = tableConfigAndUnrecognizedProperties.getLeft();
    String tableNameWithType = DatabaseUtils.translateTableName(tableConfig.getTableName(), httpHeaders);
    tableConfig.setTableName(tableNameWithType);

    // validate permission
    ResourceUtils.checkPermissionAndAccess(tableNameWithType, request, httpHeaders,
        AccessType.READ, Actions.Table.VALIDATE_TABLE_CONFIGS, _accessControlFactory, LOGGER);

    ObjectNode validationResponse = validateConfig(tableConfig, typesToSkip);
    validationResponse.set("unrecognizedProperties",
        JsonUtils.objectToJsonNode(tableConfigAndUnrecognizedProperties.getRight()));
    return validationResponse;
  }

  @VisibleForTesting
  ObjectNode validateConfig(TableConfig tableConfig, @Nullable String typesToSkip) {
    String tableNameWithType = tableConfig.getTableName();
    try {
      Schema schema = _pinotHelixResourceManager.getTableSchema(tableNameWithType);
      if (schema == null) {
        throw new SchemaNotFoundException("Failed to find schema for table: " + tableNameWithType);
      }
      TableConfigUtils.validate(tableConfig, schema, typesToSkip,
          getUnresolvedTableConfig(_pinotHelixResourceManager, tableNameWithType));
      TaskConfigUtils.validateTaskConfigs(tableConfig, schema, _pinotTaskManager, typesToSkip);
      TableConfigValidatorRegistry.validate(tableConfig, schema);
      ObjectNode tableConfigValidateStr = JsonUtils.newObjectNode();
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        tableConfigValidateStr.set(TableType.OFFLINE.name(), tableConfig.toJsonNode());
      } else {
        tableConfigValidateStr.set(TableType.REALTIME.name(), tableConfig.toJsonNode());
      }
      return tableConfigValidateStr;
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          redactedConfigError("Invalid table config: " + tableNameWithType, e, tableConfig),
          Response.Status.BAD_REQUEST);
    }
  }

  private static String redactedConfigError(String prefix, Exception exception, @Nullable TableConfig tableConfig) {
    if (tableConfig == null) {
      return prefix;
    }
    return prefix + ": " + TableConfigRedactionUtils.redactDiagnostic(exception.getMessage(), tableConfig);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/tables/{tableName}/rebalance")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.REBALANCE_TABLE)
  @ApiOperation(value = "Rebalances a table (reassign instances and segments for a table)",
      notes = "Rebalances a table (reassign instances and segments for a table)")
  public RebalanceResult rebalance(
      //@formatter:off
      @ApiParam(value = "Name of the table to rebalance", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Whether to rebalance table in dry-run mode") @DefaultValue("false") @QueryParam("dryRun")
      boolean dryRun,
      @ApiParam(value = "Whether to enable pre-checks for table, must be in dry-run mode to enable")
      @DefaultValue("false") @QueryParam("preChecks") boolean preChecks,
      @ApiParam(value = "Whether to disable summary calculation")
      @DefaultValue("false") @QueryParam("disableSummary") boolean disableSummary,
      @ApiParam(value = "Whether to reassign instances before reassigning segments") @DefaultValue("true")
      @QueryParam("reassignInstances") boolean reassignInstances,
      @ApiParam(value = "Whether to reassign CONSUMING segments for real-time table") @DefaultValue("true")
      @QueryParam("includeConsuming") boolean includeConsuming,
      @ApiParam(value = "Whether to enable minimize data movement on rebalance, DEFAULT will use "
          + "the minimizeDataMovement in table config") @DefaultValue("ENABLE")
      @QueryParam("minimizeDataMovement") Enablement minimizeDataMovement,
      @ApiParam(value = "Whether to rebalance table in bootstrap mode (regardless of minimum segment movement, "
          + "reassign all segments in a round-robin fashion as if adding new segments to an empty table)")
      @DefaultValue("false") @QueryParam("bootstrap") boolean bootstrap,
      @ApiParam(value = "Whether to allow downtime for the rebalance") @DefaultValue("false") @QueryParam("downtime")
      boolean downtime,
      @ApiParam(value = "This flag only applies to peer-download enabled tables undergoing downtime=true or "
          + "minAvailableReplicas=0 rebalance (both of which can result in possible data loss scenarios). If enabled, "
          + "this flag will allow the rebalance to continue even in cases where data loss scenarios have been "
          + "detected, otherwise the rebalance will be failed and user action will be required to rebalance again. "
          + "This flag should be used with caution and only used in scenarios where data loss is acceptable")
      @DefaultValue("false") @QueryParam("allowPeerDownloadDataLoss")
      boolean allowPeerDownloadDataLoss,
      @ApiParam(value = "For no-downtime rebalance, minimum number of replicas to keep alive during rebalance, or "
          + "maximum number of replicas allowed to be unavailable if value is negative") @DefaultValue("-1")
      @QueryParam("minAvailableReplicas") int minAvailableReplicas,
      @ApiParam(value = "For no-downtime rebalance, whether to enable low disk mode during rebalance. When enabled, "
          + "segments will first be offloaded from servers, then added to servers after offload is done while "
          + "maintaining the min available replicas. It may increase the total time of the rebalance, but can be "
          + "useful when servers are low on disk space, and we want to scale up the cluster and rebalance the table to "
          + "more servers.") @DefaultValue("false") @QueryParam("lowDiskMode") boolean lowDiskMode,
      @ApiParam(value = "Whether to use best-efforts to rebalance (not fail the rebalance when the no-downtime "
          + "contract cannot be achieved)") @DefaultValue("false") @QueryParam("bestEfforts") boolean bestEfforts,
      @ApiParam(value = "How many maximum segment adds per server to update in the IdealState in each step. For "
          + "non-strict replica group based assignment, this number will be capped at the batchSizePerServer value "
          + "per rebalance step (some servers may get fewer segments). For strict replica group based assignment, "
          + "this is a per-server best effort value since each partition of a replica group must be moved as a whole "
          + "and at least one partition in a replica group should be moved. A value of -1 is used to disable batching "
          + "(select as many segments as possible per incremental step in rebalance such that minAvailableReplicas is "
          + "honored).")
      @DefaultValue("-1") @QueryParam("batchSizePerServer") int batchSizePerServer,
      @ApiParam(value = "How often to check if external view converges with ideal states") @DefaultValue("1000")
      @QueryParam("externalViewCheckIntervalInMs") long externalViewCheckIntervalInMs,
      @ApiParam(value = "Maximum time (in milliseconds) to wait for external view to converge with ideal states. "
          + "Extends if progress has been made during the wait, otherwise times out") @DefaultValue("3600000")
      @QueryParam("externalViewStabilizationTimeoutInMs") long externalViewStabilizationTimeoutInMs,
      @ApiParam(value = "How often to make a status update (i.e. heartbeat)") @DefaultValue("300000")
      @QueryParam("heartbeatIntervalInMs") long heartbeatIntervalInMs,
      @ApiParam(value = "How long to wait for next status update (i.e. heartbeat) before the job is considered failed")
      @DefaultValue("3600000") @QueryParam("heartbeatTimeoutInMs") long heartbeatTimeoutInMs,
      @ApiParam(value = "Max number of attempts to rebalance") @DefaultValue("3") @QueryParam("maxAttempts")
      int maxAttempts,
      @ApiParam(value = "Initial delay to exponentially backoff retry") @DefaultValue("300000")
      @QueryParam("retryInitialDelayInMs") long retryInitialDelayInMs,
      @ApiParam(value = "Whether to update segment target tier as part of the rebalance") @DefaultValue("false")
      @QueryParam("updateTargetTier") boolean updateTargetTier,
      @ApiParam(value = "Disk utilization threshold override used in pre-check (0.0 to 1.0, e.g., 0.85 for 85%). "
          + "If not provided, uses " + ControllerConf.REBALANCE_DISK_UTILIZATION_THRESHOLD
          + " in the controller config")
      @DefaultValue("-1.0")
      @QueryParam("diskUtilizationThreshold") double diskUtilizationThreshold,
      @ApiParam(value = "Whether to force commit consuming segments for a REALTIME table before they are rebalanced.")
      @DefaultValue("false")
      @QueryParam("forceCommit") boolean forceCommit,
      @ApiParam(value = "Batch size for force commit operations")
      @DefaultValue(BatchConfig.DEFAULT_BATCH_SIZE + "")
      @QueryParam("forceCommitBatchSize") int forceCommitBatchSize,
      @ApiParam(value = "Interval in milliseconds for checking force commit batch status")
      @DefaultValue(BatchConfig.DEFAULT_STATUS_CHECK_INTERVAL_SEC * 1000 + "")
      @QueryParam("forceCommitBatchStatusCheckIntervalMs") int forceCommitBatchStatusCheckIntervalMs,
      @ApiParam(value = "Timeout in milliseconds for force commit batch status check")
      @DefaultValue(BatchConfig.DEFAULT_STATUS_CHECK_TIMEOUT_SEC * 1000 + "")
      @QueryParam("forceCommitBatchStatusCheckTimeoutMs") int forceCommitBatchStatusCheckTimeoutMs,
      @Context HttpHeaders headers
      //@formatter:on
  ) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    String tableNameWithType = constructTableNameWithType(tableName, tableTypeStr);
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(dryRun);
    rebalanceConfig.setPreChecks(preChecks);
    rebalanceConfig.setDisableSummary(disableSummary);
    rebalanceConfig.setReassignInstances(reassignInstances);
    rebalanceConfig.setIncludeConsuming(includeConsuming);
    rebalanceConfig.setMinimizeDataMovement(minimizeDataMovement);
    rebalanceConfig.setBootstrap(bootstrap);
    rebalanceConfig.setDowntime(downtime);
    rebalanceConfig.setAllowPeerDownloadDataLoss(allowPeerDownloadDataLoss);
    rebalanceConfig.setMinAvailableReplicas(minAvailableReplicas);
    rebalanceConfig.setLowDiskMode(lowDiskMode);
    rebalanceConfig.setBestEfforts(bestEfforts);
    rebalanceConfig.setBatchSizePerServer(batchSizePerServer);
    rebalanceConfig.setForceCommit(forceCommit);
    rebalanceConfig.setForceCommitBatchSize(forceCommitBatchSize);
    rebalanceConfig.setForceCommitBatchStatusCheckIntervalMs(forceCommitBatchStatusCheckIntervalMs);
    rebalanceConfig.setForceCommitBatchStatusCheckTimeoutMs(forceCommitBatchStatusCheckTimeoutMs);
    rebalanceConfig.setExternalViewCheckIntervalInMs(externalViewCheckIntervalInMs);
    rebalanceConfig.setExternalViewStabilizationTimeoutInMs(externalViewStabilizationTimeoutInMs);
    heartbeatIntervalInMs = Math.max(externalViewCheckIntervalInMs, heartbeatIntervalInMs);
    rebalanceConfig.setHeartbeatIntervalInMs(heartbeatIntervalInMs);
    heartbeatTimeoutInMs = Math.max(heartbeatTimeoutInMs, 3 * heartbeatIntervalInMs);
    rebalanceConfig.setHeartbeatTimeoutInMs(heartbeatTimeoutInMs);
    rebalanceConfig.setMaxAttempts(maxAttempts);
    rebalanceConfig.setRetryInitialDelayInMs(retryInitialDelayInMs);
    rebalanceConfig.setUpdateTargetTier(updateTargetTier);
    rebalanceConfig.setDiskUtilizationThreshold(diskUtilizationThreshold);
    String rebalanceJobId = TableRebalancer.createUniqueRebalanceJobIdentifier();

    try {
      if (dryRun || preChecks) {
        return _tableRebalanceManager.rebalanceTableDryRun(tableNameWithType, rebalanceConfig, rebalanceJobId);
      } else if (downtime) {
        // For rebalance with downtime, it's fine to run the rebalance synchronously since it should be a really
        // short operation.
        return _tableRebalanceManager.rebalanceTable(tableNameWithType, rebalanceConfig, rebalanceJobId, false);
      } else {
        // Make a dry-run first to get the target assignment
        rebalanceConfig.setDryRun(true);
        RebalanceResult dryRunResult =
            _tableRebalanceManager.rebalanceTableDryRun(tableNameWithType, rebalanceConfig, rebalanceJobId);

        if (dryRunResult.getStatus() == RebalanceResult.Status.DONE) {
          // If dry-run succeeded, run rebalance asynchronously
          rebalanceConfig.setDryRun(false);
          CompletableFuture<RebalanceResult> rebalanceResultFuture =
              _tableRebalanceManager.rebalanceTableAsync(tableNameWithType, rebalanceConfig, rebalanceJobId, true);
          rebalanceResultFuture.whenComplete((rebalanceResult, throwable) -> {
            if (throwable != null) {
              String errorMsg = String.format("Caught exception/error while rebalancing table: %s", tableNameWithType);
              LOGGER.error(errorMsg, throwable);
            }
          });
          boolean isJobIdPersisted =
              waitForRebalanceToPersist(dryRunResult.getJobId(), tableNameWithType, rebalanceResultFuture);

          if (rebalanceResultFuture.isDone()) {
            try {
              return rebalanceResultFuture.get();
            } catch (Throwable t) {
              if (!isJobIdPersisted) {
                // If the jobId is not persisted, we return the exception to indicate the rebalance failed.
                // Otherwise, polling the job id return NOT_FOUND indefinitely.
                throw new ControllerApplicationException(LOGGER, t.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
              }
            }
          }

          return new RebalanceResult(dryRunResult.getJobId(), RebalanceResult.Status.IN_PROGRESS,
              "In progress, check controller logs for updates", dryRunResult.getInstanceAssignment(),
              dryRunResult.getTierInstanceAssignment(), dryRunResult.getSegmentAssignment(),
              dryRunResult.getPreChecksResult(), dryRunResult.getRebalanceSummaryResult());
        } else {
          // If dry-run failed or is no-op, return the dry-run result
          return dryRunResult;
        }
      }
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.NOT_FOUND);
    } catch (RebalanceInProgressException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT);
    }
  }

  /// Waits for jobId to be persisted or the rebalance to complete using a retry policy.
  /// Tables with 100k+ segments take up to a few seconds for the jobId to persist. This ensures the jobId is present
  /// before returning the jobId to the caller, so they can correctly poll the jobId.
  public boolean waitForRebalanceToPersist(
      String jobId, String tableNameWithType, Future<RebalanceResult> rebalanceResultFuture) {
    try {
      // This retry policy waits at most for 7.5s to 15s in total. This is chosen to cover typical delays for tables
      // with many segments and avoid excessive HTTP request timeouts.
      RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0).attempt(() ->
          getControllerJobMetadata(jobId) != null || rebalanceResultFuture.isDone());
      return true;
    } catch (Exception e) {
      LOGGER.warn("waiting for jobId not successful while rebalancing table: {}", tableNameWithType);
      return false;
    }
  }

  public Map<String, String> getControllerJobMetadata(String jobId) {
    return _pinotHelixResourceManager.getControllerJobZKMetadata(jobId, ControllerJobTypes.TABLE_REBALANCE);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Path("/tables/{tableName}/rebalance")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.CANCEL_REBALANCE)
  @ApiOperation(value = "Cancel all rebalance jobs for the given table, and noop if no rebalance is running", notes =
      "Cancel all rebalance jobs for the given table, and noop if no rebalance is running")
  public List<String> cancelRebalance(
      @ApiParam(value = "Name of the table to rebalance", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    String tableNameWithType = constructTableNameWithType(tableName, tableTypeStr);
    return TableRebalanceManager.cancelRebalance(tableNameWithType, _pinotHelixResourceManager,
        RebalanceResult.Status.CANCELLED);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.READ)
  @Path("/rebalanceStatus/{jobId}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_REBALANCE_STATUS)
  @ApiOperation(value = "Gets detailed stats of a rebalance operation",
      notes = "Gets detailed stats of a rebalance operation")
  public ServerRebalanceJobStatusResponse rebalanceStatus(
      @ApiParam(value = "Rebalance Job Id", required = true) @PathParam("jobId") String jobId)
      throws JsonProcessingException {
    return _tableRebalanceManager.getRebalanceStatus(jobId);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/state")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_STATE)
  @ApiOperation(value = "Get current table state", notes = "Get current table state")
  public String getTableState(
      @ApiParam(value = "Name of the table to get its state", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
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
      @ApiParam(value = "enable|disable", required = true) @QueryParam("state") String state,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
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
  @Path("/tables/{tableName}/stats")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "table stats", notes = "Provides metadata info/stats about the table.")
  public String getTableStats(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    ObjectNode ret = JsonUtils.newObjectNode();
    if ((tableTypeStr == null || TableType.OFFLINE.name().equalsIgnoreCase(tableTypeStr))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName);
      TableStatsHumanReadable tableStats = _pinotHelixResourceManager.getTableStatsHumanReadable(tableNameWithType);
      ret.set(TableType.OFFLINE.name(), JsonUtils.objectToJsonNode(tableStats));
    }
    if ((tableTypeStr == null || TableType.REALTIME.name().equalsIgnoreCase(tableTypeStr))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
      TableStatsHumanReadable tableStats = _pinotHelixResourceManager.getTableStatsHumanReadable(tableNameWithType);
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

  @GET
  @Path("/tables/{tableName}/status")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "table status", notes = "Provides status of the table including ingestion status")
  public String getTableStatus(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
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
      @ApiParam(value = "Columns name", allowMultiple = true) @QueryParam("columns") List<String> columns,
      @ApiParam(value = "Include per-column compression stats in response (default false to avoid large responses)")
      @DefaultValue("false") @QueryParam("includeColumnCompressionStats") boolean includeColumnCompressionStats,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
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

    // compressionStatsEnabled gates server-side collection; includeColumnCompressionStats controls the per-column
    // response list.
    boolean compressionStatsEnabled = tableConfig != null && tableConfig.getIndexingConfig() != null
        && tableConfig.getIndexingConfig().isCompressionStatsEnabled();

    String segmentsMetadata;
    try {
      JsonNode segmentsMetadataJson =
          getAggregateMetadataFromServer(tableNameWithType, columns, numReplica, compressionStatsEnabled,
              includeColumnCompressionStats);
      segmentsMetadata = JsonUtils.objectToPrettyString(segmentsMetadataJson);
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    } catch (IOException ioe) {
      throw new ControllerApplicationException(LOGGER, "Error parsing Pinot server response: " + ioe.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, ioe);
    }
    return segmentsMetadata;
  }

  @GET
  @Path("/tables/{tableNameWithType}/aggregateMetadata")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableNameWithType", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the aggregate metadata of all segments for a table (deprecated endpoint)",
      notes = "Deprecated endpoint. Use /tables/{tableName}/metadata instead.")
  public String getTableAggregateMetadataDeprecated(
      @ApiParam(value = "Name of the table with type suffix", required = true) @PathParam("tableNameWithType")
      String tableNameWithType,
      @ApiParam(value = "Comma separated list of columns") @QueryParam("columns") @Nullable String columns,
      @ApiParam(value = "Include per-column compression stats in response (default false to avoid large responses)")
      @DefaultValue("false") @QueryParam("includeColumnCompressionStats") boolean includeColumnCompressionStats,
      @Context HttpHeaders headers) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    LOGGER.info("Received a request to fetch aggregate metadata for a table {}", tableNameWithType);
    String existingTableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableNameWithType, null, LOGGER).get(0);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(existingTableNameWithType);
    if (tableType == TableType.REALTIME) {
      throw new ControllerApplicationException(LOGGER, "Table type : " + tableType + " not yet supported.",
          Response.Status.NOT_IMPLEMENTED);
    }

    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(existingTableNameWithType);
    int numReplica = tableConfig == null ? 1 : tableConfig.getReplication();

    List<String> columnsList = List.of();
    if (StringUtils.isNotBlank(columns)) {
      String[] splitColumns = StringUtils.split(columns, ',');
      if (splitColumns != null && splitColumns.length > 0) {
        List<String> parsedColumns = new ArrayList<>(splitColumns.length);
        for (String column : splitColumns) {
          String trimmedColumn = StringUtils.trimToNull(column);
          if (trimmedColumn != null) {
            parsedColumns.add(trimmedColumn);
          }
        }
        columnsList = parsedColumns;
      }
    }

    boolean compressionStatsEnabled = tableConfig != null && tableConfig.getIndexingConfig() != null
        && tableConfig.getIndexingConfig().isCompressionStatsEnabled();

    try {
      JsonNode segmentsMetadataJson =
          getAggregateMetadataFromServer(existingTableNameWithType, columnsList, numReplica,
              compressionStatsEnabled, includeColumnCompressionStats);
      return JsonUtils.objectToPrettyString(segmentsMetadataJson);
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    } catch (IOException ioe) {
      throw new ControllerApplicationException(LOGGER, "Error parsing Pinot server response: " + ioe.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, ioe);
    }
  }

  @GET
  @Path("tables/{tableName}/validDocIdsMetadata")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the aggregate validDocIds metadata of all segments for a table", notes = "Get the "
      + "aggregate validDocIds metadata of all segments for a table")
  public String getTableAggregateValidDocIdsMetadata(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "A list of segments", allowMultiple = true) @QueryParam("segmentNames")
      List<String> segmentNames,
      @ApiParam(value = "Valid doc ids type") @QueryParam("validDocIdsType")
      @DefaultValue("SNAPSHOT") ValidDocIdsType validDocIdsType,
      @ApiParam(value = "Number of segments in a batch per server request")
      @QueryParam("serverRequestBatchSize") @DefaultValue("500") int serverRequestBatchSize,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    LOGGER.info("Received a request to fetch aggregate validDocIds metadata for a table {}", tableName);
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == TableType.OFFLINE) {
      throw new ControllerApplicationException(LOGGER, "Table type : " + tableTypeStr + " not yet supported.",
          Response.Status.NOT_IMPLEMENTED);
    }
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);

    String validDocIdsMetadata;
    try {
      TableMetadataReader tableMetadataReader =
          new TableMetadataReader(_executor, _connectionManager, _pinotHelixResourceManager);
      validDocIdsType = (validDocIdsType == null) ? ValidDocIdsType.SNAPSHOT : validDocIdsType;
      JsonNode segmentsMetadataJson =
          tableMetadataReader.getAggregateValidDocIdsMetadata(tableNameWithType, segmentNames,
              validDocIdsType.toString(), _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000,
              serverRequestBatchSize);
      validDocIdsMetadata = JsonUtils.objectToPrettyString(segmentsMetadataJson);
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    } catch (IOException ioe) {
      throw new ControllerApplicationException(LOGGER, "Error parsing Pinot server response: " + ioe.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, ioe);
    }
    return validDocIdsMetadata;
  }

  @GET
  @Path("tables/{tableName}/indexes")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the aggregate index details of all segments for a table", notes = "Get the aggregate "
      + "index details of all segments for a table")
  public String getTableIndexes(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    LOGGER.info("Received a request to fetch aggregate metadata for a table {}", tableName);
    TableType tableType = Constants.validateTableType(tableTypeStr);
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);

    String tableIndexMetadata;
    try {
      JsonNode segmentsMetadataJson = getAggregateIndexMetadataFromServer(tableNameWithType);
      tableIndexMetadata = JsonUtils.objectToPrettyString(segmentsMetadataJson);
    } catch (InvalidConfigException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST);
    } catch (IOException ioe) {
      throw new ControllerApplicationException(LOGGER, "Error parsing Pinot server response: " + ioe.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, ioe);
    }
    return tableIndexMetadata;
  }

  private JsonNode getAggregateIndexMetadataFromServer(String tableNameWithType)
      throws InvalidConfigException, JsonProcessingException {
    final Map<String, List<String>> serverToSegments =
        _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType);

    BiMap<String, String> serverEndPoints =
        _pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, serverEndPoints,
            _pinotHelixResourceManager.getServerAdminAuthProvider());

    List<String> serverUrls = new ArrayList<>();
    BiMap<String, String> endpointsToServers = serverEndPoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String segmentIndexesEndpoint = endpoint + String.format("/tables/%s/indexes", tableNameWithType);
      serverUrls.add(segmentIndexesEndpoint);
    }

    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, null, true, 10000);

    int totalSegments = 0;
    Map<String, Map<String, Integer>> columnToIndexCountMap = new HashMap<>();
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      String responseString = streamResponse.getValue();
      TableIndexMetadataResponse response = JsonUtils.stringToObject(responseString, TableIndexMetadataResponse.class);
      totalSegments += response.getTotalOnlineSegments();
      response.getColumnToIndexesCount().forEach((col, indexToCount) -> {
        Map<String, Integer> indexCountMap = columnToIndexCountMap.computeIfAbsent(col, c -> new HashMap<>());
        indexToCount.forEach((indexName, count) -> {
          indexCountMap.merge(indexName, count, Integer::sum);
        });
      });
    }

    TableIndexMetadataResponse tableIndexMetadataResponse =
        new TableIndexMetadataResponse(totalSegments, columnToIndexCountMap);

    return JsonUtils.objectToJsonNode(tableIndexMetadataResponse);
  }

  /// This is a helper method to get the metadata for all segments for a given table name.
  /// @param tableNameWithType name of the table along with its type
  /// @param columns name of the columns
  /// @param numReplica num or replica for the table
  /// @return aggregated metadata of the table segments
  private JsonNode getAggregateMetadataFromServer(String tableNameWithType, List<String> columns, int numReplica,
      boolean compressionStatsEnabled, boolean includeColumnCompressionStats)
      throws InvalidConfigException, IOException {
    TableMetadataReader tableMetadataReader =
        new TableMetadataReader(_executor, _connectionManager, _pinotHelixResourceManager);
    return tableMetadataReader.getAggregateTableMetadata(tableNameWithType, columns, numReplica,
        _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000, compressionStatsEnabled,
        includeColumnCompressionStats);
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
      @ApiParam(value = "Comma separated list of job types") @QueryParam("jobTypes") @Nullable String jobTypesString,
      @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    TableType tableTypeFromRequest = Constants.validateTableType(tableTypeStr);
    List<String> tableNamesWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableTypeFromRequest,
            LOGGER);
    Set<ControllerJobType> jobTypesToFilter = null;
    if (StringUtils.isNotEmpty(jobTypesString)) {
      jobTypesToFilter = new HashSet<>();
      for (String jobTypeStr : StringUtils.split(jobTypesString, ',')) {
        ControllerJobTypes jobType;
        try {
          jobType = ControllerJobTypes.valueOf(jobTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
          throw new ControllerApplicationException(LOGGER, "Unknown job type: " + jobTypeStr,
              Response.Status.BAD_REQUEST);
        }
        jobTypesToFilter.add(jobType);
      }
    }
    Map<String, Map<String, String>> result = new HashMap<>();
    for (String tableNameWithType : tableNamesWithType) {
      result.putAll(_pinotHelixResourceManager.getAllJobs(jobTypesToFilter == null
              ? new HashSet<>(EnumSet.allOf(ControllerJobTypes.class)) : jobTypesToFilter,
          jobMetadata -> jobMetadata.get(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE)
              .equals(tableNameWithType)));
    }
    return result;
  }

  @POST
  @Path("tables/{tableName}/timeBoundary")
  @Authenticate(AccessType.UPDATE)
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.UPDATE_TABLE_CONFIG)
  @ApiOperation(value = "Set hybrid table query time boundary based on offline segments' metadata", notes = "Set "
      + "hybrid table query time boundary based on offline segments' metadata")
  @Produces(MediaType.APPLICATION_JSON)
  public SuccessResponse setTimeBoundary(
      @ApiParam(value = "Name of the hybrid table (without type suffix)", required = true) @PathParam("tableName")
      String tableName, @Context HttpHeaders headers)
      throws Exception {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
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
  @Authenticate(AccessType.DELETE)
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.DELETE_TIME_BOUNDARY)
  @ApiOperation(value = "Delete hybrid table query time boundary", notes = "Delete hybrid table query time boundary")
  @Produces(MediaType.APPLICATION_JSON)
  public SuccessResponse deleteTimeBoundary(
      @ApiParam(value = "Name of the hybrid table (without type suffix)", required = true) @PathParam("tableName")
      String tableName, @Context HttpHeaders headers) {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
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
        new CompletionServiceHelper(_executor, _connectionManager, serverEndPoints,
            _pinotHelixResourceManager.getServerAdminAuthProvider());
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
        String error = "Table segment validation failed. error=" + tableSegmentValidationInfo.getInvalidReason();
        throw new ControllerApplicationException(LOGGER, error, Response.Status.PRECONDITION_FAILED);
      }
      timeBoundaryMs = Math.max(timeBoundaryMs, tableSegmentValidationInfo.getMaxEndTimeMs());
    }

    return timeBoundaryMs;
  }
}
