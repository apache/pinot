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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentConsumerInfo;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.common.restlet.resources.SegmentServerDebugInfo;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.debug.TableDebugInfo;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.controller.util.TableIngestionStatusHelper;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableStatus;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * This class implements the debugging endpoints.
 * NOTE: Debug classes are not expected to guarantee backward compatibility, and should
 * not be exposed to the client side.
 */
@Api(tags = Constants.CLUSTER_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE)}))
@Path("/debug/")
public class DebugResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DebugResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @Inject
  Executor _executor;

  @Inject
  HttpClientConnectionManager _connectionManager;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  ControllerConf _controllerConf;

  @GET
  @Path("tables/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_DEBUG_INFO)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get debug information for table.", notes = "Debug information for table.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String getTableDebugInfo(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Verbosity of debug information") @DefaultValue("0") @QueryParam("verbosity") int verbosity,
      @Context HttpHeaders headers)
      throws JsonProcessingException {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    ObjectNode root = JsonUtils.newObjectNode();
    root.put("clusterName", _pinotHelixResourceManager.getHelixClusterName());

    List<TableType> tableTypes = getValidTableTypes(tableName, tableTypeStr, _pinotHelixResourceManager);
    if (tableTypes.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Table '" + tableName + "' not found",
          Response.Status.NOT_FOUND);
    }

    List<TableDebugInfo> tableDebugInfos = new ArrayList<>();
    for (TableType type : tableTypes) {
      tableDebugInfos.add(debugTable(_pinotHelixResourceManager, tableName, type, verbosity));
    }

    return JsonUtils.objectToPrettyString(tableDebugInfos);
  }

  @GET
  @Path("segments/{tableName}/{segmentName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_DEBUG_INFO)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get debug information for segment.", notes = "Debug information for segment.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 404, message = "Segment not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public TableDebugInfo.SegmentDebugInfo getSegmentDebugInfo(
      @ApiParam(value = "Name of the table (with type)", required = true) @PathParam("tableName")
          String tableNameWithType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") String segmentName,
      @Context HttpHeaders headers)
      throws Exception {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    return debugSegment(tableNameWithType, segmentName);
  }

  /**
   * Helper method to collect debug information about the table.
   *
   * @param pinotHelixResourceManager Helix Resource Manager for the cluster
   * @param tableName Name of the table
   * @param tableType Type of the table (offline|realtime)
   * @param verbosity Verbosity level to include debug information. For level 0, only segments
   *                  with errors are included. For level > 0, all segments are included.
   * @return TableDebugInfo Debug info for table.
   */
  private TableDebugInfo debugTable(PinotHelixResourceManager pinotHelixResourceManager, String tableName,
      TableType tableType, int verbosity) {
    String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);

    // Debug information for segments of the table.
    List<TableDebugInfo.SegmentDebugInfo> segmentDebugInfos =
        debugSegments(pinotHelixResourceManager, tableNameWithType, verbosity);

    // Debug information from brokers of the table.
    List<TableDebugInfo.BrokerDebugInfo> brokerDebugInfos = debugBrokers(tableNameWithType, verbosity);

    // Debug information from servers of the table.
    List<TableDebugInfo.ServerDebugInfo> serverDebugInfos =
        debugServers(pinotHelixResourceManager, tableName, tableType);

    // Table size summary.
    TableDebugInfo.TableSizeSummary tableSizeSummary = getTableSize(tableNameWithType);

    TableStatus.IngestionStatus ingestionStatus = getIngestionStatus(tableNameWithType, tableType);

    // Number of segments in the table.
    IdealState idealState = _pinotHelixResourceManager.getTableIdealState(tableNameWithType);
    int numSegments = (idealState != null) ? idealState.getPartitionSet().size() : 0;

    return new TableDebugInfo(tableNameWithType, ingestionStatus, tableSizeSummary,
        _pinotHelixResourceManager.getBrokerInstancesForTable(tableName, tableType).size(),
        _pinotHelixResourceManager.getServerInstancesForTable(tableName, tableType).size(), numSegments,
        segmentDebugInfos, serverDebugInfos, brokerDebugInfos);
  }

  private TableStatus.IngestionStatus getIngestionStatus(String tableNameWithType, TableType tableType) {
    try {
      switch (tableType) {
        case OFFLINE:
          return TableIngestionStatusHelper
              .getOfflineTableIngestionStatus(tableNameWithType, _pinotHelixResourceManager,
                  _pinotHelixTaskResourceManager);
        case REALTIME:
          return TableIngestionStatusHelper.getRealtimeTableIngestionStatus(tableNameWithType,
              _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000, _executor, _connectionManager,
              _pinotHelixResourceManager);
        default:
          break;
      }
    } catch (Exception e) {
      return TableStatus.IngestionStatus.newIngestionStatus(TableStatus.IngestionState.UNKNOWN, e.getMessage());
    }
    return null;
  }

  private TableDebugInfo.TableSizeSummary getTableSize(String tableNameWithType) {
    TableSizeReader tableSizeReader =
        new TableSizeReader(_executor, _connectionManager, _controllerMetrics, _pinotHelixResourceManager);
    TableSizeReader.TableSizeDetails tableSizeDetails;
    try {
      tableSizeDetails = tableSizeReader
          .getTableSizeDetails(tableNameWithType, _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
    } catch (Throwable t) {
      tableSizeDetails = null;
    }

    return (tableSizeDetails != null) ? new TableDebugInfo.TableSizeSummary(tableSizeDetails._reportedSizeInBytes,
        tableSizeDetails._estimatedSizeInBytes) : new TableDebugInfo.TableSizeSummary(-1, -1);
  }

  private TableDebugInfo.SegmentDebugInfo debugSegment(String tableNameWithType, String segmentName)
      throws IOException {
    IdealState idealState = _pinotHelixResourceManager.getTableIdealState(tableNameWithType);
    if (idealState == null) {
      return null;
    }

    ExternalView externalView = _pinotHelixResourceManager.getTableExternalView(tableNameWithType);
    Map<String, String> evStateMap = (externalView != null) ? externalView.getStateMap(segmentName) : null;

    Map<String, String> isServerToStateMap = idealState.getRecord().getMapFields().get(segmentName);
    Set<String> serversHostingSegment = _pinotHelixResourceManager.getServers(tableNameWithType, segmentName);

    int serverRequestTimeoutMs = _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000;
    BiMap<String, String> serverToEndpoints;
    try {
      serverToEndpoints = _pinotHelixResourceManager.getDataInstanceAdminEndpoints(serversHostingSegment);
    } catch (InvalidConfigException e) {
      throw new WebApplicationException(
          "Caught exception when getting segment debug info for table: " + tableNameWithType);
    }

    List<String> serverUrls = new ArrayList<>(serverToEndpoints.size());
    BiMap<String, String> endpointsToServers = serverToEndpoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String segmentDebugInfoURI = String.format("%s/debug/segments/%s/%s", endpoint, tableNameWithType, segmentName);
      serverUrls.add(segmentDebugInfoURI);
    }

    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, tableNameWithType, false, serverRequestTimeoutMs);

    Map<String, SegmentServerDebugInfo> serverToSegmentDebugInfo = new HashMap<>();
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      SegmentServerDebugInfo segmentDebugInfo =
          JsonUtils.stringToObject(streamResponse.getValue(), SegmentServerDebugInfo.class);
      serverToSegmentDebugInfo.put(streamResponse.getKey(), segmentDebugInfo);
    }

    Map<String, TableDebugInfo.SegmentState> segmentServerState = new HashMap<>();
    for (String instanceName : isServerToStateMap.keySet()) {
      String isState = isServerToStateMap.get(instanceName);
      String evState = (evStateMap != null) ? evStateMap.get(instanceName) : null;
      SegmentServerDebugInfo segmentServerDebugInfo = serverToSegmentDebugInfo.get(instanceName);

      if (segmentServerDebugInfo != null) {
        segmentServerState.put(instanceName,
            new TableDebugInfo.SegmentState(isState, evState, segmentServerDebugInfo.getSegmentSize(),
                segmentServerDebugInfo.getConsumerInfo(), segmentServerDebugInfo.getErrorInfo()));
      } else {
        segmentServerState.put(instanceName, new TableDebugInfo.SegmentState(isState, evState, null, null, null));
      }
    }

    return new TableDebugInfo.SegmentDebugInfo(segmentName, segmentServerState);
  }

  /**
   * Helper method to debug segments. Computes differences between ideal state and external view for each segment.
   *
   * @param pinotHelixResourceManager Helix Resource Manager
   * @param tableNameWithType Name of table with type
   * @param verbosity Verbosity level to include debug information. For level 0, only segments
   *                  with errors are included. For level > 0, all segments are included.   * @return Debug
   *                  information for segments
   */
  private List<TableDebugInfo.SegmentDebugInfo> debugSegments(PinotHelixResourceManager pinotHelixResourceManager,
      String tableNameWithType, int verbosity) {
    ExternalView externalView = pinotHelixResourceManager.getTableExternalView(tableNameWithType);
    IdealState idealState = pinotHelixResourceManager.getTableIdealState(tableNameWithType);

    List<TableDebugInfo.SegmentDebugInfo> result = new ArrayList<>();
    if (idealState == null) {
      return result;
    }

    int serverRequestTimeoutMs = _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000;
    final Map<String, List<String>> serverToSegments =
        _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType);

    BiMap<String, String> serverToEndpoints;
    try {
      serverToEndpoints = _pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
    } catch (InvalidConfigException e) {
      throw new WebApplicationException(
          "Caught exception when getting segment debug info for table: " + tableNameWithType);
    }

    Map<String, Map<String, SegmentServerDebugInfo>> segmentsDebugInfoFromServers =
        getSegmentsDebugInfoFromServers(tableNameWithType, serverToEndpoints, serverRequestTimeoutMs);

    for (Map.Entry<String, Map<String, String>> segmentMapEntry : idealState.getRecord().getMapFields().entrySet()) {
      String segmentName = segmentMapEntry.getKey();
      Map<String, String> segmentIsMap = segmentMapEntry.getValue();

      Map<String, TableDebugInfo.SegmentState> segmentServerState = new HashMap<>();
      for (Map.Entry<String, Map<String, SegmentServerDebugInfo>> segmentEntry : segmentsDebugInfoFromServers
          .entrySet()) {
        String instanceName = segmentEntry.getKey();
        String isState = segmentIsMap.get(instanceName);

        Map<String, String> evStateMap = (externalView != null) ? externalView.getStateMap(segmentName) : null;
        String evState = (evStateMap != null) ? evStateMap.get(instanceName) : null;

        if (evState != null) {
          Map<String, SegmentServerDebugInfo> segmentServerDebugInfoMap = segmentEntry.getValue();
          SegmentServerDebugInfo segmentServerDebugInfo = segmentServerDebugInfoMap.get(segmentName);

          if (segmentServerDebugInfo != null && (verbosity > 0 || segmentHasErrors(segmentServerDebugInfo, evState))) {
            segmentServerState.put(instanceName,
                new TableDebugInfo.SegmentState(isState, evState, segmentServerDebugInfo.getSegmentSize(),
                    segmentServerDebugInfo.getConsumerInfo(), segmentServerDebugInfo.getErrorInfo()));
          }
        } else {
          segmentServerState.put(instanceName, new TableDebugInfo.SegmentState(isState, null, null, null, null));
        }
      }

      if (!segmentServerState.isEmpty()) {
        result.add(new TableDebugInfo.SegmentDebugInfo(segmentName, segmentServerState));
      }
    }

    return result;
  }

  /**
   * Helper method to check if a segment has any errors/issues.
   *
   * @param segmentServerDebugInfo Segment debug info on server
   * @param externalView external view of segment
   * @return True if there's any error/issue for the segment, false otherwise.
   */
  private boolean segmentHasErrors(SegmentServerDebugInfo segmentServerDebugInfo, String externalView) {
    // For now, we will skip cases where IS is ONLINE and EV is OFFLINE (or vice-versa), as it could happen during
    // state transitions.
    if (externalView.equals("ERROR")) {
      return true;
    }

    SegmentConsumerInfo consumerInfo = segmentServerDebugInfo.getConsumerInfo();
    if (consumerInfo != null && consumerInfo.getConsumerState()
        .equals(CommonConstants.ConsumerState.NOT_CONSUMING.toString())) {
      return true;
    }

    SegmentErrorInfo errorInfo = segmentServerDebugInfo.getErrorInfo();
    return errorInfo != null && (errorInfo.getErrorMessage() != null || errorInfo.getStackTrace() != null);
  }

  /**
   * Computes debug information for brokers. Identifies differences in ideal state and external view
   * for the broker resource.
   *
   * @param tableNameWithType Name of table with type suffix
   * @param verbosity Verbosity level
   * @return Debug information for brokers of the table
   */
  private List<TableDebugInfo.BrokerDebugInfo> debugBrokers(String tableNameWithType, int verbosity) {
    List<TableDebugInfo.BrokerDebugInfo> brokerDebugInfos = new ArrayList<>();
    HelixDataAccessor helixDataAccessor = _pinotHelixResourceManager.getHelixZkManager().getHelixDataAccessor();
    IdealState idealState =
        helixDataAccessor.getProperty(helixDataAccessor.keyBuilder().idealStates(BROKER_RESOURCE_INSTANCE));
    ExternalView externalView =
        helixDataAccessor.getProperty(helixDataAccessor.keyBuilder().externalView(BROKER_RESOURCE_INSTANCE));

    for (Map.Entry<String, String> entry : idealState.getInstanceStateMap(tableNameWithType).entrySet()) {
      String brokerName = entry.getKey();
      String isState = entry.getValue();
      String evState = externalView.getStateMap(tableNameWithType).get(brokerName);
      if (verbosity > 0 || !isState.equals(evState)) {
        brokerDebugInfos.add(new TableDebugInfo.BrokerDebugInfo(brokerName, isState, evState));
      }
    }
    return brokerDebugInfos;
  }

  /**
   * Computes debug information for servers.
   *
   * @param pinotHelixResourceManager Helix Resource Manager
   * @param tableName Name of table
   * @param tableType Type of table (offline|realtime)
   * @return Debug information for servers of the table
   */
  private List<TableDebugInfo.ServerDebugInfo> debugServers(PinotHelixResourceManager pinotHelixResourceManager,
      String tableName, TableType tableType) {
    HelixDataAccessor accessor = _pinotHelixResourceManager.getHelixZkManager().getHelixDataAccessor();
    List<TableDebugInfo.ServerDebugInfo> serverDebugInfos = new ArrayList<>();

    for (String instanceName : pinotHelixResourceManager.getServerInstancesForTable(tableName, tableType)) {
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      List<String> sessionIds = accessor.getChildNames(keyBuilder.errors(instanceName));

      if (sessionIds == null || sessionIds.isEmpty()) {
        return serverDebugInfos;
      }

      int numErrors = 0;
      String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
      for (String sessionId : sessionIds) {
        List<HelixProperty> childValues =
            accessor.getChildValues(keyBuilder.errors(instanceName, sessionId, tableNameWithType), false);
        numErrors += ((childValues != null) ? childValues.size() : 0);
      }

      List<String> messageNames = accessor.getChildNames(accessor.keyBuilder().messages(instanceName));
      int numMessages = (messageNames != null) ? messageNames.size() : 0;

      serverDebugInfos.add(new TableDebugInfo.ServerDebugInfo(instanceName, numErrors, numMessages));
    }
    return serverDebugInfos;
  }

  /**
   * This method makes a MultiGet call to all servers to get the segments debug info.
   * @return Map of ServerName ->  (map of SegmentName -> SegmentServerDebugInfo).
   */
  private Map<String, Map<String, SegmentServerDebugInfo>> getSegmentsDebugInfoFromServers(String tableNameWithType,
      BiMap<String, String> serverToEndpoints, int timeoutMs) {
    LOGGER.info("Reading segments debug info from servers: {} for table: {}", serverToEndpoints.keySet(),
        tableNameWithType);

    List<String> serverUrls = new ArrayList<>(serverToEndpoints.size());
    BiMap<String, String> endpointsToServers = serverToEndpoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String segmentDebugInfoURI = String.format("%s/debug/tables/%s", endpoint, tableNameWithType);
      serverUrls.add(segmentDebugInfoURI);
    }

    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, tableNameWithType, false, timeoutMs);

    // Map from InstanceName -> <Segment -> DebugInfo>
    Map<String, Map<String, SegmentServerDebugInfo>> serverToSegmentDebugInfoMap = new HashMap<>();
    int failedParses = 0;
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        List<SegmentServerDebugInfo> segmentDebugInfos =
            JsonUtils.stringToObject(streamResponse.getValue(), new TypeReference<List<SegmentServerDebugInfo>>() {
            });
        Map<String, SegmentServerDebugInfo> segmentsMap = segmentDebugInfos.stream()
            .collect(Collectors.toMap(SegmentServerDebugInfo::getSegmentName, Function.identity()));
        serverToSegmentDebugInfoMap.put(streamResponse.getKey(), segmentsMap);
      } catch (IOException e) {
        failedParses++;
        LOGGER.error("Unable to parse server {} response due to an error: ", streamResponse.getKey(), e);
      }
    }
    if (failedParses != 0) {
      LOGGER.warn("Failed to parse {} / {} segment size info responses from servers.", failedParses, serverUrls.size());
    }
    return serverToSegmentDebugInfoMap;
  }

  /**
   * Helper method to get valid types of table that exist in the cluster for the given table name.
   *
   * @param tableName Name of table
   * @param tableTypeStr Type of table
   * @param pinotHelixResourceManager Pinot Helix Resource manager
   * @return List of valid table types that exist in the cluster
   */
  private List<TableType> getValidTableTypes(String tableName, String tableTypeStr,
      PinotHelixResourceManager pinotHelixResourceManager) {
    TableType tableType = Constants.validateTableType(tableTypeStr);
    List<TableType> tableTypes = new ArrayList<>();

    if (tableType == null) {
      if (pinotHelixResourceManager.hasOfflineTable(tableName)) {
        tableTypes.add(TableType.OFFLINE);
      }
      if (pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        tableTypes.add(TableType.REALTIME);
      }
    } else {
      String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
      if (pinotHelixResourceManager.hasTable(tableNameWithType)) {
        tableTypes.add(tableType);
      }
    }
    return tableTypes;
  }
}
