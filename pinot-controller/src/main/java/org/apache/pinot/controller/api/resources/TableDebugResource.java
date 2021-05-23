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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.debug.TableDebugInfo;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;


/**
 * This class implements the debugging endpoints.
 */
@Api(tags = Constants.CLUSTER_TAG)
@Path("/")
public class TableDebugResource {
  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  Executor _executor;
  @Inject
  HttpConnectionManager _connectionManager;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  ControllerConf _controllerConf;

  @GET
  @Path("/debug/tables/{tableName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get debug information for table.", notes = "Debug information for table.")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")})
  public String getClusterInfo(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr)
      throws JsonProcessingException {
    ObjectNode root = JsonUtils.newObjectNode();
    root.put("clusterName", _pinotHelixResourceManager.getHelixClusterName());

    TableType tableType = Constants.validateTableType(tableTypeStr);
    List<TableType> tableTypes = (tableType == null) ? Arrays.asList(TableType.OFFLINE, TableType.REALTIME)
        : Collections.singletonList(tableType);

    List<TableDebugInfo> tableDebugInfos = new ArrayList<>();
    for (TableType type : tableTypes) {
      tableDebugInfos.add(debugTable(_pinotHelixResourceManager, tableName, type));
    }
    return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(tableDebugInfos);
  }

  /**
   * Helper method to colelct debug information about the table.
   *
   * @param pinotHelixResourceManager Helix Resource Manager for the cluster
   * @param tableName Name of the table
   * @param tableType Type of the table (offline|realtime)
   * @return TableDebugInfo
   */
  private TableDebugInfo debugTable(PinotHelixResourceManager pinotHelixResourceManager, String tableName,
      TableType tableType) {
    String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);

    // Debug information for segments of the table.
    List<TableDebugInfo.SegmentDebugInfo> segmentDebugInfos =
        debugSegments(pinotHelixResourceManager, tableNameWithType);

    // Debug information from brokers of the table.
    List<TableDebugInfo.BrokerDebugInfo> brokerDebugInfos = debugBrokers(tableNameWithType);

    // Debug information from servers of the table.
    List<TableDebugInfo.ServerDebugInfo> serverDebugInfos =
        debugServers(pinotHelixResourceManager, tableName, tableType);

    // Table size summary.
    TableDebugInfo.TableSizeSummary tableSizeSummary = getTableSize(tableNameWithType);

    // Number of segments in the table.
    IdealState idealState = _pinotHelixResourceManager.getTableIdealState(tableNameWithType);
    int numSegments = (idealState != null) ? idealState.getPartitionSet().size() : 0;

    return new TableDebugInfo(tableNameWithType, tableSizeSummary,
        _pinotHelixResourceManager.getBrokerInstancesForTable(tableName, tableType).size(),
        _pinotHelixResourceManager.getServerInstancesForTable(tableName, tableType).size(), numSegments,
        segmentDebugInfos, serverDebugInfos, brokerDebugInfos);
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

    return (tableSizeDetails != null) ? new TableDebugInfo.TableSizeSummary(tableSizeDetails.reportedSizeInBytes,
        tableSizeDetails.estimatedSizeInBytes) : new TableDebugInfo.TableSizeSummary(-1, -1);
  }

  /**
   * Helper method to debug segments. Computes differences between ideal state and external view for each segment.
   *
   * @param pinotHelixResourceManager Helix Resource Manager
   * @param tableNameWithType Name of table with type
   * @return Debug information for segments
   */
  private List<TableDebugInfo.SegmentDebugInfo> debugSegments(PinotHelixResourceManager pinotHelixResourceManager,
      String tableNameWithType) {
    ExternalView externalView = pinotHelixResourceManager.getTableExternalView(tableNameWithType);
    IdealState idealState = pinotHelixResourceManager.getTableIdealState(tableNameWithType);

    List<TableDebugInfo.SegmentDebugInfo> segmentDebugInfos = new ArrayList<>();
    if (idealState == null) {
      return segmentDebugInfos;
    }

    for (Map.Entry<String, Map<String, String>> segmentMapEntry : idealState.getRecord().getMapFields().entrySet()) {
      String segment = segmentMapEntry.getKey();
      Map<String, String> instanceStateMap = segmentMapEntry.getValue();

      List<TableDebugInfo.IsEvState> isEvStates = new ArrayList<>();
      for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
        String serverName = entry.getKey();
        String isState = entry.getValue();

        String evState = (externalView != null) ? externalView.getStateMap(segment).get(serverName) : "null";
        if (!isState.equals(evState)) {
          isEvStates.add(new TableDebugInfo.IsEvState(serverName, isState, evState));
        }
      }

      if (isEvStates.size() > 0) {
        segmentDebugInfos.add(new TableDebugInfo.SegmentDebugInfo(segment, isEvStates));
      }
    }
    return segmentDebugInfos;
  }

  /**
   * Computes debug information for brokers. Identifies differences in ideal state and external view
   * for the broker resource.
   *
   * @param tableNameWithType Name of table with type suffix
   * @return Debug information for brokers of the table
   */
  private List<TableDebugInfo.BrokerDebugInfo> debugBrokers(String tableNameWithType) {
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
      if (!isState.equals(evState)) {
        brokerDebugInfos.add(
            new TableDebugInfo.BrokerDebugInfo(brokerName, new TableDebugInfo.IsEvState(brokerName, isState, evState)));
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

      if (sessionIds == null || sessionIds.size() == 0) {
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
}
