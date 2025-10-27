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
package org.apache.pinot.server.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.restlet.resources.SegmentConsumerInfo;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.common.restlet.resources.SegmentServerDebugInfo;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.server.api.AdminApiApplication;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceTracker;
import org.apache.pinot.spi.accounting.WorkloadBudgetManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * Debug resource for Pinot Server.
 */
@Api(tags = "Debug", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/debug/")
public class DebugResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DebugResource.class);

  @Inject
  private ServerInstance _serverInstance;
  @Inject
  @Named(AdminApiApplication.SERVER_INSTANCE_ID)
  private String _instanceId;

  @GET
  @Path("tables/{tableName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get segments debug info for this table",
      notes = "This is a debug endpoint, and won't maintain backward compatibility")
  public List<SegmentServerDebugInfo> getSegmentsDebugInfo(
      @ApiParam(value = "Name of the table (with type)", required = true) @PathParam("tableName")
          String tableNameWithType, @Context HttpHeaders headers) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    return getSegmentServerDebugInfo(tableNameWithType, tableType);
  }

  @GET
  @Path("segments/{tableName}/{segmentName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get segment debug info",
      notes = "This is a debug endpoint, and won't maintain backward compatibility")
  public SegmentServerDebugInfo getSegmentDebugInfo(
      @ApiParam(value = "Name of the table (with type)", required = true) @PathParam("tableName")
          String tableNameWithType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") String segmentName,
      @Context HttpHeaders headers) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    Map<String, SegmentErrorInfo> segmentErrorsMap = tableDataManager.getSegmentErrors();
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    try {
      SegmentConsumerInfo segmentConsumerInfo = getSegmentConsumerInfo(tableDataManager, segmentDataManager, tableType);
      long segmentSize = getSegmentSize(segmentDataManager);
      SegmentErrorInfo segmentErrorInfo = segmentErrorsMap.get(segmentName);
      return new SegmentServerDebugInfo(segmentName, FileUtils.byteCountToDisplaySize(segmentSize), segmentConsumerInfo,
          segmentErrorInfo);
    } catch (Exception e) {
      throw new WebApplicationException(
          "Caught exception when getting consumer info for table: " + tableNameWithType + " segment: " + segmentName);
    } finally {
      if (segmentDataManager != null) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }

  @GET
  @Path("threads/resourceUsage")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get current resource usage of threads",
      notes = "This is a debug endpoint, and won't maintain backward compatibility")
  public Collection<? extends ThreadResourceTracker> getThreadUsage() {
    return _serverInstance.getThreadAccountant().getThreadResources();
  }

  @GET
  @Path("queries/resourceUsage")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get current resource usage of queries in this service",
      notes = "This is a debug endpoint, and won't maintain backward compatibility")
  public Collection<? extends QueryResourceTracker> getQueryUsage() {
    return _serverInstance.getThreadAccountant().getQueryResources().values();
  }

  private List<SegmentServerDebugInfo> getSegmentServerDebugInfo(String tableNameWithType, TableType tableType) {
    List<SegmentServerDebugInfo> segmentServerDebugInfos = new ArrayList<>();

    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);

    Map<String, SegmentErrorInfo> segmentErrorsMap = tableDataManager.getSegmentErrors();
    Set<String> segmentsWithDataManagers = new HashSet<>();
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        String segmentName = segmentDataManager.getSegmentName();
        segmentsWithDataManagers.add(segmentName);

        // Get segment consumer info.
        SegmentConsumerInfo segmentConsumerInfo =
            getSegmentConsumerInfo(tableDataManager, segmentDataManager, tableType);

        // Get segment size.
        long segmentSize = getSegmentSize(segmentDataManager);

        // Get segment error.
        SegmentErrorInfo segmentErrorInfo = segmentErrorsMap.get(segmentName);

        segmentServerDebugInfos.add(
            new SegmentServerDebugInfo(segmentName, FileUtils.byteCountToDisplaySize(segmentSize), segmentConsumerInfo,
                segmentErrorInfo));
      }
    } catch (Exception e) {
      throw new WebApplicationException("Caught exception when getting consumer info for table: " + tableNameWithType);
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }

    // There may be segment errors for segments without Data Managers (e.g. segment wasn't loaded).
    for (Map.Entry<String, SegmentErrorInfo> entry : segmentErrorsMap.entrySet()) {
      String segmentName = entry.getKey();

      if (!segmentsWithDataManagers.contains(segmentName)) {
        SegmentErrorInfo segmentErrorInfo = entry.getValue();
        segmentServerDebugInfos.add(new SegmentServerDebugInfo(segmentName, null, null, segmentErrorInfo));
      }
    }
    return segmentServerDebugInfos;
  }

  private long getSegmentSize(SegmentDataManager segmentDataManager) {
    return (segmentDataManager instanceof ImmutableSegmentDataManager) ? ((ImmutableSegment) segmentDataManager
        .getSegment()).getSegmentSizeBytes() : 0;
  }

  private SegmentConsumerInfo getSegmentConsumerInfo(TableDataManager tableDataManager,
      SegmentDataManager segmentDataManager, TableType tableType) {
    SegmentConsumerInfo segmentConsumerInfo = null;
    if (tableType == TableType.REALTIME) {
      RealtimeSegmentDataManager realtimeSegmentDataManager = (RealtimeSegmentDataManager) segmentDataManager;
      StreamMetadataProvider streamMetadataProvider =
          ((RealtimeTableDataManager) (tableDataManager)).getStreamMetadataProvider(realtimeSegmentDataManager);
      StreamPartitionMsgOffset latestMsgOffset;
      try {
        latestMsgOffset =
            streamMetadataProvider.fetchStreamPartitionOffset(OffsetCriteria.LARGEST_OFFSET_CRITERIA, 5000);
      } catch (Exception e) {
        LOGGER.error("Failed to fetch latest stream offset.", e);
        throw new RuntimeException(e);
      }
      Map<String, ConsumerPartitionState> partitionIdToStateMap =
          realtimeSegmentDataManager.getConsumerPartitionState(latestMsgOffset);
      Map<String, String> currentOffsets = realtimeSegmentDataManager.getPartitionToCurrentOffset();
      Map<String, String> upstreamLatest = partitionIdToStateMap.entrySet().stream().collect(
          Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getUpstreamLatestOffset().toString()));
      Map<String, String> recordsLagMap = new HashMap<>();
      Map<String, String> availabilityLagMsMap = new HashMap<>();
      Map<String, PartitionLagState> partitionToLagState =
          streamMetadataProvider.getCurrentPartitionLagState(partitionIdToStateMap);
      partitionToLagState.forEach((k, v) -> {
        recordsLagMap.put(k, v.getRecordsLag());
        availabilityLagMsMap.put(k, v.getAvailabilityLagMs());
      });

      segmentConsumerInfo =
          new SegmentConsumerInfo(
              segmentDataManager.getSegmentName(),
              realtimeSegmentDataManager.getConsumerState().toString(),
              realtimeSegmentDataManager.getLastConsumedTimestamp(),
              currentOffsets,
              new SegmentConsumerInfo.PartitionOffsetInfo(currentOffsets,
                  upstreamLatest, recordsLagMap, availabilityLagMsMap));
    }
    return segmentConsumerInfo;
  }

  /**
   * Get the instance cost (budget) information for a specific workload.
   * <p>
   * Returns CPU and memory budget information enforced on this instance for the workload.
   *
   * <p>Example response:
   * <pre>
   * {
   *   "workloadName": "testWorkload",
   *   "cpuBudgetNs": 5000000,
   *   "memoryBudgetBytes": 104857600,
   *   "cpuRemainingNs": 3500000,
   *   "memoryRemainingBytes": 73400320
   * }
   * </pre>
   *
   * @param workloadName the name of the workload to query
   * @return workload budget information including CPU and memory limits and remaining capacity
   * @throws WebApplicationException with one of the following status codes:
   *         <ul>
   *         <li>400 - if the workload name is invalid</li>
   *         <li>404 - if the workload is not found</li>
   *         <li>500 - if the WorkloadBudgetManager is not available</li>
   *         </ul>
   */
  @GET
  @Path("queryWorkloadCost/{workloadName}")
  @ApiOperation(value = "Get instance cost information for a specific workload")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Workload not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  @Produces(MediaType.APPLICATION_JSON)
  public String getWorkloadBudgetStats(
      @ApiParam(value = "Name of the workload", required = true) @PathParam("workloadName") String workloadName
  ) {
    // Input validation
    if (workloadName == null || workloadName.trim().isEmpty()) {
      throw new WebApplicationException("Workload name cannot be null or empty",
          Response.Status.BAD_REQUEST);
    }
    try {
      WorkloadBudgetManager workloadBudgetManager = requireWorkloadBudgetManager();

      WorkloadBudgetManager.BudgetStats budgetStats = workloadBudgetManager.getBudgetStats(workloadName);
      if (budgetStats == null) {
        LOGGER.warn("No budget stats found for workload: {} on instance: {}", workloadName, _instanceId);
        throw new WebApplicationException("Workload not found: " + workloadName, Response.Status.NOT_FOUND);
      }
      return ResourceUtils.convertToJsonString(toWorkloadBudgetMap(workloadName, budgetStats));
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("Error getting workload info for workload: {} on instance: {}", workloadName, _instanceId, e);
      throw new WebApplicationException("Failed to get workload info: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get the instance cost (budget) information for all workloads.
   * <p>
   * Returns CPU and memory budget information enforced on this instance for all workloads.
   *
   * <p>Example response:
   * <pre>
   * [
   *   {
   *     "workloadName": "testWorkload1",
   *     "cpuBudgetNs": 5000000,
   *     "memoryBudgetBytes": 104857600,
   *     "cpuRemainingNs": 3500000,
   *     "memoryRemainingBytes": 73400320
   *   },
   *   {
   *     "workloadName": "testWorkload2",
   *     "cpuBudgetNs": 10000000,
   *     "memoryBudgetBytes": 209715200,
   *     "cpuRemainingNs": 8000000,
   *     "memoryRemainingBytes": 157286400
   *   }
   * ]
   * </pre>
   *
   * @return list of workload budget information including CPU and memory limits and remaining capacity
   * @throws WebApplicationException on failure to retrieve the workload budget information
   */
  @GET
  @Path("queryWorkloadCosts")
  @ApiOperation(value = "Get instance cost information for all workloads")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  @Produces(MediaType.APPLICATION_JSON)
  public String getAllWorkloadBudgetStats() {
    try {
      WorkloadBudgetManager workloadBudgetManager = requireWorkloadBudgetManager();
      Map<String, WorkloadBudgetManager.BudgetStats> allBudgetStats = workloadBudgetManager.getAllBudgetStats();
      List<Map<String, Object>> response = new ArrayList<>();
      for (Map.Entry<String, WorkloadBudgetManager.BudgetStats> entry : allBudgetStats.entrySet()) {
        response.add(toWorkloadBudgetMap(entry.getKey(), entry.getValue()));
      }
      return ResourceUtils.convertToJsonString(response);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("Error getting all workload info on instance: {}", _instanceId, e);
      throw new WebApplicationException("Failed to get all workload info: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /** Returns a non-null WorkloadBudgetManager or throws a 500 WebApplicationException (and logs a warning). */
  private WorkloadBudgetManager requireWorkloadBudgetManager() {
    WorkloadBudgetManager workloadBudgetManager = WorkloadBudgetManager.get();
    if (workloadBudgetManager == null) {
      LOGGER.warn("WorkloadBudgetManager is not available on instance: {}", _instanceId);
      throw new WebApplicationException("WorkloadBudgetManager is not available",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return workloadBudgetManager;
  }

  /** Builds a stable JSON-serializable map for a single workload's budget stats. */
  private static Map<String, Object> toWorkloadBudgetMap(String workloadName,
                                                         WorkloadBudgetManager.BudgetStats stats) {
    Map<String, Object> map = new HashMap<>();
    map.put("workloadName", workloadName);
    map.put("cpuBudgetNs", stats._initialCpuBudget);
    map.put("memoryBudgetBytes", stats._initialMemoryBudget);
    map.put("cpuRemainingNs", stats._cpuRemaining);
    map.put("memoryRemainingBytes", stats._memoryRemaining);
    return map;
  }
}
