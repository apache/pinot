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
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.restlet.resources.SegmentConsumerInfo;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.common.restlet.resources.SegmentServerDebugInfo;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * Debug resource for Pinot Server.
 */
@Api(tags = "Debug", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/debug/")
public class DebugResource {

  @Inject
  private ServerInstance _serverInstance;

  @GET
  @Path("tables/{tableName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get segments debug info for this table",
      notes = "This is a debug endpoint, and won't maintain backward compatibility")
  public List<SegmentServerDebugInfo> getSegmentsDebugInfo(
      @ApiParam(value = "Name of the table (with type)", required = true) @PathParam("tableName")
          String tableNameWithType) {
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
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") String segmentName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    Map<String, SegmentErrorInfo> segmentErrorsMap = tableDataManager.getSegmentErrors();
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    try {
      SegmentConsumerInfo segmentConsumerInfo = getSegmentConsumerInfo(segmentDataManager, tableType);
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
        SegmentConsumerInfo segmentConsumerInfo = getSegmentConsumerInfo(segmentDataManager, tableType);

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

  private SegmentConsumerInfo getSegmentConsumerInfo(SegmentDataManager segmentDataManager, TableType tableType) {
    SegmentConsumerInfo segmentConsumerInfo = null;
    if (tableType == TableType.REALTIME) {
      RealtimeSegmentDataManager realtimeSegmentDataManager = (RealtimeSegmentDataManager) segmentDataManager;
      Map<String, ConsumerPartitionState> partitionStateMap = realtimeSegmentDataManager.getConsumerPartitionState();
      Map<String, String> currentOffsets = realtimeSegmentDataManager.getPartitionToCurrentOffset();
      Map<String, String> upstreamLatest = partitionStateMap.entrySet().stream().collect(
          Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getUpstreamLatestOffset().toString()));
      Map<String, String> recordsLagMap = new HashMap<>();
      Map<String, String> availabilityLagMsMap = new HashMap<>();
      realtimeSegmentDataManager.getPartitionToLagState(partitionStateMap).forEach((k, v) -> {
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
}
