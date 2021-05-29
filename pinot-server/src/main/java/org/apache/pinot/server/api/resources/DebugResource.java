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
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
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
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Debug resource for Pinot Server.
 */
@Api(tags = "Debug")
@Path("/debug/")
public class DebugResource {

  @Inject
  private ServerInstance _serverInstance;

  @GET
  @Path("tables/{tableName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get segments debug info for this table", notes = "This is a debug endpoint, and won't maintain backward compatibility")
  public List<SegmentServerDebugInfo> getSegmentsDebugInfo(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableNameWithType) {

    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    return getSegmentServerDebugInfo(tableNameWithType, tableType);
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
      String segmentName = segmentDataManager.getSegmentName();
      segmentConsumerInfo =
          new SegmentConsumerInfo(segmentName, realtimeSegmentDataManager.getConsumerState().toString(),
              realtimeSegmentDataManager.getLastConsumedTimestamp(),
              realtimeSegmentDataManager.getPartitionToCurrentOffset());
    }
    return segmentConsumerInfo;
  }
}
