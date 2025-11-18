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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.response.server.SegmentReloadFailure;
import org.apache.pinot.common.response.server.ServerReloadStatusResponse;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.utils.ReloadJobStatus;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.segment.spi.creator.name.SegmentNameUtils;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.utils.JsonUtils;


@Api(tags = "Tasks")
@Path("/")
@Singleton
public class ControllerJobStatusResource {

  private final ServerInstance _serverInstance;
  private final ServerReloadJobStatusCache _serverReloadJobStatusCache;

  @Inject
  public ControllerJobStatusResource(ServerInstance serverInstance,
      ServerReloadJobStatusCache serverReloadJobStatusCache) {
    _serverInstance = serverInstance;
    _serverReloadJobStatusCache = serverReloadJobStatusCache;
  }

  @GET
  @Path("/controllerJob/reloadStatus/{tableNameWithType}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Task status", notes = "Return the status of a given reload job")
  public String reloadJobStatus(@PathParam("tableNameWithType") String tableNameWithType,
      @QueryParam("reloadJobTimestamp") long reloadJobSubmissionTimestamp,
      @QueryParam("segmentName") String segmentName,
      @QueryParam("reloadJobId") String reloadJobId,
      @Context HttpHeaders headers)
      throws Exception {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    List<SegmentDataManager> segmentDataManagers;
    long totalSegmentCount;
    if (segmentName == null) {
      segmentDataManagers = tableDataManager.acquireAllSegments();
      totalSegmentCount = segmentDataManagers.size();
    } else {
      List<String> targetSegments = new ArrayList<>();
      Collections.addAll(targetSegments, StringUtils.split(segmentName, SegmentNameUtils.SEGMENT_NAME_SEPARATOR));
      segmentDataManagers = tableDataManager.acquireSegments(targetSegments, new ArrayList<>());
      totalSegmentCount = targetSegments.size();
    }
    try {
      long successCount = 0;
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        if (segmentDataManager.getLoadTimeMs() >= reloadJobSubmissionTimestamp) {
          successCount++;
        }
      }

      // Build response with fluent setters
      ServerReloadStatusResponse response = new ServerReloadStatusResponse()
          .setTotalSegmentCount(totalSegmentCount)
          .setSuccessCount(successCount);

      // Query cache for failure count and details if reloadJobId is provided
      if (reloadJobId != null) {
        ReloadJobStatus jobStatus = _serverReloadJobStatusCache.getJobStatus(reloadJobId);
        if (jobStatus != null) {
          response.setFailureCount((long) jobStatus.getFailureCount());

          // Get defensive copy of failed segment details
          synchronized (jobStatus) {
            List<SegmentReloadFailure> details = jobStatus.getFailedSegmentDetails();
            if (!details.isEmpty()) {
              response.setSampleSegmentReloadFailures(new ArrayList<>(details));
            }
          }
        }
      }

      return JsonUtils.objectToString(response);
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }
}
