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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.server.starter.helix.SegmentReloadStatusValue;
import org.apache.pinot.spi.utils.JsonUtils;


@Api(tags = "Tasks")
@Path("/")
public class ControllerJobStatusResource {

  @Inject
  private ServerInstance _serverInstance;

  @GET
  @Path("/controllerJob/reloadStatus/{tableNameWithType}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Task status", notes = "Return the status of a given reload job")
  public String reloadJobStatus(@PathParam("tableNameWithType") String tableNameWithType,
      @QueryParam("reloadJobTimestamp") long reloadJobSubmissionTimestamp,
      @QueryParam("segmentName") String segmentName)
      throws Exception {
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);

    if (segmentName == null) {
      // All segments
      List<SegmentDataManager> allSegments = tableDataManager.acquireAllSegments();
      List<String> pendingSegmentNames = new ArrayList<>();
      try {
        long successCount = 0;
        for (SegmentDataManager segmentDataManager : allSegments) {
          if (segmentDataManager.getLoadTimeMs() >= reloadJobSubmissionTimestamp) {
            successCount++;
          } else {
            pendingSegmentNames.add(segmentDataManager.getSegmentName());
          }
        }
        SegmentReloadStatusValue segmentReloadStatusValue =
            new SegmentReloadStatusValue(allSegments.size(), successCount, pendingSegmentNames);
        return JsonUtils.objectToString(segmentReloadStatusValue);
      } finally {
        for (SegmentDataManager segmentDataManager : allSegments) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    } else {
      SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
      if (segmentDataManager == null) {
        return JsonUtils.objectToString(new SegmentReloadStatusValue(0, 0));
      }
      try {
        SegmentReloadStatusValue segmentReloadStatusValue;
        if (segmentDataManager.getLoadTimeMs() >= reloadJobSubmissionTimestamp) {
          segmentReloadStatusValue = new SegmentReloadStatusValue(1, 1);
        } else {
          segmentReloadStatusValue =
              new SegmentReloadStatusValue(1, 0, Collections.singletonList(segmentDataManager.getSegmentName()));
        }
        return JsonUtils.objectToString(segmentReloadStatusValue);
      } finally {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }
}
