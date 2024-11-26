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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.DatabaseUtils;
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
      @QueryParam("segmentNames") String segmentNames, @Context HttpHeaders headers)
      throws Exception {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    List<SegmentDataManager> segmentDataManagers;
    long totalSegmentCount;
    if (segmentNames == null) {
      segmentDataManagers = tableDataManager.acquireAllSegments();
      totalSegmentCount = segmentDataManagers.size();
    } else {
      List<String> targetSegments = new ArrayList<>();
      Collections.addAll(targetSegments, StringUtils.split(segmentNames, ','));
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
      return JsonUtils.objectToString(new SegmentReloadStatusValue(totalSegmentCount, successCount));
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }
}
