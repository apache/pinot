/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.server.api.restlet;

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.restlet.resources.SegmentSizeInfo;
import com.linkedin.pinot.common.restlet.resources.TableSizeInfo;
import com.linkedin.pinot.common.restlet.swagger.Description;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.server.starter.ServerInstance;
import java.io.IOException;
import javax.ws.rs.Produces;
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * API to provide table sizes
 */
public class TableSizeResource extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSizeResource.class);

  InstanceDataManager dataManager;

  public TableSizeResource() {
    ServerInstance serverInstance =
        (ServerInstance) getApplication().getContext().getAttributes().get(ServerInstance.class.toString());
    dataManager = (InstanceDataManager) serverInstance.getInstanceDataManager();
  }

  @Override
  public Representation get() {
    String tableName = (String) getRequest().getAttributes().get("tableName");
    final String detailedStr = (String) getRequest().getAttributes().get("detailed");
    return getTableSize(tableName, !"false".equalsIgnoreCase(detailedStr));
  }

  @HttpVerb("get")
  @Description("Lists size of all the segments of the table")
  @Summary("Show table storage size")
  @Paths({"/table/{tableName}/size"})
  @Produces("application/json")
  public StringRepresentation getTableSize(
      @Parameter(name="tableName", in = "path", description = "Table name (Ex: myTable_OFFLINE)")
      String tableName,
      @Parameter(name="detailed", in="query",
          description = "true=List detailed segment sizes; false = list only table size",
          required = false)
      boolean detailed) {

    if (dataManager == null) {
      setStatus(Status.SERVER_ERROR_INTERNAL, "Invalid server initialization");
      return new StringRepresentation("{\"message\" : \"Server is incorrectly initialized\"}");
    }

    TableDataManager tableDataManager = dataManager.getTableDataManager(tableName);
    if (tableDataManager == null) {
      setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation("{\"message\" : \"Table " + tableName +
          " is not found\"}");
    }
    TableSizeInfo tableSizeInfo = new TableSizeInfo();
    tableSizeInfo.tableName = tableDataManager.getTableName();
    tableSizeInfo.diskSizeInBytes = 0L;

    ImmutableList<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    for (SegmentDataManager segmentDataManager : segmentDataManagers) {
      IndexSegment segment = segmentDataManager.getSegment();
      long segmentSizeBytes = segment.getDiskSizeBytes();
      if (detailed) {
        SegmentSizeInfo segmentSizeInfo = new SegmentSizeInfo(segment.getSegmentName(), segmentSizeBytes);
        tableSizeInfo.segments.add(segmentSizeInfo);
      }
      tableSizeInfo.diskSizeInBytes += segmentSizeBytes;
    }
    // we could release segmentDataManagers as we iterate in the loop above
    // but this is cleaner with clear semantics of usage. Also, above loop
    // executes fast so duration of holding segments is not a concern
    for (SegmentDataManager segmentDataManager : segmentDataManagers) {
      tableDataManager.releaseSegment(segmentDataManager);
    }
    //invalid to use the segmentDataManagers below

    try {
      return new StringRepresentation(new ObjectMapper().writeValueAsString(tableSizeInfo));
    } catch (IOException e) {
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation("{\"message\": \"Failed to convert data to json\"}");
    }
  }
}
