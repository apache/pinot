/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.server.api.resources;

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.restlet.resources.ServerLatencyInfo;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.server.starter.ServerInstance;
import io.swagger.annotations.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * API to provide server performance metrics, for example all hosted segments count and storage size
 */
@Api(tags = "ServerMetricsInfo")
@Path("/")

public class ServerMetricsInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerMetricsInfo.class);

    @Inject
    ServerInstance serverInstance;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ServerMetricsInfo/{tableName}/LatencyInfo")
    @ApiOperation(value = "Show all hosted segments count and storage size", notes = "Storage size and count of all segments hosted by a Pinot Server")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")})
    public ServerLatencyInfo getLatencyInfo(@ApiParam(value = "Table Name with type", required = true) @PathParam("tableName") String tableName) throws WebApplicationException {
        InstanceDataManager dataManager = (InstanceDataManager) serverInstance.getInstanceDataManager();
        if (dataManager == null) {
            throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
        }


        ServerLatencyInfo serverLatencyInfo = new ServerLatencyInfo();
        List<Double> server1latencies = Arrays.asList(1.0, 3.0, 5.0);
        serverLatencyInfo.set_segmentLatencyInSecs(server1latencies);
        serverLatencyInfo.set_serverName("server_1");
        serverLatencyInfo.set_serverName("table_1");

        // Add logic for retrieving data.

        return serverLatencyInfo;
    }
}