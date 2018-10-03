/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.api.resources;

import com.linkedin.pinot.broker.routing.RoutingTable;
import com.linkedin.pinot.broker.routing.TimeBoundaryService;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = "Debug")
@Path("/debug")
public class PinotBrokerDebug {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotBrokerDebug.class);

  @Inject
  private RoutingTable _routingTable;

  @Inject
  private BrokerMetrics _brokerMetrics;

  @Inject
  private TimeBoundaryService _timeBoundaryService;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/routingTable/{tableName}")
  @ApiOperation(value = "Debugging routing table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Routing table information of a table"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String debugRoutingTable(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName
  ) {
    try {
      return _routingTable.dumpSnapshot(tableName);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing GET request", e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, 1);
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/timeBoundary/{tableName}")
  @ApiOperation(value = "Debugging time boundary service")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Time boundary information of a table"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String debugTimeBoundaryService(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName
  ) {
    try {
      String response = "{}";
      if (!tableName.isEmpty()) {
        CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
        if (tableType == null) {
          tableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        }
        TimeBoundaryService.TimeBoundaryInfo tbInfo = _timeBoundaryService.getTimeBoundaryInfoFor(tableName);
        if (tbInfo != null) {
          response = tbInfo.toJsonString();
        }
      }
      return response;
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing GET request", e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, 1);
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
