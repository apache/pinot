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
package com.linkedin.pinot.broker.api.resources;

import com.linkedin.pinot.broker.requesthandler.BrokerRequestHandler;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.response.BrokerResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(tags = "Query")
@Path("/")
public class PinotClientRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClientRequest.class);

  @Inject
  private BrokerRequestHandler requestHandler;

  @Inject
  private BrokerMetrics brokerMetrics;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query")
  @ApiOperation(value = "Querying pinot")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query response"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  public String processQueryGet(
      @ApiParam(value = "Name of the table", required = false) @QueryParam("bql") String query,
      @ApiParam(value = "Trace enabled", required = false) @QueryParam("trace") @DefaultValue("false") String traceEnabled
  ) {
    try {
      JSONObject requestJson = new JSONObject().put("pql", query);
      if (traceEnabled != null) {
        requestJson.put("trace", traceEnabled);
      }
      BrokerResponse brokerResponse = requestHandler.handleRequest(requestJson, null);
      return brokerResponse.toJsonString();
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing GET request", e);
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, 1);
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("query")
  @ApiOperation(value = "Querying pinot")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Query response"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  public String processQueryPost(String query) {
    try {
      JSONObject requestJson = new JSONObject(query);
      BrokerResponse brokerResponse = requestHandler.handleRequest(requestJson, null);
      return brokerResponse.toJsonString();
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing GET request", e);
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNCAUGHT_GET_EXCEPTIONS, 1);
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
